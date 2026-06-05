import json
import os
import re
import time
import uuid

import redis
import requests
from dotenv import load_dotenv

load_dotenv()


class MyFabricAgentClient:
    def __init__(self):
        self.workspace_id = os.getenv("WORKSPACE_ID")
        self.agent_id = os.getenv("AGENT_ID")
        self.assistant_id = os.getenv("ASSISTANT_ID")
        self.api_version = os.getenv("API_VERSION")
        self.access_token = os.getenv("ACCESS_TOKEN")
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.thread_ttl_seconds = int(os.getenv("THREAD_TTL_SECONDS", "604800"))
        self.default_session_id = os.getenv("FABRIC_SESSION_ID", "default")
        self.request_timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
        self.run_timeout_seconds = int(os.getenv("RUN_TIMEOUT_SECONDS", "180"))
        self.total_response_time_seconds = None

        base_url = (
            "https://api.fabric.microsoft.com/v1"
            f"/workspaces/{self.workspace_id}"
            f"/dataagents/{self.agent_id}"
        )
        self.agent_url = f"{base_url}/aiassistant/openai"
        self.private_ai_assistant_url = f"{base_url}/__private/aiassistant"

        if not self.access_token:
            raise ValueError("Defina ACCESS_TOKEN no arquivo .env.")

        self.http = requests.Session()
        self.redis_client = self.__build_redis_client()

    def send_message(self, message, session_id=None, stream=True):
        if not message or not message.strip():
            raise ValueError("A mensagem nao pode estar vazia.")

        start_time = time.monotonic()
        session_id = session_id or self.default_session_id
        thread_id = self.__get_or_create_thread_for_session(session_id)

        try:
            self.__create_message(thread_id, message)
        except requests.HTTPError as error:
            if self.__should_retry_with_new_thread(error):
                print("Thread cached is no longer valid. Creating a new one.")
                self.__forget_thread(session_id)
                thread_id = self.__get_or_create_thread_for_session(session_id)
                self.__create_message(thread_id, message)
            else:
                raise

        if stream:
            try:
                final_response = self.__create_streaming_run(thread_id)
            except (requests.RequestException, ValueError) as error:
                print(f"Streaming failed, falling back to polling: {error}")
                final_response = self.__run_without_streaming(thread_id)
        else:
            final_response = self.__run_without_streaming(thread_id)

        self.__refresh_thread_ttl(session_id)
        self.__save_total_response_time(start_time)
        print(f"Final response: {final_response}")
        print(f"Total response time: {self.total_response_time_seconds:.2f} seconds")
        return final_response

    def get_total_response_time_seconds(self):
        return self.total_response_time_seconds

    def __run_without_streaming(self, thread_id):
        run_id = self.__create_run(thread_id, stream=False)

        if not self.__wait_until_run_completed(thread_id, run_id):
            raise TimeoutError(f"O run {run_id} nao terminou dentro do tempo limite.")

        return self.__get_final_response(thread_id, run_id)

    def __get_or_create_thread_for_session(self, session_id):
        cached_thread_id = self.__get_cached_thread_id(session_id)
        if cached_thread_id:
            print(f"Reusing thread {cached_thread_id} for session {session_id}")
            return cached_thread_id

        thread_name = self.__thread_name_for_session(session_id)
        thread_id = self.__create_thread(thread_name)
        self.__cache_thread(session_id, thread_id)
        return thread_id

    def __create_thread(self, thread_name):
        print(f"Creating or retrieving thread: {thread_name}")
        response = self.http.get(
            f"{self.private_ai_assistant_url}/threads/fabric",
            params={"tag": f'"{thread_name}"'},
            headers=self.__headers(),
            timeout=self.request_timeout_seconds,
        )
        data = self.__json_response(response)
        thread_id = data.get("id")
        if not thread_id:
            raise ValueError(f"A resposta de thread nao retornou id: {data}")
        return thread_id

    def __create_message(self, thread_id, message):
        print(f"Creating message in thread {thread_id}: {message}")

        payload = {
            "role": "user",
            "content": message,
        }

        response = self.http.post(
            f"{self.agent_url}/threads/{thread_id}/messages",
            params={"api-version": self.api_version},
            json=payload,
            headers=self.__headers(),
            timeout=self.request_timeout_seconds,
        )
        return self.__json_response(response)

    def __create_run(self, thread_id, stream):
        print(f"Creating run for thread {thread_id} with stream={stream}")

        payload = {
            "assistant_id": self.assistant_id,
            "stream": stream,
        }

        response = self.http.post(
            f"{self.agent_url}/threads/{thread_id}/runs",
            params={"api-version": self.api_version},
            json=payload,
            headers=self.__headers(),
            timeout=self.request_timeout_seconds,
        )
        data = self.__json_response(response)
        run_id = data.get("id")
        if not run_id:
            raise ValueError(f"A resposta de run nao retornou id: {data}")
        return run_id

    def __create_streaming_run(self, thread_id):
        print(f"Creating streaming run for thread {thread_id}")

        payload = {
            "assistant_id": self.assistant_id,
            "stream": True,
        }
        headers = self.__headers(accept="text/event-stream")

        response = self.http.post(
            f"{self.agent_url}/threads/{thread_id}/runs",
            params={"api-version": self.api_version},
            json=payload,
            headers=headers,
            timeout=(self.request_timeout_seconds, self.run_timeout_seconds),
            stream=True,
        )

        if not response.ok:
            self.__raise_http_error(response)

        run_id = None
        final_text = None
        delta_parts = []

        for event in self.__iter_sse_events(response):
            event_name = event.get("event")
            data = event.get("data")

            if data == "[DONE]":
                break

            payload = self.__parse_sse_payload(data)
            if not payload:
                continue

            run_id = run_id or payload.get("run_id") or self.__extract_run_id(payload)
            status = payload.get("status")

            if status in {"failed", "cancelled", "expired"}:
                raise RuntimeError(f"Run terminou com status {status}: {payload}")

            text_delta = self.__extract_text_delta(payload)
            if text_delta:
                delta_parts.append(text_delta)
                print(text_delta, end="", flush=True)

            completed_text = self.__extract_completed_message_text(payload)
            if completed_text:
                final_text = completed_text

            if event_name == "thread.message.completed" and completed_text:
                final_text = completed_text

        if delta_parts:
            print()

        if final_text:
            return final_text

        if delta_parts:
            return "".join(delta_parts)

        if run_id:
            return self.__get_final_response(thread_id, run_id)

        raise ValueError("Streaming terminou sem run_id ou resposta final.")

    def __get_run_response(self, thread_id, run_id):
        print(f"Getting run response for thread {thread_id} and run {run_id}")

        response = self.http.get(
            f"{self.agent_url}/threads/{thread_id}/runs/{run_id}",
            params={"api-version": self.api_version},
            headers=self.__headers(),
            timeout=self.request_timeout_seconds,
        )
        return self.__json_response(response)

    def __wait_until_run_completed(self, thread_id, run_id):
        print(f"Waiting for run {run_id} in thread {thread_id}")
        deadline = time.monotonic() + self.run_timeout_seconds
        sleep_seconds = 1

        while time.monotonic() < deadline:
            run = self.__get_run_response(thread_id, run_id)
            status = run.get("status")
            print(f"Run status: {status}")

            if status == "completed":
                return True
            if status in {"failed", "cancelled", "expired"}:
                raise RuntimeError(f"Run terminou com status {status}: {run}")

            time.sleep(sleep_seconds)
            sleep_seconds = min(sleep_seconds + 1, 5)

        return False

    def __get_final_response(self, thread_id, run_id):
        print(f"Getting final response for thread {thread_id}")
        response = self.http.get(
            f"{self.agent_url}/threads/{thread_id}/messages",
            params={"api-version": self.api_version, "limit": 10},
            headers=self.__headers(),
            timeout=self.request_timeout_seconds,
        )
        data = self.__json_response(response)

        for message in data.get("data", []):
            if message.get("role") == "assistant" and message.get("run_id") == run_id:
                text = self.__extract_completed_message_text(message)
                if text:
                    return text

        raise ValueError(f"Nao encontrei resposta final do assistant para o run {run_id}: {data}")

    def __build_redis_client(self):
        try:
            client = redis.Redis.from_url(self.redis_url, decode_responses=True)
            client.ping()
            return client
        except redis.RedisError as error:
            print(f"Redis unavailable. Threads will not be persisted: {error}")
            return None

    def __get_cached_thread_id(self, session_id):
        if not self.redis_client:
            return None

        try:
            return self.redis_client.get(self.__redis_thread_key(session_id))
        except redis.RedisError as error:
            print(f"Could not read thread from Redis: {error}")
            return None

    def __cache_thread(self, session_id, thread_id):
        if not self.redis_client:
            return

        try:
            self.redis_client.setex(
                self.__redis_thread_key(session_id),
                self.thread_ttl_seconds,
                thread_id,
            )
        except redis.RedisError as error:
            print(f"Could not save thread in Redis: {error}")

    def __refresh_thread_ttl(self, session_id):
        if not self.redis_client:
            return

        try:
            self.redis_client.expire(
                self.__redis_thread_key(session_id),
                self.thread_ttl_seconds,
            )
        except redis.RedisError as error:
            print(f"Could not refresh thread TTL in Redis: {error}")

    def __forget_thread(self, session_id):
        if not self.redis_client:
            return

        try:
            self.redis_client.delete(self.__redis_thread_key(session_id))
        except redis.RedisError as error:
            print(f"Could not remove thread from Redis: {error}")

    def __redis_thread_key(self, session_id):
        return f"fabric_agent:{self.workspace_id}:{self.agent_id}:session:{session_id}:thread"

    def __thread_name_for_session(self, session_id):
        safe_session_id = re.sub(r"[^A-Za-z0-9_.:-]+", "-", str(session_id)).strip("-")
        safe_session_id = safe_session_id or str(uuid.uuid4())
        return f"fabric-agent-session-{safe_session_id[:80]}"

    def __headers(self, accept="application/json"):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": accept,
            "Content-Type": "application/json",
            "ActivityId": str(uuid.uuid4()),
        }

    def __json_response(self, response):
        try:
            data = response.json()
        except ValueError:
            data = {"raw": response.text}

        if not response.ok:
            raise requests.HTTPError(
                f"{response.request.method} {response.url} retornou "
                f"{response.status_code}: {data}",
                response=response,
            )

        return data

    def __raise_http_error(self, response):
        try:
            data = response.json()
        except ValueError:
            data = {"raw": response.text}

        raise requests.HTTPError(
            f"{response.request.method} {response.url} retornou "
            f"{response.status_code}: {data}",
            response=response,
        )

    def __should_retry_with_new_thread(self, error):
        response = getattr(error, "response", None)
        return response is not None and response.status_code in {400, 404, 410}

    def __iter_sse_events(self, response):
        event = {}
        data_lines = []

        for raw_line in response.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue

            line = raw_line.strip()
            if not line:
                if data_lines:
                    event["data"] = "\n".join(data_lines)
                    yield event
                    event = {}
                    data_lines = []
                continue

            if line.startswith("event:"):
                event["event"] = line.removeprefix("event:").strip()
            elif line.startswith("data:"):
                data_lines.append(line.removeprefix("data:").strip())

        if data_lines:
            event["data"] = "\n".join(data_lines)
            yield event

    def __parse_sse_payload(self, data):
        if not data or data == "[DONE]":
            return None

        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None

    def __extract_run_id(self, payload):
        if payload.get("object") == "thread.run":
            return payload.get("id")

        return payload.get("run_id")

    def __extract_text_delta(self, payload):
        delta = payload.get("delta", {})
        for content in delta.get("content", []):
            text = content.get("text", {})
            value = text.get("value")
            if value:
                return value

        return None

    def __extract_completed_message_text(self, payload):
        content = payload.get("content", [])
        text_parts = []

        for item in content:
            text = item.get("text", {})
            value = text.get("value")
            if value:
                text_parts.append(value)

        if text_parts:
            return "\n".join(text_parts)

        return None

    def __save_total_response_time(self, start_time):
        self.total_response_time_seconds = time.monotonic() - start_time


if __name__ == "__main__":
    client = MyFabricAgentClient()
    client.send_message(
        "compare the market share of lenovo and hp in the last 5 weeks",
        session_id="teste-1",
    )
