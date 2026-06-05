import os
import time
import uuid

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

    def send_message(self, message):
        thread_id = self.__create_thread()
        start_time = time.monotonic()
        self.__create_message(thread_id, message)
        run_id = self.__create_run(thread_id)

        if not self.__wait_until_run_completed(thread_id, run_id):
            raise TimeoutError(f"O run {run_id} nao terminou dentro do tempo limite.")

        final_response = self.__get_final_response(thread_id, run_id)
        self.__save_total_response_time(start_time)
        print(f"Final response: {final_response}")
        print(f"Total response time: {self.total_response_time_seconds:.2f} seconds")
        return final_response

    def get_total_response_time_seconds(self):
        return self.total_response_time_seconds

    def __create_thread(self):
        thread_name = f"Thread-{uuid.uuid4()}"
        print(f"Creating thread: {thread_name}")
        response = requests.get(
            f"{self.private_ai_assistant_url}/threads/fabric",
            params={"tag": f'"{thread_name}"'},
            headers=self.__headers(),
            timeout=30,
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

        response = requests.post(
            f"{self.agent_url}/threads/{thread_id}/messages",
            params={"api-version": self.api_version},
            json=payload,
            headers=self.__headers(),
            timeout=30,
        )
        return self.__json_response(response)

    def __create_run(self, thread_id):
        print(f"Creating run for thread {thread_id}")

        payload = {
            "assistant_id": self.assistant_id,
            "stream": False,
        }

        response = requests.post(
            f"{self.agent_url}/threads/{thread_id}/runs",
            params={"api-version": self.api_version},
            json=payload,
            headers=self.__headers(),
            timeout=30,
        )
        data = self.__json_response(response)
        run_id = data.get("id")
        if not run_id:
            raise ValueError(f"A resposta de run nao retornou id: {data}")
        return run_id

    def __get_run_response(self, thread_id, run_id):
        print(f"Getting run response for thread {thread_id} and run {run_id}")

        response = requests.get(
            f"{self.agent_url}/threads/{thread_id}/runs/{run_id}",
            params={"api-version": self.api_version},
            headers=self.__headers(),
            timeout=30,
        )
        return self.__json_response(response)

    def __wait_until_run_completed(self, thread_id, run_id, timeout_seconds=180):
        print(f"Waiting for run {run_id} in thread {thread_id}")
        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            run = self.__get_run_response(thread_id, run_id)
            status = run.get("status")
            print(f"Run status: {status}")

            if status == "completed":
                return True
            if status in {"failed", "cancelled", "expired"}:
                raise RuntimeError(f"Run terminou com status {status}: {run}")

            time.sleep(2)

        return False

    def __get_final_response(self, thread_id, run_id):
        print(f"Getting final response for thread {thread_id}")
        response = requests.get(
            f"{self.agent_url}/threads/{thread_id}/messages",
            params={"api-version": self.api_version, "limit": 10},
            headers=self.__headers(),
            timeout=30,
        )
        data = self.__json_response(response)

        for message in data.get("data", []):
            if message.get("role") == "assistant" and message.get("run_id") == run_id:
                content = message.get("content", [])
                if content:
                    return content[0].get("text", {}).get("value")

        raise ValueError(f"Nao encontrei resposta final do assistant para o run {run_id}: {data}")

    def __headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
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

    def __save_total_response_time(self, start_time):
        self.total_response_time_seconds = time.monotonic() - start_time


if __name__ == "__main__":
    client = MyFabricAgentClient()
    client.send_message("compare the market share of dell and lenovo in the last quarter")
