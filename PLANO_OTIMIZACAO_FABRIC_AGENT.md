# Plano: Otimizacao do Fabric Data Agent Client

## Summary

- O cliente atual criava uma thread nova a cada chamada, entao nao reaproveitava contexto.
- A documentacao do Fabric Data Agent confirma que o app cliente deve gerenciar `thread_name` para manter contexto entre perguntas relacionadas.
- Reaproveitar thread pode melhorar qualidade/contexto, mas nao garante resposta mais rapida para a mesma pergunta; para isso, cache de resposta seria uma otimizacao separada.
- A primeira implementacao prioriza tempo percebido com streaming e persistencia de thread por sessao em Redis com TTL de 7 dias.

## Key Changes

- Adicionar Redis local via Docker para desenvolvimento:
  - incluir dependencia `redis` em `requirements.txt`;
  - definir `REDIS_URL`, `THREAD_TTL_SECONDS=604800` e `FABRIC_SESSION_ID` no exemplo de ambiente;
  - incluir `docker-compose.yml` para subir Redis local.

- Alterar `MyFabricAgentClient` para usar thread por sessao:
  - `send_message(message, session_id=None, stream=True)`;
  - gerar chave Redis no formato `fabric_agent:{workspace_id}:{agent_id}:session:{session_id}:thread`;
  - se existir thread valida no Redis, reutilizar `thread_id`;
  - se nao existir, criar thread com `thread_name` estavel, salvar `thread_id` e renovar TTL para 7 dias;
  - renovar o TTL a cada uso bem-sucedido.

- Implementar streaming como caminho padrao:
  - criar run com `"stream": true`;
  - consumir eventos SSE da resposta HTTP;
  - capturar texto final no evento de mensagem concluida ou acumular deltas se disponiveis;
  - manter fallback para fluxo atual sem streaming quando `stream=False` ou se o streaming falhar de forma recuperavel.

- Reduzir overhead do cliente:
  - usar `requests.Session` para reaproveitar conexoes HTTP;
  - centralizar timeouts configuraveis;
  - usar polling adaptativo apenas no fallback sem streaming;
  - filtrar a resposta final por `run_id`.

## Test Plan

- Testes manuais:
  - subir Redis local;
  - executar uma pergunta com `session_id="teste-1"` e confirmar criacao/salvamento da thread;
  - executar uma segunda pergunta com o mesmo `session_id` e confirmar reuso da mesma thread;
  - executar pergunta com outro `session_id` e confirmar isolamento;
  - validar que o TTL e renovado no Redis;
  - validar fallback sem streaming com `stream=False`.

- Testes de comportamento:
  - Redis indisponivel: cliente deve criar thread nova sem persistir;
  - run com status `failed`, `cancelled` ou `expired`: manter excecao clara;
  - resposta sem mensagem final do assistant: manter erro diagnostico com `run_id`;
  - pergunta vazia: rejeitar antes de chamar API.

## Assumptions

- Estrategia escolhida: thread por sessao.
- Prioridade escolhida: melhorar tempo percebido com streaming.
- Redis escolhido: Docker local na primeira implementacao.
- TTL escolhido: 7 dias (`604800` segundos), renovado a cada uso.
- Nao implementar cache de resposta nesta etapa; se a mesma pergunta precisa voltar instantaneamente, isso deve ser uma fase separada.
- Fontes consultadas: Microsoft Learn, `consume-data-agent-python`, e repositorio `microsoft/fabric_data_agent_client`.
