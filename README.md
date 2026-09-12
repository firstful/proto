# PROTO — Agent Communication Protocol

Lightweight, strongly-typed (Go), broker-based protocol for agent ↔ agent and
user ↔ agent communication. Explicit acks vs text content. DMs, groups,
threads, task-passing, and nested fan-out orchestration loops.

**Read [SPEC.md](SPEC.md) first** — it is the normative protocol definition.

## Layout

```
proto/            Go protocol library: envelope, ledger, broker, client, server
  envelope.go     the one wire object + validation rules
  ledger.go       task/fanout orchestration state
  broker.go       routing, rooms, durable parking, dedup, waiters
  client.go       endpoint with Delegate/Fanout/WaitTask/WaitFanout primitives
  server.go       WebSocket agent endpoint + REST API + webapp host
  journal.go      JSONL append-log, crash recovery
cmd/proto/        broker binary (embeds the webapp)
webapp/           dev copy of the single-file web UI
hermes-plugin/    Hermes backend plugin (thin Python client over the REST API)
```

## Run

```bash
go build -o proto ./cmd/proto
./proto --addr :8808 --journal /mnt/hot/proto/journal.jsonl
```

- Web UI: http://localhost:8808/  (feed of every envelope, live)
- Agents: `ws://localhost:8808/ws` (first message: `{"handle":"AgentB"}`)
- Thin clients (Hermes plugin, curl): `POST /api/envelope`, `POST /api/delegate`,
  `POST /api/fanout`, `GET /api/inbox/<handle>`, `GET /api/task/<id>`

## Test

```bash
go test ./proto/ -v     # envelope rules, routing, rooms, dedup, parking,
                        # SPEC §10 walks: delegate-and-report + fanout-gather
```

## Hermes install

```bash
cp -r hermes-plugin/proto ~/.hermes/plugins/
export PROTO_URL=http://127.0.0.1:8808
export PROTO_HANDLE=hermes-agent     # per-profile identity
```

Tools: `proto_send`, `proto_task`, `proto_delegate`, `proto_fanout`,
`proto_results`, `proto_task_info`, `proto_announce`, `proto_status`,
`proto_cmd`.

## Response classes (SPEC §2.3)

| Class | Kind | Upstream sees |
|-------|------|---------------|
| A | `msg` | conversation content |
| B | `tool_call` / `tool_result` | a status line, never prose |
| C | `ack` (`received`/`accepted`/`done`/`noop`/…) | signal badge only |
| D | `cmd` (`!{name}` → `peer:broker`) | **nothing** — broker executes, sender's reply_target gets a `status` envelope |

Built-ins: `!{ping}` `!{list_agents}` `!{list_rooms}` `!{list_tasks}`
`!{get_task} <id>` `!{task_tree} <root>`; extend with `Broker.RegisterCmd`.

## Orchestration in one breath

User → AgentA (`reply_target=user`): AgentA `proto_fanout`s to N peers
(`reply_target=AgentA`, `on_behalf_of=[user:reed]`); peers reply
`result`/`fail` with the fanout correlation id; barrier `all|first|majority`
closes the gather; AgentA composes one `result` back to the user. Nest
arbitrarily — every hop is the same task→reply_target contract.
