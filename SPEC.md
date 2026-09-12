# PROTO — Agent Communication Protocol (v0.1 draft)

**Status:** proto/spec draft · **Date:** 2026-09-12 · **Kind:** async, broker-based, structured

PROTO is a lightweight message-broker protocol for agent ↔ agent (and user ↔
agent) communication. It cleanly separates **signal** (ack/Ack/nonce) from
**content** (text/task/result), gives every interaction an identity + thread,
and supports DMs, group rooms, task passing, and nested fan-out orchestration
loops with a single routing rule: **every envelope has `reply_target`**.

It is transport-agnostic (any broker that preserves routing + order:
AMQP, Redis Streams, MQTT, NATS, WebSocket-passthrough). The reference
implementation ships a loopback broker + WebSocket chat broker and an Hermes
plugin.

---

## 1. Design goals

| # | Goal | Mechanism |
|---|------|-----------|
| 1 | Async, broker-based | All sends go through a broker; agents never call each other directly |
| 2 | Explicit ack ≠ text | `Ack` envelope is its own message type — never implied by prose |
| 3 | Lightweight | One JSON envelope; one broker rule (route to `reply_target`); no GraphQL/schema servers |
| 4 | Groups & DMs | `to` is either a peer handle or a room handle |
| 5 | Threads | `thread` id + `reply_to` message id |
| 6 | Task passing | `Task` envelope with `task_id`, first-class `Result` + `Fail` |
| 7 | Orchestration | `fanout`/`plan`/`loop`/`return` — parent_task links + reply_target |
| 8 | Identity | `REED` handles may carry `on_behalf_of` delegation chains |

---

## 2. Envelope (the ONE object)

```jsonc
{
  "v": 1,
  "kind": "msg",              // msg | task | result | fail | ack | invite | leave
  "id":   "3f9c…",            // crypto.randomUUID()
  "ts":   "2026-09-12T18:00:01.120Z",
  "from": { "handle": "AgentA", "on_behalf_of": ["user:reed"] },

  "to":     { "peer": "AgentB" },          // DM
  // or
  "to":     { "room": "crew-planning" },   // group
  // or
  "to":     { "reply_target": true },      // hand-back to reply target (loops)

  "thread": "7ad2…",          // optional: conversation thread
  "reply_to": "b1c0…",        // optional: specific envelope being answered

  "reply_target": {
    "peer": "user:reed"       // REQUIRED (except kind=ack). Where responses go.
    // or { "room": "…" }
  },
  "about": { "task_id": "…" },// optional, for result/fail/acks-of-work

  "text":  "humans & agents read this",      // kind = msg
  "nonce": "…",                               // kind = ack  (non-ionizing)
  "task": {                                   // kind = task
    "action": "summarize",
    "params": { "url": "…" },
    "deadline_ts": "…"
  },
  "result": { … },                            // kind = result
  "fail":   { "code": "…", "msg": "…" },      // kind = fail
  "fanout": { ... }                           // orchestration (see §6)
}
```

### §2.1 Rules

1. **`reply_target` is mandatory** on every envelope except `ack` — this is what
   makes every next step routable without global state.
2. **`to`** tells the broker where to *deliver*; **`reply_target`** tells the
   next hop where the *conversation continues*.
3. `on_behalf_of` is an append-only identity chain (e.g. `["user:reed",
   "AgentA"]`) — delegation provenance, readable by every hop.
4. Every envelope gets a broker-monotonic `seq` (per route) — used for ordering,
   gap detection, and idempotent redelivery.
5. Envelopes are JSON, ≤ 128 KiB. Large payloads go to an object store; reference
   by URI in `result`/`params`.

### §2.2 Acknowledgment (signal ≠ content)

Acks are a distinct `kind` — they carry **no prose**, only a nonce:

| `ack.type`   | Meaning (per RFC-2119) |
|--------------|------------------------|
| `received`   | broker delivered; receiver MAY NOT be awake yet |
| `read`       | receiver parsed the envelope |
| `accepted`   | receiver committed to the work (a task) |
| `rejected`   | receiver refuses; MUST include `nonce.reason` |
| `done`       | work finished (accompanies result, or standalone) |
| `failed`     | work aborted |

```jsonc
{ "v": 1, "kind": "ack", "to": {"reply_target": true}, "about": {"task_id":"…"},
  "ack": {"type":"accepted","nonce":"…"} }
```

An ack MAY also carry `text` — but the *signal itself* is the `ack.type` field;
widgets/UIs read that, never NLP.

### §2.3 Response classes (A/B/C/D)

Agents (and users) respond with one of four classes; each has distinct
visibility rules:

| Class | Kind(s) | Visibility |
|-------|---------|------------|
| **A — Messages** | `msg` | Full conversation content; routed to peers normally |
| **B — Tool calls/results** | `tool_call`, `tool_result` | Routed to peers but rendered as a **status line** upstream ("AgentA is running `search`…"), never as prose content |
| **C — Ack/noop** | `ack` (`noop`, `received`, `done`, …) | Pure signal, no text allowed; ledger + UI badges only |
| **D — Commands** | `cmd` | **Broker-intercepted.** `!{name} args` addressed to `peer:broker` executes INSIDE the broker; peers NEVER see it — the sender's reply_target receives a `status` envelope with the outcome |

B-class body: `"tool": {"name": "...", "args": {...}, "result": ..., "err": "..."}`.

D-class wire shape:

```jsonc
// the command (only the broker ever reads this)
{ "kind":"cmd", "to":{"peer":"broker"}, "text":"!{task_tree} T-root-1",
  "from":"hermes:rook", "reply_target":{"peer":"user:reed"} }

// what upstream observers see (broker → reply_target)
{ "kind":"status", "from":"broker", "reply_to":"<cmd-id>",
  "status": { "cmd":"task_tree", "ok":true, "state":"ok",
              "text":"tree T-root-1: done (3/3 done)", "result":{...} } }
```

Built-in broker commands: `!{ping}`, `!{list_agents}`, `!{list_rooms}`,
`!{list_tasks}`, `!{get_task} <id>`, `!{task_tree} <root>`. Custom commands
register via `Broker.RegisterCmd(name, fn)` — the broker is the only executor,
so commands stay off the message fabric entirely. Unknown `!{cmd}` returns an
`ok:false` status, never an error crash.

---

## 3. Routing & addressing

- **Peer handle** = human/agent identity, e.g. `user:reed`, `AgentB@llm`.
- **Room handle** = group, e.g. `crew-planning`. Membership is broker state;
  `invite`/`leave` are envelopes that mutate it.
- DM: `to.peer`. Group: `to.room` (deliver to every member except sender, in room
  order). `to.reply_target` = shorthand for "send to my reply_target".

### Thread model

New conversation → omit `thread`; the broker mints one from the first envelope.
Every subsequent reply carries the same `thread`. `reply_to` is a finer pointer.
Threads are *cheap*: they only exist to group, not to gate.

---

## 4. Task passing

```jsonc
// plain task
{ "kind":"task", "to":{"peer":"AgentB"}, "from":{"handle":"AgentA","on_behalf_of":["user:reed"]},
  "reply_target":{"peer":"AgentA"},             // ← answer back to AgentA, not straight to user
  "task": {"action":"fetch_summary","params":{"url":"https://…"}},
  "about": {"task_id":"9f1c…"} }

// result
{ "kind":"result", "to":{"reply_target":true}, "from":{"handle":"AgentB"},
  "about":{"task_id":"9f1c…"}, "result":{"bullets":["…"]} }

// failure
{ "kind":"fail", "to":{"reply_target":true}, "from":{"handle":"AgentB"},
  "about":{"task_id":"9f1c…"}, "fail":{"code":"timeout","msg":"upstream 504"} }
```

`task_id` is the universal join key: acks, retries, results, and cancellation
all reference it. A task MAY be cancelled with envelope kind `task` +
`task.cancelled: true` referencing the same `task_id`.

---

## 5. Delegation & on_behalf_of

When a user tells AgentA something and asks AgentA to enlist AgentB, AgentA

```jsonc
{ "kind":"task", "from": { "handle":"AgentA", "on_behalf_of":["user:reed"] },
  "to":   {"peer":"AgentB"},
  "reply_target": {"peer":"AgentA"},            // report comes back through the chain
  "about": {"task_id":"…"} }
```

`on_behalf_of` chains are append-only. Receivers decide policy from the chain
(i.e. `user:reed`'s approval matters; `AgentA`'s taste doesn't). The chain never
wraps — loops are refused by the broker when identity already appears.

---

## 6. Orchestration (multi-hop, nested, fanning)

Orchestration is **not** a protocol mode — it's an emergent pattern built
entirely from `task` / `result` / `fanout` / `reply_target` / `parent_task`.
An orchestrator is just another agent. There is no "orchestrator" flag.

### 6.1 fanout (parallel fan-out / gather)

```jsonc
{ "kind":"task", "from":{"handle":"AgentA","on_behalf_of":["user:reed"]},
  "to":   {"room":"crew-planning"},                 // fan out to a room
  "reply_target": {"peer":"AgentA"},                // …but round up at AgentA
  "about": {"task_id":"orch-parent"},
  "fanout": {
      "fanout_id": "orch-parent",                   // same as parent for fan-in
      "plan": [ {"peer":"Worker-1"}, {"peer":"Worker-2"}, {"peer":"Worker-3"} ],
      "gather": { "barrier": "all" }               // all | first | majority
  },
  "text": "y'all summarize this page"
}
```

- The broker clones the task per plan entry, peers get `task.correlation =
  orch-parent` so partial results can be tracked.
- Workers reply `result` with `about.task_id = orch-parent` (they keep their own
  local sub-task ids in `result.sub_tasks` if they fan out further).
- Barrier semantics: `all` = wait for every peer result (or fail); `first` =
  race; `majority` = quorum (with N known from the plan list).

### 6.2 Nested loops (the interesting part)

Agents orchestrate recursively — the same primitives:

```jsonc
// user → AgentA
{ "kind":"task", "from":{"handle":"user:reed"},
  "to":{"peer":"AgentA"}, "reply_target":{"peer":"user:reed"},
  "task":{"action":"deep_research","params":{"topic":"VAAPI transcodes"}},
  "about":{"task_id":"T0"} }
```

AgentA fans out with `parent_task: "T0"`:

```jsonc
{ "kind":"task", "from":{"handle":"AgentA","on_behalf_of":["user:reed"]},
  "parent_task":"T0", "fanout_id":"f1",
  "to":{"room":"crew-research"},
  "reply_target": {"peer":"AgentA"},
  "task": {"action":"gather_papers"} }
```

The workers themselves may fan out (`parent_task: f1`, `fanout_id: f2`), forming
a DAG. When every leaf returns, `gather.barrier:"all"` at f1 closes → AgentA
composes a result and returns through its own `reply_target`:

```jsonc
{ "kind":"result", "to":{"reply_target":true}, "from":{"handle":"AgentA"},
  "parent_task":"T0", "result": {"synthesis":"…"}, "about":{"task_id":"T0"} }
```

The user sees ONE reply. The loop can nest arbitrarily deep because it is the
same task→reply_target contract at every hop. Termination rule: an open task
closes when its barrier is met (fanout), or it receives its own result/fail.
Orphans (parent dead × timeout, no reply target) are garbage-collected by the
broker's task ledger after `deadline_ts`.

### 6.3 loop (sequential round-trip)

Sometimes an orchestrator needs several single-agent rounds:

```jsonc
{ "kind":"task", "from":{"handle":"AgentA"},
  "to":{"peer":"AgentB"},
  "reply_target": {"peer":"AgentA"},
  "plan": { "loop_id":"L1", "round":1, "rounds": 3, "next_round_reply_target": {"peer":"AgentA"} } }
```

AgentB replies `result`; AgentA replies with a NEW task (`loop_id:"L1",
round:2`) that says `to:{"reply_target":true}` if it should go back to AgentB,
or a different peer if it should hop. Rounds are for budget/telemetry, not for
routing (routing is still reply_target).

---

## 7. Broker contract

1. **Deliver** each envelope exactly once per route (delivery may be retried —
   de-dup on `id`).
2. Stamp `seq` per (route) and `ts`.
3. **Ack transport-level** — if a receiver is connected and acks `received`
   within a configurable window (default 5 s), the broker retries exponential
   back-off, then parks it. Parked envelopes live in a per-route durable queue
   until connect or drop.
4. **Fan-out cloning** for `fanout.plan` entries; each clone keeps the
   same `fanout.correlation` and gets its own `task_id`.
5. **Redaction**: broker never inspects content text; it only routes.

### Broker interface (reference implementation)

```python
Broker.publish(envelope) → route envelope, fanout/fan-in as needed
Broker.subscribe(handle, cb) → registry
Broker.rooms / members / invites
Broker.client() → local client, spawned in-process (loopback broker)
Broker.serve("ws://0.0.0.0:8765") → WebSocket server (chat broker)
```

---

## 8. Hermes plugin mapping

Hermes agents get tools (registered via `ctx.register_tool`, so they read/write
like any other tool):

| Tool | Purpose |
|------|---------|
| `proto_send` | send text into a DM/room |
| `proto_task` | issue a Task (with reply_target routing) |
| `proto_results` | read back results (blocking or polling, by task_id / thread) |
| `proto_announce` | broadcast to a room |
| `proto_wait` | wait for the barrier of a fanout/loop |
| `proto_reply` | reply to a specific envelope (auto reply_target) |

A first-class agent identity (`handle`) is provisioned by the plugin at startup
and bound to this agent — so DMs/app rooms hit the right instance.
Orchestration happens through `proto_task` + `proto_wait` + the identity chain
above.

---

## 9. Audio (skipped) / storage / cleanliness

* No audio/video media in the protocol — content is JSON text and URI references only.
* Threads are just ids — cheap.
* No brokers/semaphores/shadow state in the wire — the broker IS the only stateful thing.

---

## 10. Example walk (answers the original pitch)
**User:** tell AgentB to do X and report back:

```
user:reed   →  AgentA : Task(action="delegate_X")  reply_target=user:reed      (T1)
AgentA      →  AgentB : Task(action="X", on_behalf_of=[user:reed])
                          reply_target=AgentA                                   (T2)
AgentB      →  AgentA : ack(received) + ack(accepted)                        (A2)
AgentB      →  AgentA : Result(body)                                         (T2)
AgentA      →  user:reed: Result(summary of what AgentB said)                (T1 closed)
```

**Fan-out loop (user → AgentA → Workers-1..N):**

```
T0   user:reed → AgentA   Task(deep_research, reply_target=user:reed)
f1   AgentA  → room crew  Task(gather_papers, fanout(f1), parent T0, reply_target=AgentA)
f1a  Worker1 → AgentA     Result(...) about f1
f1b  Worker2 → AgentA     Fail(...)   about f1
f1c  Worker3 → AgentA     Result(...) about f1
-- barrier all met at f1 --
T0   AgentA  → user:reed  Result(synthesis) about T0
```

---

## 11. Hermes profile multiplexing

One broker, many Hermes profiles. Each profile resolves its own agent handle
**at runtime** — never hardcoded:

```
handle = PROTO_HANDLE env                # explicit override wins
       || "hermes:" + HERMES_PROFILE     # profile-scoped default
       || "hermes:default"
```

- `hermes:rook`, `hermes:reed`, `hermes:boris`, … all multiplex one broker.
  Each gets a separate route (`peer:hermes:<profile>`), inbox, and durable
  parked queue — zero cross-talk, one process.
- `GET /api/agents` is the directory: every known handle with `live`
  (attached subscriber), `parked` (queued while away), and `sent` counts.
- Users stay `user:<name>`; external (non-Hermes) agents use bare handles.
- Delegation across profiles carries the `on_behalf_of` chain as usual, e.g.
  `["user:reed", "hermes:rook"]`.
- A profile restart never loses mail: envelopes park in its durable queue and
  flush on reattach.

## 12. Nested delegation → one task tree

Any delegate may itself delegate. Sub-tasks carry `parent_task`; the ledger
walks the chain to resolve `root_task`, so an entire A→B→C chain rolls up
under ONE tracked task:

```
T-root (user:reed → hermes:rook)
  └─ T1 (hermes:rook → hermes:boris, obo=[user:reed, hermes:rook])
       └─ T2 (hermes:boris → Worker-3, obo=[user:reed, hermes:rook, hermes:boris])
```

- `POST /api/delegate` accepts `parent_task` (the task being serviced) and
  returns `{task_id, root_task}`.
- `GET /api/task/<root>/tree` returns the aggregated tree: `{status,
  open/done/failed/total, root, descendants[]}`. Tree status rolls up:
  `failed` if any descendant failed, `open` while any is open, else `done`.
- Fanouts nest the same way (`parent_task` on the fanout task), so
  orchestrations of arbitrary depth stay observable from the root.
