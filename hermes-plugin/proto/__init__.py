"""PROTO Hermes plugin — thin Python shim over the Go proto broker.

All protocol logic lives in the Go broker (gitea.ffilt.us/reeds/proto); this
plugin exposes tools that speak its REST API. Start the broker with:

    proto --addr :8808 --journal /mnt/hot/proto/journal.jsonl

Profile multiplexing: every Hermes profile gets its OWN agent handle on the
shared broker, derived at runtime — `hermes:<profile>` — so multiple profiles
(rook, reed, boris, ...) multiplex one broker with separate inboxes and no
cross-talk. Resolution order:
    PROTO_HANDLE (explicit override) → HERMES_PROFILE env → 'default'.

Config via env:
    PROTO_URL      broker base URL   (default http://127.0.0.1:8808)
    PROTO_HANDLE   explicit handle override (default hermes:<profile>)
"""
from __future__ import annotations

import json
import os
import urllib.request

_BROKER = os.environ.get("PROTO_URL", "http://127.0.0.1:8808")


def _resolve_handle() -> str:
    """Runtime profile-scoped handle (multiplexing; see SPEC §11)."""
    explicit = os.environ.get("PROTO_HANDLE")
    if explicit:
        return explicit
    profile = os.environ.get("HERMES_PROFILE") or "default"
    return f"hermes:{profile}"


_HANDLE = _resolve_handle()
_TIMEOUT = 60


def _req(method: str, path: str, body: dict | None = None, timeout: int = _TIMEOUT) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        _BROKER + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


# ---------------------------------------------------------------- tools ----

def _handle_status(args: dict) -> dict:
    rooms = _req("GET", "/api/rooms")
    agents = _req("GET", "/api/agents")
    return {"handle": _HANDLE, "broker": _BROKER, "rooms": rooms,
            "agents": agents}


_STATUS_SCHEMA = {
    "name": "proto_status",
    "description": "PROTO protocol status: my handle, broker URL, rooms.",
    "parameters": {"type": "object", "properties": {}},
}


def _handle_send(args: dict) -> dict:
    to = {"peer": args["peer"]} if args.get("peer") else {"room": args["room"]}
    env = {"v": 1, "kind": "msg", "from": _HANDLE, "to": to, "text": args["text"],
           "reply_target": {"peer": args.get("reply_target", _HANDLE)}}
    if args.get("on_behalf_of"):
        env["on_behalf_of"] = args["on_behalf_of"]
    return _req("POST", "/api/envelope", env)


_SEND_SCHEMA = {
    "name": "proto_send",
    "description": "Send a PROTO message to a peer (DM) or room (group).",
    "parameters": {"type": "object", "properties": {
        "peer": {"type": "string", "description": "Peer handle for a DM"},
        "room": {"type": "string", "description": "Room handle for a group"},
        "text": {"type": "string"},
        "reply_target": {"type": "string", "description": "Where replies go (default: me)"},
        "on_behalf_of": {"type": "array", "items": {"type": "string"},
                          "description": "Delegation chain, e.g. [user:reed, AgentA]"}},
        "anyOf": [{"required": ["peer"]}, {"required": ["room"]}, {"required": ["text"]}]},
}


def _handle_task(args: dict) -> dict:
    to = {"peer": args["peer"]} if args.get("peer") else {"room": args["room"]}
    env = {"v": 1, "kind": "task", "from": _HANDLE, "to": to,
           "task": {"action": args["action"], **({"params": args["params"]} if args.get("params") else {})},
           "reply_target": {"peer": args.get("reply_target", _HANDLE)}}
    if args.get("on_behalf_of"):
        env["on_behalf_of"] = args["on_behalf_of"]
    if args.get("fanout_peers"):
        # fanout: the broker clones per peer; results gather at reply_target
        env["fanout"] = {"fanout_id": f"fo-{args['action'][:8]}",
                         "plan": [{"peer": p} for p in args["fanout_peers"]],
                         **({"barrier": args["barrier"]} if args.get("barrier") else {})}
        to = {"peer": args["fanout_peers"][0]}
        env["to"] = to
    return _req("POST", "/api/envelope", env)


_TASK_SCHEMA = {
    "name": "proto_task",
    "description": "Issue a PROTO task to a peer or room; optionally fanout to multiple peers with a gather barrier (all/first/majority).",
    "parameters": {"type": "object", "properties": {
        "peer": {"type": "string"},
        "room": {"type": "string"},
        "action": {"type": "string"},
        "params": {"type": "object"},
        "reply_target": {"type": "string"},
        "on_behalf_of": {"type": "array", "items": {"type": "string"}},
        "fanout_peers": {"type": "array", "items": {"type": "string"},
                          "description": "Fan this task out to N peers in parallel"},
        "barrier": {"type": "string", "enum": ["all", "first", "majority"]}},
        "required": ["action"]},
}


def _handle_results(args: dict) -> dict:
    handle = args.get("handle", _HANDLE)
    return {"inbox": _req("GET", f"/api/inbox/{handle}")}


_RESULTS_SCHEMA = {
    "name": "proto_results",
    "description": "Poll my PROTO inbox (results, fails, acks, parked messages).",
    "parameters": {"type": "object", "properties": {
        "handle": {"type": "string", "description": "Override handle (default: this agent)"}}},
}


def _handle_task_info(args: dict) -> dict:
    return _req("GET", f"/api/task/{args['task_id']}")


_TASKINFO_SCHEMA = {
    "name": "proto_task_info",
    "description": "Read a task's ledger record (status, results, fails) by task id.",
    "parameters": {"type": "object", "properties": {
        "task_id": {"type": "string"}}, "required": ["task_id"]},
}


def _handle_delegate(args: dict) -> dict:
    body = {"from": _HANDLE, "peer": args["peer"], "action": args["action"],
            "timeout_sec": args.get("timeout", 30)}
    if args.get("params"):
        body["params"] = args["params"]
    if args.get("on_behalf_of"):
        body["on_behalf_of"] = args["on_behalf_of"]
    out = _req("POST", "/api/delegate", body,
               timeout=args.get("timeout", 30) + 10)
    if out.get("fail"):
        return {"status": "failed", "fail": out["fail"]}
    if out.get("error"):
        return {"status": "error", "error": out["error"]}
    return {"status": "done", "result": out.get("result")}


_DELEGATE_SCHEMA = {
    "name": "proto_delegate",
    "description": "Delegate a task to a peer agent and BLOCK until their result/fail comes back (report-back pattern).",
    "parameters": {"type": "object", "properties": {
        "peer": {"type": "string"},
        "action": {"type": "string"},
        "params": {"type": "object"},
        "on_behalf_of": {"type": "array", "items": {"type": "string"},
                          "description": "Delegation chain, e.g. [user:reed]"},
        "timeout": {"type": "number", "default": 30}},
        "required": ["peer", "action"]},
}


def _handle_fanout(args: dict) -> dict:
    body = {"from": _HANDLE, "peers": args["peers"], "action": args["action"],
            "barrier": args.get("barrier", "all"),
            "timeout_sec": args.get("timeout", 60)}
    if args.get("params"):
        body["params"] = args["params"]
    if args.get("on_behalf_of"):
        body["on_behalf_of"] = args["on_behalf_of"]
    out = _req("POST", "/api/fanout", body,
               timeout=args.get("timeout", 60) + 10)
    if out.get("error"):
        return {"status": "error", "error": out["error"],
                "partial_results": out.get("results", {})}
    return {"status": "done", "results": out.get("results", {})}


_FANOUT_SCHEMA = {
    "name": "proto_fanout",
    "description": "Fan a task out to N peers in parallel and GATHER results (barrier: all/first/majority). The orchestration loop primitive.",
    "parameters": {"type": "object", "properties": {
        "peers": {"type": "array", "items": {"type": "string"}},
        "action": {"type": "string"},
        "params": {"type": "object"},
        "barrier": {"type": "string", "enum": ["all", "first", "majority"]},
        "on_behalf_of": {"type": "array", "items": {"type": "string"}},
        "timeout": {"type": "number", "default": 60}},
        "required": ["peers", "action"]},
}


def _handle_announce(args: dict) -> dict:
    _req("POST", f"/api/rooms/{args['room']}/join/{_HANDLE}")
    env = {"v": 1, "kind": "msg", "from": _HANDLE, "to": {"room": args["room"]},
           "text": args["text"], "reply_target": {"peer": _HANDLE}}
    return _req("POST", "/api/envelope", env)


_ANNOUNCE_SCHEMA = {
    "name": "proto_announce",
    "description": "Join a room (if not already) and broadcast a message to it.",
    "parameters": {"type": "object", "properties": {
        "room": {"type": "string"}, "text": {"type": "string"}},
        "required": ["room", "text"]},
}


def _handle_cmd(args: dict) -> dict:
    """D-class: broker-intercepted command. Peers never see it; we get a
    status envelope back at our reply_target."""
    cmd = args["cmd"].strip()
    if not cmd.startswith("!{"):
        cmd = f"!{{{cmd}}}"
    env = {"v": 1, "kind": "cmd", "from": _HANDLE, "to": {"peer": "broker"},
           "text": cmd, "reply_target": {"peer": _HANDLE}}
    _req("POST", "/api/envelope", env)
    # status comes back to our route; poll the inbox briefly
    import time
    for _ in range(20):
        time.sleep(0.1)
        inbox = _req("GET", f"/api/inbox/{_HANDLE}")
        for e in inbox or []:
            if e.get("kind") == "status" and (e.get("status") or {}).get("cmd"):
                return e["status"]
    return {"ok": False, "state": "timeout", "text": "no status within 2s"}


_CMD_SCHEMA = {
    "name": "proto_cmd",
    "description": "Run a broker command (!{ping}, !{list_agents}, !{task_tree} <root>, !{get_task} <id>, ...). The broker executes it directly; peers never see it — you get a structured status back.",
    "parameters": {"type": "object", "properties": {
        "cmd": {"type": "string", "description": "e.g. 'task_tree T-root-1' or the raw '!{...}' form"}},
        "required": ["cmd"]},
}


_TOOLS = (
    (_handle_status,    _STATUS_SCHEMA),
    (_handle_send,      _SEND_SCHEMA),
    (_handle_task,      _TASK_SCHEMA),
    (_handle_results,   _RESULTS_SCHEMA),
    (_handle_task_info, _TASKINFO_SCHEMA),
    (_handle_delegate,  _DELEGATE_SCHEMA),
    (_handle_fanout,    _FANOUT_SCHEMA),
    (_handle_announce,  _ANNOUNCE_SCHEMA),
    (_handle_cmd,       _CMD_SCHEMA),
)


def register(ctx) -> None:
    """Hermes plugin entrypoint."""
    for handler, schema in _TOOLS:
        ctx.register_tool(
            name=schema["name"],
            toolset="proto",
            schema=schema,
            handler=handler,
            description=schema["description"],
            emoji="🔗",
        )
