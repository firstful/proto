"""PROTO Hermes plugin — thin Python shim over the Go proto broker.

All protocol logic lives in the Go broker (gitea.ffilt.us/reeds/proto); this
plugin exposes tools that speak its REST API. Start the broker with:

    proto --addr :8808 --journal /mnt/hot/proto/journal.jsonl

Profile multiplexing: every Hermes profile gets its OWN agent handle on the
shared broker, derived at runtime — `hermes:<profile>` — so multiple profiles
(rook, reed, boris, ...) multiplex one broker with separate inboxes and no
cross-talk. Resolution order:
    PROTO_HANDLE (full handle override) → HERMES_SESSION_PROFILE /
    HERMES_PROFILE env → 'default' (name overridden to 'rook'; set
    PROTO_DEFAULT_NAME to change that).

Config via env:
    PROTO_URL          broker base URL   (default http://proto.homelab.internal)
    PROTO_HANDLE       explicit handle override (default hermes:<profile>)
    PROTO_NAME         explicit nickname override (default <profile>)
    PROTO_DEFAULT_NAME broker name for the default profile (default rook)
    PROTO_NICKNAME     legacy nickname override (still honored)

Username allocation:
    At startup the plugin AUTO-CLAIMS its profile-derived nickname (FCFS,
    Ed25519-signed, journaled broker-side). Set PROTO_NAME or PROTO_NICKNAME
    to claim something other than the profile name.
"""

from __future__ import annotations

import json
import os
import time
import base64
from pathlib import Path
from typing import Optional, Dict, Tuple

# Crypto imports for key generation and signing
try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.hazmat.primitives import serialization
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

import urllib.request

_BROKER = os.environ.get("PROTO_URL", "http://proto.homelab.internal")

# Profile-name override: the default profile identifies as "rook" on the
# broker; every other profile auto-connects as its own profile name.
_DEFAULT_PROFILE_OVERRIDE_ENV = "PROTO_DEFAULT_NAME"
_DEFAULT_PROFILE_NAME = "rook"


def _profile_name() -> str:
    """Name this session identifies with on the broker.

    Order: PROTO_HANDLE (full handle override) → PROTO_NAME (name override)
    → HERMES_SESSION_PROFILE / HERMES_PROFILE (active profile) → default.
    """
    explicit = os.environ.get("PROTO_HANDLE")
    if explicit:
        return explicit.split(":", 1)[-1] if explicit.startswith("hermes:") else explicit
    named = os.environ.get("PROTO_NAME")
    if named:
        return named
    profile = (
        os.environ.get("HERMES_SESSION_PROFILE")
        or os.environ.get("HERMES_PROFILE")
        or "default"
    )
    if profile == "default":
        return os.environ.get(_DEFAULT_PROFILE_OVERRIDE_ENV, _DEFAULT_PROFILE_NAME)
    return profile


def _resolve_handle() -> str:
    """Runtime profile-scoped handle (multiplexing; see SPEC §11)."""
    explicit = os.environ.get("PROTO_HANDLE")
    if explicit:
        return explicit
    return f"hermes:{_profile_name()}"


_TIMEOUT = 60

# Username allocation storage
_KEY_DIR = Path(os.environ.get("HERMES_HOME", "~/.hermes")).expanduser() / "proto_keys"
_USERNAME_CLAIM_ROOM = "username_claims"  # Room for all username claims
_CLAIMED_USERNAMES: Dict[str, Tuple[str, str, str]] = {}  # nickname -> (handle, pubkey_b64, signature)
_KEY_PATHS: Dict[str, Path] = {}  # handle -> key file (lazy, per-handle)


def _handle() -> str:
    """Current session's broker handle — resolved lazily per call so gateway
    sessions on different profiles multiplex correctly in one process."""
    return _resolve_handle()


def _ensure_crypto():
    """Ensure crypto dependencies are available."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError(
            "Username allocation requires cryptography package. "
            "Install with: pip install cryptography"
        )

def _get_keypair() -> Tuple[Path, 'ed25519.Ed25519PrivateKey']:
    """Get or create the persistent keypair for this handle (on-disk, reused)."""
    handle = _handle()
    _ensure_crypto()
    key_path = _KEY_PATHS.get(handle)
    if key_path is None:
        _KEY_DIR.mkdir(parents=True, exist_ok=True)
        key_path = _KEY_DIR / f"{handle.replace(':', '_')}.key"
        _KEY_PATHS[handle] = key_path
    _KEY_PATH = key_path

    if key_path.exists():
        # Load existing key
        try:
            with open(key_path, "rb") as f:
                return key_path, serialization.load_pem_private_key(
                    f.read(), password=None)
        except Exception:
            pass  # corrupted — fall through and regenerate

    # Generate new key atomically (write temp, rename) so concurrent startups
    # can't interleave a partial key file.
    private_key = ed25519.Ed25519PrivateKey.generate()
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    tmp = key_path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        f.write(pem)
    os.chmod(tmp, 0o600)
    os.replace(tmp, key_path)
    return key_path, private_key

def _sign_message(private_key, message: str) -> str:
    """Sign a message and return base64-encoded signature."""
    _ensure_crypto()
    signature = private_key.sign(message.encode('utf-8'))
    return base64.b64encode(signature).decode('ascii')

def _get_public_key_b64(private_key) -> str:
    """Get base64-encoded public key."""
    _ensure_crypto()
    public_key = private_key.public_key()
    public_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    )
    return base64.b64encode(public_bytes).decode('ascii')

def _verify_signature(pubkey_b64: str, message: str, signature_b64: str) -> bool:
    """Verify an Ed25519 signature."""
    _ensure_crypto()
    try:
        public_key = ed25519.Ed25519PublicKey.from_public_bytes(
            base64.b64decode(pubkey_b64)
        )
        public_key.verify(
            base64.b64decode(signature_b64),
            message.encode('utf-8')
        )
        return True
    except Exception:
        return False

def _load_claimed_usernames() -> Dict[str, Tuple[str, str, str]]:
    """Load all currently claimed usernames from the broker's claim store."""
    global _CLAIMED_USERNAMES
    try:
        claims = _req("GET", "/api/username_claims")
        _CLAIMED_USERNAMES = {}
        for c in claims or []:
            nick = c.get("nickname", "")
            if nick:
                _CLAIMED_USERNAMES[nick] = (
                    c.get("handle", ""),
                    c.get("public_key", ""),
                    c.get("signature", "")
                )
    except Exception:
        pass
    return _CLAIMED_USERNAMES

def _claim_nickname(nickname: str) -> bool:
    """
    Attempt to claim a nickname via first-come-first-serve.
    
    Returns True if the nickname was successfully claimed.
    """
    if not nickname or not nickname.strip():
        return False
        
    nickname = nickname.strip()
    
    try:
        _, private_key = _get_keypair()
        pubkey_b64 = _get_public_key_b64(private_key)
        
        # Create the claim payload
        claim_msg = f"{nickname}\n{pubkey_b64}"
        signature = _sign_message(private_key, claim_msg)
        
        # Submit claim to the broker's atomic claim store
        body = {
            "nickname": nickname,
            "handle": _handle(),
            "public_key": pubkey_b64,
            "signature": signature,
            "timestamp": int(time.time()),
            "claim_message": claim_msg
        }
        result = _req("POST", "/api/username_claims", body)

        winner = result.get("winner") or {}
        won = bool(result.get("ok"))
        if not won and winner:
            # FCFS loss: someone already holds this nickname. Accept only if
            # it's us (restart re-claim) AND their signature verifies.
            ours = winner.get("handle") == _handle()
            legit = _verify_signature(
                winner.get("public_key", ""),
                winner.get("claim_message", ""),
                winner.get("signature", ""),
            ) if ours else False
            if ours and legit:
                won = True
            else:
                print(f"Nickname '{nickname}' already claimed by {winner.get('handle')}")
                return False

        if won:
            # Trust but verify whatever the broker accepted.
            if winner and not _verify_signature(
                winner.get("public_key", ""),
                winner.get("claim_message", ""),
                winner.get("signature", ""),
            ):
                print(f"Broker accepted a claim for '{nickname}' with invalid signature")
                return False
            # Cache the winning record
            _CLAIMED_USERNAMES[nickname] = (
                winner.get("handle", _handle()),
                winner.get("public_key", pubkey_b64),
                winner.get("signature", signature),
            )
            return True
        return False
        
    except Exception as e:
        print(f"Failed to claim nickname {nickname}: {e}")
        return False

def _req(method: str, path: str, body: dict | None = None, timeout: int = _TIMEOUT) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        _BROKER + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())

# ---------------------------------------------------------------- tools ---

def _handle_status(args: dict, **_) -> dict:
    rooms = _req("GET", "/api/rooms")
    agents = _req("GET", "/api/agents")
    # Also return claimed usernames for awareness
    return {
        "handle": _handle(), 
        "broker": _BROKER, 
        "rooms": rooms,
        "agents": agents,
        "claimed_usernames": {
            nick: {"handle": h, "pubkey": pk} 
            for nick, (h, pk, _) in _CLAIMED_USERNAMES.items()
        }
    }

_STATUS_SCHEMA = {
    "name": "proto_status",
    "description": "PROTO protocol status: my handle, broker URL, rooms, and claimed usernames.",
    "parameters": {"type": "object", "properties": {}},
}

def _handle_send(args: dict, **_) -> dict:
    to = {"peer": args["peer"]} if args.get("peer") else {"room": args["room"]}
    env = {
        "v": 1, "kind": "msg", "from": _handle(), "to": to, "text": args["text"],
        "reply_target": {"peer": args.get("reply_target", _handle())}
    }
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

def _handle_task(args: dict, **_) -> dict:
    to = {"peer": args["peer"]} if args.get("peer") else {"room": args["room"]}
    env = {
        "v": 1, "kind": "task", "from": _handle(), "to": to,
        "task": {"action": args["action"], **( {"params": args["params"]} if args.get("params") else {})},
        "reply_target": {"peer": args.get("reply_target", _handle())}
    }
    if args.get("on_behalf_of"):
        env["on_behalf_of"] = args["on_behalf_of"]
    if args.get("fanout_peers"):
        env["fanout"] = {
            "fanout_id": f"fo-{args['action'][:8]}",
            "plan": [{"peer": p} for p in args["fanout_peers"]],
            **( {"barrier": args["barrier"]} if args.get("barrier") else {})
        }
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

def _handle_results(args: dict, **_) -> dict:
    handle = args.get("handle", _handle())
    return {"inbox": _req("GET", f"/api/inbox/{handle}")}

_RESULTS_SCHEMA = {
    "name": "proto_results",
    "description": "Poll my PROTO inbox (results, fails, acks, parked messages).",
    "parameters": {"type": "object", "properties": {
        "handle": {"type": "string", "description": "Override handle (default: this agent)"}}},
}

def _handle_task_info(args: dict, **_) -> dict:
    return _req("GET", f"/api/task/{args['task_id']}")

_TASKINFO_SCHEMA = {
    "name": "proto_task_info",
    "description": "Read a task's ledger record (status, results, fails) by task id.",
    "parameters": {"type": "object", "properties": {
        "task_id": {"type": "string"}}, "required": ["task_id"]},
}

def _handle_delegate(args: dict, **_) -> dict:
    body = {
        "from": _handle(), "peer": args["peer"], "action": args["action"],
        "timeout_sec": args.get("timeout", 30)
    }
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

def _handle_fanout(args: dict, **_) -> dict:
    body = {
        "from": _handle(), "peers": args["peers"], "action": args["action"],
        "barrier": args.get("barrier", "all"),
        "timeout_sec": args.get("timeout", 60)
    }
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

def _handle_announce(args: dict, **_) -> dict:
    _req("POST", f"/api/rooms/{args['room']}/join/{_handle()}")
    env = {
        "v": 1, "kind": "msg", "from": _handle(), "to": {"room": args["room"]},
        "text": args["text"], "reply_target": {"peer": _handle()}}
    return _req("POST", "/api/envelope", env)

_ANNOUNCE_SCHEMA = {
    "name": "proto_announce",
    "description": "Join a room (if not already) and broadcast a message to it.",
    "parameters": {"type": "object", "properties": {
        "room": {"type": "string"}, "text": {"type": "string"}},
        "required": ["room", "text"]},
}

def _handle_cmd(args: dict, **_) -> dict:
    """D-class: broker-intercepted command. Peers never see it; we get a
    status envelope back at our reply_target."""
    cmd = args["cmd"].strip()
    if not cmd.startswith("!{"):
        cmd = f"!{{{cmd}}}"
    env = {
        "v": 1, "kind": "cmd", "from": _handle(), "to": {"peer": "broker"},
        "text": cmd, "reply_target": {"peer": _handle()}}
    _req("POST", "/api/envelope", env)
    # status comes back to our route; poll the inbox briefly
    import time
    for _ in range(20):
        time.sleep(0.1)
        inbox = _req("GET", f"/api/inbox/{_handle()}")
        for e in inbox or []:
            if e.get("kind") == "status" and (e.get("status") or {}).get("cmd"):
                return e["status"]
    return {"ok": False, "state": "timeout", "text": "no status within 2s"}

_CMD_SCHEMA = {
    "name": "proto_cmd",
    "description": "Run a broker command (!{ping}, !{list_agents}, !{task_tree} <root>, !{get_task} <id>, ...). The broker executes it directly; peers never see it — you get a structured status back.",
    "parameters": {"type": "object", "properties": {
        "cmd": {"type": "string", "description": "e.g. 'task_tree T-root-1' or the raw '!{...}' form'"}},
        "required": ["cmd"]},
}

def _handle_claim_username(args: dict, **_) -> dict:
    """Explicitly attempt to claim a username."""
    nickname = args.get("nickname")
    if not nickname:
        return {"error": "nickname required"}
    
    success = _claim_nickname(nickname)
    if success:
        return {
            "status": "claimed",
            "nickname": nickname,
            "handle": _handle(),
            "message": f"Successfully claimed username '{nickname}'"
        }
    else:
        return {
            "status": "failed",
            "nickname": nickname,
            "error": f"Failed to claim username '{nickname}' (may already be taken)"
        }

_CLAIM_USERNAME_SCHEMA = {
    "name": "proto_claim_username",
    "description": "Attempt to claim a username via first-come-first-serve allocation.",
    "parameters": {"type": "object", "properties": {
        "nickname": {"type": "string", "description": "The username to attempt to claim"}},
        "required": ["nickname"]},
}

def _handle_list_usernames(args: dict, **_) -> dict:
    """List all currently claimed usernames (refreshed from the broker)."""
    claims = _req("GET", "/api/username_claims")
    return {
        "claimed_usernames": [
            {
                "nickname": c.get("nickname", ""),
                "handle": c.get("handle", ""),
                "public_key": c.get("public_key", ""),
                "timestamp": c.get("timestamp", 0),
            }
            for c in claims or []
        ],
        "total": len(claims or []),
    }

_LIST_USERNAMES_SCHEMA = {
    "name": "proto_list_usernames",
    "description": "List all currently claimed usernames in the system.",
    "parameters": {"type": "object", "properties": {}},
}

# Tool registration tuple list
def _json_result(handler):
    """Hermes tool handlers must return str; JSON-encode dict results."""
    import json as _json
    def wrapper(args: dict, **kw):
        out = handler(args, **kw)
        return out if isinstance(out, str) else _json.dumps(out, ensure_ascii=False)
    wrapper.__name__ = handler.__name__
    return wrapper


_TOOLS = (
    (_json_result(_handle_status),    _STATUS_SCHEMA),
    (_json_result(_handle_send),      _SEND_SCHEMA),
    (_json_result(_handle_task),      _TASK_SCHEMA),
    (_json_result(_handle_results),   _RESULTS_SCHEMA),
    (_json_result(_handle_task_info), _TASKINFO_SCHEMA),
    (_json_result(_handle_delegate),  _DELEGATE_SCHEMA),
    (_json_result(_handle_fanout),    _FANOUT_SCHEMA),
    (_json_result(_handle_announce),  _ANNOUNCE_SCHEMA),
    (_json_result(_handle_cmd),       _CMD_SCHEMA),
    (_json_result(_handle_claim_username), _CLAIM_USERNAME_SCHEMA),
    (_json_result(_handle_list_usernames), _LIST_USERNAMES_SCHEMA),
)

def _register_agent() -> None:
    """Announce this profile on the broker: a ping cmd envelope both registers
    the agent handle (agents are derived from envelope traffic) and verifies
    the broker is reachable. Idempotent."""
    try:
        env = {
            "v": 1, "kind": "cmd", "from": _handle(), "to": {"peer": "broker"},
            "text": "!{ping}", "reply_target": {"peer": _handle()}}
        _req("POST", "/api/envelope", env)
        # drain the status reply so it doesn't sit in the inbox
        try:
            _req("GET", f"/api/inbox/{_handle()}")
        except Exception:
            pass
    except Exception:
        pass  # broker down — tools will surface connection errors on use


def register(ctx) -> None:
    """Hermes plugin entrypoint: auto-connect as this profile's name."""
    nickname = os.environ.get("PROTO_NICKNAME") or _profile_name()
    _register_agent()
    print(f"[proto] connecting as {_handle()} (nickname: {nickname})")
    if _claim_nickname(nickname):
        print(f"[proto] username '{nickname}' claimed")
    else:
        print(f"[proto] username '{nickname}' unavailable (already claimed)")
    
    # Register all tools
    for handler, schema in _TOOLS:
        ctx.register_tool(
            name=schema["name"],
            toolset="proto",
            schema=schema,
            handler=handler,
            description=schema["description"],
            emoji="🔗",
        )