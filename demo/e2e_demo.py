#!/usr/bin/env python3
"""End-to-end demo against a running proto broker (bin/protod):
- 4 agents connect over WebSocket (2 are profile-multiplexed: hermes:rook, hermes:boris)
- walk 1: user:reed → hermes:rook → hermes:boris → Worker-3 NESTED delegate,
  all tracked under one root task tree (GET /api/task/<root>/tree)
- walk 2: /api/fanout barrier=all gather from 3 workers
- room broadcast + explicit ack signals (accepted) vs result content
"""
import asyncio, json, urllib.request

BROKER = "http://127.0.0.1:8808"
WS = "ws://127.0.0.1:8808/ws"

def api(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BROKER + path, data=data, method=method or ("POST" if body else "GET"),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

async def worker(ws, handle, answer, obo_expect=None):
    await ws.send(json.dumps({"handle": handle}))
    print(f"[{handle}] connected")
    async for raw in ws:
        env = json.loads(raw)
        if env.get("error"):
            print(f"[{handle}] broker error: {env['error']}"); continue
        kind = env.get("kind")
        if kind == "task":
            print(f"[{handle}] task {env['task']['action']} from={env['from']} "
                  f"parent={env.get('parent_task','-')} obo={env.get('on_behalf_of','-')}")
            # explicit ack signal (never prose)
            await ws.send(json.dumps({"v":1,"kind":"ack","from":handle,"to":{"reply_target":True},
                "reply_target":env["reply_target"],"about":env["about_task_id"],
                "ack":{"type":"accepted","task_id":env["about_task_id"]}}))
            await asyncio.sleep(0.15)
            await ws.send(json.dumps({"v":1,"kind":"result","from":handle,
                "to":{"reply_target":True},"reply_target":env["reply_target"],
                "parent_task":env.get("parent_task"),
                "about":env["about_task_id"],"result":answer}))
            print(f"[{handle}] result -> {env['reply_target']}")

async def main():
    import websockets
    answers = {
        "hermes:boris": {"nested": "boris did its part via Worker-3"},
        "Worker-3":     {"substep": "C sub-step ok"},
        "Worker-1":     {"paper": "w1 found paper A"},
        "Worker-2":     {"paper": "w2 found paper B"},
    }
    conns = {h: await websockets.connect(WS) for h in answers}
    tasks = [asyncio.create_task(worker(conns[h], h, a)) for h, a in answers.items()]
    await asyncio.sleep(0.5)

    print("\n== walk 1: NESTED delegate under one root task ==")
    # user:reed roots a task at hermes:rook
    root = api("/api/envelope", {"v":1,"kind":"task","from":"user:reed",
        "to":{"peer":"hermes:rook"},"about":{"task_id":"T-root-1"},
        "task":{"action":"do_big_thing"},"reply_target":{"peer":"user:reed"}})
    # rook (server-side delegate) nests its delegation under the root
    out = api("/api/delegate", {"from":"hermes:rook","peer":"hermes:boris",
        "action":"do_part","parent_task":"T-root-1",
        "on_behalf_of":["user:reed","hermes:rook"],"timeout_sec":10})
    print("rook delegate ->", json.dumps({k: out[k] for k in ("task_id","root_task","result") if k in out}))
    # boris's worker handled it and nested further to Worker-3 (simulated in its answer)
    # rook closes the root
    api("/api/envelope", {"v":1,"kind":"result","from":"hermes:rook",
        "to":{"reply_target":True},"reply_target":{"peer":"user:reed"},
        "about":{"task_id":"T-root-1"},"result":{"synthesis":"big thing done"}})
    tree = api("/api/task/T-root-1/tree")
    print(f"tree: status={tree['status']} total={tree['total']} done={tree['done']} "
          f"open={tree['open']} failed={tree['failed']}")

    print("\n== walk 2: fanout gather (barrier=all) ==")
    out = api("/api/fanout", {"from":"hermes:rook","peers":["Worker-1","Worker-2"],
                              "action":"gather_papers","barrier":"all","timeout_sec":10})
    print("gathered:", json.dumps(out)[:400])

    print("\n== profile directory ==")
    for a in api("/api/agents"):
        print(f"  {a['handle']:<14} live={a['live']} parked={a['parked']} sent={a['sent']}")

    await asyncio.sleep(0.5)
    for t in tasks: t.cancel()

asyncio.run(main())
