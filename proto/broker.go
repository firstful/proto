package proto

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// Handler is a subscriber callback for a route.
type Handler func(*Envelope)

// Subscriber pairs a handler with a cancel function (websocket conns, etc).
type subscriber struct {
	id     string
	route  string
	fn     Handler
	cancel func()
}

// Broker routes envelopes (SPEC §7). One Broker per process.
type Broker struct {
	mu        sync.RWMutex
	subs      map[string][]*subscriber // route -> subs
	rooms     map[string]map[string]bool
	durable   map[string]*list.List // parked envelopes per route
	seq       map[string]int64
	seen      map[string]bool // envelope id dedup
	ledger    *Ledger
	ring      *list.List // recent envelopes for web UI / API
	ringMax   int
	journal   *Journal
	waiters   map[string][]chan struct{} // "task:<id>" / "fanout:<id>" gather keys
	waitersMu sync.Mutex
	cmds      map[string]CmdFunc // !{name} broker commands (D-class)
}

func NewBroker(journalPath string) *Broker {
	b := &Broker{
		subs:    map[string][]*subscriber{},
		rooms:   map[string]map[string]bool{},
		durable: map[string]*list.List{},
		seq:     map[string]int64{},
		seen:    map[string]bool{},
		ledger:  NewLedger(),
		ring:    list.New(),
		ringMax: 5000,
		waiters: map[string][]chan struct{}{},
	}
	if journalPath != "" {
		b.journal = OpenJournal(journalPath)
	}
	registerBuiltinCmds(b)
	return b
}

// registerBuiltinCmds installs the D-class command set. Handlers run INSIDE
// the broker; senders see only a status envelope back at their reply_target.
func registerBuiltinCmds(b *Broker) {
	b.RegisterCmd("ping", func(string) (any, string, error) {
		return map[string]any{"pong": Now(), "version": "proto/0.1"}, "pong", nil
	})
	b.RegisterCmd("list_agents", func(string) (any, string, error) {
		return b.Agents(), "agents", nil
	})
	b.RegisterCmd("list_rooms", func(string) (any, string, error) {
		return b.Rooms(), "rooms", nil
	})
	b.RegisterCmd("list_tasks", func(args string) (any, string, error) {
		return b.Ledger().Tasks(), "tasks", nil
	})
	b.RegisterCmd("get_task", func(args string) (any, string, error) {
		if args == "" {
			return nil, "", fmt.Errorf("get_task needs a task id")
		}
		rec := b.Ledger().GetTask(args)
		if rec == nil {
			return nil, "", fmt.Errorf("no task %s", args)
		}
		return rec, "task " + args, nil
	})
	b.RegisterCmd("task_tree", func(args string) (any, string, error) {
		if args == "" {
			return nil, "", fmt.Errorf("task_tree needs a root task id")
		}
		tree := b.Ledger().Tree(args)
		if tree == nil {
			return nil, "", fmt.Errorf("no task %s", args)
		}
		return tree, fmt.Sprintf("tree %s: %s (%d/%d done)", args, tree.Status, tree.Done, tree.Total), nil
	})
}

func (b *Broker) Ledger() *Ledger { return b.ledger }

// Subscribe registers a handler for a route; flushes any parked envelopes.
func (b *Broker) Subscribe(route, subID string, fn Handler, cancel func()) {
	b.mu.Lock()
	b.subs[route] = append(b.subs[route], &subscriber{id: subID, route: route, fn: fn, cancel: cancel})
	parked := b.durable[route]
	delete(b.durable, route)
	b.mu.Unlock()
	if parked != nil {
		for e := parked.Front(); e != nil; e = e.Next() {
			if env, ok := e.Value.(*Envelope); ok {
				fn(env)
			}
		}
	}
}

// Unsubscribe removes a subscriber by id.
func (b *Broker) Unsubscribe(route, subID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.subs[route][:0]
	for _, s := range b.subs[route] {
		if s.id != subID {
			out = append(out, s)
		} else if s.cancel != nil {
			go s.cancel()
		}
	}
	b.subs[route] = out
}

// Publish validates and routes an envelope. Synchronous and goroutine-safe.
func (b *Broker) Publish(env *Envelope) error {
	if err := env.Validate(); err != nil {
		return err
	}
	b.mu.Lock()
	if b.seen[env.ID] {
		b.mu.Unlock()
		return nil // idempotent redelivery
	}
	b.seen[env.ID] = true
	b.mu.Unlock()

	// D-class: broker-intercepted commands (SPEC §2.3 D). The command is
	// NEVER delivered to peers — the sender's reply_target gets a status
	// envelope with the outcome, and the cmd itself lands in the ring only.
	if env.Kind == KindCmd {
		return b.execCmd(env)
	}

	// room fan-out: clone per member (except sender)
	if route := env.Route(); len(route) > 5 && route[:5] == "room:" {
		room := route[5:]
		b.mu.Lock()
		members := make([]string, 0, len(b.rooms[room]))
		for m := range b.rooms[room] {
			if m != env.From {
				members = append(members, m)
			}
		}
		b.mu.Unlock()
		for _, m := range members {
			clone := env.Clone()
			clone.ID = NewID()
			clone.Room = room
			b.deliver("peer:"+m, clone)
		}
		b.journalAppend(env, "room")
		return nil
	}

	// task/ledger bookkeeping
	switch env.Kind {
	case KindTask:
		if env.AboutTaskID != "" {
			b.ledger.OpenTask(env)
			if env.Fanout != nil && b.ledger.GetFanout(env.Fanout.FanoutID) == nil {
				b.ledger.OpenFanout(env)
			}
		}
	case KindResult, KindFail:
		if env.AboutTaskID != "" {
			isFail := env.Kind == KindFail
			b.ledger.RecordResult(env.AboutTaskID, isFail)
			if env.Fanout != nil || isFanoutReply(env) {
				fid := env.FanoutID()
				if fid != "" && b.ledger.GetFanout(fid) != nil {
					if b.ledger.RecordFanoutResult(fid, env.From, isFail) {
						b.notifyWaiters("fanout:" + fid)
					}
				}
			} else {
				status := "done"
				if isFail {
					status = "failed"
				}
				b.ledger.CloseTask(env.AboutTaskID, status)
				b.notifyWaiters("task:" + env.AboutTaskID)
			}
		}
	}

	b.deliver(env.Route(), env)
	return nil
}

// FanoutID returns the fanout correlation id for result/fail envelopes.
// Workers echo the fanout id in AboutTaskID when replying to a fanout task;
// the broker records the mapping when it clones the fanout.
func (e *Envelope) FanoutID() string {
	if e.Fanout != nil {
		return e.Fanout.FanoutID
	}
	return ""
}

func isFanoutReply(env *Envelope) bool {
	return env.Fanout != nil
}

// CmdFunc executes a !{name} command; registered with RegisterCmd.
type CmdFunc func(args string) (result any, text string, err error)

// RegisterCmd installs a broker command handler (!{name} → status upstream).
func (b *Broker) RegisterCmd(name string, fn CmdFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cmds == nil {
		b.cmds = map[string]CmdFunc{}
	}
	b.cmds[name] = fn
}

func (b *Broker) execCmd(env *Envelope) error {
	m := cmdRe.FindStringSubmatch(env.Text)
	name, args := m[1], m[2]
	b.mu.RLock()
	fn := b.cmds[name]
	b.mu.RUnlock()

	status := &StatusBody{Cmd: name}
	if fn == nil {
		status.OK = false
		status.State = "error"
		status.Text = "unknown command: " + name
	} else if result, text, err := fn(args); err != nil {
		status.OK = false
		status.State = "error"
		status.Text = text
		if status.Text == "" {
			status.Text = err.Error()
		}
	} else {
		status.OK = true
		status.State = "ok"
		status.Text = text
		if result != nil {
			raw, _ := json.Marshal(result)
			status.Result = raw
		}
	}
	// ring-keep the cmd (audit) but deliver only the status upstream
	b.mu.Lock()
	b.pushRing(env)
	b.mu.Unlock()
	b.journalAppend(env, "cmd")
	out := &Envelope{
		Kind: KindStatus, From: "broker", To: Target{ReplyTarget: true},
		ReplyTarget: env.ReplyTarget, Status: status, Thread: env.Thread,
		ReplyTo: env.ID,
	}
	if env.AboutTaskID != "" {
		out.AboutTaskID = env.AboutTaskID
	}
	// status goes UPSTREAM to the sender's reply_target — never to peer:broker
	b.deliver(env.ReplyRoute(), out)
	return nil
}

func (b *Broker) deliver(route string, env *Envelope) {
	b.mu.Lock()
	b.seq[route]++
	env.Seq = b.seq[route]
	b.pushRing(env)
	b.mu.Unlock()
	b.journalAppend(env, "deliver")

	b.mu.RLock()
	subs := append([]*subscriber(nil), b.subs[route]...)
	b.mu.RUnlock()
	if len(subs) == 0 {
		b.mu.Lock()
		l := b.durable[route]
		if l == nil {
			l = list.New()
			b.durable[route] = l
		}
		l.PushBack(env)
		b.mu.Unlock()
		return
	}
	for _, s := range subs {
		s.fn(env)
	}
}

func (b *Broker) pushRing(env *Envelope) {
	b.ring.PushBack(env)
	if b.ring.Len() > b.ringMax {
		b.ring.Remove(b.ring.Front())
	}
}

// Recent returns up to n recent envelopes (newest last) for the web UI.
func (b *Broker) Recent(n int) []*Envelope {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*Envelope, 0, n)
	for e := b.ring.Back(); e != nil && len(out) < n; e = e.Prev() {
		out = append([]*Envelope{e.Value.(*Envelope)}, out...)
	}
	return out
}

// -- rooms -----------------------------------------------------------------

func (b *Broker) Join(room, handle string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rooms[room] == nil {
		b.rooms[room] = map[string]bool{}
	}
	b.rooms[room][handle] = true
}

func (b *Broker) Leave(room, handle string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.rooms[room], handle)
}

func (b *Broker) Rooms() map[string][]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := map[string][]string{}
	for r, members := range b.rooms {
		ms := make([]string, 0, len(members))
		for m := range members {
			ms = append(ms, m)
		}
		out[r] = ms
	}
	return out
}

// AgentInfo is one handle in the multiplexed directory.
type AgentInfo struct {
	Handle  string `json:"handle"`
	Live    bool   `json:"live"`    // has an attached subscriber (ws/api client)
	Parked  int    `json:"parked"`  // envelopes waiting in its durable queue
	Sent    int64  `json:"sent"`    // envelopes routed to it
}

// Agents lists every handle the broker has routed to — Hermes profiles
// (hermes:<profile>), external agents, users (user:<name>) — with liveness.
func (b *Broker) Agents() []AgentInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	seen := map[string]*AgentInfo{}
	add := func(handle string) {
		if _, ok := seen[handle]; !ok {
			seen[handle] = &AgentInfo{Handle: handle}
		}
	}
	for route := range b.seq {
		if len(route) > 5 && route[:5] == "peer:" {
			add(route[5:])
		}
	}
	for route := range b.subs {
		if len(route) > 5 && route[:5] == "peer:" {
			add(route[5:])
		}
	}
	for _, members := range b.rooms {
		for m := range members {
			add(m)
		}
	}
	out := make([]AgentInfo, 0, len(seen))
	for _, a := range seen {
		a.Live = len(b.subs["peer:"+a.Handle]) > 0
		if l := b.durable["peer:"+a.Handle]; l != nil {
			a.Parked = l.Len()
		}
		a.Sent = b.seq["peer:"+a.Handle]
		out = append(out, *a)
	}
	return out
}

// -- waiters (orchestration gather) -----------------------------------------

func (b *Broker) notifyWaiters(key string) {
	b.waitersMu.Lock()
	chans := b.waiters[key]
	delete(b.waiters, key)
	b.waitersMu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
}

// WaitFor blocks until the task/fanout key fires or the context expires.
func (b *Broker) WaitFor(ctx context.Context, key string) bool {
	ch := make(chan struct{})
	b.waitersMu.Lock()
	b.waiters[key] = append(b.waiters[key], ch)
	b.waitersMu.Unlock()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// -- journal -----------------------------------------------------------------

func (b *Broker) journalAppend(env *Envelope, tag string) {
	if b.journal == nil {
		return
	}
	b.journal.Append(tag, env)
}

// Restore replays a journal into the ring + ledger (crash recovery).
func (b *Broker) Restore() error {
	if b.journal == nil {
		return nil
	}
	envs, err := b.journal.Replay()
	if err != nil {
		return err
	}
	for _, tagged := range envs {
		env := tagged.Env
		b.mu.Lock()
		b.seen[env.ID] = true
		b.pushRing(env)
		b.mu.Unlock()
		switch env.Kind {
		case KindTask:
			if env.AboutTaskID != "" {
				b.ledger.OpenTask(env)
				if env.Fanout != nil {
					b.ledger.OpenFanout(env)
				}
			}
		case KindResult, KindFail:
			if env.AboutTaskID != "" {
				b.ledger.RecordResult(env.AboutTaskID, env.Kind == KindFail)
			}
		}
	}
	log.Printf("proto: restored %d envelopes from journal", len(envs))
	return nil
}

var _ = json.Marshal // keep json import for future persistence
var _ = time.Now
