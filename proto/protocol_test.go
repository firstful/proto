package proto

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func mustPublish(t *testing.T, b *Broker, env *Envelope) {
	t.Helper()
	if err := b.Publish(env); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestEnvelopeValidation(t *testing.T) {
	// reply_target required for non-ack
	env := &Envelope{Kind: KindMsg, From: "AgentA", To: Target{Peer: "AgentB"}, Text: "hi"}
	if err := env.Validate(); err == nil {
		t.Fatal("msg without reply_target must fail")
	}
	env.ReplyTarget = &Target{Peer: "user:reed"}
	if err := env.Validate(); err != nil {
		t.Fatalf("valid msg: %v", err)
	}
	// user:reed style handles route fine
	if env.Route() != "peer:AgentB" {
		t.Fatalf("route = %q", env.Route())
	}
	// acks are signal-only
	ack := &Envelope{Kind: KindAck, From: "B", To: Target{Peer: "A"},
		Ack: &Ack{Type: AckAccepted}, Text: "nope"}
	if err := ack.Validate(); err == nil {
		t.Fatal("ack with text must fail")
	}
	ack.Text = ""
	if err := ack.Validate(); err != nil {
		t.Fatalf("valid ack: %v", err)
	}
	// bad ack type
	ack.Ack.Type = "maybe"
	if err := ack.Validate(); err == nil {
		t.Fatal("bad ack.type must fail")
	}
	// bad kind
	if err := (&Envelope{Kind: "nope", From: "A", To: Target{Peer: "B"}}).Validate(); err == nil {
		t.Fatal("bad kind must fail")
	}
	// serde roundtrip
	env.AboutTaskID = "t1"
	env.OnBehalfOf = []string{"user:reed", "AgentA"}
	b, _ := json.Marshal(env)
	var env2 Envelope
	if err := json.Unmarshal(b, &env2); err != nil {
		t.Fatal(err)
	}
	if env2.AboutTaskID != "t1" || len(env2.OnBehalfOf) != 2 {
		t.Fatal("roundtrip lost fields")
	}
}

func TestDMRoutingAndSeq(t *testing.T) {
	b := NewBroker("")
	got := make(chan *Envelope, 1)
	b.Subscribe("peer:AgentB", "t", func(e *Envelope) { got <- e }, nil)
	mustPublish(t, b, &Envelope{Kind: KindMsg, From: "AgentA", To: Target{Peer: "AgentB"},
		Text: "hello", ReplyTarget: &Target{Peer: "user:reed"}})
	select {
	case e := <-got:
		if e.Seq != 1 || e.From != "AgentA" {
			t.Fatalf("seq=%d from=%s", e.Seq, e.From)
		}
	case <-time.After(time.Second):
		t.Fatal("no delivery")
	}
}

func TestDurableParking(t *testing.T) {
	b := NewBroker("")
	mustPublish(t, b, &Envelope{Kind: KindMsg, From: "AgentA", To: Target{Peer: "sleepy"},
		Text: "wake up", ReplyTarget: &Target{Peer: "AgentA"}})
	got := make(chan *Envelope, 4)
	b.Subscribe("peer:sleepy", "t", func(e *Envelope) { got <- e }, nil)
	select {
	case e := <-got:
		if e.Text != "wake up" {
			t.Fatal("parked envelope lost")
		}
	case <-time.After(time.Second):
		t.Fatal("parked envelope not flushed")
	}
}

func TestRoomFanoutExcludesSender(t *testing.T) {
	b := NewBroker("")
	b.Join("crew", "AgentA")
	b.Join("crew", "AgentB")
	b.Join("crew", "AgentC")
	gotB := make(chan *Envelope, 1)
	gotC := make(chan *Envelope, 1)
	b.Subscribe("peer:AgentB", "t", func(e *Envelope) { gotB <- e }, nil)
	b.Subscribe("peer:AgentC", "t", func(e *Envelope) { gotC <- e }, nil)
	mustPublish(t, b, &Envelope{Kind: KindMsg, From: "AgentA", To: Target{Room: "crew"},
		Text: "all hands", ReplyTarget: &Target{Peer: "AgentA"}})
	eb, ec := <-gotB, <-gotC
	if eb.Room != "crew" || eb.ID == ec.ID {
		t.Fatal("room clones bad")
	}
}

func TestIdempotentRedelivery(t *testing.T) {
	b := NewBroker("")
	got := make(chan *Envelope, 2)
	b.Subscribe("peer:B", "t", func(e *Envelope) { got <- e }, nil)
	env := &Envelope{Kind: KindMsg, From: "A", To: Target{Peer: "B"},
		Text: "once", ReplyTarget: &Target{Peer: "A"}}
	mustPublish(t, b, env)
	mustPublish(t, b, env)
	if len(got) != 1 {
		t.Fatalf("dedup failed: %d deliveries", len(got))
	}
}

// SPEC §10 walk 1: user → AgentA → AgentB → report back.
func TestDelegateAndReport(t *testing.T) {
	b := NewBroker("")
	user := Attach(b, "user:reed")
	a := Attach(b, "AgentA")

	// AgentB worker loop: on task, ack accepted then result
	done := make(chan struct{})
	err := b.Publish(&Envelope{Kind: KindTask, From: user.Handle, To: Target{Peer: "AgentA"},
		AboutTaskID: "T1", Task: &Task{Action: "delegate_X"},
		ReplyTarget: &Target{Peer: "user:reed"}})
	if err != nil {
		t.Fatal(err)
	}

	// AgentA receives the task, delegates onward on behalf of the user
	inbound := <-waitInbox(t, a)
	if inbound.Kind != KindTask {
		t.Fatalf("AgentA got %s", inbound.Kind)
	}
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		result, fail, err := a.Delegate(ctx, "AgentB", "do_X", nil,
			[]string{"user:reed"})
		if err != nil || fail != nil {
			t.Errorf("delegate failed: err=%v fail=%v", err, fail)
			return
		}
		var summary string
		_ = json.Unmarshal(result, &summary)
		if summary != "done" {
			t.Errorf("summary = %q", summary)
		}
		// AgentA reports back to the user
		res, _ := json.Marshal(map[string]string{"summary": "AgentB says: done"})
		mustPublish(t, b, &Envelope{Kind: KindResult, From: a.Handle,
			To: Target{ReplyTarget: true}, ReplyTarget: inbound.ReplyTarget,
			AboutTaskID: inbound.AboutTaskID, Result: res})
	}()

	// AgentB: receive the delegated task (with auto-ack), reply with result
	// (a real peer would be another Client; simulate its inbox via broker sub)
	b.Subscribe("peer:AgentB", "worker", func(env *Envelope) {
		if env.Kind != KindTask {
			return
		}
		if env.OnBehalfOf == nil || env.OnBehalfOf[0] != "user:reed" {
			t.Errorf("OBO chain missing: %v", env.OnBehalfOf)
		}
		if env.ReplyTarget == nil || env.ReplyTarget.Peer != "AgentA" {
			t.Errorf("reply_target must be AgentA, got %v", env.ReplyTarget)
		}
		res, _ := json.Marshal("done")
		_ = b.Publish(&Envelope{Kind: KindResult, From: "AgentB",
			To: Target{ReplyTarget: true}, ReplyTarget: env.ReplyTarget,
			AboutTaskID: env.AboutTaskID, Result: res})
	}, nil)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("walk 1 timed out")
	}
	// user got the final result
	final := <-waitInboxPred(t, user, func(e *Envelope) bool {
		return e.Kind == KindResult && e.From == "AgentA"
	})
	var m map[string]string
	_ = json.Unmarshal(final.Result, &m)
	if m["summary"] != "AgentB says: done" {
		t.Fatalf("user final = %v", m)
	}
	// ledger closed T1
	if rec := b.Ledger().GetTask("T1"); rec == nil || rec.Status != "done" {
		t.Fatalf("T1 ledger: %+v", rec)
	}
}

// SPEC §10 walk 2: user → AgentA → 3 workers parallel → gather → one reply.
func TestFanoutGather(t *testing.T) {
	b := NewBroker("")
	user := Attach(b, "user:reed")
	a := Attach(b, "AgentA")

	// 3 workers
	workerDone := make(chan string, 3)
	for _, w := range []string{"Worker-1", "Worker-2", "Worker-3"} {
		w := w
		b.Subscribe("peer:"+w, "worker", func(env *Envelope) {
			if env.Kind != KindTask {
				return
			}
			if env.Fanout == nil || env.Fanout.FanoutID == "" {
				t.Errorf("worker %s missing fanout correlation", w)
			}
			res, _ := json.Marshal(map[string]string{"worker": w, "answer": "paper"})
			_ = b.Publish(&Envelope{Kind: KindResult, From: w,
				To: Target{ReplyTarget: true}, ReplyTarget: env.ReplyTarget,
				AboutTaskID: env.AboutTaskID, Result: res})
			workerDone <- w
		}, nil)
	}

	// user asks AgentA (T0), reply_target = user
	err := b.Publish(&Envelope{Kind: KindTask, From: user.Handle,
		To: Target{Peer: "AgentA"}, AboutTaskID: "T0",
		Task: &Task{Action: "deep_research"},
		ReplyTarget: &Target{Peer: "user:reed"}})
	if err != nil {
		t.Fatal(err)
	}
	inbound := <-waitInbox(t, a)

	// AgentA fans out to 3 workers, gather all, in a goroutine
	gathered := make(chan map[string]*Envelope, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		got, err := a.Fanout(ctx, []string{"Worker-1", "Worker-2", "Worker-3"},
			"gather_papers", nil, BarrierAll, []string{"user:reed"})
		if err != nil {
			t.Errorf("fanout: %v", err)
		}
		gathered <- got
	}()

	got := <-gathered
	if len(got) != 3 {
		t.Fatalf("gathered %d results, want 3", len(got))
	}
	for peer, e := range got {
		if e.Kind != KindResult {
			t.Fatalf("%s returned %s", peer, e.Kind)
		}
	}
	// workers replied to AgentA, not the user
	_ = workerDone
	for _, e := range user.Inbox() {
		if e.From == "Worker-1" || e.From == "Worker-2" || e.From == "Worker-3" {
			t.Fatal("worker result leaked to user")
		}
	}
	// AgentA composes the single synthesis back to the user
	res, _ := json.Marshal(map[string]string{"synthesis": "3 workers reported"})
	_ = b.Publish(&Envelope{Kind: KindResult, From: a.Handle, To: Target{ReplyTarget: true},
		ReplyTarget: inbound.ReplyTarget, AboutTaskID: inbound.AboutTaskID, Result: res})
	final := <-waitInboxPred(t, user, func(e *Envelope) bool { return e.Kind == KindResult })
	var m map[string]string
	_ = json.Unmarshal(final.Result, &m)
	if m["synthesis"] != "3 workers reported" {
		t.Fatalf("final synthesis = %v", m)
	}
}

func TestBarrierFirst(t *testing.T) {
	b := NewBroker("")
	a := Attach(b, "AgentA")
	b.Subscribe("peer:Fast", "w", func(env *Envelope) {
		if env.Kind != KindTask {
			return
		}
		res, _ := json.Marshal("fast")
		_ = b.Publish(&Envelope{Kind: KindResult, From: "Fast",
			To: Target{ReplyTarget: true}, ReplyTarget: env.ReplyTarget,
			AboutTaskID: env.AboutTaskID, Result: res})
	}, nil)
	go func() {
		time.Sleep(200 * time.Millisecond)
		// Slow never replies — barrier first must not wait
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := a.Fanout(ctx, []string{"Fast", "Slow"}, "race", nil, BarrierFirst, nil)
	if err != nil {
		t.Fatalf("fanout first: %v", err)
	}
	if _, ok := got["Fast"]; !ok {
		t.Fatalf("expected Fast, got %v", got)
	}
	if _, ok := got["Slow"]; ok {
		t.Fatal("barrier=first should not include Slow")
	}
}

func TestFailEnvelopeAndLedger(t *testing.T) {
	b := NewBroker("")
	a := Attach(b, "AgentA")
	b.Subscribe("peer:AgentB", "w", func(env *Envelope) {
		if env.Kind != KindTask {
			return
		}
		_ = b.Publish(&Envelope{Kind: KindFail, From: "AgentB",
			To: Target{ReplyTarget: true}, ReplyTarget: env.ReplyTarget,
			AboutTaskID: env.AboutTaskID, Fail: &Failure{Code: "timeout", Msg: "upstream 504"}})
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, fail, err := a.Delegate(ctx, "AgentB", "risky", nil, nil)
	if err != nil {
		t.Fatalf("delegate transport error: %v", err)
	}
	if fail == nil || fail.Code != "timeout" {
		t.Fatalf("fail = %+v", fail)
	}
}

func TestLedgerFanoutBarrier(t *testing.T) {
	l := NewLedger()
	l.OpenFanout(&Envelope{Fanout: &Fanout{FanoutID: "f1",
		Plan: []Target{{Peer: "w1"}, {Peer: "w2"}, {Peer: "w3"}}, Barrier: BarrierMajority}})
	if l.RecordFanoutResult("f1", "w1", false) {
		t.Fatal("1/3 should not close majority")
	}
	if !l.RecordFanoutResult("f1", "w2", false) {
		t.Fatal("2/3 should close majority")
	}
}

// -- helpers -----------------------------------------------------------------

func waitInbox(t *testing.T, c *Client) chan *Envelope {
	ch := make(chan *Envelope, 1)
	go func() {
		for {
			for _, e := range c.Inbox() {
				ch <- e
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return ch
}

func waitInboxPred(t *testing.T, c *Client, pred func(*Envelope) bool) chan *Envelope {
	ch := make(chan *Envelope, 1)
	go func() {
		for {
			for _, e := range c.Inbox() {
				if pred(e) {
					ch <- e
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return ch
}
