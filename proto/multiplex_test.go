package proto

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// SPEC §12: A→B→C nested delegates roll up under one root task tree.
func TestNestedDelegateTaskTree(t *testing.T) {
	b := NewBroker("")
	a := Attach(b, "hermes:rook")

	// B services tasks by delegating further to C (nested), then reports.
	b.Subscribe("peer:hermes:boris", "w-b", func(env *Envelope) {
		if env.Kind != KindTask {
			return
		}
		if env.ParentTask == "" {
			t.Errorf("B's task missing parent_task (root linkage)")
		}
		// B delegates to C, nesting under ITS parent (the root)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, fail, err := Attach(b, "hermes:boris").Delegate(ctx, "Worker-3", "sub_step",
				nil, []string{"user:reed", "hermes:rook", "hermes:boris"}, env.ParentTask)
			if err != nil || fail != nil {
				t.Errorf("B→C delegate: err=%v fail=%v", err, fail)
				return
			}
			res, _ := json.Marshal("B+C done")
			_ = b.Publish(&Envelope{Kind: KindResult, From: "hermes:boris",
				To: Target{ReplyTarget: true}, ReplyTarget: env.ReplyTarget,
				AboutTaskID: env.AboutTaskID, Result: res})
		}()
	}, nil)

	b.Subscribe("peer:Worker-3", "w-c", func(env *Envelope) {
		if env.Kind != KindTask {
			return
		}
		if env.ParentTask == "" {
			t.Errorf("C's task missing parent_task")
		}
		res, _ := json.Marshal("C sub-step ok")
		_ = b.Publish(&Envelope{Kind: KindResult, From: "Worker-3",
			To: Target{ReplyTarget: true}, ReplyTarget: env.ReplyTarget,
			AboutTaskID: env.AboutTaskID, Result: res})
	}, nil)

	// A (rook) services the user's root task by delegating to B (nested).
	// Simulate: root task opened first, then A delegates with parent=root.
	rootID := "T-root"
	_ = b.Publish(&Envelope{Kind: KindTask, From: "user:reed", To: Target{Peer: "hermes:rook"},
		AboutTaskID: rootID, Task: &Task{Action: "do_big_thing"},
		ReplyTarget: &Target{Peer: "user:reed"}})
	drainClient(t, a)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, fail, err := a.Delegate(ctx, "hermes:boris", "do_part", nil,
		[]string{"user:reed", "hermes:rook"}, rootID)
	if err != nil || fail != nil {
		t.Fatalf("A→B: err=%v fail=%v", err, fail)
	}

	// A composes and closes the root task (result back to the user)
	_ = b.Publish(&Envelope{Kind: KindResult, From: "hermes:rook",
		To: Target{ReplyTarget: true}, ReplyTarget: &Target{Peer: "user:reed"},
		AboutTaskID: rootID, Result: mustJSON("big thing done")})

	// tree: root → A→B task → B→C task (C is a leaf worker), all under T-root
	tree := b.Ledger().Tree(rootID)
	if tree == nil {
		t.Fatal("no tree for root")
	}
	if tree.Total != 3 {
		t.Fatalf("tree total = %d, want 3 (root+2 nested): %+v", tree.Total, tree.Descendant)
	}
	if tree.Status != "done" {
		t.Fatalf("tree status = %s (open=%d failed=%d)", tree.Status, tree.Open, tree.Failed)
	}
	for _, d := range tree.Descendant {
		if d.RootTask != rootID {
			t.Errorf("descendant %s root=%s, want %s", d.TaskID, d.RootTask, rootID)
		}
	}
}

// SPEC §11: profile multiplexing — separate inboxes, no cross-talk.
func TestProfileMultiplexing(t *testing.T) {
	b := NewBroker("")
	rook := Attach(b, "hermes:rook")
	reed := Attach(b, "hermes:reed")

	// message to hermes:rook must NOT land in hermes:reed's inbox
	_ = b.Publish(&Envelope{Kind: KindMsg, From: "user:reed",
		To: Target{Peer: "hermes:rook"}, Text: "for rook only",
		ReplyTarget: &Target{Peer: "user:reed"}})
	time.Sleep(50 * time.Millisecond) // delivery is synchronous; small settle
	if got := rook.Inbox(); len(got) != 1 || got[0].Text != "for rook only" {
		t.Fatalf("rook inbox = %+v", got)
	}
	if got := reed.Inbox(); len(got) != 0 {
		t.Fatalf("cross-talk! reed got %+v", got)
	}

	// directory shows both profiles
	agents := b.Agents()
	found := map[string]bool{}
	for _, a := range agents {
		found[a.Handle] = true
	}
	if !found["hermes:rook"] || !found["hermes:reed"] {
		t.Fatalf("agents dir missing profiles: %+v", agents)
	}
}

// parked mail survives until the profile reattaches.
func TestProfileParkedMailFlushOnReattach(t *testing.T) {
	b := NewBroker("")
	_ = b.Publish(&Envelope{Kind: KindMsg, From: "user:reed",
		To: Target{Peer: "hermes:offline"}, Text: "while you were away",
		ReplyTarget: &Target{Peer: "user:reed"}})
	// no subscriber yet — parked
	got := make(chan *Envelope, 1)
	b.Subscribe("peer:hermes:offline", "reattach", func(e *Envelope) { got <- e }, nil)
	select {
	case e := <-got:
		if e.Text != "while you were away" {
			t.Fatal("parked mail corrupted")
		}
	case <-time.After(time.Second):
		t.Fatal("parked mail not flushed on reattach")
	}
}

func drainClient(t *testing.T, c *Client) {
	t.Helper()
	_ = c.Inbox()
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
