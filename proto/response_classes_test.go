package proto

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// SPEC §2.3: agents respond with A messages, B tool calls/results, C ack/noop,
// D broker commands. D is intercepted: peers never see the cmd, only status.
func TestResponseClasses(t *testing.T) {
	b := NewBroker("")
	a := Attach(b, "AgentA")

	// A: plain message routes normally
	msgSeen := make(chan *Envelope, 1)
	b.Subscribe("peer:AgentB", "s", func(e *Envelope) {
		if e.Kind == KindMsg {
			msgSeen <- e
		}
	}, nil)
	_ = b.Publish(&Envelope{Kind: KindMsg, From: "AgentA", To: Target{Peer: "AgentB"},
		Text: "hi", ReplyTarget: &Target{Peer: "AgentA"}})
	select {
	case e := <-msgSeen:
		_ = e
	case <-time.After(time.Second):
		t.Fatal("A-class msg not routed")
	}

	// B: tool_call + tool_result route to peers, rendered as status upstream
	toolSeen := make(chan *Envelope, 2)
	b.Subscribe("peer:AgentB", "s2", func(e *Envelope) {
		if e.Kind == KindToolCall || e.Kind == KindToolResult {
			toolSeen <- e
		}
	}, nil)
	_ = b.Publish(&Envelope{Kind: KindToolCall, From: "AgentA", To: Target{Peer: "AgentB"},
		Tool:        &ToolCall{Name: "search", Args: map[string]any{"q": "x"}},
		ReplyTarget: &Target{Peer: "AgentA"}})
	_ = b.Publish(&Envelope{Kind: KindToolResult, From: "AgentA", To: Target{Peer: "AgentB"},
		Tool:        &ToolCall{Name: "search", Result: json.RawMessage(`{"hits":3}`)},
		ReplyTarget: &Target{Peer: "AgentA"}})
	for i := 0; i < 2; i++ {
		select {
		case e := <-toolSeen:
			if e.Tool == nil {
				t.Fatal("tool envelope lost Tool body")
			}
		case <-time.After(time.Second):
			t.Fatal("B-class tool envelope not routed")
		}
	}

	// C: noop ack validates as a pure signal (routed to the acknowledged work's origin)
	noop := AckEnvelope("AgentB", AckNoop, "")
	noop.ReplyTarget = &Target{Peer: "AgentA"}
	if err := noop.Validate(); err != nil {
		t.Fatalf("noop ack: %v", err)
	}
	if noop.Text != "" {
		t.Fatal("noop must be signal-only")
	}

	// D: cmd to broker — AgentB must NOT receive it; AgentA gets a status.
	cmds := make(chan *Envelope, 2)
	b.Subscribe("peer:AgentB", "s3", func(e *Envelope) {
		if e.Kind == KindCmd {
			cmds <- e
		}
	}, nil)
	statusSeen := make(chan *Envelope, 2)
	b.Subscribe("peer:AgentA", "s4", func(e *Envelope) {
		if e.Kind == KindStatus {
			statusSeen <- e
		}
	}, nil)
	st := &Target{Peer: "AgentA"}
	_ = b.Publish(&Envelope{Kind: KindCmd, From: "AgentA", To: Target{Peer: "broker"},
		Text: "!{list_agents}", ReplyTarget: st})
	select {
	case e := <-statusSeen:
		if e.Status == nil || e.Status.Cmd != "list_agents" || !e.Status.OK {
			t.Fatalf("bad status: %+v", e.Status)
		}
		if e.From != "broker" {
			t.Fatalf("status must come from broker, got %s", e.From)
		}
	case <-time.After(time.Second):
		t.Fatal("D-class status not returned to sender")
	}
	select {
	case <-cmds:
		t.Fatal("cmd leaked to a peer — must be broker-intercepted")
	case <-time.After(100 * time.Millisecond):
	}

	// unknown cmd → error status
	_ = b.Publish(&Envelope{Kind: KindCmd, From: "AgentA", To: Target{Peer: "broker"},
		Text: "!{bogus_cmd}", ReplyTarget: st})
	select {
	case e := <-statusSeen:
		if e.Status.OK || e.Status.State != "error" {
			t.Fatalf("unknown cmd should error, got %+v", e.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("no error status for unknown cmd")
	}

	// bad cmd shapes rejected at validation
	for _, bad := range []string{"!{UPPER}", "hello", "!{} x"} {
		env := &Envelope{Kind: KindCmd, From: "AgentA", To: Target{Peer: "broker"},
			Text: bad, ReplyTarget: st}
		if err := env.Validate(); err == nil {
			t.Fatalf("accepted bad cmd %q", bad)
		}
	}

	// delegate still works alongside (sanity of the whole loop)
	b.Subscribe("peer:AgentC", "s5", func(e *Envelope) {
		if e.Kind == KindTask {
			res, _ := json.Marshal("ok")
			_ = b.Publish(&Envelope{Kind: KindResult, From: "AgentC",
				To: Target{ReplyTarget: true}, ReplyTarget: e.ReplyTarget,
				AboutTaskID: e.AboutTaskID, Result: res})
		}
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, fail, err := a.Delegate(ctx, "AgentC", "x", nil, nil)
	if err != nil || fail != nil {
		t.Fatalf("delegate broke: %v %v", err, fail)
	}
}
