package proto

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Client is a PROTO endpoint: one handle, an inbox, and orchestration
// primitives (delegate / fanout / gather). It subscribes itself to the broker.
type Client struct {
	Broker *Broker
	Handle string
	route  string

	mu    sync.Mutex
	inbox []*Envelope

	autoAck bool // auto-send ack(received) for tasks
}

// Attach creates a client bound to a broker route and subscribes it.
func Attach(b *Broker, handle string) *Client {
	c := &Client{Broker: b, Handle: handle, route: "peer:" + handle}
	b.Subscribe(c.route, c.Handle, c.onEnvelope, nil)
	return c
}

func (c *Client) onEnvelope(env *Envelope) {
	c.mu.Lock()
	c.inbox = append(c.inbox, env)
	c.mu.Unlock()
	if c.autoAck && env.Kind == KindTask && env.ReplyTarget != nil {
		ack := AckEnvelope(c.Handle, AckReceived, env.AboutTaskID)
		ack.ReplyTarget = env.ReplyTarget
		_ = c.Broker.Publish(ack)
	}
}

// Inbox returns and clears pending envelopes.
func (c *Client) Inbox() []*Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.inbox
	c.inbox = nil
	return out
}

// waitForEnvelope polls the inbox every d until pred matches.
func (c *Client) waitForEnvelope(ctx context.Context, pred func(*Envelope) bool) (*Envelope, error) {
	for {
		c.mu.Lock()
		for i, e := range c.inbox {
			if pred(e) {
				c.inbox = append(c.inbox[:i], c.inbox[i+1:]...)
				c.mu.Unlock()
				return e, nil
			}
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(15 * time.Millisecond):
		}
	}
}

// -- builders ----------------------------------------------------------------

// Msg sends a message to a target.
func (c *Client) Msg(to Target, text string, rt Target) error {
	return c.Broker.Publish(&Envelope{
		Kind: KindMsg, From: c.Handle, To: to, Text: text, Thread: threadOf(text),
		ReplyTarget: &rt,
	})
}

// Task sends a task envelope. Returns the task id.
func (c *Client) Task(to Target, action string, params map[string]any, rt Target, opt ...EnvelopeOpt) (string, error) {
	env := &Envelope{
		Kind: KindTask, From: c.Handle, To: to,
		AboutTaskID: NewID(),
		Task:        &Task{Action: action, Params: params},
		ReplyTarget: &rt,
	}
	for _, o := range opt {
		o(env)
	}
	return env.AboutTaskID, c.Broker.Publish(env)
}

// EnvelopeOpt customises a task before publishing.
type EnvelopeOpt func(*Envelope)

// WithOBO sets the on_behalf_of delegation chain.
func WithOBO(chain ...string) EnvelopeOpt {
	return func(e *Envelope) { e.OnBehalfOf = chain }
}

// WithThread sets a thread id.
func WithThread(id string) EnvelopeOpt { return func(e *Envelope) { e.Thread = id } }

// WithParentTask links a fanout to its orchestrating parent task.
func WithParentTask(id string) EnvelopeOpt { return func(e *Envelope) { e.ParentTask = id } }

// Ack sends an ack envelope to a peer (or ack-of-work with taskID).
func (c *Client) Ack(to Target, t AckType, taskID, nonce string) error {
	env := AckEnvelope(c.Handle, t, taskID)
	if nonce != "" {
		env.Ack.Nonce = nonce
	}
	env.To = to
	return c.Broker.Publish(env)
}

// AckEnvelope builds a signal-only ack (routed back to the reply_target of
// the work it acknowledges; set env.To explicitly for direct acks).
func AckEnvelope(from string, t AckType, taskID string) *Envelope {
	ack := &Ack{Type: t, TaskID: taskID, Nonce: NewID()[:8]}
	env := &Envelope{Kind: KindAck, From: from, To: Target{ReplyTarget: true}, Ack: ack}
	if taskID != "" {
		env.AboutTaskID = taskID
	}
	return env
}

// threadOf mints a thread id for fresh conversations (SPEC §3: threads are cheap).
func threadOf(_ string) string { return NewID()[:12] }

// -- orchestration ------------------------------------------------------------

// Delegate delegates a task to a peer and BLOCKS for their result/fail.
// Reports come back through reply_target; OBO chains provenance (SPEC §5).
// Pass parentTask (from the task envelope you're servicing) to nest under the
// same root: A→B→C chains all roll up into one tracked tree.
func (c *Client) Delegate(ctx context.Context, peer, action string,
	params map[string]any, obo []string, parentTask ...string) (result []byte, failed *Failure, err error) {

	rt := Target{Peer: c.Handle}
	var opts []EnvelopeOpt
	if len(obo) > 0 {
		opts = append(opts, WithOBO(obo...))
	}
	if len(parentTask) > 0 && parentTask[0] != "" {
		opts = append(opts, WithParentTask(parentTask[0]))
	}
	taskID, err := c.Task(Target{Peer: peer}, action, params, rt, opts...)
	if err != nil {
		return nil, nil, err
	}
	env, err := c.waitForEnvelope(ctx, func(e *Envelope) bool {
		return e.AboutTaskID == taskID && (e.Kind == KindResult || e.Kind == KindFail)
	})
	if err != nil {
		return nil, nil, fmt.Errorf("delegate %s→%s: %w", action, peer, err)
	}
	if env.Kind == KindFail {
		return nil, env.Fail, nil
	}
	return env.Result, nil, nil
}

// Fanout fans a task out to N peers in parallel (SPEC §6.1). Returns
// result/fail envelopes per peer once the barrier is met.
func (c *Client) Fanout(ctx context.Context, peers []string, action string,
	params map[string]any, barrier Barrier, obo []string) (map[string]*Envelope, error) {

	fanoutID := NewID()[:12]
	rt := Target{Peer: c.Handle}
	plan := make([]Target, len(peers))
	for i, p := range peers {
		plan[i] = Target{Peer: p}
	}
	// Every clone carries the same fanout block so replies correlate.
	for _, p := range peers {
		env := &Envelope{
			Kind: KindTask, From: c.Handle, To: Target{Peer: p},
			AboutTaskID: NewID(),
			Task:        &Task{Action: action, Params: params},
			ReplyTarget: &rt,
			OnBehalfOf:  obo,
			Fanout:      &Fanout{FanoutID: fanoutID, Plan: plan, Barrier: barrier},
		}
		if err := c.Broker.Publish(env); err != nil {
			return nil, err
		}
	}
	need := len(peers) // all
	switch barrier {
	case BarrierFirst:
		need = 1
	case BarrierMajority:
		need = len(peers)/2 + 1
	}
	got := map[string]*Envelope{}
	// ledger close, so also use our own waiter for the barrier
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx2 := ctx
		if dl, ok := ctx.Deadline(); !ok || dl.After(deadline) {
			var cancel context.CancelFunc
			ctx2, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		env, err := c.waitForEnvelope(ctx2, func(e *Envelope) bool {
			return e.Kind == KindResult || e.Kind == KindFail
		})
		if err != nil {
			if len(got) > 0 {
				return got, nil // partial
			}
			return got, err
		}
		got[env.From] = env
		if len(got) >= need {
			return got, nil
		}
	}
}

// WaitTask blocks until the task's result/fail lands in the ledger.
func (c *Client) WaitTask(ctx context.Context, taskID string) *TaskRecord {
	ok := c.Broker.WaitFor(ctx, "task:"+taskID)
	if !ok {
		return nil
	}
	return c.Broker.Ledger().GetTask(taskID)
}

// WaitFanout blocks until the fanout barrier is met (or ctx expires).
func (c *Client) WaitFanout(ctx context.Context, fanoutID string) *FanoutRecord {
	ok := c.Broker.WaitFor(ctx, "fanout:"+fanoutID)
	if !ok {
		return nil
	}
	return c.Broker.Ledger().GetFanout(fanoutID)
}
