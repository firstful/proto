// Package proto implements the PROTO agent communication protocol (see SPEC.md).
package proto

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// Kind is the envelope discriminator.
type Kind string

const (
	KindMsg    Kind = "msg"
	KindTask   Kind = "task"
	KindResult Kind = "result"
	KindFail   Kind = "fail"
	KindAck    Kind = "ack"
	KindInvite Kind = "invite"
	KindLeave  Kind = "leave"
	// Response classes (SPEC §2.3): an agent may answer with any of these.
	KindToolCall   Kind = "tool_call"   // B: tool invocation, shown as status
	KindToolResult Kind = "tool_result" // B: tool outcome, shown as status
	KindCmd        Kind = "cmd"         // D: broker executes; peers never see it
	KindStatus     Kind = "status"      // broker/tool status visible upstream
)

// AckType is the explicit signal type (RFC-2119 semantics, see SPEC §2.2).
type AckType string

const (
	AckReceived AckType = "received"
	AckRead     AckType = "read"
	AckAccepted AckType = "accepted"
	AckRejected AckType = "rejected"
	AckDone     AckType = "done"
	AckFailed   AckType = "failed"
	AckNoop     AckType = "noop" // C: alive, nothing to do — pure signal
)

// Barrier is a fanout gather policy.
type Barrier string

const (
	BarrierAll      Barrier = "all"
	BarrierFirst    Barrier = "first"
	BarrierMajority Barrier = "majority"
)

const MaxEnvelopeBytes = 128 * 1024

var (
	handleRe = regexp.MustCompile(`^[A-Za-z0-9_.\-:]{1,64}$`)
	routeRe  = regexp.MustCompile(`^(peer|room|user):[A-Za-z0-9_.@:\-]{1,128}$`)
	// D-class commands: !{list_users}, !{room_stats} crew, … (SPEC §2.3 D)
	cmdRe = regexp.MustCompile(`^!\{([a-z_][a-z0-9_]*)\}\s*(.*)$`)
)

// ProtoError is a protocol validation error.
type ProtoError struct {
	Msg   string `json:"msg"`
	Field string `json:"field,omitempty"`
}

func (e *ProtoError) Error() string { return "proto: " + e.Msg }

func perr(format string, args ...any) error {
	return &ProtoError{Msg: fmt.Sprintf(format, args...)}
}

func perrField(field, format string, args ...any) error {
	return &ProtoError{Field: field, Msg: fmt.Sprintf(format, args...)}
}

// NewID returns a random 16-byte hex id (GUID-shaped, no deps).
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(b)
}

// Now returns an RFC3339 UTC timestamp in milliseconds.
func Now() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00") }

// Target is a delivery selector: exactly one of Peer/Room must be set.
type Target struct {
	Peer string `json:"peer,omitempty"`
	Room string `json:"room,omitempty"`
	// ReplyTarget=true means "deliver to my reply_target" (loops).
	ReplyTarget bool `json:"reply_target,omitempty"`
}

func (t Target) String() string {
	switch {
	case t.Peer != "":
		return "peer:" + t.Peer
	case t.Room != "":
		return "room:" + t.Room
	default:
		return "reply_target"
	}
}

// Task is a unit of work request.
type Task struct {
	Action string         `json:"action"`
	Params map[string]any `json:"params,omitempty"`
}

// Failure is a structured error.
type Failure struct {
	Code string `json:"code"`
	Msg  string `json:"msg,omitempty"`
}

// Ack is an explicit signal — never prose.
type Ack struct {
	Type      AckType `json:"type"`
	Nonce     string  `json:"nonce,omitempty"`
	TaskID    string  `json:"task_id,omitempty"`
	Reason    string  `json:"reason,omitempty"`
	FanoutID  string  `json:"fanout_id,omitempty"`
	Timestamp string  `json:"ts,omitempty"`
}

// Fanout describes a parallel fan-out plan with a gather barrier.
type Fanout struct {
	FanoutID string   `json:"fanout_id"`
	Plan     []Target `json:"plan"`
	Barrier  Barrier  `json:"barrier,omitempty"` // all (default) | first | majority
	Timeout  int      `json:"timeout_ms,omitempty"`
}

// ToolCall is a B-class response: a tool invocation or its outcome,
// surfaced upstream as a status line (never as conversation content).
type ToolCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args,omitempty"`
	// Result carries the outcome on KindToolResult envelopes.
	Result json.RawMessage `json:"result,omitempty"`
	Err    string          `json:"err,omitempty"`
}

// StatusBody is a D-class response produced by the BROKER for a cmd.
// Upstream observers see only this — never the raw command mechanics.
type StatusBody struct {
	Cmd    string          `json:"cmd,omitempty"` // e.g. "list_users"
	OK     bool            `json:"ok"`
	State  string          `json:"state,omitempty"`  // running | ok | error
	Text   string          `json:"text,omitempty"`   // human/agent-readable line
	Result json.RawMessage `json:"result,omitempty"` // structured payload
}

// Envelope is THE protocol object (SPEC §2).
type Envelope struct {
	V           int      `json:"v"`
	Kind        Kind     `json:"kind"`
	ID          string   `json:"id"`
	TS          string   `json:"ts"`
	From        string   `json:"from"`
	To          Target   `json:"to"`
	ReplyTarget *Target  `json:"reply_target,omitempty"` // REQUIRED unless kind=ack
	Thread      string   `json:"thread,omitempty"`
	ReplyTo     string   `json:"reply_to,omitempty"`
	ParentTask  string   `json:"parent_task,omitempty"`
	OnBehalfOf  []string `json:"on_behalf_of,omitempty"` // delegation chain

	Text   string          `json:"text,omitempty"`
	Ack    *Ack            `json:"ack,omitempty"`
	Task   *Task           `json:"task,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Fail   *Failure        `json:"fail,omitempty"`
	Fanout *Fanout         `json:"fanout,omitempty"`
	Tool   *ToolCall       `json:"tool,omitempty"`   // kind=tool_call|tool_result
	Status *StatusBody     `json:"status,omitempty"` // kind=status

	// AboutTaskID is the task lifecycle join key. Accepts BOTH wire shapes:
	// "about":"<id>" and "about":{"task_id":"<id>"} (SPEC §2 object form).
	AboutTaskID string `json:"about,omitempty"`

	// broker-stamped
	Seq  int64  `json:"seq,omitempty"`
	Room string `json:"room,omitempty"` // stamped on room delivery
}

// UnmarshalJSON tolerates about as string | {task_id} object.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	type alias Envelope
	aux := &struct {
		About       json.RawMessage `json:"about,omitempty"`
		AboutTaskID string          `json:"-"`
		*alias
	}{alias: (*alias)(e)}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if len(aux.About) > 0 {
		var s string
		if err := json.Unmarshal(aux.About, &s); err == nil {
			e.AboutTaskID = s
		} else {
			var obj struct {
				TaskID string `json:"task_id"`
			}
			if err := json.Unmarshal(aux.About, &obj); err != nil {
				return perr("about must be a task id string or {task_id}")
			}
			e.AboutTaskID = obj.TaskID
		}
	}
	return nil
}

// MarshalJSON always writes the SPEC §2 object form: "about":{"task_id":...}.
func (e *Envelope) MarshalJSON() ([]byte, error) {
	type alias Envelope
	aux := struct {
		About       json.RawMessage `json:"about,omitempty"`
		AboutTaskID string          `json:"-"`
		*alias
	}{alias: (*alias)(e)}
	if e.AboutTaskID != "" {
		b, err := json.Marshal(map[string]string{"task_id": e.AboutTaskID})
		if err != nil {
			return nil, err
		}
		aux.About = b
	}
	return json.Marshal(aux)
}

// Route returns the canonical delivery route key for the envelope's To.
func (e *Envelope) Route() string {
	if e.To.ReplyTarget {
		if e.ReplyTarget == nil {
			return ""
		}
		return e.ReplyTarget.String()
	}
	return e.To.String()
}

// ReplyRoute returns the route for responses to this envelope.
func (e *Envelope) ReplyRoute() string {
	if e.ReplyTarget == nil {
		return ""
	}
	return e.ReplyTarget.String()
}

// Validate enforces the wire rules (SPEC §2.1, §2.3).
func (e *Envelope) Validate() error {
	switch e.Kind {
	case KindMsg, KindTask, KindResult, KindFail, KindAck, KindInvite, KindLeave,
		KindToolCall, KindToolResult, KindCmd, KindStatus:
	default:
		return perr("bad kind %q", e.Kind)
	}
	if !handleRe.MatchString(e.From) {
		return perr("bad from handle %q", e.From)
	}
	if e.V == 0 {
		e.V = 1
	}
	if e.ID == "" {
		e.ID = NewID()
	}
	if e.TS == "" {
		e.TS = Now()
	}
	route := e.Route()
	if route == "" || !routeRe.MatchString(route) {
		return perr("bad route %q", route)
	}
	if e.Kind != KindAck && e.ReplyTarget == nil {
		return perr("kind=%s requires reply_target", e.Kind)
	}
	if e.Kind == KindAck {
		if e.Ack == nil || e.Ack.Type == "" {
			return perr("ack requires ack.type")
		}
		switch e.Ack.Type {
		case AckReceived, AckRead, AckAccepted, AckRejected, AckDone, AckFailed, AckNoop:
		default:
			return perr("bad ack.type %q", e.Ack.Type)
		}
		if e.Text != "" {
			return perr("pure acks carry no text (signal ≠ content)")
		}
	}
	if e.Kind == KindTask && (e.Task == nil || e.Task.Action == "") {
		return perr("task requires task.action")
	}
	if e.Kind == KindFail && e.Fail == nil {
		return perr("fail requires fail{code,msg}")
	}
	if e.Kind == KindResult && len(e.Result) == 0 {
		return perr("result requires result payload")
	}
	// B-class: tool envelopes carry a tool body
	if (e.Kind == KindToolCall || e.Kind == KindToolResult) && e.Tool == nil {
		return perr("kind=%s requires tool{name,...}", e.Kind)
	}
	if e.Kind == KindToolCall && e.Tool.Name == "" {
		return perr("tool_call requires tool.name")
	}
	// D-class: cmds are !{name} text addressed to the broker pseudo-agent
	if e.Kind == KindCmd {
		if e.Text == "" || !cmdRe.MatchString(e.Text) {
			return perr("cmd requires text '!{name} ...'")
		}
		if e.To.Peer != "broker" && e.To.Room == "" {
			return perr("cmd must address peer:broker (or a room)")
		}
	}
	// status envelopes carry a status body produced by the broker or a tool
	if e.Kind == KindStatus && e.Status == nil {
		return perr("status requires status{cmd,ok,text,...}")
	}
	b, _ := json.Marshal(e)
	if len(b) > MaxEnvelopeBytes {
		return perr("envelope > %d bytes; use an object store reference", MaxEnvelopeBytes)
	}
	return nil
}

// Clone deep-ish copies the envelope (per-member room delivery).
func (e *Envelope) Clone() *Envelope {
	c := *e
	if e.OnBehalfOf != nil {
		c.OnBehalfOf = append([]string(nil), e.OnBehalfOf...)
	}
	if e.Fanout != nil {
		f := *e.Fanout
		c.Fanout = &f
	}
	return &c
}
