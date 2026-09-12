package proto

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Server wraps a Broker with a WebSocket agent endpoint, a REST API for thin
// clients (Hermes plugin, curl), and the web app.
type Server struct {
	Broker *Broker
	mu     sync.Mutex
	conns  map[string]*wsConn // subID -> conn
}

func NewServer(b *Broker) *Server {
	return &Server{Broker: b, conns: map[string]*wsConn{}}
}

type wsConn struct {
	ws     *websocket.Conn
	handle string
	send   chan *Envelope
}

// Handler returns the http.Handler for everything.
func (s *Server) Handler(webapp []byte) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(webapp) //nolint:errcheck
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("GET /api/log", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, s.Broker.Recent(500))
	})
	mux.HandleFunc("GET /api/rooms", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, s.Broker.Rooms())
	})
	mux.HandleFunc("GET /api/agents", func(w http.ResponseWriter, _ *http.Request) {
		// profile-multiplexing directory: every handle the broker knows,
		// with live-subscriber + parked-inbox status.
		writeJSON(w, s.Broker.Agents())
	})
	mux.HandleFunc("POST /api/rooms/{room}/join/{handle}", func(w http.ResponseWriter, r *http.Request) {
		s.Broker.Join(r.PathValue("room"), r.PathValue("handle"))
		writeJSON(w, map[string]any{"ok": true})
	})
	mux.HandleFunc("POST /api/envelope", func(w http.ResponseWriter, r *http.Request) {
		var env Envelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := s.Broker.Publish(&env); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "id": env.ID})
	})
	mux.HandleFunc("GET /api/inbox/{handle}", func(w http.ResponseWriter, r *http.Request) {
		// parked + live envelopes for a handle (thin clients poll this)
		handle := r.PathValue("handle")
		out := s.drainInbox(handle)
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /api/task/{id}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Broker.Ledger().GetTask(r.PathValue("id")))
	})
	mux.HandleFunc("GET /api/task/{id}/tree", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Broker.Ledger().Tree(r.PathValue("id")))
	})
	mux.HandleFunc("GET /api/fanout/{id}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.Broker.Ledger().GetFanout(r.PathValue("id")))
	})
	mux.HandleFunc("POST /api/delegate", func(w http.ResponseWriter, r *http.Request) {
		// Server-side blocking delegate for thin clients (Hermes plugin).
		// parent_task nests this delegation under an existing task tree so
		// A→B→C chains roll up under one root (GET /api/task/<root>/tree).
		var req struct {
			From       string         `json:"from"`
			Peer       string         `json:"peer"`
			Action     string         `json:"action"`
			Params     map[string]any `json:"params"`
			OnBehalfOf []string       `json:"on_behalf_of"`
			ParentTask string         `json:"parent_task"`
			TimeoutSec int            `json:"timeout_sec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if req.TimeoutSec <= 0 {
			req.TimeoutSec = 30
		}
		ctx, cancel := contextWithTimeout(r, req.TimeoutSec)
		defer cancel()
		c := Attach(s.Broker, req.From)
		defer s.Broker.Unsubscribe("peer:"+req.From, req.From)
		result, fail, err := c.Delegate(ctx, req.Peer, req.Action, req.Params,
			req.OnBehalfOf, req.ParentTask)
		resp := map[string]any{"result": result, "fail": fail, "error": errString(err)}
		// hand back the task id(s) for tree tracking: find the task we opened
		if rec := s.newestTaskFrom(req.From); rec != nil {
			resp["task_id"] = rec.TaskID
			resp["root_task"] = rec.RootTask
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("POST /api/fanout", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			From       string         `json:"from"`
			Peers      []string       `json:"peers"`
			Action     string         `json:"action"`
			Params     map[string]any `json:"params"`
			Barrier    Barrier        `json:"barrier"`
			OnBehalfOf []string       `json:"on_behalf_of"`
			TimeoutSec int            `json:"timeout_sec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if req.TimeoutSec <= 0 {
			req.TimeoutSec = 60
		}
		ctx, cancel := contextWithTimeout(r, req.TimeoutSec)
		defer cancel()
		c := Attach(s.Broker, req.From)
		defer s.Broker.Unsubscribe("peer:"+req.From, req.From)
		got, err := c.Fanout(ctx, req.Peers, req.Action, req.Params, req.Barrier, req.OnBehalfOf)
		out := map[string]any{}
		for p, e := range got {
			out[p] = e
		}
		writeJSON(w, map[string]any{"results": out, "error": errString(err)})
	})
	mux.HandleFunc("GET /ws", s.handleWS)
	return mux
}

func contextWithTimeout(r *http.Request, sec int) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), time.Duration(sec)*time.Second)
}

// helpers kept tiny
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// drainInbox returns parked envelopes for a handle and clears them.
func (s *Server) drainInbox(handle string) []*Envelope {
	route := "peer:" + handle
	// parked (no live subscriber) envelopes
	s.Broker.mu.Lock()
	l := s.Broker.durable[route]
	var out []*Envelope
	if l != nil {
		for e := l.Front(); e != nil; e = e.Next() {
			out = append(out, e.Value.(*Envelope))
		}
		s.Broker.durable[route] = nil
	}
	// also live subscriber's buffered inbox (thin client not streaming)
	for _, sub := range s.Broker.subs[route] {
		_ = sub
	}
	s.Broker.mu.Unlock()
	return out
}

// newestTaskFrom returns the most recently opened task authored by a handle
// (used to echo task_id/root_task back to thin clients after a delegate).
func (s *Server) newestTaskFrom(from string) *TaskRecord {
	var newest *TaskRecord
	for _, t := range s.Broker.Ledger().Tasks() {
		if t.From == from && (newest == nil || t.OpenedTS.After(newest.OpenedTS)) {
			newest = t
		}
	}
	return newest
}

// handleWS is the agent endpoint: first message must be {"handle": "..."}.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ws, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	var hello struct{ Handle string `json:"handle"` }
	if err := ws.ReadJSON(&hello); err != nil || hello.Handle == "" {
		ws.Close()
		return
	}
	conn := &wsConn{ws: ws, handle: hello.Handle, send: make(chan *Envelope, 256)}
	subID := fmt.Sprintf("ws-%s-%d", hello.Handle, time.Now().UnixNano())
	s.mu.Lock()
	s.conns[subID] = conn
	s.mu.Unlock()
	s.Broker.Subscribe("peer:"+hello.Handle, subID, func(env *Envelope) {
		select {
		case conn.send <- env:
		default: // slow consumer; park instead of blocking the broker
			s.Broker.mu.Lock()
			l := s.Broker.durable["peer:"+hello.Handle]
			if l == nil {
				l = list.New()
				s.Broker.durable["peer:"+hello.Handle] = l
			}
			l.PushBack(env)
			s.Broker.mu.Unlock()
		}
	}, func() {
		s.mu.Lock()
		delete(s.conns, subID)
		s.mu.Unlock()
		ws.Close()
	})
	defer func() {
		s.Broker.Unsubscribe("peer:"+hello.Handle, subID)
		s.mu.Lock()
		delete(s.conns, subID)
		s.mu.Unlock()
		ws.Close()
	}()

	go func() {
		for env := range conn.send {
			if err := ws.WriteJSON(env); err != nil {
				return
			}
		}
	}()

	for {
		var env Envelope
		if err := ws.ReadJSON(&env); err != nil {
			break
		}
		if err := s.Broker.Publish(&env); err != nil {
			ws.WriteJSON(map[string]any{"error": err.Error()}) //nolint:errcheck
		}
	}
}
