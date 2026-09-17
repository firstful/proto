package proto

import (
	"sync"
	"time"
)

// TaskRecord tracks one task's lifecycle in the ledger.
type TaskRecord struct {
	TaskID     string     `json:"task_id"`
	RootTask   string     `json:"root_task,omitempty"` // walks parent_task chain to the top
	ParentTask string     `json:"parent_task,omitempty"`
	Children   []string   `json:"children,omitempty"` // child task ids
	Kind       Kind       `json:"kind"`
	From       string     `json:"from"`
	To         Target     `json:"to"`
	OpenedTS   time.Time  `json:"opened_ts"`
	Status     string     `json:"status"` // open | done | failed
	ClosedTS   *time.Time `json:"closed_ts,omitempty"`
	Results    int        `json:"results"`
	Fails      int        `json:"fails"`
}

// FanoutRecord tracks a fanout barrier.
type FanoutRecord struct {
	FanoutID   string     `json:"fanout_id"`
	ParentTask string     `json:"parent_task,omitempty"`
	Expected   []string   `json:"expected"` // peer handles in the plan
	Barrier    Barrier    `json:"barrier"`
	Got        []string   `json:"got"` // peers that reported
	Status     string     `json:"status"`
	OpenedTS   time.Time  `json:"opened_ts"`
	ClosedTS   *time.Time `json:"closed_ts,omitempty"`
}

// Ledger is the broker's orchestration state (SPEC §6).
type Ledger struct {
	mu      sync.RWMutex
	tasks   map[string]*TaskRecord
	fanouts map[string]*FanoutRecord
}

func NewLedger() *Ledger {
	return &Ledger{tasks: map[string]*TaskRecord{}, fanouts: map[string]*FanoutRecord{}}
}

func (l *Ledger) OpenTask(env *Envelope) *TaskRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	root := env.AboutTaskID
	if env.ParentTask != "" {
		// resolve the delegation tree root by walking the parent chain
		root = env.ParentTask
		for {
			parent, ok := l.tasks[root]
			if !ok || parent.ParentTask == "" || parent.ParentTask == root {
				break
			}
			root = parent.ParentTask
		}
		if pt, ok := l.tasks[env.ParentTask]; ok {
			pt.Children = append(pt.Children, env.AboutTaskID)
		}
	}
	t := &TaskRecord{
		TaskID:     env.AboutTaskID,
		RootTask:   root,
		ParentTask: env.ParentTask,
		Kind:       env.Kind,
		From:       env.From,
		To:         env.To,
		OpenedTS:   time.Now().UTC(),
		Status:     "open",
	}
	l.tasks[env.AboutTaskID] = t
	return t
}

func (l *Ledger) CloseTask(taskID, status string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.tasks[taskID]; ok && t.Status == "open" {
		now := time.Now().UTC()
		t.Status = status
		t.ClosedTS = &now
	}
}

func (l *Ledger) RecordResult(taskID string, isFail bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.tasks[taskID]; ok {
		if isFail {
			t.Fails++
		} else {
			t.Results++
		}
	}
}

func (l *Ledger) GetTask(taskID string) *TaskRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.tasks[taskID]
}

func (l *Ledger) OpenFanout(env *Envelope) *FanoutRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	expected := make([]string, 0, len(env.Fanout.Plan))
	for _, t := range env.Fanout.Plan {
		if t.Peer != "" {
			expected = append(expected, t.Peer)
		}
	}
	f := &FanoutRecord{
		FanoutID:   env.Fanout.FanoutID,
		ParentTask: env.ParentTask,
		Expected:   expected,
		Barrier:    env.Fanout.Barrier,
		Status:     "open",
		OpenedTS:   time.Now().UTC(),
	}
	if f.Barrier == "" {
		f.Barrier = BarrierAll
	}
	l.fanouts[f.FanoutID] = f
	return f
}

// RecordFanoutResult records a worker's result; returns true when barrier met.
func (l *Ledger) RecordFanoutResult(fanoutID, peer string, isFail bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.fanouts[fanoutID]
	if !ok || f.Status != "open" {
		return false
	}
	for _, p := range f.Got {
		if p == peer {
			return false // duplicate report
		}
	}
	f.Got = append(f.Got, peer)
	n, need := len(f.Got), len(f.Expected)
	met := (f.Barrier == BarrierAll && n >= need) ||
		(f.Barrier == BarrierFirst && n >= 1) ||
		(f.Barrier == BarrierMajority && n > need/2)
	if met {
		now := time.Now().UTC()
		f.Status = "closed"
		f.ClosedTS = &now
	}
	return met
}

func (l *Ledger) GetFanout(fanoutID string) *FanoutRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.fanouts[fanoutID]
}

func (l *Ledger) Tasks() map[string]*TaskRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[string]*TaskRecord, len(l.tasks))
	for k, v := range l.tasks {
		cp := *v
		out[k] = &cp
	}
	return out
}

func (l *Ledger) Fanouts() map[string]*FanoutRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make(map[string]*FanoutRecord, len(l.fanouts))
	for k, v := range l.fanouts {
		cp := *v
		out[k] = &cp
	}
	return out
}

// TaskTree is a recursive rollup of a delegation tree under one root task.
type TaskTree struct {
	TaskID     string        `json:"task_id"`
	RootTask   string        `json:"root_task"`
	Status     string        `json:"status"` // aggregated: done iff all descendants done
	Open       int           `json:"open"`
	Done       int           `json:"done"`
	Failed     int           `json:"failed"`
	Total      int           `json:"total"`
	Root       *TaskRecord   `json:"root"`
	Descendant []*TaskRecord `json:"descendants,omitempty"`
}

// Tree walks the parent→children links from a root task and aggregates status.
// A root is "done" only when every descendant closed done; any failed child
// marks the tree "failed" (partial-failure visibility, SPEC §6.2).
func (l *Ledger) Tree(rootID string) *TaskTree {
	l.mu.RLock()
	defer l.mu.RUnlock()
	root, ok := l.tasks[rootID]
	if !ok {
		return nil
	}
	tree := &TaskTree{TaskID: rootID, RootTask: rootID, Status: root.Status, Root: root}
	var walk func(id string)
	walk = func(id string) {
		t, ok := l.tasks[id]
		if !ok {
			return
		}
		tree.Total++
		switch t.Status {
		case "done":
			tree.Done++
		case "failed":
			tree.Failed++
		default:
			tree.Open++
		}
		if id != rootID {
			tree.Descendant = append(tree.Descendant, t)
		}
		for _, c := range t.Children {
			walk(c)
		}
	}
	walk(rootID)
	// aggregate: root's own status is authoritative only when it has no children;
	// otherwise roll up descendants
	if len(tree.Descendant) > 0 {
		switch {
		case tree.Failed > 0:
			tree.Status = "failed"
		case tree.Open > 0:
			tree.Status = "open"
		default:
			tree.Status = "done"
		}
	}
	return tree
}
