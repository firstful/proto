package proto

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// Journal is a JSONL append-only log of delivered envelopes (crash recovery).
type Journal struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

type taggedEnvelope struct {
	Tag string    `json:"tag"`
	Env *Envelope `json:"env"`
}

func OpenJournal(path string) *Journal {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil
	}
	return &Journal{path: path, f: f}
}

func (j *Journal) Append(tag string, env *Envelope) {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := json.Marshal(taggedEnvelope{Tag: tag, Env: env})
	if err != nil {
		return
	}
	j.f.Write(append(b, '\n')) //nolint:errcheck
}

func (j *Journal) Replay() ([]taggedEnvelope, error) {
	if j == nil {
		return nil, nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	f, err := os.Open(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []taggedEnvelope
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var t taggedEnvelope
		if err := json.Unmarshal(sc.Bytes(), &t); err == nil && t.Env != nil {
			out = append(out, t)
		}
	}
	return out, sc.Err()
}
