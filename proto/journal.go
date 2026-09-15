package proto

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// Journal is a JSONL append-only log (crash recovery). Lines carry either an
// envelope (taggedEnvelope) or a username claim record (taggedClaim).
type Journal struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

type taggedEnvelope struct {
	Tag string    `json:"tag"`
	Env *Envelope `json:"env"`
}

type taggedClaim struct {
	Claim *ClaimRecord `json:"claim"`
}

// journalLine is the union shape used when replaying: a line has exactly one
// of Env or Claim set.
type journalLine struct {
	Tag   string       `json:"tag,omitempty"`
	Env   *Envelope    `json:"env,omitempty"`
	Claim *ClaimRecord `json:"claim,omitempty"`
}

func OpenJournal(path string) *Journal {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil
	}
	return &Journal{path: path, f: f}
}

func (j *Journal) Close() error {
	if j == nil || j.f == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.f.Close()
	j.f = nil
	return err
}

func (j *Journal) Append(tag string, env *Envelope) {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := json.Marshal(journalLine{Tag: tag, Env: env})
	if err != nil {
		return
	}
	j.f.Write(append(b, '\n')) //nolint:errcheck
}

// AppendClaim durably records a username claim (used on first-valid-wins).
func (j *Journal) AppendClaim(record *ClaimRecord) {
	if j == nil || record == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := json.Marshal(journalLine{Claim: record})
	if err != nil {
		return
	}
	j.f.Write(append(b, '\n')) //nolint:errcheck
}

func (j *Journal) Replay() ([]journalLine, error) {
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
	var out []journalLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var line journalLine
		if err := json.Unmarshal(sc.Bytes(), &line); err == nil && (line.Env != nil || line.Claim != nil) {
			out = append(out, line)
		}
	}
	return out, sc.Err()
}
