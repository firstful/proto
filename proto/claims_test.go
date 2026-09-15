package proto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestClaim(t *testing.T, nickname, handle string, priv ed25519.PrivateKey, ts int64) *ClaimRecord {
	t.Helper()
	if priv == nil {
		_, p, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		priv = p
	}
	pubB64 := base64.StdEncoding.EncodeToString(priv.Public().(ed25519.PublicKey))
	msg := nickname + "\n" + pubB64
	if ts == 0 {
		ts = time.Now().Unix()
	}
	rec := &ClaimRecord{
		Nickname:  nickname,
		Handle:    handle,
		PublicKey: pubB64,
		Timestamp: ts,
		ClaimMsg:  msg,
	}
	rec.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(msg)))
	return rec
}

func TestClaimFCFSAndConflict(t *testing.T) {
	s := NewClaimsStore()
	recA := newTestClaim(t, "reed", "hermes:a", nil, 0)
	recB := newTestClaim(t, "reed", "hermes:b", nil, 0)

	winner, ok, err := s.Claim(recA)
	if err != nil || !ok || winner != recA {
		t.Fatalf("first claim should win: ok=%v err=%v", ok, err)
	}
	winner, ok, err = s.Claim(recB)
	if err != nil {
		t.Fatalf("conflict should not be an error: %v", err)
	}
	if ok || winner != recA {
		t.Fatalf("second claim should lose to existing: ok=%v winner=%v", ok, winner)
	}
}

func TestClaimBadSignature(t *testing.T) {
	s := NewClaimsStore()
	rec := newTestClaim(t, "alice", "hermes:a", nil, 0)
	rec.Signature = base64.StdEncoding.EncodeToString(make([]byte, 64))
	_, _, err := s.Claim(rec)
	if err != errBadSignature {
		t.Fatalf("want errBadSignature, got %v", err)
	}

	rec2 := newTestClaim(t, "alice", "hermes:a", nil, 0)
	rec2.ClaimMsg = "forged\npayload"
	_, _, err = s.Claim(rec2)
	if err != errInvalidClaim && err != errBadSignature {
		t.Fatalf("mismatched claim_message should be rejected, got %v", err)
	}
}

func TestClaimExpired(t *testing.T) {
	s := NewClaimsStore()
	rec := newTestClaim(t, "bob", "hermes:b", nil, time.Now().Unix()-400)
	_, _, err := s.Claim(rec)
	if err != errClaimExpired {
		t.Fatalf("want errClaimExpired, got %v", err)
	}
}

func TestClaimConcurrentOnlyOneWins(t *testing.T) {
	s := NewClaimsStore()
	const n = 20
	wins := make(chan int, n)
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := map[string]int{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := newTestClaim(t, "carol", "hermes:c"+string(rune('a'+i%3)), nil, 0)
			_, ok, err := s.Claim(rec)
			if err == nil && ok {
				wins <- i
				mu.Lock()
				winners[rec.Handle]++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	count := 0
	for range wins {
		count++
	}
	if count != 1 {
		t.Fatalf("exactly one concurrent claim should win, got %d", count)
	}
	total := 0
	for _, c := range winners {
		total += c
	}
	if total != 1 {
		t.Fatalf("exactly one handle should hold the win, got %v", winners)
	}
}

func TestClaimRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	jpath := filepath.Join(dir, "journal.jsonl")

	b1 := NewBroker(jpath)
	rec := newTestClaim(t, "dave", "hermes:d", nil, 0)
	winner, ok, err := b1.Claims().Claim(rec)
	if err != nil || !ok || winner != rec {
		t.Fatalf("claim should win: ok=%v err=%v", ok, err)
	}

	// Simulate restart with the same journal.
	b2 := NewBroker(jpath)
	if err := b2.Restore(); err != nil {
		t.Fatal(err)
	}
	got := b2.Claims().Get("dave")
	if got == nil {
		t.Fatal("claim lost after restart")
	}
	if got.Handle != "hermes:d" {
		t.Fatalf("wrong handle restored: %v", got.Handle)
	}

	// And the same claim replayed must NOT be re-journaled (no duplicate win).
	_, ok, err = b2.Claims().Claim(rec)
	if err != nil || ok {
		t.Fatalf("replay of existing claim should lose silently: ok=%v err=%v", ok, err)
	}
}

func TestJournalMixedReplay(t *testing.T) {
	dir := t.TempDir()
	jpath := filepath.Join(dir, "journal.jsonl")
	j := OpenJournal(jpath)

	rec := newTestClaim(t, "erin", "hermes:e", nil, 0)
	j.AppendClaim(rec)
	// Also append a plain envelope line (mixed journal) — replay must skip
	// malformed lines but keep both kinds.
	env := &Envelope{ID: "env1", Kind: KindMsg, From: "hermes:e", Text: "hi"}
	j.Append("delivered", env)
	j.Close()

	j2 := OpenJournal(jpath)
	lines, err := j2.Replay()
	j2.Close()
	if len(lines) != 2 {
		t.Fatalf("want 2 replayed lines, got %d", len(lines))
	}
	if lines[0].Claim == nil || lines[1].Env == nil {
		t.Fatalf("replay lost records: %+v", lines)
	}
	if lines[0].Claim.Nickname != "erin" {
		t.Fatalf("claim record corrupted: %+v", lines[0].Claim)
	}

	// Corrupt one line; replay must survive.
	bad := filepath.Join(dir, "bad.jsonl")
	_ = os.WriteFile(bad, []byte("{\"claim\":{}}\nnot-json-at-all\n"), 0o600)
	j3 := OpenJournal(bad)
	lines, err = j3.Replay()
	j3.Close()
	if err != nil {
		t.Fatalf("replay should tolerate garbage: %v", err)
	}
}
