package proto

import (
	"crypto/ed25519"
	"encoding/base64"
	"sync"
	"time"
)

// ClaimRecord is a durable username claim.
type ClaimRecord struct {
	Nickname  string `json:"nickname"`
	Handle    string `json:"handle"`
	PublicKey string `json:"public_key"`
	Signature string `json:"signature"`
	Timestamp int64  `json:"timestamp"`
	ClaimMsg  string `json:"claim_message"`
}

// ClaimsStore is a thread-safe map of nickname -> ClaimRecord.
type ClaimsStore struct {
	mu      sync.RWMutex
	claims  map[string]*ClaimRecord
	journal *Journal
}

func NewClaimsStore() *ClaimsStore {
	return &ClaimsStore{claims: map[string]*ClaimRecord{}}
}

// SetJournal sets the journal for persistence.
func (s *ClaimsStore) SetJournal(j *Journal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.journal = j
}

// journalClaim durably appends a winning claim to the journal (if set).
// Caller must hold s.mu.
func (s *ClaimsStore) journalClaim(record *ClaimRecord) {
	if s.journal != nil {
		s.journal.AppendClaim(record)
	}
}

// Verify checks the Ed25519 signature over the canonical claim message.
func (c *ClaimRecord) Verify() bool {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(c.PublicKey)
	if err != nil {
		return false
	}
	pubKey := ed25519.PublicKey(pubKeyBytes)
	sig, err := base64.StdEncoding.DecodeString(c.Signature)
	if err != nil {
		return false
	}
	return ed25519.Verify(pubKey, []byte(c.ClaimMsg), sig)
}

// CanonicalMessage rebuilds the signed message from the record.
func (c *ClaimRecord) CanonicalMessage() string {
	return c.Nickname + "\n" + c.PublicKey
}

// Claim atomically inserts a claim if the nickname is unclaimed and the signature is valid.
// Returns (winner, true) if this claim won, (existing, false) if it lost.
func (s *ClaimsStore) Claim(record *ClaimRecord) (*ClaimRecord, bool, error) {
	if record == nil || record.Nickname == "" || record.Handle == "" {
		return nil, false, errInvalidClaim
	}
	if record.PublicKey == "" || record.Signature == "" {
		return nil, false, errInvalidClaim
	}
	if record.Timestamp == 0 {
		return nil, false, errInvalidClaim
	}
	if time.Now().Unix()-record.Timestamp > 300 {
		return nil, false, errClaimExpired
	}
	if record.ClaimMsg != record.CanonicalMessage() {
		return nil, false, errInvalidClaim
	}
	if !record.Verify() {
		return nil, false, errBadSignature
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.claims[record.Nickname]; ok {
		return existing, false, nil
	}

	s.claims[record.Nickname] = record
	s.journalClaim(record)
	return record, true, nil
}

// Restore directly inserts a claim without conflict checking (for journal replay).
func (s *ClaimsStore) Restore(record *ClaimRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claims[record.Nickname] = record
}

// Get returns the claim for a nickname, or nil.
func (s *ClaimsStore) Get(nickname string) *ClaimRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.claims[nickname]
}

// List returns all claims.
func (s *ClaimsStore) List() []*ClaimRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ClaimRecord, 0, len(s.claims))
	for _, c := range s.claims {
		out = append(out, c)
	}
	return out
}

var (
	errInvalidClaim = &ProtoError{Msg: "invalid claim record", Field: "record"}
	errClaimExpired = &ProtoError{Msg: "claim expired", Field: "timestamp"}
	errBadSignature = &ProtoError{Msg: "bad signature", Field: "signature"}
)
