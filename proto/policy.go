package proto

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// PolicyEngine enforces per-profile messaging policy AT THE BROKER (the API
// layer). The Hermes plugin is a dumb shim: it just relays; the broker is the
// single choke point that decides allow/deny.
//
// Files (both root-owned; profiles are fs-jailed so they cannot touch these):
//   /etc/proto/policies/<profile>.yaml   — base declarative policy (fail-closed)
//   /etc/proto/grants.json               — rook-minted temporary send channels
//
// Hot reload: both are re-stat'ed per check (cheap) and re-parsed only on
// mtime change, so root edits take effect immediately with no restart.
// Missing policy file for a named profile = DENY ALL (fail-closed). The
// special profile "rook" (the default/root profile) is unrestricted.

// Policy is the declarative per-profile policy.
type Policy struct {
	Default      string   `yaml:"default"     json:"default"`
	AllowedPeers []string `yaml:"allowed_peers" json:"allowed_peers"`
	AllowedRooms []string `yaml:"allowed_rooms" json:"allowed_rooms"`
	// AllowedActions gates envelope kinds: msg, task, results (result), ack,
	// task_info, cmd, claim, announce, delegate, fanout.
	AllowedActions []string `yaml:"allowed_actions" json:"allowed_actions"`
	OnBehalfOf     string   `yaml:"on_behalf_of" json:"on_behalf_of"` // allow|deny
}

// Grant is a temporary one-directional send channel from → to.
type Grant struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Action  string `json:"action"` // always "send"; never widened
	Expires int64  `json:"expires"`
	Reason  string `json:"reason,omitempty"`
}

// PolicyError is returned when a send/delegate/fanout violates policy.
type PolicyError struct {
	Msg     string `json:"msg"`
	Profile string `json:"profile,omitempty"`
	Action  string `json:"action,omitempty"`
	Peer    string `json:"peer,omitempty"`
	Room    string `json:"room,omitempty"`
	Field   string `json:"field,omitempty"`
}

func (e *PolicyError) Error() string { return "proto: " + e.Msg }

// ProfileFromHandle maps a broker handle ("hermes:<profile>", "rook", …) to
// its profile name for policy lookup.
func ProfileFromHandle(handle string) string {
	if i := strings.Index(handle, ":"); i >= 0 {
		return handle[i+1:]
	}
	return handle
}

const (
	defaultPolicyDir  = "/etc/proto/policies"
	defaultGrantsPath = "/etc/proto/grants.json"
	adminProfile      = "rook" // the default/root profile: unrestricted
)

// policyDirFromEnv returns the policy dir, or "" when enforcement is disabled
// (unset PROTO_POLICY_DIR → library/test mode, no enforcement).
func policyDirFromEnv() string { return os.Getenv("PROTO_POLICY_DIR") }

// PolicyEngine is the broker's policy + grant store. Inactive (deny nothing)
// unless PROTO_POLICY_DIR is set in the environment — production sets it,
// tests and library consumers don't.
type PolicyEngine struct {
	mu        sync.RWMutex
	policies  map[string]*policyEntry
	grants    []Grant
	grantPath string
	policyDir string
	active    bool
}

type policyEntry struct {
	pol   *Policy
	mtime time.Time
	size  int64
}

// NewPolicyEngine returns an engine reading from PROTO_POLICY_DIR (active) or
// an inactive engine when unset (library/test mode).
func NewPolicyEngine() *PolicyEngine {
	dir := policyDirFromEnv()
	return &PolicyEngine{
		policies:  map[string]*policyEntry{},
		grantPath: defaultGrantsPath,
		policyDir: dir,
		active:    dir != "",
	}
}

// enabled reports whether enforcement is on. Inactive engines allow all.
func (pe *PolicyEngine) enabled() bool {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	return pe.active
}

// Check verifies that `from` may perform `action` toward `peer`/`room`.
// Returns nil (allow) or *PolicyError. onBehalfOf non-empty requires the
// policy to opt in AND the first chain element to be the admin profile.
func (pe *PolicyEngine) Check(from, action, peer, room string, onBehalfOf []string) error {
	if !pe.enabled() {
		return nil
	}
	profile := ProfileFromHandle(from)

	// Admin profile is the policy authority: unrestricted.
	if profile == adminProfile {
		return nil
	}

	// Grants: temporary rook-minted send channels (checked before base policy
	// peer list, but AFTER action gating — grants never widen the action set).
	pe.mu.RLock()
	grants := pe.grants
	pe.mu.RUnlock()
	now := time.Now().Unix()
	for _, g := range grants {
		if g.From == profile && g.To == ProfileFromHandle(peer) &&
			now < g.Expires {
			if action == "send" || action == "msg" {
				return nil
			}
			return &PolicyError{Msg: fmt.Sprintf(
				"profile '%s': grant to '%s' covers send only, not '%s' (ask %s)",
				profile, peer, action, adminProfile)}
		}
	}

	pol := pe.loadPolicy(profile)
	if pol == nil {
		return &PolicyError{Msg: fmt.Sprintf(
			"profile '%s': no policy file — outbound messaging denied (ask %s)",
			profile, adminProfile)}
	}

	if len(pol.AllowedActions) == 0 || !contains(pol.AllowedActions, action) {
		return &PolicyError{Msg: fmt.Sprintf(
			"action '%s' not permitted for profile '%s'", action, profile)}
	}
	if peer != "" && !contains(pol.AllowedPeers, ProfileFromHandle(peer)) {
		return &PolicyError{Msg: fmt.Sprintf(
			"peer '%s' not in allowed_peers for profile '%s' (no active temporary grant — ask %s)",
			peer, profile, adminProfile)}
	}
	if room != "" && !contains(pol.AllowedRooms, room) {
		return &PolicyError{Msg: fmt.Sprintf(
			"room '%s' not in allowed_rooms for profile '%s'", room, profile)}
	}
	if len(onBehalfOf) > 0 {
		if pol.OnBehalfOf != "allow" {
			return &PolicyError{Msg: fmt.Sprintf(
				"on_behalf_of not permitted for profile '%s'", profile)}
		}
	}

	// Rooms: non-admin profiles must be a member of the room too (rook keeps
	// join authority; Join() is broker-side and only reachable via cmd/API).
	return nil
}

// CheckRoomJoin gates POST /api/rooms/{room}/join/{handle}.
func (pe *PolicyEngine) CheckRoomJoin(room, handle string) error {
	if !pe.enabled() {
		return nil
	}
	profile := ProfileFromHandle(handle)
	if profile == adminProfile {
		return nil
	}
	pol := pe.loadPolicy(profile)
	if pol == nil {
		return &PolicyError{Msg: fmt.Sprintf(
			"profile '%s': no policy file — room join denied", profile)}
	}
	if !contains(pol.AllowedRooms, room) {
		return &PolicyError{Msg: fmt.Sprintf(
			"room '%s' not in allowed_rooms for profile '%s'", room, profile)}
	}
	return nil
}

// loadPolicy returns the hot-reloaded policy for a profile (nil = deny).
// Policy files use flat KEY=VALUE lines (no YAML dep): e.g.
//
//	allowed_peers=rook,kube
//	allowed_actions=status,send,results,task_info
//	allowed_rooms=
//	on_behalf_of=deny
func (pe *PolicyEngine) loadPolicy(profile string) *Policy {
	path := filepath.Join(pe.policyDir, profile+".policy")
	st, err := os.Stat(path)
	if err != nil {
		pe.mu.Lock()
		delete(pe.policies, profile)
		pe.mu.Unlock()
		return nil
	}
	pe.mu.RLock()
	entry, ok := pe.policies[profile]
	pe.mu.RUnlock()
	if ok && entry.mtime.Equal(st.ModTime()) && entry.size == st.Size() {
		return entry.pol
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	pol := parsePolicy(string(data))
	if pol == nil {
		return nil // unparsable = deny
	}
	pe.mu.Lock()
	pe.policies[profile] = &policyEntry{pol: pol, mtime: st.ModTime(), size: st.Size()}
	pe.mu.Unlock()
	return pol
}

// parsePolicy parses flat KEY=VALUE lines; "#" comments allowed.
func parsePolicy(text string) *Policy {
	pol := &Policy{OnBehalfOf: "deny"}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil // malformed line = deny (nil policy)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch k {
		case "default":
			pol.Default = v
		case "allowed_peers":
			pol.AllowedPeers = splitList(v)
		case "allowed_rooms":
			pol.AllowedRooms = splitList(v)
		case "allowed_actions":
			pol.AllowedActions = splitList(v)
		case "on_behalf_of":
			pol.OnBehalfOf = v
		default:
			return nil // unknown key = deny
		}
	}
	if pol.Default != "" && pol.Default != "deny" {
		return nil // fail-closed: only "deny" (or empty) defaults accepted
	}
	return pol
}

func splitList(v string) []string {
	if v == "" {
		return nil
	}
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// ReloadGrants re-reads the grants file (hot-reload; cheap per call batch).
func (pe *PolicyEngine) ReloadGrants() {
	data, err := os.ReadFile(pe.grantPath)
	if err != nil {
		pe.mu.Lock()
		pe.grants = nil
		pe.mu.Unlock()
		return
	}
	var doc struct {
		Grants []Grant `json:"grants"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return // corrupt: keep last known grants
	}
	pe.mu.Lock()
	pe.grants = doc.Grants
	pe.mu.Unlock()
}

// Grant adds (or replaces) a temporary send channel; admin-only path.
func (pe *PolicyEngine) Grant(from, to string, ttl time.Duration, reason string) Grant {
	g := Grant{From: from, To: to, Action: "send",
		Expires: time.Now().Add(ttl).Unix(), Reason: reason}
	pe.mu.Lock()
	out := make([]Grant, 0, len(pe.grants)+1)
	for _, old := range pe.grants {
		if old.From != from || old.To != to {
			out = append(out, old)
		}
	}
	pe.grants = append(out, g)
	pe.mu.Unlock()
	pe.persistGrants()
	return g
}

// Revoke removes all grants from→to.
func (pe *PolicyEngine) Revoke(from, to string) int {
	pe.mu.Lock()
	n := 0
	out := pe.grants[:0]
	for _, g := range pe.grants {
		if g.From == from && g.To == to {
			n++
			continue
		}
		out = append(out, g)
	}
	pe.grants = out
	pe.mu.Unlock()
	pe.persistGrants()
	return n
}

// Grants returns live (non-expired) grants.
func (pe *PolicyEngine) Grants() []Grant {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	now := time.Now().Unix()
	var out []Grant
	for _, g := range pe.grants {
		if now < g.Expires {
			out = append(out, g)
		}
	}
	return out
}

func (pe *PolicyEngine) persistGrants() {
	pe.mu.RLock()
	doc := struct {
		Grants []Grant `json:"grants"`
	}{Grants: pe.grants}
	pe.mu.RUnlock()
	data, _ := json.MarshalIndent(doc, "", "  ")
	tmp := pe.grantPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return
	}
	_ = os.Rename(tmp, pe.grantPath)
}

// SweepGrants drops expired entries (housekeeping; also runs lazily on check).
func (pe *PolicyEngine) SweepGrants() int {
	pe.mu.Lock()
	now := time.Now().Unix()
	n := 0
	out := pe.grants[:0]
	for _, g := range pe.grants {
		if now < g.Expires {
			out = append(out, g)
		} else {
			n++
		}
	}
	pe.grants = out
	pe.mu.Unlock()
	if n > 0 {
		pe.persistGrants()
	}
	return n
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
