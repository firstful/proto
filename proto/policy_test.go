package proto

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// setupPolicyDir writes policy files into a temp dir and points PROTO_POLICY_DIR at it.
func setupPolicyDir(t *testing.T, files map[string]string) *PolicyEngine {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name+".policy"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	pe := NewPolicyEngine()
	pe.policyDir = dir
	pe.grantPath = filepath.Join(dir, "grants.json")
	pe.active = true
	t.Cleanup(func() { os.Unsetenv("PROTO_POLICY_DIR") })
	return pe
}

func TestPolicyEnforcement(t *testing.T) {
	pe := setupPolicyDir(t, map[string]string{
		"dev":   "default=deny\nallowed_peers=rook\nallowed_actions=send,status,results,task_info\non_behalf_of=deny\n",
		"cloud": "default=deny\nallowed_peers=rook\nallowed_actions=status,send\non_behalf_of=deny\n",
	})

	// dev -> rook: allowed
	if err := pe.Check("hermes:dev", "send", "hermes:rook", "", nil); err != nil {
		t.Fatalf("dev→rook should pass: %v", err)
	}
	// dev -> kube: denied (no policy for kube direction)
	if err := pe.Check("hermes:dev", "send", "hermes:kube", "", nil); err == nil {
		t.Fatal("dev→kube should be denied")
	}
	// dev delegate: action not in policy
	if err := pe.Check("hermes:dev", "task", "hermes:rook", "", nil); err == nil {
		t.Fatal("dev task should be denied (action not permitted)")
	}
	// dev obo: denied
	if err := pe.Check("hermes:dev", "send", "hermes:rook", "", []string{"user:reed"}); err == nil {
		t.Fatal("on_behalf_of should be denied")
	}
	// unknown profile: fail-closed
	if err := pe.Check("hermes:nobody", "send", "hermes:rook", "", nil); err == nil {
		t.Fatal("unknown profile should be denied")
	}
	// admin: unrestricted
	if err := pe.Check("rook", "send", "hermes:dev", "", nil); err != nil {
		t.Fatalf("rook should be unrestricted: %v", err)
	}
}

func TestGrantLifecycle(t *testing.T) {
	pe := setupPolicyDir(t, map[string]string{
		"dev":  "default=deny\nallowed_peers=rook\nallowed_actions=send,status\non_behalf_of=deny\n",
		"kube": "default=deny\nallowed_peers=rook\nallowed_actions=send,status\non_behalf_of=deny\n",
	})

	// blocked before grant
	if err := pe.Check("hermes:dev", "send", "hermes:kube", "", nil); err == nil {
		t.Fatal("dev→kube should be denied before grant")
	}

	// mint grant (as rook would via API) — 2s so Unix-second expiry is live
	g := pe.Grant("dev", "kube", 2*time.Second, "test")
	if g.Expires <= time.Now().Unix() {
		t.Fatal("grant should be live")
	}

	// allowed during grant — but only send
	if err := pe.Check("hermes:dev", "send", "hermes:kube", "", nil); err != nil {
		t.Fatalf("dev→kube send should pass under grant: %v", err)
	}
	// grants do NOT widen task actions
	if err := pe.Check("hermes:dev", "task", "hermes:kube", "", nil); err == nil {
		t.Fatal("grant must not widen task action")
	}
	// reverse direction unaffected
	if err := pe.Check("hermes:kube", "send", "hermes:dev", "", nil); err == nil {
		t.Fatal("kube→dev should still be denied (one-way grant)")
	}

	// expiry
	time.Sleep(2100 * time.Millisecond)
	if err := pe.Check("hermes:dev", "send", "hermes:kube", "", nil); err == nil {
		t.Fatal("grant should have expired")
	}
}

func TestParsePolicyFailClosed(t *testing.T) {
	if parsePolicy("default=allow\nallowed_peers=rook\n") != nil {
		t.Fatal("default=allow must be rejected")
	}
	if parsePolicy("garbage line no equals") != nil {
		t.Fatal("malformed line must be rejected")
	}
	if parsePolicy("unknown_key=x\n") != nil {
		t.Fatal("unknown key must be rejected")
	}
	p := parsePolicy("# comment\nallowed_peers=rook,scribe\nallowed_actions=send\n")
	if p == nil || len(p.AllowedPeers) != 2 || p.OnBehalfOf != "deny" {
		t.Fatalf("valid policy misparsed: %+v", p)
	}
}

func TestPublishEnforcement(t *testing.T) {
	pe := setupPolicyDir(t, map[string]string{
		"boris": "default=deny\nallowed_peers=rook\nallowed_actions=send\non_behalf_of=deny\n",
	})
	b := NewBroker("")
	b.policy = pe

	// msg to allowed peer: routed
	if err := b.Publish(&Envelope{Kind: KindMsg, From: "hermes:boris",
		To: Target{Peer: "rook"}, ReplyTarget: &Target{Peer: "hermes:boris"},
		Text: "hi"}); err != nil {
		t.Fatalf("boris→rook msg should pass: %v", err)
	}
	// msg to blocked peer: PolicyError at the API layer
	err := b.Publish(&Envelope{Kind: KindMsg, From: "hermes:boris",
		To: Target{Peer: "kube"}, ReplyTarget: &Target{Peer: "hermes:boris"},
		Text: "hi"})
	if _, ok := err.(*PolicyError); !ok {
		t.Fatalf("boris→kube msg should be PolicyError, got %v", err)
	}
}
