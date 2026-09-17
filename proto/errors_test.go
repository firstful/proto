package proto

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStructuredErrorResponse(t *testing.T) {
	// Test that writeAPIError with a validation error
	req := httptest.NewRequest("POST", "/api/envelope", nil)
	w := httptest.NewRecorder()

	// Create a fake envelope for context
	env := &Envelope{Kind: "msg", From: "test", To: Target{Peer: "target"}}

	// Call the actual function
	writeAPIError(w, http.StatusUnprocessableEntity, ErrCodeMissingReplyTarget,
		"reply_target is required for this envelope kind", nil, req, env, "")

	// Check response
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("Expected status 422, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Expected Content-Type application/json, got %s", w.Header().Get("Content-Type"))
	}

	// Parse response body
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode JSON: %v", err)
	}

	// Check structure
	if resp["error"] == nil {
		t.Fatalf("Missing 'error' field in response")
	}
	errorObj := resp["error"].(map[string]any)
	if errorObj["code"] != string(ErrCodeMissingReplyTarget) {
		t.Fatalf("Expected error.code MISSING_REPLY_TARGET, got %v", errorObj["code"])
	}
	if errorObj["message"] != "reply_target is required for this envelope kind" {
		t.Fatalf("Expected error.message match, got %v", errorObj["message"])
	}

	// Check request info
	if resp["request"] == nil {
		t.Fatalf("Missing 'request' field in response")
	}
	reqInfo := resp["request"].(map[string]any)
	if reqInfo["method"] != "POST" {
		t.Fatalf("Expected request.method POST, got %v", reqInfo["method"])
	}
	if reqInfo["path"] != "/api/envelope" {
		t.Fatalf("Expected request.path /api/envelope, got %v", reqInfo["path"])
	}
	if reqInfo["handle"] != "test" {
		t.Fatalf("Expected request.handle test, got %v", reqInfo["handle"])
	}
	if reqInfo["envelope_id"] != nil && reqInfo["envelope_id"] != "" {
		t.Fatalf("Expected request.envelope_id nil or empty, got %v", reqInfo["envelope_id"])
	}
	if reqInfo["envelope_kind"] != "msg" {
		t.Fatalf("Expected request.envelope_kind msg, got %v", reqInfo["envelope_kind"])
	}
}

func TestErrorMapping(t *testing.T) {
	// Test validation error mapping
	code, msg, _ := mapEnvelopeError(&ProtoError{Msg: "bad kind \"invalid\"", Field: "kind"})
	if code != ErrCodeBadKind {
		t.Fatalf("Expected BAD_KIND, got %v", code)
	}
	if !strings.Contains(msg, "Invalid envelope kind") {
		t.Fatalf("Expected message about invalid kind, got %v", msg)
	}

	// Test policy error mapping
	code, msg, _ = mapPolicyError(&PolicyError{
		Msg:     "action 'task' not permitted for profile 'hermes:dev'",
		Field:   "allowed_actions",
		Profile: "hermes:dev",
		Action:  "task",
	})
	if code != ErrCodeActionNotPermitted {
		t.Fatalf("Expected ACTION_NOT_PERMITTED, got %v", code)
	}
	if !strings.Contains(msg, "not permitted") {
		t.Fatalf("Expected message about action not permitted, got %v", msg)
	}

	// Test claim error mapping
	code, msg, _ = mapClaimError(errInvalidClaim)
	if code != ErrCodeInvalidClaimRecord {
		t.Fatalf("Expected INVALID_CLAIM_RECORD, got %v", code)
	}
	if !strings.Contains(msg, "required fields") {
		t.Fatalf("Expected message about claim required fields, got %v", msg)
	}

	code, msg, _ = mapClaimError(errClaimExpired)
	if code != ErrCodeClaimExpired {
		t.Fatalf("Expected CLAIM_EXPIRED, got %v", code)
	}
	if !strings.Contains(msg, "too old") {
		t.Fatalf("Expected message about claim expired, got %v", msg)
	}

	code, msg, _ = mapClaimError(errBadSignature)
	if code != ErrCodeBadSignature {
		t.Fatalf("Expected BAD_SIGNATURE, got %v", code)
	}
	if !strings.Contains(msg, "signature") {
		t.Fatalf("Expected message about bad signature, got %v", msg)
	}
}

func TestNewErrorResponse(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/test", nil)
	env := &Envelope{Kind: "task", From: "user:reed", ID: "test123", To: Target{Peer: "AgentA"}}

	resp := newErrorResponse(ErrCodeInvalidJSON, "Invalid JSON", ErrorDetail{"field": "body"}, req, env, "user:reed")

	if resp.Error.Code != ErrCodeInvalidJSON {
		t.Fatalf("Expected InvalidJSON code, got %v", resp.Error.Code)
	}
	if resp.Error.Message != "Invalid JSON" {
		t.Fatalf("Expected Invalid JSON message, got %v", resp.Error.Message)
	}
	if resp.Error.Details["field"] != "body" {
		t.Fatalf("Expected details.field body, got %v", resp.Error.Details["field"])
	}
	if resp.Request.Method != "GET" {
		t.Fatalf("Expected GET method, got %v", resp.Request.Method)
	}
	if resp.Request.Path != "/api/test" {
		t.Fatalf("Expected /api/test path, got %v", resp.Request.Path)
	}
	if resp.Request.Handle != "user:reed" {
		t.Fatalf("Expected user:reed handle, got %v", resp.Request.Handle)
	}
	if resp.Request.EnvelopeID != "test123" {
		t.Fatalf("Expected test123 envelope ID, got %v", resp.Request.EnvelopeID)
	}
	if resp.Request.EnvelopeKind != "task" {
		t.Fatalf("Expected task envelope kind, got %v", resp.Request.EnvelopeKind)
	}
}

func TestEnvelopeValidationError(t *testing.T) {
	// Missing reply_target
	env := Envelope{Kind: KindMsg, From: "AgentA", To: Target{Peer: "AgentB"}, Text: "hi"}
	err := env.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}
	code, msg, details := mapEnvelopeError(err)
	if code != ErrCodeMissingReplyTarget {
		t.Fatalf("Expected MISSING_REPLY_TARGET, got %v", code)
	}
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
	if details == nil {
		t.Fatal("expected error details")
	}
	if details["field"] == nil {
		t.Fatal("expected field in error details")
	}
}

func TestWriteAPIErrorJSONFormat(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/envelope", nil)
	w := httptest.NewRecorder()

	writeAPIError(w, http.StatusUnprocessableEntity, ErrCodeBadKind,
		"Invalid envelope kind", ErrorDetail{"field": "kind", "value": "invalid"}, req, nil, "")

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode JSON: %v", err)
	}

	// Verify the error object has the right shape
	e, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatal("missing error key")
	}
	if e["code"] != string(ErrCodeBadKind) {
		t.Fatalf("code = %v", e["code"])
	}
	if _, ok := e["message"]; !ok {
		t.Fatal("missing error.message")
	}
	if _, ok := e["details"]; !ok {
		t.Fatal("missing error.details")
	}
}
