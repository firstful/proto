package proto

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

// ErrorCode is a machine-readable error code for structured API responses.
type ErrorCode string

const (
	// Request and envelope validation errors.
	ErrCodeInvalidJSON          ErrorCode = "INVALID_JSON"
	ErrCodeInvalidEnvelope      ErrorCode = "INVALID_ENVELOPE"
	ErrCodeBadKind              ErrorCode = "BAD_KIND"
	ErrCodeBadFromHandle        ErrorCode = "BAD_FROM_HANDLE"
	ErrCodeBadRoute             ErrorCode = "BAD_ROUTE"
	ErrCodeMissingReplyTarget   ErrorCode = "MISSING_REPLY_TARGET"
	ErrCodeMissingAckType       ErrorCode = "MISSING_ACK_TYPE"
	ErrCodeBadAckType           ErrorCode = "BAD_ACK_TYPE"
	ErrCodeAckCarriesText       ErrorCode = "ACK_CARRIES_TEXT"
	ErrCodeMissingTaskAction    ErrorCode = "MISSING_TASK_ACTION"
	ErrCodeMissingFailBody      ErrorCode = "MISSING_FAIL_BODY"
	ErrCodeMissingResultPayload ErrorCode = "MISSING_RESULT_PAYLOAD"
	ErrCodeMissingToolBody      ErrorCode = "MISSING_TOOL_BODY"
	ErrCodeMissingToolName      ErrorCode = "MISSING_TOOL_NAME"
	ErrCodeBadCmdFormat         ErrorCode = "BAD_CMD_FORMAT"
	ErrCodeCmdWrongTarget       ErrorCode = "CMD_WRONG_TARGET"
	ErrCodeMissingStatusBody    ErrorCode = "MISSING_STATUS_BODY"
	ErrCodeEnvelopeTooLarge     ErrorCode = "ENVELOPE_TOO_LARGE"
	ErrCodeInvalidAboutField    ErrorCode = "INVALID_ABOUT_FIELD"

	// Policy errors.
	ErrCodeForbidden              ErrorCode = "FORBIDDEN"
	ErrCodeMissingFields          ErrorCode = "MISSING_FIELDS"
	ErrCodePolicyDenied           ErrorCode = "POLICY_DENIED"
	ErrCodeNoPolicyFile           ErrorCode = "NO_POLICY_FILE"
	ErrCodeActionNotPermitted     ErrorCode = "ACTION_NOT_PERMITTED"
	ErrCodePeerNotAllowed         ErrorCode = "PEER_NOT_ALLOWED"
	ErrCodeRoomNotAllowed         ErrorCode = "ROOM_NOT_ALLOWED"
	ErrCodeOnBehalfOfNotPermitted ErrorCode = "ON_BEHALF_OF_NOT_PERMITTED"
	ErrCodeGrantOnlyCoversSend    ErrorCode = "GRANT_ONLY_COVERS_SEND"
	ErrCodeGrantExpired           ErrorCode = "GRANT_EXPIRED"

	// Username claim errors.
	ErrCodeInvalidClaimRecord ErrorCode = "INVALID_CLAIM_RECORD"
	ErrCodeClaimExpired       ErrorCode = "CLAIM_EXPIRED"
	ErrCodeBadSignature       ErrorCode = "BAD_SIGNATURE"
	ErrCodeNicknameTaken      ErrorCode = "NICKNAME_TAKEN"

	ErrCodeInternal ErrorCode = "INTERNAL_ERROR"
)

// ErrorDetail contains non-secret, actionable context for an error.
type ErrorDetail map[string]any

// ErrorResponse is the common JSON error response for API failures.
type ErrorResponse struct {
	Error   ErrorInfo   `json:"error"`
	Request RequestInfo `json:"request,omitempty"`
}

// ErrorInfo is the machine-readable error object.
type ErrorInfo struct {
	Code    ErrorCode   `json:"code"`
	Message string      `json:"message"`
	Details ErrorDetail `json:"details,omitempty"`
}

// RequestInfo is a redacted view of the request that caused an error.
type RequestInfo struct {
	Method       string `json:"method"`
	Path         string `json:"path"`
	RemoteAddr   string `json:"remote_addr,omitempty"`
	Profile      string `json:"profile,omitempty"`
	Handle       string `json:"handle,omitempty"`
	EnvelopeID   string `json:"envelope_id,omitempty"`
	EnvelopeKind string `json:"envelope_kind,omitempty"`
	Timestamp    string `json:"timestamp"`
}

var rejectedRequestLogger = log.New(log.Writer(), "", 0)

// newErrorResponse builds a structured error response.
func newErrorResponse(code ErrorCode, message string, details ErrorDetail,
	req *http.Request, envelope *Envelope, handle string) *ErrorResponse {

	if details == nil {
		details = ErrorDetail{}
	}
	resp := &ErrorResponse{Error: ErrorInfo{Code: code, Message: message, Details: details}}
	if req == nil {
		return resp
	}

	ri := RequestInfo{
		Method:     req.Method,
		Path:       req.URL.Path,
		RemoteAddr: req.RemoteAddr,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
	}
	if handle != "" {
		ri.Handle = handle
		ri.Profile = ProfileFromHandle(handle)
	}
	if envelope != nil {
		ri.Handle = envelope.From
		ri.Profile = ProfileFromHandle(envelope.From)
		ri.EnvelopeID = envelope.ID
		ri.EnvelopeKind = string(envelope.Kind)
	}
	resp.Request = ri
	return resp
}

// writeAPIError writes a structured response and emits a redacted structured log.
func writeAPIError(w http.ResponseWriter, status int, code ErrorCode, message string,
	details ErrorDetail, req *http.Request, envelope *Envelope, handle string) {

	resp := newErrorResponse(code, message, details, req, envelope, handle)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
	logRejectedRequest(req, envelope, handle, code, message, details)
}

// logRejectedRequest writes one JSON log event. It deliberately never logs
// request bodies, public keys, signatures, or claim messages.
func logRejectedRequest(req *http.Request, envelope *Envelope, handle string,
	code ErrorCode, reason string, details ErrorDetail) {

	path := ""
	method := ""
	remoteAddr := ""
	if req != nil {
		method = req.Method
		if req.URL != nil {
			path = req.URL.Path
		}
		remoteAddr = req.RemoteAddr
	}
	if handle == "" && envelope != nil {
		handle = envelope.From
	}
	if details == nil {
		details = ErrorDetail{}
	}
	event := map[string]any{
		"method":        method,
		"path":          path,
		"remote_addr":   remoteAddr,
		"profile":       ProfileFromHandle(handle),
		"handle":        handle,
		"envelope_id":   "",
		"envelope_kind": "",
		"error_code":    string(code),
		"reason":        reason,
		"details":       details,
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
	}
	if envelope != nil {
		event["envelope_id"] = envelope.ID
		event["envelope_kind"] = string(envelope.Kind)
	}

	line, err := json.Marshal(event)
	if err != nil {
		log.Printf("proto: could not marshal rejection log: %v", err)
		return
	}
	_ = rejectedRequestLogger.Output(2, string(line))
}

// mapJSONError classifies malformed request bodies.
func mapJSONError(err error) (ErrorCode, string, ErrorDetail) {
	if errors.Is(err, io.EOF) {
		return ErrCodeInvalidJSON, "Request body is empty", ErrorDetail{"reason": "JSON body is required"}
	}
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return ErrCodeInvalidJSON, "Request body is not valid JSON", ErrorDetail{
			"reason": "JSON parse failed",
			"offset": syntaxErr.Offset,
		}
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return ErrCodeInvalidJSON, "Request body has an invalid JSON type", ErrorDetail{
			"reason": "JSON field type is invalid",
			"field":  typeErr.Field,
			"type":   typeErr.Type.String(),
		}
	}
	return ErrCodeInvalidJSON, "Request body is not valid JSON", ErrorDetail{"reason": "JSON parse failed"}
}

// mapEnvelopeError classifies validation and policy failures from Broker.Publish.
func mapEnvelopeError(err error) (ErrorCode, string, ErrorDetail) {
	var policyErr *PolicyError
	if errors.As(err, &policyErr) {
		return mapPolicyError(err)
	}
	return mapValidationError(err)
}

// mapValidationError classifies Envelope.Validate failures.
func mapValidationError(err error) (ErrorCode, string, ErrorDetail) {
	var protoErr *ProtoError
	if !errors.As(err, &protoErr) {
		return ErrCodeInvalidEnvelope, "Invalid envelope", ErrorDetail{"reason": "unknown envelope validation error"}
	}

	details := ErrorDetail{"reason": protoErr.Msg}
	switch protoErr.Field {
	case "kind":
		return ErrCodeBadKind, "Invalid envelope kind", ErrorDetail{"field": "kind", "reason": protoErr.Msg}
	case "from":
		return ErrCodeBadFromHandle, "Invalid sender handle", ErrorDetail{"field": "from", "reason": protoErr.Msg}
	case "to":
		if strings.Contains(protoErr.Msg, "peer:broker") {
			return ErrCodeCmdWrongTarget, "Cmd must be addressed to peer:broker or a room", ErrorDetail{"field": "to", "reason": protoErr.Msg}
		}
		return ErrCodeBadRoute, "Invalid delivery route", ErrorDetail{"field": "to", "reason": protoErr.Msg}
	case "reply_target":
		return ErrCodeMissingReplyTarget, "reply_target is required for this envelope kind", ErrorDetail{"field": "reply_target", "reason": protoErr.Msg}
	case "ack.type":
		if strings.Contains(protoErr.Msg, "requires") {
			return ErrCodeMissingAckType, "ack envelope requires ack.type", ErrorDetail{"field": "ack.type", "reason": protoErr.Msg}
		}
		return ErrCodeBadAckType, "Invalid ack.type value", ErrorDetail{"field": "ack.type", "reason": protoErr.Msg}
	case "text":
		if strings.Contains(protoErr.Msg, "pure acks") {
			return ErrCodeAckCarriesText, "Ack envelopes must not contain text", ErrorDetail{"field": "text", "reason": protoErr.Msg}
		}
		if strings.Contains(protoErr.Msg, "cmd requires") {
			return ErrCodeBadCmdFormat, "Cmd envelope requires text in format '!{name} ...'", ErrorDetail{"field": "text", "reason": protoErr.Msg}
		}
	case "task.action":
		return ErrCodeMissingTaskAction, "Task envelope requires task.action", ErrorDetail{"field": "task.action", "reason": protoErr.Msg}
	case "fail":
		return ErrCodeMissingFailBody, "Fail envelope requires fail{code,msg}", ErrorDetail{"field": "fail", "reason": protoErr.Msg}
	case "result":
		return ErrCodeMissingResultPayload, "Result envelope requires result payload", ErrorDetail{"field": "result", "reason": protoErr.Msg}
	case "tool":
		return ErrCodeMissingToolBody, "Tool envelope requires a tool body", ErrorDetail{"field": "tool", "reason": protoErr.Msg}
	case "tool.name":
		return ErrCodeMissingToolName, "Tool call requires tool.name", ErrorDetail{"field": "tool.name", "reason": protoErr.Msg}
	case "status":
		return ErrCodeMissingStatusBody, "Status envelope requires a status body", ErrorDetail{"field": "status", "reason": protoErr.Msg}
	case "size":
		return ErrCodeEnvelopeTooLarge, "Envelope exceeds the maximum size", ErrorDetail{"field": "size", "max_bytes": MaxEnvelopeBytes, "reason": protoErr.Msg}
	case "about":
		return ErrCodeInvalidAboutField, "about must be a task id string or {task_id}", ErrorDetail{"field": "about", "reason": protoErr.Msg}
	}
	// Fallback: message-based matching for errors from perr() that don't set Field
	msg := protoErr.Msg
	switch {
	case strings.Contains(msg, "bad kind"):
		return ErrCodeBadKind, "Invalid envelope kind", ErrorDetail{"field": "kind", "reason": msg}
	case strings.Contains(msg, "bad from handle"):
		return ErrCodeBadFromHandle, "Invalid sender handle", ErrorDetail{"field": "from", "reason": msg}
	case strings.Contains(msg, "bad route"):
		return ErrCodeBadRoute, "Invalid delivery route", ErrorDetail{"field": "to", "reason": msg}
	case strings.Contains(msg, "requires reply_target"):
		return ErrCodeMissingReplyTarget, "reply_target is required for this envelope kind", ErrorDetail{"field": "reply_target", "reason": msg}
	case strings.Contains(msg, "ack requires ack.type"):
		return ErrCodeMissingAckType, "ack envelope requires ack.type", ErrorDetail{"field": "ack.type", "reason": msg}
	case strings.Contains(msg, "bad ack.type"):
		return ErrCodeBadAckType, "Invalid ack.type value", ErrorDetail{"field": "ack.type", "reason": msg}
	case strings.Contains(msg, "pure acks"):
		return ErrCodeAckCarriesText, "Ack envelopes must not contain text", ErrorDetail{"field": "text", "reason": msg}
	case strings.Contains(msg, "task requires task.action"):
		return ErrCodeMissingTaskAction, "Task envelope requires task.action", ErrorDetail{"field": "task.action", "reason": msg}
	case strings.Contains(msg, "fail requires fail"):
		return ErrCodeMissingFailBody, "Fail envelope requires fail{code,msg}", ErrorDetail{"field": "fail", "reason": msg}
	case strings.Contains(msg, "result requires result"):
		return ErrCodeMissingResultPayload, "Result envelope requires result payload", ErrorDetail{"field": "result", "reason": msg}
	case strings.Contains(msg, "requires tool"):
		return ErrCodeMissingToolBody, "Tool envelope requires a tool body", ErrorDetail{"field": "tool", "reason": msg}
	case strings.Contains(msg, "tool_call requires tool.name"):
		return ErrCodeMissingToolName, "Tool call requires tool.name", ErrorDetail{"field": "tool.name", "reason": msg}
	case strings.Contains(msg, "cmd requires text"):
		return ErrCodeBadCmdFormat, "Cmd envelope requires text in format '!{name} ...'", ErrorDetail{"field": "text", "reason": msg}
	case strings.Contains(msg, "cmd must address"):
		return ErrCodeCmdWrongTarget, "Cmd must be addressed to peer:broker or a room", ErrorDetail{"field": "to", "reason": msg}
	case strings.Contains(msg, "status requires status"):
		return ErrCodeMissingStatusBody, "Status envelope requires a status body", ErrorDetail{"field": "status", "reason": msg}
	case strings.Contains(msg, "envelope >"):
		return ErrCodeEnvelopeTooLarge, "Envelope exceeds the maximum size", ErrorDetail{"field": "size", "max_bytes": MaxEnvelopeBytes, "reason": msg}
	case strings.Contains(msg, "about must be"):
		return ErrCodeInvalidAboutField, "about must be a task id string or {task_id}", ErrorDetail{"field": "about", "reason": msg}
	}
	return ErrCodeInvalidEnvelope, "Invalid envelope", details
}

// mapPolicyError classifies PolicyEngine failures.
func mapPolicyError(err error) (ErrorCode, string, ErrorDetail) {
	var policyErr *PolicyError
	if !errors.As(err, &policyErr) {
		return ErrCodePolicyDenied, "Messaging policy denied this request", ErrorDetail{"reason": "unknown policy error"}
	}

	details := ErrorDetail{"reason": policyErr.Msg}
	if policyErr.Profile != "" {
		details["profile"] = policyErr.Profile
	}
	if policyErr.Peer != "" {
		details["target"] = policyErr.Peer
	}
	if policyErr.Room != "" {
		details["room"] = policyErr.Room
	}
	if policyErr.Action != "" {
		details["action"] = policyErr.Action
	}
	if policyErr.Field != "" {
		details["field"] = policyErr.Field
	}

	switch policyErr.Field {
	case "policy":
		return ErrCodeNoPolicyFile, "No policy file is configured for this profile", details
	case "allowed_actions":
		return ErrCodeActionNotPermitted, "This action is not permitted for the profile", details
	case "allowed_peers":
		return ErrCodePeerNotAllowed, "This target peer is not permitted for the profile", details
	case "allowed_rooms":
		return ErrCodeRoomNotAllowed, "This target room is not permitted for the profile", details
	case "on_behalf_of":
		return ErrCodeOnBehalfOfNotPermitted, "Delegation is not permitted for this profile", details
	case "grant":
		return ErrCodeGrantOnlyCoversSend, "The active grant only permits send actions", details
	}
	return ErrCodePolicyDenied, "Messaging policy denied this request", details
}

// mapClaimError classifies username claim validation failures.
func mapClaimError(err error) (ErrorCode, string, ErrorDetail) {
	var protoErr *ProtoError
	if !errors.As(err, &protoErr) {
		return ErrCodeInvalidClaimRecord, "Invalid username claim", ErrorDetail{"reason": "unknown claim error"}
	}

	details := ErrorDetail{"reason": protoErr.Msg}
	switch protoErr.Field {
	case "record":
		return ErrCodeInvalidClaimRecord, "Claim record is missing required fields", details
	case "timestamp":
		return ErrCodeClaimExpired, "Claim timestamp is too old", details
	case "claim_message":
		return ErrCodeInvalidClaimRecord, "Claim message does not match the public key", details
	case "signature":
		return ErrCodeBadSignature, "Claim signature is invalid", details
	}
	return ErrCodeInvalidClaimRecord, "Invalid username claim", details
}

// RecoverAndLog recovers from panics in HTTP handlers and writes a structured log.
func RecoverAndLog(req *http.Request) {
	if recovered := recover(); recovered != nil {
		logRejectedRequest(req, nil, "", ErrCodeInternal, "Internal server error", ErrorDetail{
			"reason": "handler panicked",
		})
		log.Printf("proto: panic in %s %s: %v\n%s", req.Method, req.URL.Path, recovered, debug.Stack())
	}
}
