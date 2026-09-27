package cexy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Error categories. An *APIError matches at most one of them with errors.Is, except that
// ErrJurisdictionBlocked also matches ErrForbidden. An error with a code this SDK does not
// know matches none of them: check [APIError.Code] instead.
var (
	ErrValidation          = errors.New("cexy: validation failed")            // 400
	ErrAuthentication      = errors.New("cexy: authentication failed")        // 401
	ErrForbidden           = errors.New("cexy: forbidden")                    // 403 (and 451)
	ErrJurisdictionBlocked = errors.New("cexy: blocked in this jurisdiction") // 451 JURISDICTION_BLOCKED
	ErrNotFound            = errors.New("cexy: not found")                    // 404
	ErrConflict            = errors.New("cexy: conflict")                     // 409
	ErrUnprocessable       = errors.New("cexy: refused by a business rule")   // 422
	ErrRateLimited         = errors.New("cexy: rate limited")                 // 429
	ErrServer              = errors.New("cexy: server error")                 // 5xx
	ErrUnexpectedRedirect  = errors.New("cexy: unexpected redirect")          // 3xx (never followed)
)

// CodeUnexpectedRedirect is the code of the *APIError for a 3xx response. The SDK never follows
// redirects, so that credentials and orders are never re-sent to another URL.
const CodeUnexpectedRedirect ErrorCode = "UNEXPECTED_REDIRECT"

// APIError is an error response from the API (the {"error": {...}} envelope).
type APIError struct {
	// HTTP status.
	Status int
	// Machine-readable code. A code missing from errors.yaml is passed through unchanged;
	// a response without the envelope (a proxy error page) gets "HTTP_<status>".
	Code ErrorCode
	// Human-readable, may change between releases. Credentials are redacted from it.
	Message string
	// Structured context: string, int64 or bool values.
	Details map[string]any
	// Per-field validation messages (400).
	Fields map[string]string
	// Quote it in support requests. Empty when the server sent none.
	RequestID string
	// Whether an identical retry could succeed.
	Retryable bool
	// How long the server asked to wait (Retry-After or details.retry_after_seconds); 0 if
	// it gave no hint.
	RetryAfter time.Duration

	kind error
}

func (e *APIError) Error() string {
	rid := ""
	if e.RequestID != "" {
		rid = " (request_id " + e.RequestID + ")"
	}
	return fmt.Sprintf("cexy: [%d %s] %s%s", e.Status, e.Code, e.Message, rid)
}

// Is reports whether the error belongs to category target (ErrNotFound, ErrRateLimited, ...).
func (e *APIError) Is(target error) bool {
	if e.kind == nil {
		return false
	}
	if target == e.kind {
		return true
	}
	return e.kind == ErrJurisdictionBlocked && target == ErrForbidden
}

// Known reports whether Code is listed in errors.yaml.
func (e *APIError) Known() bool { return knownErrorCodes[e.Code] }

// ConfigError is an invalid client configuration or call, detected before anything is sent.
type ConfigError struct{ Msg string }

func (e *ConfigError) Error() string { return "cexy: " + e.Msg }

// InvalidAmountError is a malformed decimal string in an amount field, detected before sending.
type InvalidAmountError struct {
	Field string
	Msg   string
}

func (e *InvalidAmountError) Error() string { return "cexy: " + e.Msg }

// ConnectionError means the request produced no HTTP response (DNS, TLS, connection reset,
// or the per-attempt timeout). It is always retryable.
type ConnectionError struct {
	Method, Path string
	// True when the per-attempt timeout expired.
	Timeout bool
	Err     error
}

func (e *ConnectionError) Error() string {
	if e.Timeout {
		return fmt.Sprintf("cexy: %s %s timed out", e.Method, e.Path)
	}
	return fmt.Sprintf("cexy: %s %s failed: %v", e.Method, e.Path, e.Err)
}

func (e *ConnectionError) Unwrap() error { return e.Err }

// OrderStateUnknownError means PlaceOrder failed ambiguously (network error or 5xx) and the
// lookup by client_order_id failed too, so it is unknown whether the order exists. Check
// Trading.OrderByClientID(ClientOrderID) before placing it again.
type OrderStateUnknownError struct {
	ClientOrderID string
	Err           error
}

func (e *OrderStateUnknownError) Error() string {
	return fmt.Sprintf("cexy: order state unknown for client_order_id %s; check Trading.OrderByClientID before retrying: %v",
		e.ClientOrderID, e.Err)
}

func (e *OrderStateUnknownError) Unwrap() error { return e.Err }

var statusKinds = map[int]error{
	400: ErrValidation,
	401: ErrAuthentication,
	403: ErrForbidden,
	404: ErrNotFound,
	409: ErrConflict,
	422: ErrUnprocessable,
	451: ErrJurisdictionBlocked,
}

var defaultRetryableStatus = map[int]bool{408: true, 429: true, 500: true, 502: true, 503: true, 504: true}

type envelope struct {
	Error *struct {
		Code      *string                    `json:"code"`
		Message   *string                    `json:"message"`
		Details   map[string]json.RawMessage `json:"details"`
		Fields    map[string]string          `json:"fields"`
		RequestID *string                    `json:"request_id"`
		Retryable *bool                      `json:"retryable"`
	} `json:"error"`
}

// errorFromResponse builds the *APIError for an error response. A known code maps by HTTP
// status; an unknown code keeps no category (never a crash); a body without the envelope maps
// by status with the code HTTP_<status>.
func errorFromResponse(status int, body []byte, h http.Header, redact func(string) string) *APIError {
	e := &APIError{Status: status, RequestID: h.Get("X-Request-Id"), Retryable: defaultRetryableStatus[status]}
	var env envelope
	hasEnvelope := json.Unmarshal(body, &env) == nil && env.Error != nil && env.Error.Code != nil
	if hasEnvelope {
		b := env.Error
		e.Code = ErrorCode(*b.Code)
		e.Message = *b.Code
		if b.Message != nil {
			e.Message = *b.Message
		}
		e.Details = decodeDetails(b.Details)
		e.Fields = b.Fields
		if redact != nil {
			// The server may echo request values back, even nested or as object keys; keep
			// credentials out of all of them.
			e.Details, _ = redactValue(e.Details, redact).(map[string]any)
			fields := make(map[string]string, len(e.Fields))
			for k, v := range e.Fields {
				fields[redact(k)] = redact(v)
			}
			e.Fields = fields
		}
		if b.RequestID != nil && *b.RequestID != "" {
			e.RequestID = *b.RequestID
		}
		if b.Retryable != nil {
			e.Retryable = *b.Retryable
		}
	} else {
		e.Code = ErrorCode("HTTP_" + strconv.Itoa(status))
		e.Message = "HTTP " + strconv.Itoa(status)
	}
	if redact != nil {
		e.Message = redact(e.Message)
	}
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	if e.Fields == nil {
		e.Fields = map[string]string{}
	}
	e.RetryAfter = retryAfter(h, e.Details)

	switch {
	case status == 429 || e.Code == CodeRateLimited:
		e.kind = ErrRateLimited
	case e.Code == CodeJurisdictionBlocked:
		e.kind = ErrJurisdictionBlocked
	case hasEnvelope && !knownErrorCodes[e.Code]:
		e.kind = nil
	case status >= 500:
		e.kind = ErrServer
	default:
		e.kind = statusKinds[status]
	}
	return e
}

// redirectError is the *APIError for a 3xx response. Details["location"] holds the Location
// header, if any.
func redirectError(status int, h http.Header, redact func(string) string) *APIError {
	loc := h.Get("Location")
	if redact != nil {
		loc = redact(loc)
	}
	msg := fmt.Sprintf("unexpected redirect (HTTP %d); the SDK does not follow redirects", status)
	details := map[string]any{}
	if loc != "" {
		msg += " (Location: " + loc + ")"
		details["location"] = loc
	}
	return &APIError{Status: status, Code: CodeUnexpectedRedirect, Message: msg, Details: details,
		Fields: map[string]string{}, RequestID: h.Get("X-Request-Id"), kind: ErrUnexpectedRedirect}
}

// redactValue redacts every string in a decoded JSON value, map keys included, recursively.
func redactValue(v any, redact func(string) string) any {
	switch x := v.(type) {
	case string:
		return redact(x)
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[redact(k)] = redactValue(val, redact)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = redactValue(val, redact)
		}
		return out
	default:
		return v
	}
}

func decodeDetails(raw map[string]json.RawMessage) map[string]any {
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		dec := json.NewDecoder(bytes.NewReader(v))
		dec.UseNumber()
		var x any
		if dec.Decode(&x) != nil {
			continue
		}
		if n, ok := x.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				x = i
			} else if f, err := n.Float64(); err == nil {
				x = f
			}
		}
		out[k] = x
	}
	return out
}

// retryAfter reads Retry-After (seconds or an HTTP date) and details.retry_after_seconds, and
// returns the larger. Both are untrusted: unparseable, negative, NaN or infinite values are
// ignored, and a huge value saturates instead of overflowing (callers never wait longer than
// MaxServerWait).
func retryAfter(h http.Header, details map[string]any) time.Duration {
	var best time.Duration
	if v := strings.TrimSpace(h.Get("Retry-After")); v != "" {
		if secs, err := strconv.ParseFloat(v, 64); err == nil {
			best = secondsDuration(secs)
		} else if at, err := http.ParseTime(v); err == nil {
			if d := time.Until(at); d > 0 {
				best = d
			}
		}
	}
	if d := detailSeconds(details["retry_after_seconds"]); d > best {
		best = d
	}
	return best
}

func detailSeconds(v any) time.Duration {
	var secs float64
	switch x := v.(type) {
	case int64:
		secs = float64(x)
	case float64:
		secs = x
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0
		}
		secs = f
	default:
		return 0
	}
	return secondsDuration(secs)
}

// secondsDuration converts untrusted seconds to a Duration: 0 for NaN, infinite, zero or
// negative input, and saturating at the largest Duration instead of overflowing.
func secondsDuration(secs float64) time.Duration {
	if math.IsNaN(secs) || math.IsInf(secs, 0) || secs <= 0 {
		return 0
	}
	if secs >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(secs * float64(time.Second))
}

// isRetryable: a connection failure, or an API error marked retryable (including 409
// CONCURRENT_MODIFICATION).
func isRetryable(err error) bool {
	var ce *ConnectionError
	if errors.As(err, &ce) {
		return true
	}
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.Retryable || ae.Code == CodeConcurrentModification
	}
	return false
}

// isAmbiguous: a failure after which it is unknown whether a mutation took effect.
func isAmbiguous(err error) bool {
	var ce *ConnectionError
	if errors.As(err, &ce) {
		return true
	}
	var ae *APIError
	return errors.As(err, &ae) && ae.Status >= 500
}
