package cexy

import (
	"log/slog"
	"net/http"
	"strings"
)

const redacted = "[REDACTED]"

// Authenticator adds credentials to requests for operations that need an API key.
//
// Authenticate is called once per attempt (retries included), so a future signing scheme
// can put a fresh timestamp and nonce on each attempt. It is never called for public
// operations. body is the exact JSON that will be sent, or nil.
type Authenticator interface {
	// Kind is a short, non-secret description such as "api-key".
	Kind() string
	Authenticate(req *http.Request, body []byte) error
	// Redact removes any secret material from text (used on error messages).
	Redact(text string) string
}

// APIKeyAuthenticator is today's scheme: X-API-Key and X-API-Secret headers on every private
// request (the default). HMACAuthenticator (Options.Auth "hmac") is the request-signing scheme.
//
// Its String, GoString and LogValue methods never reveal the secret.
type APIKeyAuthenticator struct {
	key, secret string
}

// NewAPIKeyAuthenticator checks the pair and returns the authenticator.
func NewAPIKeyAuthenticator(apiKey, apiSecret string) (*APIKeyAuthenticator, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, &ConfigError{Msg: "APIKey must be a non-empty string"}
	}
	if strings.TrimSpace(apiSecret) == "" {
		return nil, &ConfigError{Msg: "APISecret must be a non-empty string"}
	}
	if strings.ContainsAny(apiKey, "\r\n") || strings.ContainsAny(apiSecret, "\r\n") {
		return nil, &ConfigError{Msg: "APIKey and APISecret must not contain line breaks"}
	}
	return &APIKeyAuthenticator{key: apiKey, secret: apiSecret}, nil
}

// Kind returns "api-key".
func (a *APIKeyAuthenticator) Kind() string { return "api-key" }

// KeyHint is a non-secret hint for logs: the first characters of the key id.
func (a APIKeyAuthenticator) KeyHint() string {
	if len(a.key) <= 6 {
		return a.key + "…"
	}
	return a.key[:6] + "…"
}

// Authenticate sets the two headers. Credentials never go into the URL.
func (a *APIKeyAuthenticator) Authenticate(req *http.Request, _ []byte) error {
	req.Header.Set("X-API-Key", a.key)
	req.Header.Set("X-API-Secret", a.secret)
	return nil
}

// Redact replaces the secret and the key id in text.
func (a *APIKeyAuthenticator) Redact(text string) string {
	for _, s := range []string{a.secret, a.key} {
		if s != "" {
			text = strings.ReplaceAll(text, s, redacted)
		}
	}
	return text
}

// String never reveals the secret. Value receivers make both APIKeyAuthenticator and
// *APIKeyAuthenticator redact in fmt and log/slog.
func (a APIKeyAuthenticator) String() string {
	return "APIKeyAuthenticator(" + a.KeyHint() + ", secret=" + redacted + ")"
}

// GoString keeps %#v from printing the fields.
func (a APIKeyAuthenticator) GoString() string { return a.String() }

// LogValue keeps log/slog from printing the fields.
func (a APIKeyAuthenticator) LogValue() slog.Value { return slog.StringValue(a.String()) }
