package cexy

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SigningScheme is the request-signing scheme (PLANNED: the API does not accept signed requests
// yet; keep the default Options.Auth until it does).
const SigningScheme = "CEXY-HMAC-SHA256-v1"

// MaxClockOffset is the furthest the client clock may be corrected after SIGNATURE_EXPIRED.
const MaxClockOffset = time.Hour

func isUnreserved(b byte) bool {
	return 'A' <= b && b <= 'Z' || 'a' <= b && b <= 'z' || '0' <= b && b <= '9' || b == '-' || b == '.' || b == '_' || b == '~'
}

// encodeRFC3986 keeps unreserved bytes and writes everything else as %XX (uppercase hex).
func encodeRFC3986(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isUnreserved(c) {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// percentDecode decodes %XX sequences to bytes; a % not followed by two hex digits is kept.
func percentDecode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
				b.WriteByte(byte(v))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// canonicalPath splits on "/" BEFORE decoding; each segment is decoded, then re-encoded.
func canonicalPath(path string) string {
	segs := strings.Split(path, "/")
	for i, s := range segs {
		segs[i] = encodeRFC3986(percentDecode(s))
	}
	return strings.Join(segs, "/")
}

// canonicalQuery splits on "&" (empty parts dropped); decodes and re-encodes names and values;
// sorts bytewise by name, then value. query is everything after the FIRST "?" of the request
// target, so a further "?" is data.
func canonicalQuery(query string) string {
	if query == "" {
		return ""
	}
	type pair struct{ n, v string }
	var pairs []pair
	for _, part := range strings.Split(query, "&") {
		if part == "" {
			continue // "a=1&&b=2" is "a=1&b=2"
		}
		n, v, _ := strings.Cut(part, "=")
		pairs = append(pairs, pair{encodeRFC3986(percentDecode(n)), encodeRFC3986(percentDecode(v))})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].n != pairs[j].n {
			return pairs[i].n < pairs[j].n
		}
		return pairs[i].v < pairs[j].v
	})
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.n + "=" + p.v
	}
	return strings.Join(out, "&")
}

// canonicalRequest is the string that is signed.
func canonicalRequest(method, path, query, timestamp, nonce string, body []byte) string {
	sum := sha256.Sum256(body)
	return strings.Join([]string{SigningScheme, strings.ToUpper(method), canonicalPath(path), canonicalQuery(query),
		timestamp, nonce, hex.EncodeToString(sum[:])}, "\n")
}

func hmacHex(secret, message string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(message))
	return hex.EncodeToString(m.Sum(nil))
}

// newNonce returns 16 bytes from the OS CSPRNG as base64url without padding (22 characters).
func newNonce() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("cexy: crypto/rand failed: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// encodeQuery is url.Values.Encode with RFC 3986 escaping (%20 for a space, %2B for a plus), so
// the query that is signed is exactly the query that is sent.
func encodeQuery(v url.Values) string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		for _, val := range v[k] {
			parts = append(parts, encodeRFC3986(k)+"="+encodeRFC3986(val))
		}
	}
	return strings.Join(parts, "&")
}

// HMACAuthenticator signs every private request (PLANNED scheme, see SigningScheme). Only
// X-API-Key, X-API-Timestamp, X-API-Nonce and X-API-Signature are sent: the secret never leaves
// the process. Authenticate runs once per attempt, so every retry is signed with a fresh
// timestamp and nonce. The HMAC key is the UTF-8 bytes of the secret string as issued.
//
// Its String, GoString and LogValue methods never reveal the secret.
type HMACAuthenticator struct {
	key, secret string
	now         func() time.Time
	nonce       func() string
	mu          sync.Mutex
	offset      time.Duration
}

// NewHMACAuthenticator checks the pair and returns the authenticator.
func NewHMACAuthenticator(apiKey, apiSecret string) (*HMACAuthenticator, error) {
	if _, err := NewAPIKeyAuthenticator(apiKey, apiSecret); err != nil {
		return nil, err
	}
	return &HMACAuthenticator{key: apiKey, secret: apiSecret, now: time.Now, nonce: newNonce}, nil
}

// Kind returns "hmac".
func (a *HMACAuthenticator) Kind() string { return "hmac" }

// ClockOffset is the correction applied to the local clock after SIGNATURE_EXPIRED.
func (a *HMACAuthenticator) ClockOffset() time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.offset
}

// adjustClock adopts the server clock; false (nothing changed) beyond MaxClockOffset.
func (a *HMACAuthenticator) adjustClock(serverMs int64) bool {
	offset := time.Duration(serverMs-a.now().UnixMilli()) * time.Millisecond
	if offset > MaxClockOffset || offset < -MaxClockOffset {
		return false
	}
	a.mu.Lock()
	a.offset = offset
	a.mu.Unlock()
	return true
}

// Authenticate signs what is on the wire: the escaped path and raw query of req.URL (exactly the
// request line Go sends) and the exact body bytes.
func (a *HMACAuthenticator) Authenticate(req *http.Request, body []byte) error {
	ts := strconv.FormatInt(a.now().Add(a.ClockOffset()).UnixMilli(), 10)
	nonce := a.nonce()
	canonical := canonicalRequest(req.Method, req.URL.EscapedPath(), req.URL.RawQuery, ts, nonce, body)
	req.Header.Del("X-API-Secret")
	req.Header.Set("X-API-Key", a.key)
	req.Header.Set("X-API-Timestamp", ts)
	req.Header.Set("X-API-Nonce", nonce)
	req.Header.Set("X-API-Signature", hmacHex(a.secret, canonical))
	return nil
}

// SignWebSocketChallenge returns the key id and the signature for a WebSocket auth_key.
func (a *HMACAuthenticator) SignWebSocketChallenge(connectionID, challenge string) (keyID, signature string) {
	return a.key, hmacHex(a.secret, "CEXY-WS-AUTH-v1\n"+connectionID+"\n"+challenge)
}

// Redact replaces the secret and the key id in text.
func (a *HMACAuthenticator) Redact(text string) string {
	for _, s := range []string{a.secret, a.key} {
		if s != "" {
			text = strings.ReplaceAll(text, s, redacted)
		}
	}
	return text
}

func (a *HMACAuthenticator) String() string {
	hint := a.key
	if len(hint) > 6 {
		hint = hint[:6]
	}
	return "HMACAuthenticator(" + hint + "…, secret=" + redacted + ")"
}

// GoString keeps %#v from printing the fields.
func (a *HMACAuthenticator) GoString() string { return a.String() }

// LogValue keeps log/slog from printing the fields.
func (a *HMACAuthenticator) LogValue() slog.Value { return slog.StringValue(a.String()) }
