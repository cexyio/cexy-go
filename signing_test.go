package cexy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type signingVectors struct {
	Scheme    string `json:"scheme"`
	KeyID     string `json:"key_id"`
	Secret    string `json:"secret"`
	Timestamp string `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Rest      []struct {
		Name             string            `json:"name"`
		Method           string            `json:"method"`
		RequestTarget    string            `json:"request_target"`
		Body             string            `json:"body"`
		CanonicalPath    string            `json:"canonical_path"`
		CanonicalQuery   string            `json:"canonical_query"`
		BodySHA256       string            `json:"body_sha256"`
		CanonicalRequest string            `json:"canonical_request"`
		Headers          map[string]string `json:"headers"`
	} `json:"rest"`
	WS struct {
		Welcome struct {
			ConnectionID string `json:"connection_id"`
			Challenge    string `json:"challenge"`
		} `json:"welcome"`
		Message string         `json:"message"`
		AuthKey map[string]any `json:"auth_key"`
	} `json:"ws"`
	Negative struct {
		Name             string `json:"name"`
		CanonicalRequest string `json:"canonical_request"`
		WrongSecret      string `json:"wrong_secret"`
		WithWrongSecret  string `json:"signature_with_wrong_secret"`
		WithRightSecret  string `json:"signature_with_right_secret"`
	} `json:"negative"`
}

func loadSigningVectors(t *testing.T) signingVectors {
	t.Helper()
	var v signingVectors
	readJSON(t, filepath.Join(specDir(t), "conformance", "signing", "vectors.json"), &v)
	return v
}

func splitTarget(target string) (path, query string) {
	path, query, _ = strings.Cut(target, "?")
	return path, query
}

func TestSigningVectors(t *testing.T) {
	v := loadSigningVectors(t)
	if v.Scheme != SigningScheme {
		t.Fatalf("scheme %q", v.Scheme)
	}
	if len(v.Rest) == 0 {
		t.Fatal("no REST vectors")
	}
	for _, c := range v.Rest {
		t.Run(c.Name, func(t *testing.T) {
			path, query := splitTarget(c.RequestTarget)
			if got := canonicalPath(path); got != c.CanonicalPath {
				t.Errorf("canonical path %q, want %q", got, c.CanonicalPath)
			}
			if got := canonicalQuery(query); got != c.CanonicalQuery {
				t.Errorf("canonical query %q, want %q", got, c.CanonicalQuery)
			}
			got := canonicalRequest(c.Method, path, query, v.Timestamp, v.Nonce, []byte(c.Body))
			if got != c.CanonicalRequest {
				t.Errorf("canonical request\n%q\nwant\n%q", got, c.CanonicalRequest)
			}
			if sig := hmacHex(v.Secret, got); sig != c.Headers["X-API-Signature"] {
				t.Errorf("signature %s, want %s", sig, c.Headers["X-API-Signature"])
			}

			// The authenticator produces exactly the vector's headers for the request as sent.
			a := &HMACAuthenticator{key: v.KeyID, secret: v.Secret, nonce: func() string { return v.Nonce },
				now: func() time.Time { ms, _ := strconv.ParseInt(v.Timestamp, 10, 64); return time.UnixMilli(ms) }}
			req, err := http.NewRequest(c.Method, "https://api.cexy.io"+c.RequestTarget, strings.NewReader(c.Body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-API-Secret", "must-be-removed")
			if err := a.Authenticate(req, []byte(c.Body)); err != nil {
				t.Fatal(err)
			}
			for h, want := range c.Headers {
				if got := req.Header.Get(h); got != want {
					t.Errorf("%s = %q, want %q", h, got, want)
				}
			}
			if req.Header.Get("X-API-Secret") != "" {
				t.Error("X-API-Secret sent in hmac mode")
			}
		})
	}
	a := &HMACAuthenticator{key: v.KeyID, secret: v.Secret}
	keyID, sig := a.SignWebSocketChallenge(v.WS.Welcome.ConnectionID, v.WS.Welcome.Challenge)
	if keyID != v.WS.AuthKey["key_id"] || sig != v.WS.AuthKey["signature"] {
		t.Errorf("ws auth_key %s %s, want %v", keyID, sig, v.WS.AuthKey)
	}
	n := v.Negative
	if hmacHex(n.WrongSecret, n.CanonicalRequest) != n.WithWrongSecret || hmacHex(v.Secret, n.CanonicalRequest) != n.WithRightSecret ||
		n.WithWrongSecret == n.WithRightSecret {
		t.Errorf("negative vector %s", n.Name)
	}
}

func TestEncodeQueryRFC3986(t *testing.T) {
	got := encodeQuery(map[string][]string{"b": {"x y+z/é~"}, "a": {"1"}})
	if got != "a=1&b=x%20y%2Bz%2F%C3%A9~" {
		t.Fatalf("got %q", got)
	}
	if n := newNonce(); len(n) != 22 || strings.ContainsAny(n, "+/=") {
		t.Fatalf("nonce %q", n)
	}
	if newNonce() == newNonce() {
		t.Fatal("nonce repeated")
	}
}

const (
	sigTestKey    = "ak_test_key"
	sigTestSecret = "test_secret_for_signing"
)

// An independent canonicaliser, written from the spec text (not the SDK's code): regexp decoding
// and a table encoder, so the recording server catches canonicalisation bugs too.
var pctRE = regexp.MustCompile(`%[0-9A-Fa-f]{2}`)

func indepEnc(s string) string {
	raw := pctRE.ReplaceAllFunc([]byte(s), func(m []byte) []byte {
		b, _ := strconv.ParseUint(string(m[1:]), 16, 8)
		return []byte{byte(b)}
	})
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
	var sb strings.Builder
	for _, b := range raw {
		if strings.IndexByte(unreserved, b) >= 0 {
			sb.WriteByte(b)
		} else {
			fmt.Fprintf(&sb, "%%%02X", b)
		}
	}
	return sb.String()
}

func indepPath(p string) string {
	segs := strings.Split(p, "/")
	for i := range segs {
		segs[i] = indepEnc(segs[i])
	}
	return strings.Join(segs, "/")
}

func indepQuery(q string) string {
	var pairs [][2]string
	for _, part := range strings.Split(q, "&") {
		if part == "" {
			continue
		}
		n, v, _ := strings.Cut(part, "=")
		pairs = append(pairs, [2]string{indepEnc(n), indepEnc(v)})
	}
	slices.SortFunc(pairs, func(a, b [2]string) int {
		if c := strings.Compare(a[0], b[0]); c != 0 {
			return c
		}
		return strings.Compare(a[1], b[1])
	})
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p[0] + "=" + p[1]
	}
	return strings.Join(out, "&")
}

func indepCanonical(method, target, ts, nonce string, body []byte) string {
	path, query := splitTarget(target)
	sum := sha256.Sum256(body)
	return strings.Join([]string{"CEXY-HMAC-SHA256-v1", method, indepPath(path), indepQuery(query), ts, nonce, hex.EncodeToString(sum[:])}, "\n")
}

func TestIndependentCanonicaliserAgreesWithVectors(t *testing.T) {
	v := loadSigningVectors(t)
	for _, c := range v.Rest {
		path, query := splitTarget(c.RequestTarget)
		if indepPath(path) != c.CanonicalPath || indepQuery(query) != c.CanonicalQuery {
			t.Errorf("%s: independent canonicaliser disagrees with the vector", c.Name)
		}
		if got := indepCanonical(c.Method, c.RequestTarget, v.Timestamp, v.Nonce, []byte(c.Body)); got != c.CanonicalRequest {
			t.Errorf("%s: %q", c.Name, got)
		}
	}
}

func TestQueryRules(t *testing.T) {
	if got := canonicalQuery("a=1&&b=2&"); got != "a=1&b=2" {
		t.Errorf("empty parts: %q", got)
	}
	if got := canonicalQuery("?a=1"); got != "%3Fa=1" {
		t.Errorf("a '?' inside the query is data: %q", got)
	}
}

// signedRequest is what the recording server saw, checked against the RAW request line and body.
type signedRequest struct {
	requestURI string
	body       string
	headers    http.Header
	valid      bool
	nonce      string
	timestamp  int64
}

// signingServer recomputes every signature from the raw request (r.RequestURI and the body bytes
// as received), independent of what the client thinks it sent.
type signingServer struct {
	mu   sync.Mutex
	reqs []signedRequest
}

func (s *signingServer) record(r *http.Request) signedRequest {
	body, _ := io.ReadAll(r.Body)
	canonical := indepCanonical(r.Method, r.RequestURI, r.Header.Get("X-API-Timestamp"), r.Header.Get("X-API-Nonce"), body)
	ts, _ := strconv.ParseInt(r.Header.Get("X-API-Timestamp"), 10, 64)
	sr := signedRequest{requestURI: r.RequestURI, body: string(body), headers: r.Header.Clone(), nonce: r.Header.Get("X-API-Nonce"), timestamp: ts,
		valid: r.Header.Get("X-API-Key") == sigTestKey && r.Header.Get("X-API-Signature") == hmacHex(sigTestSecret, canonical)}
	s.mu.Lock()
	s.reqs = append(s.reqs, sr)
	s.mu.Unlock()
	return sr
}

func (s *signingServer) all() []signedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]signedRequest(nil), s.reqs...)
}

type fixedClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fixedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// newSigningClient returns a hmac client for a server whose handler also gets the recorded request.
func newSigningClient(t *testing.T, handler func(http.ResponseWriter, *http.Request, signedRequest, int)) (*Client, *signingServer, *fixedClock, *HMACAuthenticator) {
	t.Helper()
	s := &signingServer{}
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sr := s.record(r)
		if !sr.valid {
			writeJSON(w, 401, apiErr("INVALID_SIGNATURE", "bad signature", false))
			return
		}
		handler(w, r, sr, int(n.Add(1)))
	}))
	t.Cleanup(srv.Close)
	clock := &fixedClock{t: time.UnixMilli(1790000000000)}
	a, err := NewHMACAuthenticator(sigTestKey, sigTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	a.now = clock.now
	fs := &fakeSleep{now: time.Now()}
	c, err := New(Options{BaseURL: srv.URL, AllowInsecure: true, DisableRateLimit: true, Authenticator: a,
		sleep: fs.sleep, now: fs.clock, random: func() float64 { return 0.5 }})
	if err != nil {
		t.Fatal(err)
	}
	return c, s, clock, a
}

func TestHMACClientOption(t *testing.T) {
	c, err := New(Options{APIKey: sigTestKey, APISecret: sigTestSecret})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.t.auth.(*HMACAuthenticator); !ok {
		t.Fatalf(`the default must be "hmac" (request signing), got %T`, c.t.auth)
	}
	c, err = New(Options{APIKey: sigTestKey, APISecret: sigTestSecret, Auth: "hmac"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.t.auth.(*HMACAuthenticator); !ok {
		t.Fatalf("auth %T", c.t.auth)
	}
	c, err = New(Options{APIKey: sigTestKey, APISecret: sigTestSecret, Auth: "headers"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.t.auth.(*APIKeyAuthenticator); !ok {
		t.Fatalf(`Auth "headers" must still select the header scheme, got %T`, c.t.auth)
	}
	if _, err := New(Options{APIKey: sigTestKey, APISecret: sigTestSecret, Auth: "HMAC"}); err == nil {
		t.Fatal("unknown Auth accepted")
	}
}

func TestSignatureRequiredNamesTheFix(t *testing.T) {
	var n atomic.Int32
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, Auth: "headers"}, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		writeJSON(w, 400, apiErr("SIGNATURE_REQUIRED", "This API key must sign its requests; sending the secret is no longer accepted.", false))
	})
	_, err := c.Account.Balances(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "SIGNATURE_REQUIRED" || ae.Retryable || !strings.Contains(ae.Message, `Auth "hmac"`) {
		t.Fatalf("got %v", err)
	}
	if n.Load() != 1 {
		t.Fatalf("%d requests: SIGNATURE_REQUIRED must not be retried", n.Load())
	}
}

func TestSignedRequestsMatchTheWire(t *testing.T) {
	c, s, _, _ := newSigningClient(t, func(w http.ResponseWriter, r *http.Request, sr signedRequest, n int) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/v1/trading/orders/by-client-id/"):
			writeJSON(w, 404, apiErr("ORDER_NOT_FOUND", "no such order", false))
		case r.URL.Path == "/api/v1/trading/orders/history":
			writeJSON(w, 200, map[string]any{"data": []any{}, "next_cursor": nil})
		default:
			writeJSON(w, 200, dataEnv([]any{}))
		}
	})
	ctx := context.Background()
	if _, err := c.Account.Balances(ctx); err != nil {
		t.Fatal(err)
	}
	// A path parameter with reserved and non-ASCII characters: %2F must reach the server intact.
	_, _ = c.Trading.OrderByClientID(ctx, "a/b+c d~é")
	sym, cursor := "BTC/USDT", "x y+z=&"
	if _, err := c.Trading.OrderHistory(ctx, &OrderHistoryParams{Symbol: &sym, Cursor: &cursor}); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Trading.PlaceOrder(ctx, PlaceOrderRequest{Symbol: "BTC/USDT", Side: "buy", Type: "limit",
		Price: Ptr(Amount("100.5")), Quantity: Ptr(Amount("0.1"))})

	reqs := s.all()
	if len(reqs) < 4 {
		t.Fatalf("%d requests", len(reqs))
	}
	for _, r := range reqs {
		if !r.valid {
			t.Errorf("%s: signature does not match the raw request", r.requestURI)
		}
		if r.headers.Get("X-API-Secret") != "" {
			t.Errorf("%s: X-API-Secret sent in hmac mode", r.requestURI)
		}
	}
	if reqs[1].requestURI != "/api/v1/trading/orders/by-client-id/a%2Fb%2Bc%20d~%C3%A9" {
		t.Errorf("path sent as %q", reqs[1].requestURI)
	}
	if !strings.Contains(reqs[2].requestURI, "symbol=BTC%2FUSDT") || !strings.Contains(reqs[2].requestURI, "cursor=x%20y%2Bz%3D%26") {
		t.Errorf("query sent as %q", reqs[2].requestURI)
	}
	last := reqs[len(reqs)-1]
	if !strings.HasPrefix(last.body, "{") {
		t.Errorf("body %q", last.body)
	}
}

func TestSignedRetryUsesFreshNonce(t *testing.T) {
	c, s, _, _ := newSigningClient(t, func(w http.ResponseWriter, r *http.Request, sr signedRequest, n int) {
		if n == 1 {
			writeJSON(w, 503, apiErr("SERVICE_UNAVAILABLE", "busy", true))
			return
		}
		writeJSON(w, 200, dataEnv([]any{}))
	})
	if _, err := c.Account.Balances(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := s.all()
	if len(reqs) != 2 || !reqs[0].valid || !reqs[1].valid || reqs[0].nonce == reqs[1].nonce {
		t.Fatalf("%+v", reqs)
	}
}

func TestNonceStoreWarmingWaitsRetryAfterAndKeepsTheOffset(t *testing.T) {
	s := &signingServer{}
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sr := s.record(r); !sr.valid {
			writeJSON(w, 401, apiErr("INVALID_SIGNATURE", "bad signature", false))
			return
		}
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "2")
			writeJSON(w, 503, map[string]any{"error": map[string]any{"code": "SERVICE_UNAVAILABLE", "message": "x",
				"retryable": true, "details": map[string]any{"reason": "nonce_store_warming"}}})
			return
		}
		writeJSON(w, 200, dataEnv([]any{}))
	}))
	t.Cleanup(srv.Close)
	a, _ := NewHMACAuthenticator(sigTestKey, sigTestSecret)
	fs := &fakeSleep{now: time.Now()}
	c, err := New(Options{BaseURL: srv.URL, AllowInsecure: true, DisableRateLimit: true, Authenticator: a,
		sleep: fs.sleep, now: fs.clock, random: func() float64 { return 0.5 }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Account.Balances(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs, waits := s.all(), fs.all()
	if len(reqs) != 2 {
		t.Fatalf("%d requests, want 2", len(reqs))
	}
	if reqs[0].nonce == reqs[1].nonce {
		t.Fatalf("nonce reused on the retry: %q", reqs[0].nonce)
	}
	if len(waits) != 1 || waits[0] < 2*time.Second || waits[0] > 3*time.Second {
		t.Fatalf("waits %v, want one Retry-After wait of 2 s (+ jitter)", waits)
	}
	if a.ClockOffset() != 0 {
		t.Fatalf("offset %v: warming must not touch the clock", a.ClockOffset())
	}
}

func expiredErr(serverMs int64) map[string]any {
	return map[string]any{"error": map[string]any{"code": "SIGNATURE_EXPIRED", "message": "timestamp outside the window",
		"retryable": true, "details": map[string]any{"server_time_ms": serverMs}}}
}

func TestSignatureExpiredResendsOnceWithServerClock(t *testing.T) {
	serverMs := int64(1790000000000 + 10*60*1000) // the local clock is 10 minutes slow
	c, s, _, a := newSigningClient(t, func(w http.ResponseWriter, r *http.Request, sr signedRequest, n int) {
		if sr.timestamp < serverMs-30000 {
			writeJSON(w, 401, expiredErr(serverMs))
			return
		}
		writeJSON(w, 200, dataEnv([]any{}))
	})
	if _, err := c.Account.Balances(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := s.all()
	if len(reqs) != 2 || reqs[1].timestamp != serverMs || reqs[0].nonce == reqs[1].nonce {
		t.Fatalf("%+v", reqs)
	}
	if a.ClockOffset() != 10*time.Minute {
		t.Fatalf("offset %v", a.ClockOffset())
	}
	// The next request is signed with the corrected clock straight away.
	if _, err := c.Account.Balances(context.Background()); err != nil || len(s.all()) != 3 {
		t.Fatal(err, len(s.all()))
	}
}

func TestSignatureExpiredTwiceIsReturned(t *testing.T) {
	c, s, _, _ := newSigningClient(t, func(w http.ResponseWriter, r *http.Request, sr signedRequest, n int) {
		writeJSON(w, 401, expiredErr(1790000000000+5*60*1000))
	})
	_, err := c.Account.Balances(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "SIGNATURE_EXPIRED" {
		t.Fatalf("got %v", err)
	}
	if n := len(s.all()); n != 2 {
		t.Fatalf("%d requests, want 2 (one resend, outside the retry budget)", n)
	}
}

func TestSignatureExpiredBeyondAnHourIsAClockError(t *testing.T) {
	c, s, _, a := newSigningClient(t, func(w http.ResponseWriter, r *http.Request, sr signedRequest, n int) {
		writeJSON(w, 401, expiredErr(1790000000000+2*60*60*1000))
	})
	_, err := c.Account.Balances(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "SIGNATURE_EXPIRED" || ae.Retryable || !strings.Contains(ae.Message, "clock") {
		t.Fatalf("got %v", err)
	}
	if n := len(s.all()); n != 1 || a.ClockOffset() != 0 {
		t.Fatalf("%d requests, offset %v", n, a.ClockOffset())
	}
}

func TestKeyNotSignableHasNoFallback(t *testing.T) {
	c, s, _, _ := newSigningClient(t, func(w http.ResponseWriter, r *http.Request, sr signedRequest, n int) {
		writeJSON(w, 401, apiErr("KEY_NOT_SIGNABLE", "this key cannot sign", false))
	})
	_, err := c.Account.Balances(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "KEY_NOT_SIGNABLE" ||
		ae.Message != "create a new API key; keys issued before request signing can't sign" {
		t.Fatalf("got %v", err)
	}
	reqs := s.all()
	if len(reqs) != 1 || reqs[0].headers.Get("X-API-Secret") != "" {
		t.Fatalf("%d requests (no fallback to headers auth expected)", len(reqs))
	}
}

func TestHMACAuthenticatorNeverPrintsTheSecret(t *testing.T) {
	a, err := NewHMACAuthenticator(sigTestKey, sigTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Options{Authenticator: a})
	if err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	slog.New(slog.NewTextHandler(&buf, nil)).Info("x", "auth", a)
	out := fmt.Sprintf("%v %+v %#v %s %v %+v", a, a, a, a, c, c) + buf.String()
	if strings.Contains(out, sigTestSecret) || strings.Contains(out, sigTestKey) {
		t.Fatalf("leaked: %s", out)
	}
	if got := a.Redact("k=" + sigTestKey + " s=" + sigTestSecret); strings.Contains(got, sigTestSecret) || strings.Contains(got, sigTestKey) {
		t.Fatalf("redact: %s", got)
	}
}

// keyAuthWS is a WebSocket server with challenges: every welcome and every auth_key reply
// carries a fresh challenge, each accepted once.
type keyAuthWS struct {
	mu       sync.Mutex
	conns    []*websocket.Conn
	authKeys []map[string]any
	signed   map[string]bool // challenges signed so far
	issued   int
	refuse   atomic.Bool // answer auth_key with an error
	silent   atomic.Bool // hold the reply to auth_key (see release)
	held     []byte
}

func (k *keyAuthWS) nextChallenge() string {
	k.issued++
	return fmt.Sprintf("challenge-%02d", k.issued)
}

func (k *keyAuthWS) handler(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	k.mu.Lock()
	k.conns = append(k.conns, c)
	connID := fmt.Sprintf("conn-%d", len(k.conns))
	challenge := k.nextChallenge()
	k.mu.Unlock()
	write := func(v any) {
		b, _ := json.Marshal(v)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Write(ctx, websocket.MessageText, b)
	}
	write(map[string]any{"type": "welcome", "protocol_version": 1, "heartbeat_interval_seconds": 30,
		"max_subscriptions": 100, "connection_id": connID, "challenge": challenge})
	for {
		_, data, err := c.Read(context.Background())
		if err != nil {
			return
		}
		var f map[string]any
		if json.Unmarshal(data, &f) != nil {
			continue
		}
		switch f["op"] {
		case "auth_key":
			k.mu.Lock()
			k.authKeys = append(k.authKeys, f)
			want := hmacHex(sigTestSecret, "CEXY-WS-AUTH-v1\n"+connID+"\n"+challenge)
			ok := f["key_id"] == sigTestKey && f["signature"] == want && !k.signed[challenge] && !k.refuse.Load()
			k.signed[challenge] = true
			challenge = k.nextChallenge()
			k.mu.Unlock()
			reply := map[string]any{"type": "authenticated", "user_id": "u1", "auth": "api_key", "challenge": challenge, "id": f["id"]}
			if !ok {
				reply = map[string]any{"type": "error", "code": "UNAUTHENTICATED", "message": "bad key signature", "challenge": challenge, "id": f["id"]}
			}
			if k.silent.Load() {
				b, _ := json.Marshal(reply)
				k.mu.Lock()
				k.held = b
				k.mu.Unlock()
				continue
			}
			write(reply)
		case "subscribe":
			write(map[string]any{"type": "subscribed", "channels": f["channels"], "id": f["id"]})
		case "ping":
			if f["id"] != nil {
				write(map[string]any{"type": "pong", "id": f["id"]})
			}
		}
	}
}

// release sends the held auth_key reply late, on the latest connection.
func (k *keyAuthWS) release() {
	k.mu.Lock()
	c, b := k.conns[len(k.conns)-1], k.held
	k.held = nil
	k.mu.Unlock()
	_ = c.Write(context.Background(), websocket.MessageText, b)
}

// pushLast sends a frame on the latest connection.
func (k *keyAuthWS) pushLast(v any) {
	k.mu.Lock()
	c := k.conns[len(k.conns)-1]
	k.mu.Unlock()
	b, _ := json.Marshal(v)
	_ = c.Write(context.Background(), websocket.MessageText, b)
}

func (k *keyAuthWS) sent() []map[string]any {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]map[string]any(nil), k.authKeys...)
}

func (k *keyAuthWS) drop() {
	k.mu.Lock()
	c := k.conns[len(k.conns)-1]
	k.mu.Unlock()
	c.CloseNow()
}

func (k *keyAuthWS) connCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.conns)
}

func setupKeyAuthWS(t *testing.T, auth string, opts WSOptions) (*WebSocket, *keyAuthWS) {
	t.Helper()
	k := &keyAuthWS{signed: map[string]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(k.handler))
	t.Cleanup(srv.Close)
	c, err := New(Options{BaseURL: srv.URL, AllowInsecure: true, DisableRateLimit: true, APIKey: sigTestKey, APISecret: sigTestSecret, Auth: auth})
	if err != nil {
		t.Fatal(err)
	}
	opts.ReconnectBaseDelay = 10 * time.Millisecond
	if opts.AckTimeout == 0 {
		opts.AckTimeout = time.Second
	}
	ws, err := c.WebSocket(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws, k
}

func TestWSAuthKeyNeedsHMACClient(t *testing.T) { // a client with Auth "headers" cannot sign
	ws, _ := setupKeyAuthWS(t, "headers", WSOptions{})
	_, err := ws.AuthKey(context.Background())
	var ce *ConfigError
	if !errors.As(err, &ce) {
		t.Fatalf("got %v", err)
	}
}

func TestWSAuthKeySignsEachChallengeOnce(t *testing.T) {
	var changes []AuthChange
	var mu sync.Mutex
	ws, k := setupKeyAuthWS(t, "hmac", WSOptions{Handlers: WSHandlers{OnAuthChanged: func(c AuthChange) {
		mu.Lock()
		changes = append(changes, c)
		mu.Unlock()
	}}})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := ws.AuthKey(ctx)
	if err != nil || res.UserID != "u1" || res.Auth != "api_key" {
		t.Fatalf("%+v %v", res, err)
	}
	// A second AuthKey signs the challenge carried by the first reply, not the welcome's.
	if _, err := ws.AuthKey(ctx); err != nil {
		t.Fatal(err)
	}
	// After a reconnect, only the new welcome's challenge is signed.
	k.drop()
	eventually(t, "re-auth after reconnect", func() bool { return k.connCount() == 2 && len(k.sent()) == 3 })
	sigs := map[any]bool{}
	for _, f := range k.sent() {
		if sigs[f["signature"]] {
			t.Fatal("a challenge was signed twice")
		}
		sigs[f["signature"]] = true
		if _, ok := f["secret"]; ok {
			t.Fatal("secret sent")
		}
	}
	eventually(t, "authenticated again", func() bool { return ws.UserID() == "u1" })
}

func TestWSRefusedAuthKeyStopsReauth(t *testing.T) {
	ws, k := setupKeyAuthWS(t, "hmac", WSOptions{})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	k.refuse.Store(true)
	_, err := ws.AuthKey(ctx)
	var we *WSError
	if !errors.As(err, &we) || !we.FromServer || we.Code != "UNAUTHENTICATED" {
		t.Fatalf("got %v", err)
	}
	// The refusal carried the next challenge: a manual retry signs that one.
	k.refuse.Store(false)
	if _, err := ws.AuthKey(ctx); err != nil {
		t.Fatal(err)
	}
	k.refuse.Store(true)
	if _, err := ws.AuthKey(ctx); err == nil {
		t.Fatal("refused auth_key succeeded")
	}
	k.drop()
	eventually(t, "reconnect", func() bool { return k.connCount() == 2 && ws.Connected() })
	time.Sleep(100 * time.Millisecond)
	if n := len(k.sent()); n != 3 {
		t.Fatalf("%d auth_key frames: a refused key must not be re-sent automatically", n)
	}
}

func TestWSAuthKeyLateReplyRacingReconnect(t *testing.T) {
	ws, k := setupKeyAuthWS(t, "hmac", WSOptions{AckTimeout: 100 * time.Millisecond})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	k.silent.Store(true) // the reply is held back
	if _, err := ws.AuthKey(ctx); err == nil {
		t.Fatal("expected a timeout")
	}
	k.silent.Store(false)
	k.release() // the late reply (with its next challenge) arrives ...
	k.drop()    // ... as the connection drops
	eventually(t, "re-auth on the new connection", func() bool { return k.connCount() == 2 && len(k.sent()) == 2 })
	eventually(t, "authenticated with the new welcome's challenge", func() bool { return ws.UserID() == "u1" })
	s := k.sent()
	if s[0]["signature"] == s[1]["signature"] {
		t.Fatal("the old challenge was signed again")
	}
}

func TestWSAuthKeyChallengeIsConsumed(t *testing.T) {
	ws, k := setupKeyAuthWS(t, "hmac", WSOptions{AckTimeout: 100 * time.Millisecond})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	k.silent.Store(true)
	var we *WSError
	if _, err := ws.AuthKey(ctx); !errors.As(err, &we) || we.Code != "TIMEOUT" {
		t.Fatalf("got %v", err)
	}
	if _, err := ws.AuthKey(ctx); !errors.As(err, &we) || we.Code != "NO_CHALLENGE" {
		t.Fatalf("got %v", err)
	}
	if n := len(k.sent()); n != 1 {
		t.Fatalf("%d auth_key frames", n)
	}
}

func TestWSLateRefusalStopsKeyReauth(t *testing.T) {
	ws, k := setupKeyAuthWS(t, "hmac", WSOptions{AckTimeout: 100 * time.Millisecond})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	k.silent.Store(true)
	if _, err := ws.AuthKey(ctx); err == nil {
		t.Fatal("expected a timeout")
	}
	k.pushLast(map[string]any{"type": "error", "code": "UNAUTHENTICATED", "message": "bad key", "challenge": "late", "id": k.sent()[0]["id"]})
	eventually(t, "keyAuth cleared", func() bool {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		return !ws.keyAuth
	})
	k.silent.Store(false)
	k.drop()
	eventually(t, "reconnect", func() bool { return k.connCount() == 2 && ws.Connected() })
	time.Sleep(100 * time.Millisecond)
	if n := len(k.sent()); n != 1 {
		t.Fatalf("%d auth_key frames after a late refusal", n)
	}
}

// slowSigner blocks its first signature until released (a KMS or HSM).
type slowSigner struct {
	a     *HMACAuthenticator
	gate  chan struct{}
	calls atomic.Int32
}

func (s *slowSigner) SignWebSocketChallenge(connectionID, challenge string) (string, string) {
	if s.calls.Add(1) == 1 {
		<-s.gate
	}
	return s.a.SignWebSocketChallenge(connectionID, challenge)
}

func TestWSSlowSignerRacingReconnect(t *testing.T) {
	a, _ := NewHMACAuthenticator(sigTestKey, sigTestSecret)
	slow := &slowSigner{a: a, gate: make(chan struct{})}
	ws, k := setupKeyAuthWS(t, "hmac", WSOptions{KeySigner: slow})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := ws.AuthKey(ctx); done <- err }()
	eventually(t, "signing started", func() bool { return slow.calls.Load() == 1 })
	k.drop() // the connection changes while the signer works
	eventually(t, "re-auth on the new connection", func() bool { return k.connCount() == 2 && ws.UserID() == "u1" })
	close(slow.gate)
	var we *WSError
	if err := <-done; !errors.As(err, &we) || we.Code != "STALE_CHALLENGE" {
		t.Fatalf("got %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := len(k.sent()); n != 1 {
		t.Fatalf("%d auth_key frames: the stale signature must never be sent", n)
	}
}

func TestSignatureExpiredWithoutServerTime(t *testing.T) {
	c, s, _, _ := newSigningClient(t, func(w http.ResponseWriter, r *http.Request, sr signedRequest, n int) {
		writeJSON(w, 401, apiErr("SIGNATURE_EXPIRED", "timestamp outside the window", true))
	})
	_, err := c.Account.Balances(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "SIGNATURE_EXPIRED" || strings.Contains(ae.Message, "clock") {
		t.Fatalf("got %v", err)
	}
	if n := len(s.all()); n != 1 {
		t.Fatalf("%d requests", n)
	}
}

func TestWSKeyRevokedSignsOut(t *testing.T) {
	changes := make(chan AuthChange, 4)
	ws, k := setupKeyAuthWS(t, "hmac", WSOptions{Handlers: WSHandlers{OnAuthChanged: func(c AuthChange) { changes <- c }}})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.AuthKey(ctx); err != nil {
		t.Fatal(err)
	}
	k.mu.Lock()
	c := k.conns[0]
	k.mu.Unlock()
	b, _ := json.Marshal(map[string]any{"type": "signed_out", "reason": "key_revoked"})
	_ = c.Write(ctx, websocket.MessageText, b)
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ch := <-changes:
			if ch.Reason == AuthKeyRevoked {
				if ch.PreviousUserID != "u1" {
					t.Fatalf("%+v", ch)
				}
				k.drop()
				eventually(t, "reconnect", func() bool { return k.connCount() == 2 && ws.Connected() })
				time.Sleep(100 * time.Millisecond)
				if n := len(k.sent()); n != 1 {
					t.Fatalf("%d auth_key frames after key_revoked", n)
				}
				return
			}
		case <-deadline:
			t.Fatal("no key_revoked change")
		}
	}
}
