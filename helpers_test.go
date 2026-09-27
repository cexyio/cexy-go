package cexy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	testKey    = "ak_test_key"
	testSecret = "test_secret"
)

// specDir finds the cexy-api-spec checkout ($CEXY_API_SPEC or ../cexy-api-spec). Without it
// the spec and conformance tests are skipped, unless CEXY_REQUIRE_SPEC=1 (set in CI).
func specDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("CEXY_API_SPEC")
	if dir == "" {
		dir = filepath.Join("..", "cexy-api-spec")
	}
	if _, err := os.Stat(filepath.Join(dir, "surface.yaml")); err != nil {
		if os.Getenv("CEXY_REQUIRE_SPEC") == "1" {
			t.Fatalf("cexy-api-spec not found at %s", dir)
		}
		t.Skipf("cexy-api-spec not found at %s", dir)
	}
	return dir
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// fakeSleep records waits instead of sleeping, and advances a fake clock by them (the rate
// limiter reads it).
type fakeSleep struct {
	mu    sync.Mutex
	waits []time.Duration
	now   time.Time
}

func (f *fakeSleep) sleep(ctx context.Context, d time.Duration) error {
	f.mu.Lock()
	f.waits = append(f.waits, d)
	f.now = f.now.Add(d)
	f.mu.Unlock()
	return ctx.Err()
}

func (f *fakeSleep) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeSleep) all() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.waits...)
}

// recorder keeps every request a test server received.
type recorder struct {
	mu   sync.Mutex
	reqs []*http.Request
	body [][]byte
}

func (r *recorder) add(req *http.Request) {
	b := make([]byte, 0)
	if req.Body != nil {
		buf := make([]byte, 1<<16)
		n, _ := req.Body.Read(buf)
		b = buf[:n]
	}
	r.mu.Lock()
	r.reqs = append(r.reqs, req)
	r.body = append(r.body, b)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reqs)
}

func (r *recorder) get(i int) (*http.Request, []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reqs[i], r.body[i]
}

// newTestClient starts a server running handler and returns a client for it with fake
// sleeps and deterministic jitter.
func newTestClient(t *testing.T, opts Options, handler http.HandlerFunc) (*Client, *recorder, *fakeSleep) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	fs := &fakeSleep{now: time.Now()}
	opts.BaseURL = srv.URL
	opts.AllowInsecure = true
	opts.sleep = fs.sleep
	opts.now = fs.clock
	opts.random = func() float64 { return 0.5 }
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c, rec, fs
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func dataEnv(v any) map[string]any { return map[string]any{"data": v} }

func apiErr(code, msg string, retryable bool) map[string]any {
	return map[string]any{"error": map[string]any{"code": code, "message": msg, "retryable": retryable}}
}

var testOrder = map[string]any{
	"id": "ord_1", "symbol": "BTC/USDT", "side": "buy", "type": "limit", "time_in_force": "gtc", "status": "open",
	"quantity": "0.1", "filled_quantity": "0", "remaining_quantity": "0.1", "filled_quote_quantity": "0",
	"fee_paid": "0", "reserved_remaining": "10", "created_at": "2026-09-27T10:00:00Z", "updated_at": "2026-09-27T10:00:00Z",
}
