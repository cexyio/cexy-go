package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type errorCase struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    map[string]any    `json:"body"`
	Expect  struct {
		Retry              *bool          `json:"retry"`
		WaitSecondsAtLeast float64        `json:"wait_seconds_at_least"`
		ErrorCode          string         `json:"error_code"`
		ErrorClass         string         `json:"error_class"`
		Details            map[string]any `json:"details"`
		SameIdempotencyKey bool           `json:"same_idempotency_key"`
	} `json:"expect"`
}

func TestErrorConformance(t *testing.T) {
	dir := specDir(t)
	files, _ := filepath.Glob(filepath.Join(dir, "conformance", "errors", "*.json"))
	if len(files) == 0 {
		t.Fatal("no error cases")
	}
	for _, f := range files {
		var tc errorCase
		readJSON(t, f, &tc)
		t.Run(filepath.Base(f), func(t *testing.T) {
			var calls atomic.Int32
			c, rec, fs := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, MaxRetries: 1},
				func(w http.ResponseWriter, r *http.Request) {
					if calls.Add(1) == 1 {
						for k, v := range tc.Headers {
							w.Header().Set(k, v)
						}
						writeJSON(w, tc.Status, tc.Body)
						return
					}
					if r.Method == http.MethodPost {
						writeJSON(w, 200, dataEnv(map[string]any{"base_amount": "1", "quote_amount": "1", "shares": "1"}))
						return
					}
					writeJSON(w, 200, dataEnv([]any{}))
				})
			ctx := context.Background()
			var err error
			if tc.Expect.SameIdempotencyKey {
				// A mutation that honours Idempotency-Key.
				_, err = c.Pools.Join(ctx, "BTC/USDT", JoinPoolRequest{BaseAmount: "1", QuoteAmount: "60000"})
			} else {
				_, err = c.Account.Balances(ctx)
			}
			retried := rec.count() == 2
			if tc.Expect.Retry != nil && retried != *tc.Expect.Retry {
				t.Fatalf("retried=%v, want %v (err %v)", retried, *tc.Expect.Retry, err)
			}
			if retried {
				if err != nil {
					t.Fatalf("retry failed: %v", err)
				}
				waits := fs.all()
				if min := time.Duration(tc.Expect.WaitSecondsAtLeast * float64(time.Second)); len(waits) == 0 || waits[len(waits)-1] < min {
					t.Fatalf("waits %v, want >= %v", waits, min)
				}
				if tc.Expect.SameIdempotencyKey {
					a, _ := rec.get(0)
					b, _ := rec.get(1)
					k := a.Header.Get("Idempotency-Key")
					if k == "" || k != b.Header.Get("Idempotency-Key") {
						t.Fatalf("idempotency keys %q / %q", k, b.Header.Get("Idempotency-Key"))
					}
				}
				return
			}
			var ae *APIError
			if !errors.As(err, &ae) {
				t.Fatalf("got %T %v", err, err)
			}
			if tc.Expect.ErrorCode != "" && string(ae.Code) != tc.Expect.ErrorCode {
				t.Errorf("code %s", ae.Code)
			}
			if tc.Expect.ErrorClass == "CexyApiError" {
				for _, k := range []error{ErrValidation, ErrAuthentication, ErrForbidden, ErrNotFound, ErrConflict,
					ErrUnprocessable, ErrRateLimited, ErrServer} {
					if errors.Is(err, k) {
						t.Errorf("unknown code matched %v", k)
					}
				}
				if ae.Known() {
					t.Error("Known() true for an unknown code")
				}
			}
			for k, v := range tc.Expect.Details {
				if ae.Details[k] != v {
					t.Errorf("details[%s] = %v, want %v", k, ae.Details[k], v)
				}
			}
		})
	}
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		status int
		code   string
		want   error
	}{
		{400, "VALIDATION_FAILED", ErrValidation},
		{401, "UNAUTHENTICATED", ErrAuthentication},
		{403, "FORBIDDEN", ErrForbidden},
		{403, "API_KEY_NOT_ALLOWED", ErrForbidden},
		{451, "JURISDICTION_BLOCKED", ErrJurisdictionBlocked},
		{451, "JURISDICTION_BLOCKED", ErrForbidden}, // also a forbidden error
		{404, "NOT_FOUND", ErrNotFound},
		{409, "ALREADY_EXISTS", ErrConflict},
		{422, "INSUFFICIENT_FUNDS", ErrUnprocessable},
		{429, "RATE_LIMITED", ErrRateLimited},
		{503, "SERVICE_UNAVAILABLE", ErrServer},
	}
	for _, tc := range cases {
		body := []byte(`{"error":{"code":"` + tc.code + `","message":"m","retryable":false}}`)
		err := errorFromResponse(tc.status, body, http.Header{}, nil)
		if !errors.Is(err, tc.want) {
			t.Errorf("%d %s: not %v", tc.status, tc.code, tc.want)
		}
	}
	// No envelope (a proxy page): HTTP_<status>, mapped by status.
	err := errorFromResponse(502, []byte("<html>bad gateway</html>"), http.Header{"X-Request-Id": {"r1"}}, nil)
	if err.Code != "HTTP_502" || !errors.Is(err, ErrServer) || !err.Retryable || err.RequestID != "r1" {
		t.Fatalf("%+v", err)
	}
	// Retry-After as an HTTP date.
	h := http.Header{"Retry-After": {time.Now().Add(3 * time.Second).UTC().Format(http.TimeFormat)}}
	if d := errorFromResponse(429, nil, h, nil).RetryAfter; d < time.Second || d > 4*time.Second {
		t.Fatalf("RetryAfter %v", d)
	}
}

func TestRetriesConnectionErrorsThenGivesUp(t *testing.T) {
	var calls atomic.Int32
	c, _, fs := newTestClient(t, Options{MaxRetries: 2}, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close() // no response at all
	})
	_, err := c.Markets.List(context.Background())
	var ce *ConnectionError
	if !errors.As(err, &ce) || calls.Load() != 3 || len(fs.all()) != 2 {
		t.Fatalf("err %v calls %d waits %v", err, calls.Load(), fs.all())
	}
}

func TestTimeoutIsConnectionError(t *testing.T) {
	c, _, _ := newTestClient(t, Options{NoRetries: true, Timeout: 50 * time.Millisecond}, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	_, err := c.Markets.List(context.Background())
	var ce *ConnectionError
	if !errors.As(err, &ce) || !ce.Timeout {
		t.Fatalf("got %v", err)
	}
}

func TestContextCancelStopsRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	c, _, _ := newTestClient(t, Options{MaxRetries: 5}, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		cancel()
		writeJSON(w, 503, apiErr("SERVICE_UNAVAILABLE", "down", true))
	})
	_, err := c.Markets.List(ctx)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrServer) {
		t.Fatalf("got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls %d", calls.Load())
	}
}

func TestNonRetryableIsNotRetried(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 404, apiErr("NOT_FOUND", "no market", false))
	})
	if _, err := c.Markets.Get(context.Background(), "X/Y"); !errors.Is(err, ErrNotFound) || rec.count() != 1 {
		t.Fatalf("err %v count %d", err, rec.count())
	}
}

func TestOnRetryHook(t *testing.T) {
	var infos []RetryInfo
	var calls atomic.Int32
	c, _, _ := newTestClient(t, Options{OnRetry: func(i RetryInfo) { infos = append(infos, i) }},
		func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				w.Header().Set("Retry-After", strconv.Itoa(1))
				writeJSON(w, 429, apiErr("RATE_LIMITED", "slow down", true))
				return
			}
			writeJSON(w, 200, dataEnv([]any{}))
		})
	if _, err := c.Markets.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Operation != OpListMarkets || infos[0].Attempt != 1 || infos[0].Delay < time.Second {
		t.Fatalf("%+v", infos)
	}
}

// QA: credentials echoed back by the server are redacted everywhere in Details and Fields,
// nested values and object keys included.
func TestErrorDetailsAreRedactedRecursively(t *testing.T) {
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, MaxRetries: 0},
		func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 400, map[string]any{"error": map[string]any{
				"code": "VALIDATION_FAILED", "message": "bad " + testSecret, "retryable": false,
				"details": map[string]any{
					"nested": map[string]any{"echo": "x" + testSecret, testSecret: 1},
					"list":   []any{testKey, map[string]any{"deep": testSecret}},
					testKey:  "key as a key",
				},
				"fields": map[string]any{testSecret: "value " + testKey},
			}})
		})
	_, err := c.Account.Balances(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) {
		t.Fatalf("err %v", err)
	}
	dump, _ := json.Marshal(map[string]any{"details": ae.Details, "fields": ae.Fields, "message": ae.Message})
	for _, s := range []string{testSecret, testKey} {
		if strings.Contains(string(dump), s) || strings.Contains(fmt.Sprintf("%+v %v", ae, ae), s) {
			t.Fatalf("%q leaked: %s", s, dump)
		}
	}
	if ae.Details["nested"].(map[string]any)["echo"] == nil {
		t.Fatalf("structure lost: %s", dump)
	}
}
