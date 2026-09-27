package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type serverWaitsCase struct {
	ID                  string `json:"id"`
	ResponsesRepeatLast bool   `json:"responses_repeat_last"`
	Responses           []struct {
		HTTPStatus int               `json:"http_status"`
		Headers    map[string]string `json:"headers"`
		Error      json.RawMessage   `json:"error"`
		Body       json.RawMessage   `json:"body"`
	} `json:"responses"`
	ThenCalls int `json:"then_calls"`
	Expect    struct {
		Calls            int       `json:"calls"`
		SleepsS          []float64 `json:"sleeps_s"`
		OK               bool      `json:"ok"`
		ErrorCode        string    `json:"error_code"`
		RetryAfterS      float64   `json:"retry_after_s"`
		NoSleepLongerThn float64   `json:"no_sleep_longer_than_s"`
	} `json:"expect"`
}

// Server-controlled waits are untrusted: never a panic, overflow or hang; a hint above
// MaxServerWait fails fast (conformance/transport/server_waits.json).
func TestServerWaitsConformance(t *testing.T) {
	dir := specDir(t)
	var spec struct {
		MaxServerWaitS float64           `json:"max_server_wait_s"`
		Cases          []serverWaitsCase `json:"cases"`
	}
	readJSON(t, filepath.Join(dir, "conformance", "transport", "server_waits.json"), &spec)
	if len(spec.Cases) == 0 || spec.MaxServerWaitS != MaxServerWait.Seconds() {
		t.Fatalf("cases %d, max_server_wait_s %v", len(spec.Cases), spec.MaxServerWaitS)
	}
	for _, tc := range spec.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			var n atomic.Int32
			c, _, fs := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				i := int(n.Add(1)) - 1
				if i >= len(tc.Responses) {
					i = len(tc.Responses) - 1
				}
				item := tc.Responses[i]
				for k, v := range item.Headers {
					w.Header().Set(k, v)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(item.HTTPStatus)
				if item.HTTPStatus >= 400 {
					_, _ = w.Write([]byte(`{"error":` + string(item.Error) + `}`))
					return
				}
				_, _ = w.Write(item.Body)
			})
			ctx := context.Background()
			_, err := c.Time(ctx)
			for i := 0; i < tc.ThenCalls && err == nil; i++ {
				_, err = c.Time(ctx)
			}
			if got := int(n.Load()); got != tc.Expect.Calls {
				t.Fatalf("calls %d, want %d", got, tc.Expect.Calls)
			}
			if tc.Expect.OK != (err == nil) {
				t.Fatalf("err %v, ok want %v", err, tc.Expect.OK)
			}
			if tc.Expect.ErrorCode != "" {
				var ae *APIError
				if !errors.As(err, &ae) || string(ae.Code) != tc.Expect.ErrorCode {
					t.Fatalf("err %v, want %s", err, tc.Expect.ErrorCode)
				}
				if tc.Expect.RetryAfterS > 0 && math.Abs(ae.RetryAfter.Seconds()-tc.Expect.RetryAfterS) > 1 {
					t.Fatalf("RetryAfter %v, want %vs", ae.RetryAfter, tc.Expect.RetryAfterS)
				}
			}
			sleeps := fs.all()
			if tc.Expect.SleepsS != nil {
				if len(sleeps) != len(tc.Expect.SleepsS) {
					t.Fatalf("sleeps %v, want %v", sleeps, tc.Expect.SleepsS)
				}
				for i, want := range tc.Expect.SleepsS {
					if got := sleeps[i].Seconds(); got < want || got > want+1 {
						t.Fatalf("sleeps %v, want %v (+ up to 1 s)", sleeps, tc.Expect.SleepsS)
					}
				}
			}
			if lim := tc.Expect.NoSleepLongerThn; lim > 0 {
				for _, d := range sleeps {
					if d.Seconds() > lim {
						t.Fatalf("slept %v, limit %vs", d, lim)
					}
				}
			}
		})
	}
}

func TestSecondsDurationNeverOverflows(t *testing.T) {
	for _, secs := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -5, 0, 1e300, 1e10} {
		d := secondsDuration(secs)
		if d < 0 {
			t.Fatalf("secondsDuration(%v) = %v", secs, d)
		}
	}
	if secondsDuration(1e300) != time.Duration(math.MaxInt64) || secondsDuration(1.5) != 1500*time.Millisecond {
		t.Fatal("saturation or conversion wrong")
	}
	now := time.Now()
	for _, reset := range []float64{1e300, 9999999999, 1e13, 5} {
		if d := resetDuration(reset, now); d < 0 || d > MaxServerWait {
			t.Fatalf("resetDuration(%v) = %v", reset, d)
		}
	}
}
