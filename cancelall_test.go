package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type cancelAllCase struct {
	ID                  string            `json:"id"`
	Options             map[string]int    `json:"options"`
	ResponsesRepeatLast bool              `json:"responses_repeat_last"`
	Responses           []json.RawMessage `json:"responses"`
	Expect              struct {
		Calls         int               `json:"calls"`
		SleepsS       []float64         `json:"sleeps_s"`
		Stopped       string            `json:"stopped"`
		Cancelled     []string          `json:"cancelled"`
		AlreadyClosed []string          `json:"already_closed"`
		Failed        []string          `json:"failed"`
		FailureCodes  map[string]string `json:"failure_codes"`
		LastErrorCode string            `json:"last_error_code"`
		ErrorCode     string            `json:"error_code"`
		Partial       []string          `json:"partial_cancelled"`
	} `json:"expect"`
}

// cancelAllItem is one scripted response: a `data` object, or an error response when
// http_status is set.
type cancelAllItem struct {
	HTTPStatus int               `json:"http_status"`
	Headers    map[string]string `json:"headers"`
	Error      json.RawMessage   `json:"error"`
}

// sequence answers cancel-all with the given data objects in order (repeating the last one when
// repeat is set) and counts the calls.
func sequence(t *testing.T, responses []json.RawMessage, repeat bool) (http.HandlerFunc, *atomic.Int32) {
	var n atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		i := int(n.Add(1)) - 1
		if i >= len(responses) {
			if !repeat {
				t.Errorf("unexpected call %d", i+1)
				writeJSON(w, 500, apiErr("INTERNAL", "no more responses", false))
				return
			}
			i = len(responses) - 1
		}
		var item cancelAllItem
		_ = json.Unmarshal(responses[i], &item)
		w.Header().Set("Content-Type", "application/json")
		if item.HTTPStatus != 0 {
			for k, v := range item.Headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(item.HTTPStatus)
			_, _ = w.Write([]byte(`{"error":` + string(item.Error) + `}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":` + string(responses[i]) + `}`))
	}, &n
}

func sameSet(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

func TestCancelAllUntilDoneConformance(t *testing.T) {
	dir := specDir(t)
	var spec struct {
		Cases []cancelAllCase `json:"cases"`
	}
	readJSON(t, filepath.Join(dir, "conformance", "trading", "cancel_all_until_done.json"), &spec)
	if len(spec.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, tc := range spec.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			handler, calls := sequence(t, tc.Responses, tc.ResponsesRepeatLast)
			c, rec, fs := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, DisableRateLimit: true}, handler)
			o := CancelAllOptions{Symbol: "BTC/USDT", MaxRounds: tc.Options["max_rounds"],
				TimeBudget: time.Duration(tc.Options["time_budget_s"]) * time.Second}
			got, err := c.Trading.CancelAllUntilDone(context.Background(), o)
			if tc.Expect.ErrorCode != "" {
				var ae *APIError
				if !errors.As(err, &ae) || string(ae.Code) != tc.Expect.ErrorCode {
					t.Fatalf("err %v, want %s", err, tc.Expect.ErrorCode)
				}
				if int(calls.Load()) != tc.Expect.Calls || !sameSet(got.Cancelled, tc.Expect.Partial) {
					t.Fatalf("calls %d, partial %+v", calls.Load(), got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.LastErrorCode != tc.Expect.LastErrorCode {
				t.Fatalf("last error code %q, want %q", got.LastErrorCode, tc.Expect.LastErrorCode)
			}
			if int(calls.Load()) != tc.Expect.Calls || got.Rounds != tc.Expect.Calls {
				t.Fatalf("calls %d, rounds %d, want %d", calls.Load(), got.Rounds, tc.Expect.Calls)
			}
			var sleeps []float64
			for _, d := range fs.all() {
				sleeps = append(sleeps, d.Seconds())
			}
			if !slices.Equal(sleeps, tc.Expect.SleepsS) && !(len(sleeps) == 0 && len(tc.Expect.SleepsS) == 0) {
				t.Fatalf("sleeps %v, want %v", sleeps, tc.Expect.SleepsS)
			}
			if got.Stopped != tc.Expect.Stopped {
				t.Fatalf("stopped %q, want %q", got.Stopped, tc.Expect.Stopped)
			}
			if !sameSet(got.Cancelled, tc.Expect.Cancelled) || !sameSet(got.AlreadyClosed, tc.Expect.AlreadyClosed) ||
				!sameSet(got.Failed, tc.Expect.Failed) {
				t.Fatalf("got %+v", got)
			}
			codes := map[string]string{}
			for _, f := range got.Failures {
				codes[f.OrderID] = f.Code
			}
			if len(codes) != len(tc.Expect.FailureCodes) {
				t.Fatalf("failure codes %v, want %v", codes, tc.Expect.FailureCodes)
			}
			for id, code := range tc.Expect.FailureCodes {
				if codes[id] != code {
					t.Fatalf("failure codes %v, want %v", codes, tc.Expect.FailureCodes)
				}
			}
			for i := 0; i < rec.count(); i++ {
				if r, _ := rec.get(i); r.Header.Get("Idempotency-Key") != "" {
					t.Fatal("cancel-all sent an Idempotency-Key")
				}
			}
		})
	}
}

func TestCancelAllIsOneCallByDefault(t *testing.T) {
	handler, calls := sequence(t, []json.RawMessage{
		json.RawMessage(`{"cancelled":["o1"],"already_closed":["o2"],"failed":["o3"],"has_more":true,` +
			`"failures":[{"order_id":"o3","code":"INVALID_STATE","message":"still being placed"}]}`),
	}, false)
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, handler)
	res, err := c.Trading.CancelAll(context.Background(), "BTC/USDT")
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || !res.HasMore || res.AlreadyClosed[0] != "o2" || res.Failures[0].Code != "INVALID_STATE" {
		t.Fatalf("calls %d, res %+v", calls.Load(), res)
	}
	if r, _ := rec.get(0); r.Header.Get("Idempotency-Key") != "" {
		t.Fatal("cancel-all sent an Idempotency-Key")
	}
}

func TestCancelAllV1ResponseGetsEmptySlices(t *testing.T) {
	handler, _ := sequence(t, []json.RawMessage{json.RawMessage(`{"cancelled":["o1"],"failed":[]}`)}, false)
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, handler)
	res, err := c.Trading.CancelAllMarkets(context.Background())
	if err != nil || res.AlreadyClosed == nil || res.Failures == nil || res.HasMore {
		t.Fatalf("res %+v, err %v", res, err)
	}
}

func TestCancelAllUntilDoneNeedsAnExplicitTarget(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {})
	ctx := context.Background()
	if _, err := c.Trading.CancelAllUntilDone(ctx, CancelAllOptions{}); err == nil {
		t.Fatal("no target accepted")
	}
	if _, err := c.Trading.CancelAllUntilDone(ctx, CancelAllOptions{Symbol: "BTC/USDT", AllMarkets: true}); err == nil {
		t.Fatal("both targets accepted")
	}
	if rec.count() != 0 {
		t.Fatal("request sent")
	}
}

// QA: 20 rounds of has_more, each with progress, send at most MaxRounds (default 20) requests,
// which keeps the loop under the server's 30 calls a minute.
func TestCancelAllUntilDoneNeverExceedsMaxRounds(t *testing.T) {
	var n atomic.Int32
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		i := n.Add(1)
		writeJSON(w, 200, dataEnv(map[string]any{"cancelled": []string{"o" + strconv.Itoa(int(i))},
			"already_closed": []string{}, "failed": []string{}, "failures": []any{}, "has_more": true}))
	})
	got, err := c.Trading.CancelAllUntilDone(context.Background(), CancelAllOptions{AllMarkets: true})
	if err != nil {
		t.Fatal(err)
	}
	if n.Load() != 20 || got.Rounds != 20 || got.Stopped != CancelStoppedMaxRounds || len(got.Cancelled) != 20 {
		t.Fatalf("requests %d, summary %+v", n.Load(), got)
	}
}

// QA: a 429 inside the loop is retried after the server's wait, and that wait counts against the
// time budget (the loop and the transport share the clock).
func TestCancelAllUntilDone429WaitCountsAgainstTheBudget(t *testing.T) {
	stuck := map[string]any{"cancelled": []string{}, "already_closed": []string{}, "failed": []string{"p1"}, "has_more": false,
		"failures": []map[string]string{{"order_id": "p1", "code": "INVALID_STATE", "message": "still being placed"}}}
	var n atomic.Int32
	c, _, fs := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, DisableRateLimit: true},
		func(w http.ResponseWriter, r *http.Request) {
			if n.Add(1) == 2 {
				w.Header().Set("Retry-After", "110")
				e := apiErr("RATE_LIMITED", "30 calls a minute", true)
				e["error"].(map[string]any)["details"] = map[string]any{"retry_after_seconds": 110}
				writeJSON(w, 429, e)
				return
			}
			writeJSON(w, 200, dataEnv(stuck))
		})
	got, err := c.Trading.CancelAllUntilDone(context.Background(), CancelAllOptions{Symbol: "BTC/USDT"})
	if err != nil {
		t.Fatal(err)
	}
	var total time.Duration
	for _, d := range fs.all() {
		total += d
	}
	if got.Stopped != CancelStoppedTimeBudget || total >= 120*time.Second || total < 110*time.Second {
		t.Fatalf("stopped %q after %v of sleeps (%v)", got.Stopped, total, fs.all())
	}
	if !slices.Contains(fs.all(), 110*time.Second) { // the loop waits Retry-After exactly
		t.Fatalf("429 wait not honoured: %v", fs.all())
	}
}

// QA: an id that ends cancelled or already_closed never also appears in Failed or Failures.
func TestCancelAllUntilDoneFinalStateWins(t *testing.T) {
	handler, _ := sequence(t, []json.RawMessage{
		json.RawMessage(`{"cancelled":[],"already_closed":[],"failed":["a","b","c"],"has_more":false,"failures":[` +
			`{"order_id":"a","code":"INVALID_STATE","message":"x"},{"order_id":"b","code":"INVALID_STATE","message":"x"},` +
			`{"order_id":"c","code":"SERVICE_UNAVAILABLE","message":"x"}]}`),
		json.RawMessage(`{"cancelled":["a"],"already_closed":["b"],"failed":["c"],"has_more":false,"failures":[` +
			`{"order_id":"c","code":"MARKET_UNAVAILABLE","message":"halted"}]}`),
	}, false)
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, DisableRateLimit: true}, handler)
	got, err := c.Trading.CancelAllUntilDone(context.Background(), CancelAllOptions{Symbol: "BTC/USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if !sameSet(got.Cancelled, []string{"a"}) || !sameSet(got.AlreadyClosed, []string{"b"}) || !sameSet(got.Failed, []string{"c"}) {
		t.Fatalf("got %+v", got)
	}
	if len(got.Failures) != 1 || got.Failures[0].OrderID != "c" || got.Failures[0].Code != "MARKET_UNAVAILABLE" {
		t.Fatalf("failures %+v", got.Failures)
	}
}

func TestCancelAllUntilDoneStopsWhenContextEnds(t *testing.T) {
	handler, _ := sequence(t, []json.RawMessage{json.RawMessage(`{"cancelled":[],"already_closed":[],"failed":["p"],"has_more":false,` +
		`"failures":[{"order_id":"p","code":"INVALID_STATE","message":"x"}]}`)}, true)
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, DisableRateLimit: true}, handler)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := c.Trading.CancelAllUntilDone(ctx, CancelAllOptions{Symbol: "BTC/USDT"})
	if err == nil {
		t.Fatalf("no error, summary %+v", got)
	}
}

// QA M2: every round is one HTTP request, even when every round fails with a retryable 503.
func TestCancelAllUntilDoneRoundsAreSingleRequests(t *testing.T) {
	var n atomic.Int32
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, DisableRateLimit: true},
		func(w http.ResponseWriter, r *http.Request) {
			n.Add(1)
			writeJSON(w, 503, apiErr("SERVICE_UNAVAILABLE", "busy", true))
		})
	got, err := c.Trading.CancelAllUntilDone(context.Background(), CancelAllOptions{Symbol: "BTC/USDT",
		MaxRounds: 20, TimeBudget: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if n.Load() != 20 || got.Rounds != 20 || got.Stopped != CancelStoppedMaxRounds || got.LastErrorCode != "SERVICE_UNAVAILABLE" {
		t.Fatalf("requests %d, summary %+v", n.Load(), got)
	}
}

// QA M2: a 429 asking for more than the remaining budget stops the loop without sleeping past it.
func TestCancelAllUntilDoneRefusesAWaitPastTheBudget(t *testing.T) {
	c, _, fs := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, DisableRateLimit: true},
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "300")
			writeJSON(w, 429, apiErr("RATE_LIMITED", "slow down", true))
		})
	got, err := c.Trading.CancelAllUntilDone(context.Background(), CancelAllOptions{AllMarkets: true})
	if err != nil {
		t.Fatal(err)
	}
	if got.Stopped != CancelStoppedTimeBudget || got.Rounds != 1 || len(fs.all()) != 0 {
		t.Fatalf("summary %+v, sleeps %v", got, fs.all())
	}
}
