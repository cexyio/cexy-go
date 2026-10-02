package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// Every futures operation: method, path, query, and whether it is signed.
func TestFuturesOperations(t *testing.T) {
	bodies := map[string]any{
		"/api/v1/futures/markets":                 map[string]any{"as_of": "2026-10-02T10:00:00Z", "markets": []any{map[string]any{"coin": "BTC", "max_leverage": 40}}, "stale": false},
		"/api/v1/futures/markets/BTC":             map[string]any{"as_of": "2026-10-02T10:00:00Z", "market": map[string]any{"coin": "BTC"}, "stale": false},
		"/api/v1/futures/markets/BTC/orderbook":   map[string]any{"as_of": "2026-10-02T10:00:00Z", "coin": "BTC", "bids": []any{map[string]any{"price": "1", "size": "2"}}, "asks": []any{}, "stale": true},
		"/api/v1/futures/markets/BTC/candles":     map[string]any{"as_of": "2026-10-02T10:00:00Z", "coin": "BTC", "interval": "1h", "candles": []any{map[string]any{"open_time": 1, "close": "1"}}, "stale": false},
		"/api/v1/futures/markets/k%2FPEPE/trades": map[string]any{"coin": "k/PEPE", "trades": []any{map[string]any{"side": "buy", "price": "1", "size": "1", "time": 5}}, "stale": false},
		"/api/v1/futures/positions":               map[string]any{"has_account": true, "positions": map[string]any{"account_value": "10", "positions": []any{map[string]any{"coin": "BTC", "size": "0.1"}}}, "stale": false},
		"/api/v1/futures/orders":                  map[string]any{"has_account": true, "orders": []any{map[string]any{"id": "o1", "coin": "BTC"}}, "stale": false},
		"/api/v1/futures/fills":                   map[string]any{"has_account": true, "fills": []any{map[string]any{"id": "f1"}}, "next_cursor": "c 1"},
		"/api/v1/futures/funding":                 map[string]any{"has_account": true, "funding": []any{map[string]any{"coin": "BTC", "rate": "0.0001"}}, "next_cursor": nil},
	}
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		b, ok := bodies[r.URL.EscapedPath()]
		if !ok {
			writeJSON(w, 404, apiErr("NOT_FOUND", r.URL.EscapedPath(), false))
			return
		}
		writeJSON(w, 200, dataEnv(b))
	})
	ctx := context.Background()
	cases := []struct {
		op     OperationID
		path   string
		query  string
		signed bool
		run    func() (any, error)
		check  func(v any) bool
	}{
		{OpMarkets, "/api/v1/futures/markets", "", false,
			func() (any, error) { return c.Futures.Markets(ctx) },
			func(v any) bool {
				m := v.(FuturesMarkets)
				return len(m.Markets) == 1 && m.Markets[0].MaxLeverage == 40
			}},
		{OpMarket, "/api/v1/futures/markets/BTC", "", false,
			func() (any, error) { return c.Futures.Market(ctx, "BTC") },
			func(v any) bool { return v.(FuturesMarket).Market.Coin == "BTC" }},
		{OpOrderbook, "/api/v1/futures/markets/BTC/orderbook", "depth=5", false,
			func() (any, error) { return c.Futures.OrderBook(ctx, "BTC", 5) },
			func(v any) bool { b := v.(FuturesBook); return b.Stale && len(b.Bids) == 1 && b.Bids[0].Size == "2" }},
		{OpCandles, "/api/v1/futures/markets/BTC/candles", "before=1790000000000&interval=1h", false,
			func() (any, error) {
				return c.Futures.Candles(ctx, "BTC", CandlesParams{Interval: "1h", Before: Ptr(int64(1790000000000))})
			},
			func(v any) bool { return len(v.(FuturesCandles).Candles) == 1 }},
		{OpTrades, "/api/v1/futures/markets/k%2FPEPE/trades", "limit=10", false,
			func() (any, error) { return c.Futures.Trades(ctx, "k/PEPE", 10) },
			func(v any) bool { tr := v.(FuturesTrades); return len(tr.Trades) == 1 && tr.Trades[0].Time == 5 }},
		{OpPositions, "/api/v1/futures/positions", "", true,
			func() (any, error) { return c.Futures.Positions(ctx) },
			func(v any) bool {
				p := v.(FuturesPositions)
				return p.HasAccount && p.Positions != nil && len(p.Positions.Positions) == 1
			}},
		{OpOpenOrders, "/api/v1/futures/orders", "", true,
			func() (any, error) { return c.Futures.OpenOrders(ctx) },
			func(v any) bool { return len(v.(FuturesOpenOrders).Orders) == 1 }},
		{OpFills, "/api/v1/futures/fills", "cursor=a%2Bb", true,
			func() (any, error) { return c.Futures.Fills(ctx, "a+b") },
			func(v any) bool {
				f := v.(FuturesFills)
				return len(f.Fills) == 1 && f.NextCursor != nil && *f.NextCursor == "c 1"
			}},
		{OpFunding, "/api/v1/futures/funding", "", true,
			func() (any, error) { return c.Futures.Funding(ctx, "") },
			func(v any) bool { f := v.(FuturesFunding); return len(f.Funding) == 1 && f.NextCursor == nil }},
	}
	covered := map[OperationID]bool{}
	for i, tc := range cases {
		t.Run(string(tc.op), func(t *testing.T) {
			info := operations[tc.op]
			if info.Method != http.MethodGet || !strings.HasPrefix(info.Path, "/api/v1/futures/") {
				t.Fatalf("operation table: %+v", info)
			}
			if (info.Auth == "api_key") != tc.signed || (tc.signed && info.Scope != "read") {
				t.Fatalf("auth %s scope %s", info.Auth, info.Scope)
			}
			v, err := tc.run()
			if err != nil {
				t.Fatal(err)
			}
			if !tc.check(v) {
				t.Fatalf("decoded %+v", v)
			}
			req, _ := rec.get(i)
			if req.Method != http.MethodGet || req.URL.EscapedPath() != tc.path || req.URL.RawQuery != tc.query {
				t.Fatalf("sent %s %s?%s", req.Method, req.URL.EscapedPath(), req.URL.RawQuery)
			}
			if signed := req.Header.Get("X-API-Signature") != ""; signed != tc.signed {
				t.Fatalf("signed = %v", signed)
			}
			if req.Header.Get("X-API-Secret") != "" {
				t.Fatal("secret sent")
			}
			covered[tc.op] = true
		})
	}
	for op, info := range operations {
		if strings.HasPrefix(info.Path, "/api/v1/futures/") && !covered[op] {
			t.Errorf("%s (%s) not covered", op, info.Path)
		}
	}
	if len(covered) != 9 {
		t.Fatalf("covered %d futures operations, want 9", len(covered))
	}
}

func TestFuturesLocalChecks(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv(map[string]any{}))
	})
	ctx := context.Background()
	var ce *ConfigError
	if _, err := c.Futures.Candles(ctx, "BTC", CandlesParams{}); !errors.As(err, &ce) {
		t.Fatalf("missing interval: %v", err)
	}
	if _, err := c.Futures.Positions(ctx); !errors.As(err, &ce) {
		t.Fatalf("no key: %v", err)
	}
	for range c.Futures.AllFills(ctx) {
	}
	if _, err := c.Futures.Market(ctx, ".."); !errors.As(err, &ce) {
		t.Fatalf("dot segment: %v", err)
	}
	if rec.count() != 0 {
		t.Fatalf("requests = %d, want 0", rec.count())
	}
	// Defaults: no depth and no limit are sent.
	if _, err := c.Futures.OrderBook(ctx, "BTC", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Futures.Trades(ctx, "BTC", 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if req, _ := rec.get(i); req.URL.RawQuery != "" {
			t.Fatalf("query %q", req.URL.RawQuery)
		}
	}
}

type pagingCase struct {
	ID    string `json:"id"`
	Pages []struct {
		RequestCursor *string         `json:"request_cursor"`
		Response      json.RawMessage `json:"response"`
	} `json:"pages"`
	Expect struct {
		IDs            []string `json:"ids"`
		IDsBeforeError []string `json:"ids_before_error"`
		Requests       int      `json:"requests"`
		Sleeps         int      `json:"sleeps"`
		EncodedQuery2  string   `json:"encoded_query_of_request_2"`
		ErrorCode      string   `json:"error_code"`
		ErrorRetryable bool     `json:"error_retryable"`
		HasAccount     *bool    `json:"has_account"`
	} `json:"expect"`
}

// pagingServer answers the case's pages in order, failing the test when the SDK sends another
// cursor than the one the page expects.
func pagingServer(t *testing.T, tc pagingCase, rename func(json.RawMessage) json.RawMessage) http.HandlerFunc {
	var mu sync.Mutex
	i := 0
	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n := i
		i++
		mu.Unlock()
		if n >= len(tc.Pages) {
			t.Errorf("request %d: only %d pages", n+1, len(tc.Pages))
			writeJSON(w, 500, apiErr("INTERNAL_ERROR", "no more pages", false))
			return
		}
		page := tc.Pages[n]
		q := r.URL.Query()
		switch {
		case page.RequestCursor == nil && q.Has("cursor"):
			t.Errorf("request %d: cursor %q sent, want none", n+1, q.Get("cursor"))
		case page.RequestCursor != nil && q.Get("cursor") != *page.RequestCursor:
			t.Errorf("request %d: cursor %q, want %q", n+1, q.Get("cursor"), *page.RequestCursor)
		}
		body := page.Response
		if rename != nil {
			body = rename(body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":` + string(body) + `}`))
	}
}

// Conformance: conformance/futures/history_paging.json, against AllFills and (with the rows
// renamed) AllFunding.
func TestFuturesHistoryPagingConformance(t *testing.T) {
	dir := specDir(t)
	var spec struct {
		Operation string       `json:"operation"`
		MaxBusy   int          `json:"max_busy_retries"`
		Cases     []pagingCase `json:"cases"`
	}
	readJSON(t, filepath.Join(dir, "conformance", "futures", "history_paging.json"), &spec)
	if spec.Operation != "GET /api/v1/futures/fills" || spec.MaxBusy != DefaultMaxBusyRetries || len(spec.Cases) == 0 {
		t.Fatalf("unexpected file: %s, max_busy_retries %d, %d cases", spec.Operation, spec.MaxBusy, len(spec.Cases))
	}
	// Funding rows have no id: the conformance ids are carried in "coin" for the funding run.
	toFunding := func(b json.RawMessage) json.RawMessage {
		var page map[string]any
		_ = json.Unmarshal(b, &page)
		var rows []any
		for _, f := range page["fills"].([]any) {
			rows = append(rows, map[string]any{"coin": f.(map[string]any)["id"], "amount": "1", "rate": "0.0001",
				"position_size": "1", "time": f.(map[string]any)["time"]})
		}
		if rows == nil {
			rows = []any{}
		}
		delete(page, "fills")
		page["funding"] = rows
		out, _ := json.Marshal(page)
		return out
	}
	seen := map[string]bool{}
	for _, tc := range spec.Cases {
		seen[tc.ID] = true
		for _, kind := range []string{"fills", "funding"} {
			t.Run(tc.ID+"/"+kind, func(t *testing.T) {
				var rename func(json.RawMessage) json.RawMessage
				path := "/api/v1/futures/fills"
				if kind == "funding" {
					rename, path = toFunding, "/api/v1/futures/funding"
				}
				// Retries disabled: the busy-page retries are a setting of their own.
				c, rec, fs := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, NoRetries: true}, pagingServer(t, tc, rename))
				ctx := context.Background()
				ids := []string{}
				var gotErr error
				if kind == "fills" {
					for f, err := range c.Futures.AllFills(ctx) {
						if err != nil {
							gotErr = err
							break
						}
						ids = append(ids, f.ID)
					}
				} else {
					for f, err := range c.Futures.AllFunding(ctx) {
						if err != nil {
							gotErr = err
							break
						}
						ids = append(ids, f.Coin)
					}
				}
				want := tc.Expect.IDs
				if tc.Expect.ErrorCode != "" {
					want = tc.Expect.IDsBeforeError
				}
				if want == nil {
					want = []string{}
				}
				if !reflect.DeepEqual(ids, want) {
					t.Errorf("ids %v, want %v", ids, want)
				}
				if rec.count() != tc.Expect.Requests {
					t.Errorf("requests %d, want %d", rec.count(), tc.Expect.Requests)
				}
				if got := len(fs.all()); got != tc.Expect.Sleeps {
					t.Errorf("sleeps %d, want %d", got, tc.Expect.Sleeps)
				}
				for i := 0; i < rec.count(); i++ {
					req, _ := rec.get(i)
					if req.URL.Path != path || req.Header.Get("X-API-Signature") == "" {
						t.Errorf("request %d: %s signed=%v", i+1, req.URL.Path, req.Header.Get("X-API-Signature") != "")
					}
				}
				if tc.Expect.EncodedQuery2 != "" {
					req, _ := rec.get(1)
					if req.URL.RawQuery != tc.Expect.EncodedQuery2 {
						t.Errorf("request 2 query %q, want %q", req.URL.RawQuery, tc.Expect.EncodedQuery2)
					}
				}
				if tc.Expect.ErrorCode == "" {
					if gotErr != nil {
						t.Fatalf("unexpected error %v", gotErr)
					}
				} else {
					var ae *APIError
					if !errors.As(gotErr, &ae) {
						t.Fatalf("error %v (%T), want *APIError", gotErr, gotErr)
					}
					if string(ae.Code) != tc.Expect.ErrorCode || (ae.Code != CodePagingStalled && ae.Code != CodePagingCursorRepeated) {
						t.Errorf("code %s, want %s", ae.Code, tc.Expect.ErrorCode)
					}
					if ae.Retryable != tc.Expect.ErrorRetryable || isRetryable(gotErr) != tc.Expect.ErrorRetryable {
						t.Errorf("retryable %v/%v, want %v", ae.Retryable, isRetryable(gotErr), tc.Expect.ErrorRetryable)
					}
					last := tc.Pages[len(tc.Pages)-1].RequestCursor
					if ae.Status != 0 || last == nil || ae.Details["cursor"] != *last {
						t.Errorf("status %d details %v", ae.Status, ae.Details)
					}
					if !strings.Contains(gotErr.Error(), tc.Expect.ErrorCode) {
						t.Errorf("message %q", gotErr.Error())
					}
				}
				if tc.Expect.HasAccount != nil && kind == "fills" {
					// The iterator cannot say why it ended: one page through Fills does.
					c2, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, pagingServer(t, tc, nil))
					p, err := c2.Futures.Fills(ctx, "")
					if err != nil || p.HasAccount != *tc.Expect.HasAccount {
						t.Errorf("has_account %v (%v), want %v", p.HasAccount, err, *tc.Expect.HasAccount)
					}
				}
			})
		}
	}
	for _, id := range []string{"short_pages_until_null", "cursor_sent_back_verbatim", "busy_provider_same_cursor_retried",
		"busy_provider_gives_up_after_max_retries", "nonempty_page_repeating_cursor_fails", "no_futures_account"} {
		if !seen[id] {
			t.Errorf("case %s missing from the conformance file", id)
		}
	}
}

func stalledHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("cursor") {
			writeJSON(w, 200, dataEnv(map[string]any{"has_account": true, "fills": []any{map[string]any{"id": "f1"}}, "next_cursor": "c:S"}))
			return
		}
		writeJSON(w, 200, dataEnv(map[string]any{"has_account": true, "fills": []any{}, "next_cursor": r.URL.Query().Get("cursor")}))
	}
}

func TestFuturesBusyRetriesAreTheirOwnSetting(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		opts     Options
		iter     []IterOption
		requests int
	}{
		{"default with retries off", Options{NoRetries: true}, nil, 5},
		{"default with client retries 8", Options{MaxRetries: 8}, nil, 5},
		{"busy 5", Options{NoRetries: true}, []IterOption{WithMaxBusyRetries(5)}, 7},
		{"busy 0", Options{}, []IterOption{WithMaxBusyRetries(0)}, 2},
		{"busy 1, call retries 0", Options{}, []IterOption{WithMaxBusyRetries(1), WithCallOptions(WithMaxRetries(0))}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.APIKey, tc.opts.APISecret = testKey, testSecret
			var retries []RetryInfo
			tc.opts.OnRetry = func(i RetryInfo) { retries = append(retries, i) }
			c, rec, fs := newTestClient(t, tc.opts, stalledHandler())
			var got error
			for _, err := range c.Futures.AllFills(ctx, tc.iter...) {
				got = err
			}
			var ae *APIError
			if !errors.As(got, &ae) || ae.Code != CodePagingStalled {
				t.Fatalf("err %v", got)
			}
			if rec.count() != tc.requests || len(fs.all()) != tc.requests-2 || len(retries) != tc.requests-2 {
				t.Fatalf("requests %d sleeps %d retries %d", rec.count(), len(fs.all()), len(retries))
			}
			for _, r := range retries {
				if r.Operation != OpFills || r.Delay <= 0 {
					t.Fatalf("retry info %+v", r)
				}
			}
		})
	}
}

func TestFuturesPagingStopsOnCancelAndMaxItems(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, stalledHandler())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var got error
	for _, err := range c.Futures.AllFills(ctx) {
		got = err
	}
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("err %v", got)
	}

	c, rec, _ = newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, stalledHandler())
	n := 0
	for _, err := range c.Futures.AllFills(context.Background(), WithMaxItems(1)) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 1 || rec.count() != 1 {
		t.Fatalf("n %d requests %d", n, rec.count())
	}
}

func TestFuturesPagingServerError(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, NoRetries: true}, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "2")
		writeJSON(w, 503, map[string]any{"error": map[string]any{"code": "SERVICE_UNAVAILABLE", "message": "busy", "retryable": true,
			"details": map[string]any{"reason": "futures_data_unavailable"}}})
	})
	var got error
	for _, err := range c.Futures.AllFunding(context.Background()) {
		got = err
	}
	var ae *APIError
	if !errors.As(got, &ae) || ae.Status != 503 || !ae.Retryable || !errors.Is(got, ErrServer) || rec.count() != 1 {
		t.Fatalf("err %v requests %d", got, rec.count())
	}
}

func TestFuturesRepeatedCursorWithRowsStops(t *testing.T) {
	c, rec, fs := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv(map[string]any{"has_account": true, "funding": []any{map[string]any{"coin": "BTC"}}, "next_cursor": "same"}))
	})
	n := 0
	var got error
	for _, err := range c.Futures.AllFunding(context.Background()) {
		if err != nil {
			got = err
			break
		}
		n++
	}
	var ae *APIError
	if !errors.As(got, &ae) || ae.Code != CodePagingCursorRepeated || ae.Retryable || isRetryable(got) || ae.Details["cursor"] != "same" {
		t.Fatalf("err %v", got)
	}
	if n != 2 || rec.count() != 2 || len(fs.all()) != 0 {
		t.Fatalf("rows %d requests %d sleeps %d", n, rec.count(), len(fs.all()))
	}
}
