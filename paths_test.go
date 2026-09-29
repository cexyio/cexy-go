package cexy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestPathBuilderRejectsDotSegments(t *testing.T) {
	// A dot segment would be resolved by the URL layer, a proxy or the server's router (even as
	// %2E) and reach a different route.
	tr := &transport{baseURL: "https://api.cexy.io"}
	for _, v := range []string{".", ".."} {
		_, err := tr.buildURL(operations[OpSubAccountBalances], call{op: OpSubAccountBalances, pathParams: map[string]string{"id": v}})
		var ce *ConfigError
		if !errors.As(err, &ce) {
			t.Fatalf("%q: err = %v, want *ConfigError", v, err)
		}
	}
}

func TestPathValuesStayOneSegment(t *testing.T) {
	cases := map[string]string{
		"a/b":    "a%2Fb",
		"%2F":    "%252F",
		"a?b":    "a%3Fb",
		"a#b":    "a%23b",
		"é✓":     "%C3%A9%E2%9C%93",
		"%2e%2e": "%252e%252e",
		"a b":    "a%20b",
		"...":    "...",
		".a":     ".a",
	}
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv([]any{}))
	})
	i := 0
	for v, seg := range cases {
		if _, err := c.Account.SubAccountBalances(context.Background(), v); err != nil {
			t.Fatalf("%q: %v", v, err)
		}
		req, _ := rec.get(i)
		i++
		want := "/api/v1/account/sub-accounts/" + seg + "/balances"
		if req.URL.EscapedPath() != want || req.URL.RawQuery != "" {
			t.Fatalf("%q: path %s query %q, want %s", v, req.URL.EscapedPath(), req.URL.RawQuery, want)
		}
	}
}

func TestDotSegmentsRejectedBeforeAnyRequest(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv(map[string]any{}))
	})
	ctx := context.Background()
	for _, v := range []string{".", ".."} {
		calls := map[string]error{}
		_, calls["SubAccountBalances"] = c.Account.SubAccountBalances(ctx, v)
		_, calls["OrderByClientID"] = c.Trading.OrderByClientID(ctx, v)
		_, calls["CancelOrder"] = c.Trading.CancelOrder(ctx, v)
		_, calls["Pools.Join"] = c.Pools.Join(ctx, v, JoinPoolRequest{BaseAmount: "1", QuoteAmount: "2"})
		for name, err := range calls {
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("%s(%q): err = %v, want *ConfigError", name, v, err)
			}
		}
	}
	if rec.count() != 0 {
		t.Fatalf("requests = %d, want 0", rec.count())
	}
}

func TestClientErrorsAreNeverRetriedEvenIfMarkedRetryable(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{400, "VALIDATION_FAILED"}, {404, "NOT_FOUND"}, {408, "HTTP_408"}, {409, "ALREADY_EXISTS"}, {422, "INVALID_STATE"}} {
		t.Run(fmt.Sprint(tc.status, tc.code), func(t *testing.T) {
			c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, apiErr(tc.code, "x", true))
			})
			if _, err := c.Markets.List(context.Background()); err == nil {
				t.Fatal("want an error")
			}
			if rec.count() != 1 {
				t.Fatalf("requests = %d, want 1", rec.count())
			}
		})
	}
}

func TestRateLimitAndConcurrentModificationStillRetried(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{429, "RATE_LIMITED"}, {409, "CONCURRENT_MODIFICATION"}} {
		t.Run(tc.code, func(t *testing.T) {
			n := 0
			c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				n++
				if n == 1 {
					writeJSON(w, tc.status, apiErr(tc.code, "x", true))
					return
				}
				writeJSON(w, 200, dataEnv([]any{}))
			})
			if _, err := c.Markets.List(context.Background()); err != nil {
				t.Fatal(err)
			}
			if rec.count() != 2 {
				t.Fatalf("requests = %d, want 2", rec.count())
			}
		})
	}
}

func TestPlaceOrderDoesNotRetryOther409EvenIfRetryable(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 409, apiErr("ALREADY_EXISTS", "client order id in use", true))
	})
	_, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("0.01"))})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if rec.count() != 1 {
		t.Fatalf("requests = %d, want 1", rec.count())
	}
}

func TestMutationThatIsNotRepeatSafeIsSentOnce(t *testing.T) {
	// Every public mutation is repeat-safe (PlaceOrder and CancelOrder have their own policies;
	// pool join and exit carry an Idempotency-Key; cancel-all is repeatable), so exercise the
	// transport rule directly: a pool join sent through request WITHOUT a key.
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 409, apiErr("CONCURRENT_MODIFICATION", "busy", true))
	})
	_, err := c.t.request(context.Background(), call{op: OpJoinPool, pathParams: map[string]string{"symbol": "BTC/USDT"},
		body: JoinPoolRequest{BaseAmount: "1", QuoteAmount: "2"}}, nil)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	if rec.count() != 1 {
		t.Fatalf("requests = %d, want 1", rec.count())
	}
	if req, _ := rec.get(0); req.Header.Get("Idempotency-Key") != "" {
		t.Fatal("unexpected Idempotency-Key")
	}
}
