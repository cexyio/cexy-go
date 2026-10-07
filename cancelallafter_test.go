package cexy

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func armedResponse(symbol any, ms int) map[string]any {
	return dataEnv(map[string]any{"armed": ms != 0, "deadline": "2026-10-07T10:00:10Z",
		"server_time": "2026-10-07T10:00:00Z", "symbol": symbol, "timeout_ms": ms})
}

func TestCancelAllAfterBodies(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, armedResponse("BTC/USDT", 10000))
	})
	ctx := context.Background()
	res, err := c.Trading.CancelAllAfter(ctx, "BTC/USDT", 10*time.Second)
	if err != nil || !res.Armed || res.Deadline == nil || res.TimeoutMs != 10000 || res.Symbol == nil || *res.Symbol != "BTC/USDT" ||
		res.ServerTime.IsZero() {
		t.Fatalf("%+v %v", res, err)
	}
	if _, err := c.Trading.CancelAllAfterMarkets(ctx, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Trading.CancelAllAfter(ctx, "BTC/USDT", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Trading.CancelAllAfterMarkets(ctx, 0); err != nil {
		t.Fatal(err)
	}
	req, one := rec.get(0)
	_, all := rec.get(1)
	_, off := rec.get(2)
	_, offAll := rec.get(3)
	if req.URL.Path != "/api/v1/trading/orders/cancel-all-after" || req.Method != http.MethodPost {
		t.Fatalf("%s %s", req.Method, req.URL.Path)
	}
	if string(one) != `{"symbol":"BTC/USDT","timeout_ms":10000}` || string(all) != `{"symbol":null,"timeout_ms":10000}` ||
		string(off) != `{"symbol":"BTC/USDT","timeout_ms":0}` || string(offAll) != `{"symbol":null,"timeout_ms":0}` {
		t.Fatalf("bodies %s %s %s %s", one, all, off, offAll)
	}
	if req.Header.Get("Idempotency-Key") != "" {
		t.Fatal("Idempotency-Key sent")
	}
}

func TestCancelAllAfterRefusesBadInputLocally(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, armedResponse(nil, 0))
	})
	ctx := context.Background()
	var ce *ConfigError
	for _, sym := range []string{"", " ", "\t\n"} {
		if _, err := c.Trading.CancelAllAfter(ctx, sym, 10*time.Second); !errors.As(err, &ce) {
			t.Errorf("symbol %q: %v", sym, err)
		}
	}
	for _, d := range []time.Duration{-1, -time.Second, 999 * time.Microsecond, time.Nanosecond, 1500 * time.Microsecond, 5*time.Second + time.Nanosecond} {
		if _, err := c.Trading.CancelAllAfter(ctx, "BTC/USDT", d); !errors.As(err, &ce) {
			t.Errorf("timeout %v: %v", d, err)
		}
		if _, err := c.Trading.CancelAllAfterMarkets(ctx, d); !errors.As(err, &ce) {
			t.Errorf("markets timeout %v: %v", d, err)
		}
	}
	if rec.count() != 0 {
		t.Fatalf("%d requests sent", rec.count())
	}
	// No range check here: the server owns it.
	if _, err := c.Trading.CancelAllAfter(ctx, "BTC/USDT", time.Millisecond); err != nil || rec.count() != 1 {
		t.Fatalf("1 ms: %v", err)
	}
}

func TestCancelAllAfterRetriedAfterConnectionError(t *testing.T) {
	var calls atomic.Int32
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, MaxRetries: 2}, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			conn.Close()
			return
		}
		writeJSON(w, 200, armedResponse("BTC/USDT", 10000))
	})
	res, err := c.Trading.CancelAllAfter(context.Background(), "BTC/USDT", 10*time.Second)
	if err != nil || !res.Armed || rec.count() != 2 {
		t.Fatalf("%+v %v count %d", res, err, rec.count())
	}
}

func TestDeadManNotArmedOnPlaceOrderIsNotRetriedNorRecovered(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, MaxRetries: 3}, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, 200, dataEnv(testOrder)) // a lookup would "find" an order
			return
		}
		e := apiErr("DEAD_MAN_NOT_ARMED", "arm cancel-all-after first", false)
		e["error"].(map[string]any)["details"] = map[string]any{"market": "BTC/USDT"}
		writeJSON(w, 409, e)
	})
	_, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("0.1")), ClientOrderID: Ptr("mine-1")})
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != CodeDeadManNotArmed || ae.Retryable || ae.Details["market"] != "BTC/USDT" {
		t.Fatalf("got %v", err)
	}
	if !errors.Is(err, ErrDeadManNotArmed) || !errors.Is(err, ErrConflict) || !ae.Known() {
		t.Fatalf("categories: %v", err)
	}
	if rec.count() != 1 {
		t.Fatalf("%d requests; want exactly the placement", rec.count())
	}
}
