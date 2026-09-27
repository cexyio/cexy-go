package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
)

func placeResponse() map[string]any {
	return dataEnv(map[string]any{"order": testOrder, "fills": []any{}})
}

func TestPlaceOrderGeneratesClientOrderID(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, placeResponse())
	})
	res, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeLimit, Price: Ptr(Amount("60000")), Quantity: Ptr(Amount("0.1"))})
	if err != nil || res.Recovered || res.ClientOrderID == "" || res.Order.ID != "ord_1" {
		t.Fatalf("%+v %v", res, err)
	}
	_, body := rec.get(0)
	var sent map[string]any
	_ = json.Unmarshal(body, &sent)
	if sent["client_order_id"] != res.ClientOrderID || sent["price"] != "60000" {
		t.Fatalf("sent %s", body)
	}
}

func TestPlaceOrderRejectsBadAmountsLocally(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {})
	_, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeLimit, Price: Ptr(Amount("6e4")), Quantity: Ptr(Amount("0.1"))})
	var ie *InvalidAmountError
	if !errors.As(err, &ie) || ie.Field != "price" || rec.count() != 0 {
		t.Fatalf("err %v count %d", err, rec.count())
	}
}

// An ambiguous failure (5xx) is followed by a lookup; the order exists, so it is returned.
func TestPlaceOrderRecoversByClientOrderID(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writeJSON(w, 502, apiErr("INTERNAL", "gateway", true))
			return
		}
		writeJSON(w, 200, dataEnv(testOrder))
	})
	res, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("0.1")), ClientOrderID: Ptr("mine-1")})
	if err != nil || !res.Recovered || res.ClientOrderID != "mine-1" || rec.count() != 2 {
		t.Fatalf("%+v %v count %d", res, err, rec.count())
	}
	req, _ := rec.get(1)
	if req.URL.Path != "/api/v1/trading/orders/by-client-id/mine-1" {
		t.Fatalf("lookup path %s", req.URL.Path)
	}
}

// Lookup says 404: the order is resent with the SAME client_order_id.
func TestPlaceOrderResendsWithSameClientOrderID(t *testing.T) {
	var posts atomic.Int32
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if posts.Add(1) == 1 {
				writeJSON(w, 503, apiErr("SERVICE_UNAVAILABLE", "busy", true))
				return
			}
			writeJSON(w, 200, placeResponse())
			return
		}
		writeJSON(w, 404, apiErr("NOT_FOUND", "no such order", false))
	})
	res, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideSell,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("0.1"))})
	if err != nil || res.Recovered || rec.count() != 3 {
		t.Fatalf("%+v %v count %d", res, err, rec.count())
	}
	_, b1 := rec.get(0)
	_, b3 := rec.get(2)
	var first, second map[string]any
	_ = json.Unmarshal(b1, &first)
	_ = json.Unmarshal(b3, &second)
	if first["client_order_id"] == nil || first["client_order_id"] != second["client_order_id"] {
		t.Fatalf("client_order_id changed: %v / %v", first["client_order_id"], second["client_order_id"])
	}
}

// Both the POST and the lookup fail: the state is unknown.
func TestPlaceOrderStateUnknown(t *testing.T) {
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 500, apiErr("INTERNAL", "boom", true))
	})
	_, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("0.1")), ClientOrderID: Ptr("x-1")})
	var ou *OrderStateUnknownError
	if !errors.As(err, &ou) || ou.ClientOrderID != "x-1" {
		t.Fatalf("got %v", err)
	}
}

// A definitive refusal (422) is neither looked up nor retried.
func TestPlaceOrderRefusalIsFinal(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 422, apiErr("INSUFFICIENT_FUNDS", "no", false))
	})
	_, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("0.1"))})
	if !errors.Is(err, ErrUnprocessable) || rec.count() != 1 {
		t.Fatalf("err %v count %d", err, rec.count())
	}
}

// INVALID_STATE on a retry means the first attempt cancelled it: fetch and return the order.
func TestCancelOrderInvalidStateOnRetry(t *testing.T) {
	var deletes atomic.Int32
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			if deletes.Add(1) == 1 {
				writeJSON(w, 503, apiErr("SERVICE_UNAVAILABLE", "busy", true))
				return
			}
			writeJSON(w, 409, apiErr("INVALID_STATE", "no longer open", false))
			return
		}
		writeJSON(w, 200, dataEnv(testOrder))
	})
	ord, err := c.Trading.CancelOrder(context.Background(), "ord_1")
	if err != nil || ord.ID != "ord_1" || rec.count() != 3 {
		t.Fatalf("%v count %d", err, rec.count())
	}
}

func TestCancelOrderInvalidStateFirstAttemptIsError(t *testing.T) {
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 409, apiErr("INVALID_STATE", "already filled", false))
	})
	_, err := c.Trading.CancelOrder(context.Background(), "ord_1")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != CodeInvalidState {
		t.Fatalf("got %v", err)
	}
}

func TestCancelAllNeedsExplicitTarget(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv(map[string]any{"cancelled": []string{"o1"}, "failed": []string{}}))
	})
	ctx := context.Background()
	if _, err := c.Trading.CancelAll(ctx, ""); err == nil || rec.count() != 0 {
		t.Fatal("empty symbol accepted")
	}
	if _, err := c.Trading.CancelAll(ctx, "BTC/USDT"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Trading.CancelAllMarkets(ctx); err != nil {
		t.Fatal(err)
	}
	_, one := rec.get(0)
	_, all := rec.get(1)
	if string(one) != `{"symbol":"BTC/USDT"}` || string(all) != `{}` {
		t.Fatalf("bodies %s / %s", one, all)
	}
}

func TestOrderEndpointsSendIdempotencyKeyButDoNotRelyOnIt(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, placeResponse())
	})
	if _, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("1"))}); err != nil {
		t.Fatal(err)
	}
	req, _ := rec.get(0)
	if req.Header.Get("Idempotency-Key") == "" {
		t.Fatal("no Idempotency-Key header")
	}
}
