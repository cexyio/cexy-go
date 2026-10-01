package cexy

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func balanceRow(held any) map[string]any {
	row := map[string]any{"asset": "USDT", "available": "90.00", "locked": "10.00", "pending": "0", "total": "100.00"}
	if held != nil {
		row["held_incoming"] = held
	}
	return row
}

func TestBalanceHeldIncomingDecodes(t *testing.T) {
	held := []map[string]any{
		{"transfer_id": "aaaaaaaaaaaaaaaaaaaaaaaa", "amount": "4.00", "available_at": "2026-09-30T10:00:00.123Z"},
		{"transfer_id": "bbbbbbbbbbbbbbbbbbbbbbbb", "amount": "6.00", "available_at": "2026-10-01T10:00:00.456Z"},
	}
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"data": []any{balanceRow(held)}})
	})
	bs, err := c.Account.Balances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h := bs[0].HeldIncoming
	if len(h) != 2 || h[0].TransferID != "aaaaaaaaaaaaaaaaaaaaaaaa" || h[0].Amount != "4.00" {
		t.Fatalf("held = %+v", h)
	}
	want := time.Date(2026, 9, 30, 10, 0, 0, 123_000_000, time.UTC)
	if !h[0].AvailableAt.Equal(want) {
		t.Fatalf("available_at = %v, want %v", h[0].AvailableAt, want)
	}
	if bs[0].Locked != "10.00" {
		t.Fatalf("locked changed: %v", bs[0].Locked)
	}
}

func TestBalanceHeldIncomingEmptyAndMissing(t *testing.T) {
	for name, held := range map[string]any{"empty": []any{}, "missing": nil} {
		t.Run(name, func(t *testing.T) {
			c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/account/balances" {
					writeJSON(w, 200, map[string]any{"data": []any{balanceRow(held)}})
					return
				}
				writeJSON(w, 200, map[string]any{"data": balanceRow(held)})
			})
			bs, err := c.Account.Balances(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if bs[0].HeldIncoming == nil || len(bs[0].HeldIncoming) != 0 {
				t.Fatalf("Balances held = %#v, want non-nil empty", bs[0].HeldIncoming)
			}
			b, err := c.Account.Balance(context.Background(), "USDT")
			if err != nil {
				t.Fatal(err)
			}
			if b.HeldIncoming == nil || len(b.HeldIncoming) != 0 {
				t.Fatalf("Balance held = %#v, want non-nil empty", b.HeldIncoming)
			}
		})
	}
}

func TestSubAccountBalancesPathAuthAndHeld(t *testing.T) {
	held := []map[string]any{{"transfer_id": "cccccccccccccccccccccccc", "amount": "1.50", "available_at": "2026-09-30T10:00:00.001Z"}}
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"data": []any{balanceRow(held)}})
	})
	bs, err := c.Account.SubAccountBalances(context.Background(), "sub/1 ?x")
	if err != nil {
		t.Fatal(err)
	}
	if rec.count() != 1 {
		t.Fatalf("requests = %d", rec.count())
	}
	req, _ := rec.get(0)
	if req.Method != http.MethodGet || req.URL.EscapedPath() != "/api/v1/account/sub-accounts/sub%2F1%20%3Fx/balances" {
		t.Fatalf("request = %s %s", req.Method, req.URL.EscapedPath())
	}
	if req.Header.Get("X-API-Key") != testKey || req.Header.Get("X-API-Signature") == "" || req.Header.Get("X-API-Secret") != "" {
		t.Fatal("credentials not sent")
	}
	if req.Header.Get("Idempotency-Key") != "" {
		t.Fatal("unexpected Idempotency-Key")
	}
	if len(bs) != 1 || len(bs[0].HeldIncoming) != 1 || bs[0].HeldIncoming[0].Amount != "1.50" {
		t.Fatalf("balances = %+v", bs)
	}
}

func TestSubAccountBalancesMissingHeldIsEmpty(t *testing.T) {
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"data": []any{balanceRow(nil)}})
	})
	bs, err := c.Account.SubAccountBalances(context.Background(), "sub_1")
	if err != nil {
		t.Fatal(err)
	}
	if bs[0].HeldIncoming == nil || len(bs[0].HeldIncoming) != 0 {
		t.Fatalf("held = %#v, want empty non-nil", bs[0].HeldIncoming)
	}
}

func TestSubAccountBalances404IsNotFoundWithoutRetry(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 404, apiErr("NOT_FOUND", "no such sub-account", false))
	})
	_, err := c.Account.SubAccountBalances(context.Background(), "other")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if rec.count() != 1 {
		t.Fatalf("requests = %d, want exactly 1 (no retry)", rec.count())
	}
}

func TestSubAccountBalances404VariantsAreNotFoundWithoutRetry(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"no retryable field": func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 404, map[string]any{"error": map[string]any{"code": "NOT_FOUND", "message": "no such sub-account"}})
		},
		"non-JSON body": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(404)
			_, _ = w.Write([]byte("<html>not found</html>"))
		},
		"retryable true": func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 404, apiErr("NOT_FOUND", "no such sub-account", true))
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, handler)
			_, err := c.Account.SubAccountBalances(context.Background(), "other")
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("err = %v, want ErrNotFound", err)
			}
			var ae *APIError
			if name != "retryable true" && (!errors.As(err, &ae) || ae.Retryable) {
				t.Fatalf("err = %#v, want Retryable false", err)
			}
			if rec.count() != 1 {
				t.Fatalf("requests = %d, want exactly 1 (no retry)", rec.count())
			}
		})
	}
}

func TestSubAccountBalancesEmptyIDRejectedBeforeRequest(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"data": []any{}})
	})
	for _, id := range []string{"", ".", ".."} {
		_, err := c.Account.SubAccountBalances(context.Background(), id)
		var ce *ConfigError
		if !errors.As(err, &ce) {
			t.Fatalf("%q: err = %v, want *ConfigError", id, err)
		}
	}
	if rec.count() != 0 {
		t.Fatalf("requests = %d, want 0", rec.count())
	}
}
