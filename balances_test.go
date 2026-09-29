package cexy

import (
	"context"
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
