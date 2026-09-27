package cexy

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func tradePage(ids []string, next string) map[string]any {
	items := []any{}
	for _, id := range ids {
		items = append(items, map[string]any{"id": id, "price": "1", "quantity": "1", "side": "buy", "timestamp": "2026-09-27T10:00:00Z"})
	}
	p := map[string]any{"items": items, "has_more": next != ""}
	if next != "" {
		p["next_cursor"] = next
	}
	return p
}

func TestIteratorWalksPages(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("cursor") {
		case "":
			writeJSON(w, 200, tradePage([]string{"1", "2"}, "c2"))
		case "c2":
			writeJSON(w, 200, tradePage([]string{"3"}, ""))
		}
	})
	var ids []string
	for tr, err := range c.Markets.AllTrades(context.Background(), "BTC/USDT", &GetMarketTradesParams{Limit: Ptr(2)}) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, tr.ID)
	}
	if len(ids) != 3 || rec.count() != 2 {
		t.Fatalf("ids %v requests %d", ids, rec.count())
	}
	req, _ := rec.get(1)
	if req.URL.Query().Get("limit") != "2" {
		t.Fatal("params not kept across pages")
	}
}

func TestIteratorMaxItemsAndBreak(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, tradePage([]string{"1", "2"}, "again-"+r.URL.Query().Get("cursor")))
	})
	n := 0
	for _, err := range c.Markets.AllTrades(context.Background(), "BTC/USDT", nil, WithMaxItems(3)) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 3 || rec.count() != 2 {
		t.Fatalf("n %d requests %d", n, rec.count())
	}
	for range c.Markets.AllTrades(context.Background(), "BTC/USDT", nil) {
		break // stopping early fetches nothing more
	}
}

func TestIteratorRepeatedCursorStops(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, tradePage([]string{"1"}, "same"))
	})
	n := 0
	for range c.Markets.AllTrades(context.Background(), "BTC/USDT", nil) {
		n++
	}
	if n != 2 || rec.count() != 2 {
		t.Fatalf("n %d requests %d", n, rec.count())
	}
}

func TestIteratorYieldsError(t *testing.T) {
	c, _, _ := newTestClient(t, Options{NoRetries: true}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 400, apiErr("INVALID_CURSOR", "bad cursor", false))
	})
	var got error
	for _, err := range c.Markets.AllTrades(context.Background(), "BTC/USDT", nil) {
		got = err
	}
	if !errors.Is(got, ErrValidation) {
		t.Fatalf("got %v", got)
	}
}
