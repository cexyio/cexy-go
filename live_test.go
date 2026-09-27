package cexy

import (
	"context"
	"os"
	"testing"
	"time"
)

// Live smoke test against production: public, unauthenticated GETs only.
// Run with CEXY_LIVE_TESTS=1 go test -run TestLive ./...
func TestLivePublic(t *testing.T) {
	if os.Getenv("CEXY_LIVE_TESTS") != "1" {
		t.Skip("set CEXY_LIVE_TESTS=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.Time(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server, err := st.Time()
	if err != nil {
		t.Fatal(err)
	}
	if skew := time.Since(server); skew > time.Minute || skew < -time.Minute {
		t.Errorf("clock skew %v", skew)
	}
	markets, err := c.Markets.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("server time %s, %d markets", st.Iso, len(markets))

	// Every public model decodes from real responses.
	if _, err := c.Config(ctx); err != nil {
		t.Error("config:", err)
	}
	if _, err := c.Assets.List(ctx); err != nil {
		t.Error("assets:", err)
	}
	if _, err := c.Networks.List(ctx); err != nil {
		t.Error("networks:", err)
	}
	if _, err := c.Fees.List(ctx); err != nil {
		t.Error("fees:", err)
	}
	if _, err := c.Pools.List(ctx); err != nil {
		t.Error("pools:", err)
	}
	if len(markets) == 0 {
		return
	}
	sym := markets[0].Symbol
	if _, err := c.Markets.OrderBook(ctx, sym, &GetOrderBookParams{Depth: Ptr(5)}); err != nil {
		t.Error("orderbook:", err)
	}
	if _, err := c.Markets.Trades(ctx, sym, &GetMarketTradesParams{Limit: Ptr(5)}); err != nil {
		t.Error("trades:", err)
	}
	if _, err := c.Markets.Candles(ctx, sym, GetCandlesParams{Interval: CandleInterval1h, Limit: Ptr(5)}); err != nil {
		t.Error("candles:", err)
	}
}
