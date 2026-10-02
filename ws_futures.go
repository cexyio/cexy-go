package cexy

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"time"
)

// Futures WebSocket channels (conformance/ws/futures.json). Public: FuturesMidsChannel,
// FuturesOrderBookChannel, FuturesTradesChannel, FuturesCandlesChannel, FuturesStatusChannel.
// Private: FuturesAccountChannel (held until Auth or AuthKey succeeds).
//
// Public futures channels send nothing on subscribe: seed from REST (Client.Futures). Books,
// mids, positions and orders frames are full replacements; trades and candles are not replayed.
// Public futures channels are not gap-tracked. A refused futures subscribe (RATE_LIMITED,
// NOT_FOUND, VALIDATION_FAILED, SERVICE_UNAVAILABLE) is returned by Subscribe and not retried:
// WebSocket error frames carry no retry hint, so back off yourself (the server means 60 s).
const (
	// FuturesMidsChannel: every futures coin's mid price (event futures.mids, data FuturesMids).
	FuturesMidsChannel = "futures.mids"
	// FuturesStatusChannel: the server's futures market-data connection going live or degraded
	// (event futures.status, data FuturesStatus). Nothing is sent on subscribe.
	FuturesStatusChannel = "futures.status"
	// FuturesAccountChannel: the account's futures positions and open orders, in full on
	// subscribe and on change (events futures.positions and futures.orders). Private.
	FuturesAccountChannel = "futures.account"
)

// FuturesCandleIntervals are the intervals FuturesCandlesChannel accepts.
var FuturesCandleIntervals = []string{"1m", "5m", "15m", "1h", "4h", "1d"}

var futuresCoinRE = regexp.MustCompile(`^[A-Za-z0-9]{1,20}$`)

func futuresCoin(fn, coin string) error {
	if !futuresCoinRE.MatchString(coin) {
		return &ConfigError{Msg: fmt.Sprintf("%s: %q is not a futures coin (1 to 20 ASCII letters or digits, as GET /api/v1/futures/markets lists it)", fn, coin)}
	}
	return nil
}

// FuturesOrderBookChannel returns "futures.orderbook:{coin}" (event futures.orderbook.update,
// data FuturesBookUpdate). The coin is sent exactly as given: it is case-sensitive ("BTC",
// "kPEPE").
func FuturesOrderBookChannel(coin string) (string, error) {
	if err := futuresCoin("FuturesOrderBookChannel", coin); err != nil {
		return "", err
	}
	return "futures.orderbook:" + coin, nil
}

// FuturesTradesChannel returns "futures.trades:{coin}" (event futures.trades.new, data
// FuturesTradesUpdate).
func FuturesTradesChannel(coin string) (string, error) {
	if err := futuresCoin("FuturesTradesChannel", coin); err != nil {
		return "", err
	}
	return "futures.trades:" + coin, nil
}

// FuturesCandlesChannel returns "futures.candles:{coin}:{interval}" (event
// futures.candle.update, data FuturesCandleUpdate). interval is one of FuturesCandleIntervals.
func FuturesCandlesChannel(coin, interval string) (string, error) {
	if err := futuresCoin("FuturesCandlesChannel", coin); err != nil {
		return "", err
	}
	if !slices.Contains(FuturesCandleIntervals, interval) {
		return "", &ConfigError{Msg: fmt.Sprintf("FuturesCandlesChannel: interval %q must be one of 1m, 5m, 15m, 1h, 4h, 1d", interval)}
	}
	return "futures.candles:" + coin + ":" + interval, nil
}

// FuturesMids is the data of futures.mids: the full set of mids, every coin each time.
type FuturesMids struct {
	Mids map[string]Amount `json:"mids"`
	// When the server published the frame.
	AsOf time.Time `json:"as_of"`
}

// FuturesBookUpdate is the data of futures.orderbook.update: the complete book (Full is always
// true). It replaces the previous one; a level absent from it is gone.
type FuturesBookUpdate struct {
	Coin string `json:"coin"`
	Full bool   `json:"full"`
	// Best first. Levels are {price, size} objects.
	Bids []Level   `json:"bids"`
	Asks []Level   `json:"asks"`
	AsOf time.Time `json:"as_of"`
}

// FuturesTradesUpdate is the data of futures.trades.new: one or more public trades.
type FuturesTradesUpdate struct {
	Coin   string               `json:"coin"`
	Trades []FuturesPublicTrade `json:"trades"`
}

// FuturesCandleUpdate is the data of futures.candle.update: the current (possibly open) candle.
// Candle.OpenTime tells an update of the current bar from a new bar.
type FuturesCandleUpdate struct {
	Coin     string        `json:"coin"`
	Interval string        `json:"interval"`
	Candle   FuturesCandle `json:"candle"`
}

// FuturesStatus is the data of futures.status, sent on a transition only.
type FuturesStatus struct {
	// "live" or "degraded" (what is served may be out of date).
	State string    `json:"state"`
	Since time.Time `json:"since"`
}

// FuturesPositionsUpdate is the data of futures.positions on futures.account: the margin
// summary and every open position, in full.
type FuturesPositionsUpdate struct {
	Positions Positions `json:"positions"`
	AsOf      time.Time `json:"as_of"`
	// Served from an older read.
	Stale bool `json:"stale"`
}

// FuturesOrdersUpdate is the data of futures.orders on futures.account: every open order, in
// full. Replace your local list.
type FuturesOrdersUpdate struct {
	Orders []OpenOrder `json:"orders"`
	AsOf   time.Time   `json:"as_of"`
	Stale  bool        `json:"stale"`
}

// PendingChannels returns the private channels held until the next successful Auth or AuthKey:
// futures.account subscribed while signed out, and private channels dropped by a sign-out.
func (w *WebSocket) PendingChannels() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.pendingPrivate...)
}

// onFuturesResync delivers futures.resync (event plus OnFuturesResync). On futures.account the
// server's poller has stopped, and re-sending subscribe alone is a no-op: unsubscribe, then
// subscribe again.
func (w *WebSocket) onFuturesResync(ev Event) {
	w.dispatch(func() {
		if w.h.OnEvent != nil {
			w.h.OnEvent(ev)
		}
		if w.h.OnFuturesResync != nil {
			w.h.OnFuturesResync(ev.Channel)
		}
	})
	if ev.Channel != FuturesAccountChannel {
		return
	}
	w.mu.Lock()
	held := slices.Contains(w.channels, FuturesAccountChannel)
	w.mu.Unlock()
	if held {
		go w.resubscribeFuturesAccount() // not on the read loop: the acks arrive through it
	}
}

func (w *WebSocket) resubscribeFuturesAccount() {
	ctx := context.Background()
	ch := []string{FuturesAccountChannel}
	w.mu.Lock()
	w.resetSeqLocked(FuturesAccountChannel) // the new poller's sequence restarts
	w.mu.Unlock()
	unsub, err := w.begin(ctx, "unsubscribe", map[string]any{"channels": ch}, ch)
	if err != nil {
		w.emitError(err)
		return
	}
	// Written right after the unsubscribe: the server handles a connection's frames in order.
	sub, err := w.begin(ctx, "subscribe", map[string]any{"channels": ch}, ch)
	go func() { _, _ = w.wait(ctx, unsub, false) }()
	if err != nil {
		w.emitError(err)
		return
	}
	ack, err := w.wait(ctx, sub, false)
	if refusedByServer(err) && !slices.Contains(subscribed(ack), FuturesAccountChannel) {
		w.mu.Lock()
		w.drop(FuturesAccountChannel)
		w.pendingPrivate = remove(w.pendingPrivate, FuturesAccountChannel)
		w.mu.Unlock()
	}
	if err != nil {
		w.emitError(err)
	}
}
