package cexy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// WSBookDepth is how many levels per side the WebSocket book carries. REST levels deeper
// than this are never used.
const WSBookDepth = 50

// LiveOrderBookOptions are optional callbacks for a LiveOrderBook. They run on the
// WebSocket's handler goroutine, in order.
type LiveOrderBookOptions struct {
	// The book changed (a snapshot or an update was applied).
	OnUpdate func(BookSnapshot)
	// A sequence gap: the book is stale until the next update replaces it.
	OnStale func(expected, received int64)
	// The update after a gap arrived in order; the book is current again.
	OnHealed func()
	// A fresh REST snapshot is being taken (reconnect, CONCURRENT_MODIFICATION).
	OnResync func()
	// A snapshot failed; it is retried with backoff.
	OnError func(error)
	// Delay before retrying a failed snapshot. Default 1 s, doubling up to 30 s.
	SnapshotRetryDelay time.Duration
}

// BookSnapshot is a copy of a LiveOrderBook's state. Levels are best first, at most 50 a side.
type BookSnapshot struct {
	Symbol     string
	Bids, Asks []BookLevel
	// Sequence of the last applied snapshot or update on the current connection; valid when
	// HasSequence.
	Sequence    int64
	HasSequence bool
	// True after a sequence gap, until the next in-order update.
	Stale bool
	// False while waiting for a snapshot (after a reconnect or a resync).
	Synced bool
}

// Best returns the best bid and ask; ok is false for an empty side.
func (b BookSnapshot) Best() (bid BookLevel, bidOK bool, ask BookLevel, askOK bool) {
	if len(b.Bids) > 0 {
		bid, bidOK = b.Bids[0], true
	}
	if len(b.Asks) > 0 {
		ask, askOK = b.Asks[0], true
	}
	return
}

// LiveOrderBook is a local order book fed by the orderbook:{symbol} channel. Create it with
// WebSocket.OrderBook. It is safe for concurrent use.
type LiveOrderBook struct {
	symbol string
	ws     *WebSocket
	opts   LiveOrderBookOptions
	closed atomic.Bool

	mu     sync.Mutex
	bids   []BookLevel
	asks   []BookLevel
	seq    int64
	hasSeq bool
	stale  bool
	synced bool
	buffer []Event
	gen    int
	retry  *time.Timer
}

func newLiveOrderBook(symbol string, ws *WebSocket, opts LiveOrderBookOptions) *LiveOrderBook {
	if opts.SnapshotRetryDelay <= 0 {
		opts.SnapshotRetryDelay = time.Second
	}
	return &LiveOrderBook{symbol: symbol, ws: ws, opts: opts}
}

// Symbol returns the market symbol.
func (b *LiveOrderBook) Symbol() string { return b.symbol }

// Snapshot returns a copy of the current state.
func (b *LiveOrderBook) Snapshot() BookSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked()
}

func (b *LiveOrderBook) snapshotLocked() BookSnapshot {
	return BookSnapshot{Symbol: b.symbol, Bids: append([]BookLevel(nil), b.bids...), Asks: append([]BookLevel(nil), b.asks...),
		Sequence: b.seq, HasSequence: b.hasSeq, Stale: b.stale, Synced: b.synced}
}

// Close stops following the book and unsubscribes.
func (b *LiveOrderBook) Close() {
	if b.closed.Swap(true) {
		return
	}
	b.mu.Lock()
	b.gen++
	if b.retry != nil {
		b.retry.Stop()
	}
	b.mu.Unlock()
	b.ws.mu.Lock()
	if b.ws.books[b.symbol] == b {
		delete(b.ws.books, b.symbol)
	}
	b.ws.mu.Unlock()
	go func() { _ = b.ws.Unsubscribe(context.Background(), "orderbook:"+b.symbol) }()
}

// markDisconnected: the connection dropped, so sequences from the next one are unrelated.
func (b *LiveOrderBook) markDisconnected() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gen++
	b.synced, b.hasSeq, b.seq = false, false, 0
	b.buffer = nil
	if b.retry != nil {
		b.retry.Stop()
		b.retry = nil
	}
}

// initialSync takes the first snapshot and returns its error, if any.
func (b *LiveOrderBook) initialSync(ctx context.Context) error {
	b.mu.Lock()
	b.gen++
	gen := b.gen
	b.mu.Unlock()
	snap, err := b.ws.opts.snapshots.orderBookSnapshot(ctx, b.symbol, WSBookDepth)
	if err != nil {
		return err
	}
	b.applySnapshot(gen, snap)
	return nil
}

// resync takes a fresh REST snapshot and replays buffered updates newer than it.
func (b *LiveOrderBook) resync(attempt int) {
	if b.closed.Load() {
		return
	}
	b.mu.Lock()
	b.gen++
	gen := b.gen
	b.synced, b.hasSeq, b.seq = false, false, 0 // keep the buffer: newer updates are replayed
	b.mu.Unlock()
	b.ws.dispatch(func() {
		if b.opts.OnResync != nil {
			b.opts.OnResync()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	snap, err := b.ws.opts.snapshots.orderBookSnapshot(ctx, b.symbol, WSBookDepth)
	cancel()
	if err != nil {
		b.mu.Lock()
		defer b.mu.Unlock()
		if gen != b.gen || b.closed.Load() {
			return
		}
		b.ws.dispatch(func() {
			if b.opts.OnError != nil {
				b.opts.OnError(err)
			}
		})
		delay := min(30*time.Second, b.opts.SnapshotRetryDelay<<min(attempt, 10))
		b.retry = time.AfterFunc(delay, func() {
			b.mu.Lock()
			same := gen == b.gen
			b.mu.Unlock()
			if same {
				b.resync(attempt + 1)
			}
		})
		return
	}
	b.applySnapshot(gen, snap)
}

func (b *LiveOrderBook) applySnapshot(gen int, snap OrderBook) {
	b.mu.Lock()
	if gen != b.gen || b.closed.Load() {
		b.mu.Unlock()
		return // superseded
	}
	b.bids = firstLevels(snap.Bids)
	b.asks = firstLevels(snap.Asks)
	b.seq, b.hasSeq = snap.Sequence, true
	b.stale, b.synced = false, true
	buffered := b.buffer
	b.buffer = nil
	calls := []func(){b.updateCallback()}
	for _, ev := range buffered {
		calls = append(calls, b.applyLocked(ev)...)
	}
	b.mu.Unlock()
	for _, f := range calls {
		b.ws.dispatch(f)
	}
}

// onUpdate runs on the handler goroutine for every orderbook.update of this symbol.
func (b *LiveOrderBook) onUpdate(ev Event) {
	if b.closed.Load() {
		return
	}
	b.mu.Lock()
	if !b.synced {
		b.buffer = append(b.buffer, ev)
		b.mu.Unlock()
		return
	}
	calls := b.applyLocked(ev)
	b.mu.Unlock()
	for _, f := range calls {
		f()
	}
}

// applyLocked applies one update and returns the callbacks to run (outside the lock).
func (b *LiveOrderBook) applyLocked(ev Event) []func() {
	var calls []func()
	if ev.Sequence != nil && b.hasSeq {
		seq := *ev.Sequence
		if seq <= b.seq {
			return nil // already covered by the snapshot or applied
		}
		if seq != b.seq+1 {
			b.stale = true
			expected := b.seq + 1
			calls = append(calls, func() {
				if b.opts.OnStale != nil {
					b.opts.OnStale(expected, seq)
				}
			})
		} else if b.stale {
			b.stale = false
			calls = append(calls, func() {
				if b.opts.OnHealed != nil {
					b.opts.OnHealed()
				}
			})
		}
	}
	var d OrderBookUpdate
	if ev.Decode(&d) != nil {
		return calls
	}
	// Each update is the complete top 50 of both sides: replace, never merge.
	b.bids = firstLevels(d.Bids)
	b.asks = firstLevels(d.Asks)
	if ev.Sequence != nil {
		b.seq, b.hasSeq = *ev.Sequence, true
	}
	return append(calls, b.updateCallback())
}

func (b *LiveOrderBook) updateCallback() func() {
	snap := b.snapshotLocked()
	return func() {
		if b.opts.OnUpdate != nil {
			b.opts.OnUpdate(snap)
		}
	}
}

func firstLevels(raw [][]Amount) []BookLevel {
	levels := Levels(raw)
	if len(levels) > WSBookDepth {
		levels = levels[:WSBookDepth]
	}
	return levels
}
