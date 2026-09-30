package cexy

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"
)

// LiveBalancesOptions configures WebSocket.LiveBalances.
type LiveBalancesOptions struct {
	// Where snapshots come from. Default: the Client's Account.Balances (use Client.WebSocket).
	// It must return the balances of the same account the WebSocket is authenticated as.
	Snapshot func(ctx context.Context) ([]Balance, error)
	// The user id the snapshot source belongs to, compared with the WebSocket's authenticated user
	// at the start and after every account change. Default: the Client's Account.ID (GET /api/v1/account/id).
	OwnerID func(ctx context.Context) (string, error)
	// A fixed owner user id instead of OwnerID.
	AccountID string
	// Minimum time between successful snapshots (the API key's rate limit is shared). NOTE: 0
	// selects the default, 2 s (Go cannot tell unset from zero); use NoMinimum for none.
	MinSnapshotInterval time.Duration
	// Retry delay after a failed snapshot or owner lookup. Default 1 s (doubles up to 30 s).
	RetryDelay time.Duration
	// Called (on its own goroutine, one at a time) when one asset changes; nil balance: the row
	// was removed because its total reached 0. Callbacks queue up to 256 deep; while the queue is
	// full, further notifications are dropped with a warning (the state itself stays correct: read
	// it with Get and All).
	OnUpdate func(asset string, b *Balance)
	// Called after a snapshot was applied.
	OnSnapshot func(reason string)
	// ACCOUNT_MISMATCH (*AccountMismatchError: nothing merged), or a failed snapshot or owner
	// lookup (retried with backoff).
	OnError func(error)
}

// NoMinimum, as LiveBalancesOptions.MinSnapshotInterval, disables the minimum time between
// snapshots (the other SDKs spell this 0).
const NoMinimum time.Duration = -1

// AccountMismatchError: the snapshot source belongs to another account than the WebSocket session.
type AccountMismatchError struct {
	WebSocketUserID string
	SnapshotUserID  string
}

// Code returns "ACCOUNT_MISMATCH" (the same code as the other SDKs).
func (e *AccountMismatchError) Code() string { return "ACCOUNT_MISMATCH" }

func (e *AccountMismatchError) Error() string {
	return fmt.Sprintf("cexy live balances: [ACCOUNT_MISMATCH] the snapshot source belongs to %s, the WebSocket to %s; not merging",
		e.SnapshotUserID, e.WebSocketUserID)
}

// balanceUpdate is the data of balance.updated. Sequence is nil from servers that predate live
// balances.
type balanceUpdate struct {
	Asset     string `json:"asset"`
	Available Amount `json:"available"`
	Locked    Amount `json:"locked"`
	Pending   Amount `json:"pending"`
	Total     Amount `json:"total"`
	Sequence  *int64 `json:"sequence"`
}

// LiveBalances holds the live balances of the authenticated account, fed by the balances channel
// and REST snapshots. Create it with WebSocket.LiveBalances after Auth. It is safe for concurrent
// use.
//
// An event applies only if its data.sequence is greater than the stored one for that asset; a total
// of 0 removes the row (a snapshot row at or below that sequence cannot bring it back). A new
// snapshot is taken on a frame gap, balances.resync, CONCURRENT_MODIFICATION, a reconnect and after
// an account change, never because data.sequence skipped values. At the start and after every account change the snapshot
// source's owner is checked against the WebSocket user.
type LiveBalances struct {
	ws    *WebSocket
	clock WSClock
	opts  LiveBalancesOptions

	mu           sync.Mutex
	stale        bool
	rows         map[string]Balance
	tombstones   map[string]int64
	buffer       []balanceUpdate
	verifiedUser string
	fetching     bool
	again        string
	lastSuccess  time.Time
	haveSuccess  bool
	timer        WSTimer
	attempt      int
	closed       bool
	generation   int
	warnedNoSeq  bool
	lastErr      error
	cancelFetch  context.CancelFunc
	callbacks    chan func()
}

// LiveBalances subscribes balances and returns a LiveBalances that follows them. Call Auth first.
func (w *WebSocket) LiveBalances(ctx context.Context, opts LiveBalancesOptions) (*LiveBalances, error) {
	src, _ := w.opts.snapshots.(*Client)
	customSnapshot := opts.Snapshot != nil
	if opts.Snapshot == nil {
		if src == nil {
			return nil, &ConfigError{Msg: "LiveBalances needs opts.Snapshot or a WebSocket made by Client.WebSocket"}
		}
		opts.Snapshot = func(ctx context.Context) ([]Balance, error) { return src.Account.Balances(ctx) }
	}
	if opts.OwnerID == nil {
		switch {
		case opts.AccountID != "":
			fixed := opts.AccountID
			opts.OwnerID = func(context.Context) (string, error) { return fixed, nil }
		case src != nil && !customSnapshot:
			// The REST key's account owns only the REST key's own snapshots: a custom snapshot
			// source must name its owner.
			opts.OwnerID = func(ctx context.Context) (string, error) { return src.Account.ID(ctx) }
		default:
			return nil, &ConfigError{Msg: "LiveBalances needs opts.OwnerID or opts.AccountID to check the snapshot's account"}
		}
	}
	if opts.MinSnapshotInterval == 0 {
		opts.MinSnapshotInterval = 2 * time.Second
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = time.Second
	}
	lb := &LiveBalances{ws: w, clock: w.opts.Clock, opts: opts, stale: true, rows: map[string]Balance{},
		tombstones: map[string]int64{}, callbacks: make(chan func(), 256)}
	go lb.runCallbacks()
	w.mu.Lock()
	held := w.holds("balances")
	w.liveBalances = append(w.liveBalances, lb)
	if !held {
		w.balancesByUs = true
	}
	w.mu.Unlock()
	res, err := w.Subscribe(ctx, "balances")
	if err != nil || len(res.Refused) > 0 {
		lb.Close()
		if err == nil {
			err = &WSError{Code: "LOCAL_SUBSCRIPTION_LIMIT", Message: "cannot subscribe to balances: cap reached"}
		}
		return nil, err
	}
	if held {
		lb.trigger("start") // no subscribed reply comes for a held channel
	}
	return lb, nil
}

// Stale is true until the first snapshot, and from every refetch trigger until the next snapshot
// is applied.
func (lb *LiveBalances) Stale() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.stale
}

// LastError returns the last error (also passed to OnError; the first owner check can fail before
// the caller holds the LiveBalances). An *AccountMismatchError means nothing was merged. The next
// snapshot clears it.
func (lb *LiveBalances) LastError() error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.lastErr
}

// Get returns a copy of one asset's balance.
func (lb *LiveBalances) Get(asset string) (Balance, bool) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	b, ok := lb.rows[asset]
	return b, ok
}

// All returns copies of every non-zero balance.
func (lb *LiveBalances) All() []Balance {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	out := make([]Balance, 0, len(lb.rows))
	for _, b := range lb.rows {
		out = append(out, b)
	}
	return out
}

// Close stops following (unsubscribes balances unless something else on this socket needs it).
func (lb *LiveBalances) Close() {
	lb.mu.Lock()
	if lb.closed {
		lb.mu.Unlock()
		return
	}
	lb.closed = true
	lb.cancelTimerLocked()
	if lb.cancelFetch != nil {
		lb.cancelFetch()
		lb.cancelFetch = nil
	}
	close(lb.callbacks)
	lb.mu.Unlock()
	w := lb.ws
	w.mu.Lock()
	for i, x := range w.liveBalances {
		if x == lb {
			w.liveBalances = append(w.liveBalances[:i], w.liveBalances[i+1:]...)
			break
		}
	}
	unsub := len(w.liveBalances) == 0 && w.balancesByUs && !w.closedByUser
	if unsub {
		w.balancesByUs = false
	}
	w.mu.Unlock()
	if unsub {
		go func() { _ = w.Unsubscribe(context.Background(), "balances") }()
	}
}

func (lb *LiveBalances) runCallbacks() {
	for f := range lb.callbacks {
		f()
	}
}

// notifyLocked queues a callback; it runs outside the lock, in order.
func (lb *LiveBalances) notifyLocked(f func()) {
	if lb.closed {
		return
	}
	select {
	case lb.callbacks <- f:
	default:
		lb.ws.log.Warn("cexy: live balances callback queue full; dropping a notification")
	}
}

func (lb *LiveBalances) markStale() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.markStaleLocked()
}

func (lb *LiveBalances) markStaleLocked() {
	lb.stale = true
	lb.generation++
	lb.buffer = nil
}

func (lb *LiveBalances) onAuthChanged(reason AuthChangeReason) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.verifiedUser = ""
	lb.markStaleLocked()
	if reason == AuthUserChanged {
		// Another account's balances must never show: forget everything until the owner check.
		lb.rows = map[string]Balance{}
		lb.tombstones = map[string]int64{}
	}
}

func (lb *LiveBalances) onEvent(raw json.RawMessage) {
	var d balanceUpdate
	if json.Unmarshal(raw, &d) != nil || d.Asset == "" {
		return
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return
	}
	// Buffered only while a snapshot is in flight (it is applied on top). Without a verified owner
	// and no fetch (mismatch, retry backoff, signed out), events are dropped: the next snapshot is
	// complete anyway.
	if lb.fetching {
		lb.buffer = append(lb.buffer, d)
		return
	}
	if lb.verifiedUser == "" {
		return
	}
	lb.applyLocked(d, true)
}

func (lb *LiveBalances) trigger(reason string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.triggerLocked(reason)
}

func (lb *LiveBalances) triggerLocked(reason string) {
	if lb.closed {
		return
	}
	lb.stale = true
	if lb.fetching {
		if lb.again == "" {
			lb.again = reason
		}
		return
	}
	if lb.timer != nil {
		return
	}
	if lb.haveSuccess {
		if wait := lb.lastSuccess.Add(lb.opts.MinSnapshotInterval).Sub(lb.clock.Now()); wait > 0 {
			lb.timer = lb.clock.AfterFunc(wait, func() {
				lb.mu.Lock()
				lb.timer = nil
				lb.startFetchLocked(reason)
				lb.mu.Unlock()
			})
			return
		}
	}
	lb.startFetchLocked(reason)
}

func (lb *LiveBalances) cancelTimerLocked() {
	if lb.timer != nil {
		lb.timer.Stop()
	}
	lb.timer = nil
}

func (lb *LiveBalances) startFetchLocked(reason string) {
	if lb.closed || lb.fetching {
		return
	}
	wsUser := lb.ws.UserID()
	if wsUser == "" {
		return // signed out: the re-subscribe after the next auth triggers again
	}
	lb.fetching = true
	lb.buffer = nil
	lb.generation++
	ctx, cancel := context.WithCancel(context.Background())
	if lb.cancelFetch != nil {
		lb.cancelFetch()
	}
	lb.cancelFetch = cancel
	go lb.fetch(ctx, reason, wsUser, lb.generation, lb.verifiedUser != wsUser)
}

func (lb *LiveBalances) fetch(ctx context.Context, reason, wsUser string, gen int, checkOwner bool) {
	fail := func(err error) {
		lb.mu.Lock()
		defer lb.mu.Unlock()
		lb.fetching = false
		if lb.closed {
			return
		}
		lb.lastErr = err
		lb.notifyLocked(func() {
			if lb.opts.OnError != nil {
				lb.opts.OnError(err)
			}
		})
		delay := lb.opts.RetryDelay << min(lb.attempt, 5)
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		lb.attempt++
		lb.cancelTimerLocked()
		lb.timer = lb.clock.AfterFunc(delay, func() {
			lb.mu.Lock()
			lb.timer = nil
			lb.triggerLocked("retry")
			lb.mu.Unlock()
		})
	}
	overtaken := func() bool { // under lb.mu
		if lb.closed || gen != lb.generation {
			lb.fetching = false
			again := lb.again
			lb.again = ""
			if again != "" && !lb.closed {
				lb.triggerLocked(again)
			}
			return true
		}
		return false
	}
	if checkOwner {
		owner, err := lb.opts.OwnerID(ctx)
		if err != nil {
			fail(err)
			return
		}
		lb.mu.Lock()
		if overtaken() {
			lb.mu.Unlock()
			return
		}
		if owner != wsUser {
			lb.rows = map[string]Balance{}
			lb.tombstones = map[string]int64{}
			lb.fetching = false
			lb.again = ""
			mismatch := &AccountMismatchError{WebSocketUserID: wsUser, SnapshotUserID: owner}
			lb.lastErr = mismatch
			lb.notifyLocked(func() {
				if lb.opts.OnError != nil {
					lb.opts.OnError(mismatch)
				}
			})
			lb.mu.Unlock()
			return
		}
		lb.verifiedUser = wsUser
		lb.mu.Unlock()
	}
	rows, err := lb.opts.Snapshot(ctx)
	if err != nil {
		fail(err)
		return
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if overtaken() {
		return
	}
	lb.applySnapshotLocked(rows)
	lb.attempt = 0
	lb.lastSuccess = lb.clock.Now()
	lb.haveSuccess = true
	lb.stale = false
	lb.lastErr = nil
	lb.fetching = false
	lb.notifyLocked(func() {
		if lb.opts.OnSnapshot != nil {
			lb.opts.OnSnapshot(reason)
		}
	})
	again := lb.again
	lb.again = ""
	if again != "" {
		lb.triggerLocked(again)
	}
}

func isZeroAmount(a Amount) bool {
	r, ok := new(big.Rat).SetString(string(a))
	return ok && r.Sign() == 0
}

func (lb *LiveBalances) applySnapshotLocked(rows []Balance) {
	fresh := map[string]Balance{}
	for _, r := range rows {
		if tomb, ok := lb.tombstones[r.Asset]; ok {
			if r.Sequence <= tomb {
				continue
			}
			delete(lb.tombstones, r.Asset)
		}
		if isZeroAmount(r.Total) {
			continue
		}
		r.HeldIncoming = nonNilHeld(r.HeldIncoming)
		fresh[r.Asset] = r
	}
	lb.rows = fresh
	buffered := lb.buffer
	lb.buffer = nil
	for _, d := range buffered {
		lb.applyLocked(d, false)
	}
}

func (lb *LiveBalances) applyLocked(d balanceUpdate, emit bool) {
	prev, havePrev := lb.rows[d.Asset]
	current := int64(-1)
	if havePrev {
		current = prev.Sequence
	} else if t, ok := lb.tombstones[d.Asset]; ok {
		current = t
	}
	// A server that predates live balances sends no data.sequence: such an event always applies
	// and keeps the stored sequence (a later sequenced snapshot or event takes over).
	var seq int64
	if d.Sequence != nil {
		seq = *d.Sequence
		if seq <= current {
			return // duplicate or older
		}
	} else {
		if !lb.warnedNoSeq {
			lb.warnedNoSeq = true
			lb.ws.log.Warn("cexy: balance.updated without data.sequence; applying every event in arrival order")
		}
		seq = max(current, 0)
	}
	if isZeroAmount(d.Total) {
		delete(lb.rows, d.Asset)
		if d.Sequence != nil {
			lb.tombstones[d.Asset] = seq
		}
		if emit {
			asset := d.Asset
			lb.notifyLocked(func() {
				if lb.opts.OnUpdate != nil {
					lb.opts.OnUpdate(asset, nil)
				}
			})
		}
		return
	}
	row := prev
	row.Asset, row.Available, row.Locked, row.Pending, row.Total, row.Sequence = d.Asset, d.Available, d.Locked, d.Pending, d.Total, seq
	row.HeldIncoming = nonNilHeld(row.HeldIncoming)
	lb.rows[d.Asset] = row
	delete(lb.tombstones, d.Asset)
	if emit {
		asset, cp := d.Asset, row
		lb.notifyLocked(func() {
			if lb.opts.OnUpdate != nil {
				lb.opts.OnUpdate(asset, &cp)
			}
		})
	}
}
