package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// wsFakeClock runs timers only when advance passes their due time.
type wsFakeClock struct {
	mu     sync.Mutex
	now    time.Time
	seq    int
	timers map[int]*wsFakeTimer
}

type wsFakeTimer struct {
	c   *wsFakeClock
	id  int
	due time.Time
	f   func()
}

func (t *wsFakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	_, ok := t.c.timers[t.id]
	delete(t.c.timers, t.id)
	return ok
}

func newWSFakeClock() *wsFakeClock {
	return &wsFakeClock{now: time.Unix(1_790_000_000, 0), timers: map[int]*wsFakeTimer{}}
}

func (c *wsFakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *wsFakeClock) AfterFunc(d time.Duration, f func()) WSTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	t := &wsFakeTimer{c: c, id: c.seq, due: c.now.Add(max(d, 0)), f: f}
	c.timers[t.id] = t
	return t
}

func (c *wsFakeClock) advance(d time.Duration) {
	c.mu.Lock()
	end := c.now.Add(d)
	for {
		var next *wsFakeTimer
		for _, t := range c.timers {
			if !t.due.After(end) && (next == nil || t.due.Before(next.due) || (t.due.Equal(next.due) && t.id < next.id)) {
				next = t
			}
		}
		if next == nil {
			break
		}
		delete(c.timers, next.id)
		c.now = next.due
		c.mu.Unlock()
		next.f()
		c.mu.Lock()
	}
	c.now = end
	c.mu.Unlock()
}

// scriptedSource is a snapshot or owner source whose calls the script answers one by one.
type scriptedSource[T any] struct {
	mu      sync.Mutex
	calls   int
	pending []chan T
}

func (s *scriptedSource[T]) call(context.Context) (T, error) {
	ch := make(chan T, 1)
	s.mu.Lock()
	s.calls++
	s.pending = append(s.pending, ch)
	s.mu.Unlock()
	return <-ch, nil
}

func (s *scriptedSource[T]) waiting() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

func (s *scriptedSource[T]) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scriptedSource[T]) answer(v T) {
	s.mu.Lock()
	ch := s.pending[0]
	s.pending = s.pending[1:]
	s.mu.Unlock()
	ch <- v
}

type liveCase struct {
	ID      string `json:"id"`
	Options struct {
		ReorderWindowMs       *int `json:"reorder_window_ms"`
		MinSnapshotIntervalMs *int `json:"min_snapshot_interval_ms"`
	} `json:"options"`
	Steps []map[string]json.RawMessage `json:"steps"`
}

// TestWSLiveBalancesConformance runs conformance/ws/live_balances.json and
// private_sequence_gap.json step by step with an injected clock.
func TestWSLiveBalancesConformance(t *testing.T) {
	dir := specDir(t)
	for _, file := range []string{"live_balances.json", "private_sequence_gap.json"} {
		var spec struct {
			Cases []liveCase `json:"cases"`
		}
		readJSON(t, filepath.Join(dir, "conformance", "ws", file), &spec)
		if len(spec.Cases) == 0 {
			t.Fatalf("%s: no cases", file)
		}
		for _, c := range spec.Cases {
			t.Run(file+"/"+c.ID, func(t *testing.T) { runLiveCase(t, c) })
		}
	}
}

func runLiveCase(t *testing.T, c liveCase) {
	s := &scriptedWS{}
	srv := httptest.NewServer(http.HandlerFunc(s.handler))
	defer srv.Close()
	clock := newWSFakeClock()
	var evMu sync.Mutex
	var events []map[string]any
	var errs []string
	record := func(e map[string]any) {
		evMu.Lock()
		events = append(events, e)
		evMu.Unlock()
	}
	window := 250 * time.Millisecond
	if c.Options.ReorderWindowMs != nil {
		window = time.Duration(*c.Options.ReorderWindowMs) * time.Millisecond
	}
	ws, err := NewWebSocket(WSOptions{
		URL: "ws" + srv.URL[len("http"):] + "/api/v1/ws", AllowInsecure: true, NoReconnect: true, AckTimeout: 2 * time.Second,
		ReorderWindow: window, Clock: clock,
		Handlers: WSHandlers{
			OnResync: func(r ResyncReason) { record(map[string]any{"type": "resync", "reason": string(r)}) },
			OnSequenceGap: func(g SequenceGap) {
				record(map[string]any{"type": "sequence_gap", "channel": g.Channel, "expected": float64(g.Expected), "received": float64(g.Received)})
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	owner := &scriptedSource[string]{}
	snapshot := &scriptedSource[[]Balance]{}
	var lbMu sync.Mutex
	var lb *LiveBalances
	helper := func() *LiveBalances { lbMu.Lock(); defer lbMu.Unlock(); return lb }
	settle := func() {
		for i := 0; i < 2; i++ {
			if _, err := ws.Ping(ctx); err != nil {
				t.Fatal(err)
			}
		}
		done := make(chan struct{})
		ws.dispatch(func() { close(done) })
		<-done
		if h := helper(); h != nil {
			done2 := make(chan struct{})
			h.mu.Lock()
			h.notifyLocked(func() { close(done2) })
			h.mu.Unlock()
			select {
			case <-done2:
			case <-time.After(2 * time.Second):
			}
		}
	}
	minInterval := 2 * time.Second
	if c.Options.MinSnapshotIntervalMs != nil {
		minInterval = time.Duration(*c.Options.MinSnapshotIntervalMs) * time.Millisecond
		if minInterval == 0 {
			minInterval = -1 // no minimum (0 selects the default)
		}
	}
	answered := map[any]bool{}
	sentMark := 0
	for i, st := range c.Steps {
		at := func(format string, a ...any) {
			t.Helper()
			t.Fatalf("%s step %d: "+format, append([]any{c.ID, i}, a...)...)
		}
		get := func(k string, v any) bool {
			raw, ok := st[k]
			if !ok {
				return false
			}
			if err := json.Unmarshal(raw, v); err != nil {
				at("%s: %v", k, err)
			}
			return true
		}
		var client string
		var server map[string]any
		var n int
		var b bool
		var strs []string
		switch {
		case get("client", &client):
			before := len(s.requests())
			switch client {
			case "auth":
				var tok string
				get("token", &tok)
				go func() { _, _ = ws.Auth(ctx, tok) }()
			case "subscribe":
				var ch []string
				get("channels", &ch)
				go func() { _, _ = ws.Subscribe(ctx, ch...) }()
			case "live_balances":
				go func() {
					h, err := ws.LiveBalances(ctx, LiveBalancesOptions{
						Snapshot: snapshot.call, OwnerID: owner.call, MinSnapshotInterval: minInterval,
						OnError: func(e error) {
							code := "ERROR"
							if _, ok := e.(*AccountMismatchError); ok {
								code = "ACCOUNT_MISMATCH"
							}
							evMu.Lock()
							errs = append(errs, code)
							evMu.Unlock()
						},
					})
					if err == nil {
						lbMu.Lock()
						lb = h
						lbMu.Unlock()
					}
				}()
			}
			eventually(t, client+" frame", func() bool { return len(s.requests()) > before })
		case get("server", &server):
			var replyTo string
			if get("reply_to", &replyTo) {
				reqs := s.requests()
				var req map[string]any
				for j := len(reqs) - 1; j >= 0; j-- {
					if reqs[j]["op"] == replyTo && !answered[reqs[j]["id"]] {
						req = reqs[j]
						break
					}
				}
				if req == nil {
					at("no unanswered %s", replyTo)
				}
				answered[req["id"]] = true
				server["id"] = req["id"]
			}
			s.write(server)
			settle()
		case get("owner", new(string)):
			var o string
			get("owner", &o)
			eventually(t, "owner request", func() bool { return owner.waiting() > 0 })
			owner.answer(o)
			settle()
		case st["snapshot"] != nil:
			var rows []Balance
			get("snapshot", &rows)
			eventually(t, "snapshot request", func() bool { return snapshot.waiting() > 0 })
			snapshot.answer(rows)
			settle()
		case get("advance_ms", &n):
			clock.advance(time.Duration(n) * time.Millisecond)
			settle()
		case st["expect_sent"] != nil:
			var want []map[string]any
			get("expect_sent", &want)
			settle()
			reqs := s.requests()
			got := []map[string]any{}
			for _, m := range reqs[sentMark:] {
				got = append(got, normSent(m))
			}
			sentMark = len(reqs)
			w := []map[string]any{}
			for _, m := range want {
				w = append(w, normSent(m))
			}
			if !reflect.DeepEqual(got, w) {
				at("sent %v, want %v", got, w)
			}
		case st["expect_requests"] != nil:
			var want map[string]int
			get("expect_requests", &want)
			settle()
			if got := map[string]int{"owner": owner.count(), "snapshot": snapshot.count()}; !reflect.DeepEqual(got, want) {
				at("requests %v, want %v", got, want)
			}
		case st["expect_state"] != nil:
			var want map[string]struct {
				Total    string `json:"total"`
				Sequence int64  `json:"sequence"`
			}
			get("expect_state", &want)
			settle()
			got := map[string]Balance{}
			if h := helper(); h != nil {
				for _, r := range h.All() {
					got[r.Asset] = r
				}
			}
			var gk, wk []string
			for k := range got {
				gk = append(gk, k)
			}
			for k := range want {
				wk = append(wk, k)
			}
			sort.Strings(gk)
			sort.Strings(wk)
			if !reflect.DeepEqual(gk, wk) {
				at("assets %v, want %v", gk, wk)
			}
			for k, v := range want {
				if string(got[k].Total) != v.Total || got[k].Sequence != v.Sequence {
					at("%s = %s@%d, want %s@%d", k, got[k].Total, got[k].Sequence, v.Total, v.Sequence)
				}
			}
		case get("expect_stale", &b):
			settle()
			stale := true
			if h := helper(); h != nil {
				stale = h.Stale()
			}
			if stale != b {
				at("stale %v, want %v", stale, b)
			}
		case get("expect_errors", &strs):
			settle()
			evMu.Lock()
			got := errs
			errs = nil
			evMu.Unlock()
			if len(got) != len(strs) || (len(got) > 0 && !reflect.DeepEqual(got, strs)) {
				at("errors %v, want %v", got, strs)
			}
		case st["expect_events"] != nil:
			var want []map[string]any
			get("expect_events", &want)
			settle()
			evMu.Lock()
			got := events
			events = nil
			evMu.Unlock()
			if len(got) != len(want) {
				at("events %v, want %v", got, want)
			}
			for _, w := range want {
				if !slices.ContainsFunc(got, func(g map[string]any) bool { return matchEvent(g, w) }) {
					at("no event %v in %v", w, got)
				}
			}
		default:
			at("unknown step %v", st)
		}
	}
	if h := helper(); h != nil {
		h.Close()
	}
}

func TestLiveBalancesUnsequencedEventsApplyAndWarnOnce(t *testing.T) {
	var logs syncBuffer
	ws, m := setupWS(t, WSOptions{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	lb, err := ws.LiveBalances(ctx, LiveBalancesOptions{
		AccountID: "u1",
		Snapshot: func(context.Context) ([]Balance, error) {
			return []Balance{{Asset: "USDT", Available: "100", Locked: "0", Pending: "0", Total: "100", Sequence: 40}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lb.Close()
	eventually(t, "first snapshot", func() bool { return !lb.Stale() })
	push := func(d map[string]any) {
		d["available"], d["locked"], d["pending"] = "0", "0", "0"
		m.push(map[string]any{"type": "balance.updated", "channel": "balances", "data": d})
	}
	push(map[string]any{"asset": "USDT", "total": "90"})
	push(map[string]any{"asset": "USDT", "total": "80"})
	eventually(t, "unsequenced applied", func() bool { b, _ := lb.Get("USDT"); return b.Total == "80" })
	if b, _ := lb.Get("USDT"); b.Sequence != 40 {
		t.Fatalf("sequence moved: %d", b.Sequence)
	}
	if n := strings.Count(logs.String(), "without data.sequence"); n != 1 {
		t.Fatalf("%d warnings", n)
	}
	push(map[string]any{"asset": "USDT", "total": "70", "sequence": 41})
	push(map[string]any{"asset": "USDT", "total": "60", "sequence": 41})
	eventually(t, "sequenced", func() bool { b, _ := lb.Get("USDT"); return b.Total == "70" && b.Sequence == 41 })
	if _, err := ws.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if b, _ := lb.Get("USDT"); b.Total != "70" {
		t.Fatalf("duplicate applied: %s", b.Total)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b = append(s.b, p...)
	return len(p), nil
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.b)
}

func TestLiveBalancesDefaultOwnerMismatchNeverMerges(t *testing.T) {
	ws, m := setupWS(t, WSOptions{})
	m.ownerID.Store("someone_else")
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	lb, err := ws.LiveBalances(ctx, LiveBalancesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer lb.Close()
	eventually(t, "mismatch", func() bool { return lb.LastError() != nil })
	var mm *AccountMismatchError
	if !errors.As(lb.LastError(), &mm) || mm.SnapshotUserID != "someone_else" || mm.WebSocketUserID != "u1" {
		t.Fatalf("last error %v", lb.LastError())
	}
	if n := m.balCalls.Load(); n != 0 || len(lb.All()) != 0 || !lb.Stale() {
		t.Fatalf("merged after a mismatch: %d balance calls, %d rows, stale %v", n, len(lb.All()), lb.Stale())
	}
}

func TestLiveBalancesReviewFixes(t *testing.T) {
	t.Run("custom snapshot needs an owner", func(t *testing.T) {
		ws, m := setupWS(t, WSOptions{})
		m.ownerID.Store("u1")
		ctx := context.Background()
		if _, err := ws.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.Auth(ctx, "good"); err != nil {
			t.Fatal(err)
		}
		_, err := ws.LiveBalances(ctx, LiveBalancesOptions{Snapshot: func(context.Context) ([]Balance, error) { return nil, nil }})
		var ce *ConfigError
		if !errors.As(err, &ce) {
			t.Fatalf("want ConfigError, got %v", err)
		}
	})
	t.Run("no buffering while unverified; Close marks stale; Code", func(t *testing.T) {
		ws, m := setupWS(t, WSOptions{})
		ctx := context.Background()
		if _, err := ws.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.Auth(ctx, "good"); err != nil {
			t.Fatal(err)
		}
		owner := "someone_else"
		var mu sync.Mutex
		lb, err := ws.LiveBalances(ctx, LiveBalancesOptions{
			OwnerID: func(context.Context) (string, error) { mu.Lock(); defer mu.Unlock(); return owner, nil },
			Snapshot: func(context.Context) ([]Balance, error) {
				return []Balance{{Asset: "USDT", Available: "5", Locked: "0", Pending: "0", Total: "5", Sequence: 10}}, nil
			},
			MinSnapshotInterval: NoMinimum,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer lb.Close()
		eventually(t, "mismatch", func() bool { return lb.LastError() != nil })
		var mm *AccountMismatchError
		if !errors.As(lb.LastError(), &mm) || mm.Code() != "ACCOUNT_MISMATCH" {
			t.Fatalf("last error %v", lb.LastError())
		}
		for i := 0; i < 500; i++ {
			m.push(map[string]any{"type": "balance.updated", "channel": "balances",
				"data": map[string]any{"asset": "USDT", "available": "9", "locked": "0", "pending": "0", "total": "9", "sequence": 50 + i}})
		}
		if _, err := ws.Ping(ctx); err != nil {
			t.Fatal(err)
		}
		lb.mu.Lock()
		n := len(lb.buffer)
		lb.mu.Unlock()
		if n != 0 {
			t.Fatalf("%d events buffered while unverified", n)
		}
		mu.Lock()
		owner = "u1"
		mu.Unlock()
		m.push(map[string]any{"type": "balances.resync", "channel": "balances", "data": map[string]any{}})
		eventually(t, "snapshot", func() bool { return !lb.Stale() })
		if b, _ := lb.Get("USDT"); b.Total != "5" || b.Sequence != 10 {
			t.Fatalf("dropped events applied: %+v", b)
		}
		_ = ws.Close()
		if !lb.Stale() {
			t.Fatal("not stale after Close")
		}
	})
	t.Run("signed_out payloads", func(t *testing.T) {
		var mu sync.Mutex
		var lost []Event
		var changes []AuthChange
		ws, m := setupWS(t, WSOptions{Handlers: WSHandlers{
			OnAuthLost:    func(e Event) { mu.Lock(); lost = append(lost, e); mu.Unlock() },
			OnAuthChanged: func(c AuthChange) { mu.Lock(); changes = append(changes, c); mu.Unlock() },
		}})
		ctx := context.Background()
		if _, err := ws.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.Auth(ctx, "good"); err != nil {
			t.Fatal(err)
		}
		m.push(map[string]any{"type": "signed_out", "reason": "revoked"})
		eventually(t, "authLost", func() bool { mu.Lock(); defer mu.Unlock(); return len(lost) == 1 })
		mu.Lock()
		got := lost[0]
		mu.Unlock()
		var data map[string]any
		_ = json.Unmarshal(got.Data, &data)
		if got.Type != "session.revoked" || got.Channel != "account" || data["session_id"] != nil || data["reason"] != "signed_out" || data["current"] != true || len(data) != 3 {
			t.Fatalf("synthetic authLost %+v %v", got, data)
		}
		if _, err := ws.Auth(ctx, "good"); err != nil {
			t.Fatal(err)
		}
		m.push(map[string]any{"type": "signed_out"})
		eventually(t, "unknown", func() bool {
			mu.Lock()
			defer mu.Unlock()
			last := changes[len(changes)-1]
			return last.Reason == AuthSignedOut && last.Code == "unknown"
		})
	})
}

func TestLiveBalancesFollowUps(t *testing.T) {
	t.Run("owner-lookup events dropped on mismatch", func(t *testing.T) {
		ws, m := setupWS(t, WSOptions{})
		ctx := context.Background()
		if _, err := ws.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.Auth(ctx, "good"); err != nil {
			t.Fatal(err)
		}
		answer := make(chan string)
		lb, err := ws.LiveBalances(ctx, LiveBalancesOptions{
			OwnerID:  func(context.Context) (string, error) { return <-answer, nil },
			Snapshot: func(context.Context) ([]Balance, error) { return nil, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer lb.Close()
		for i := 0; i < 5; i++ {
			m.push(map[string]any{"type": "balance.updated", "channel": "balances",
				"data": map[string]any{"asset": "USDT", "available": "1", "locked": "0", "pending": "0", "total": "1", "sequence": i + 1}})
		}
		buffered := func() int { lb.mu.Lock(); defer lb.mu.Unlock(); return len(lb.buffer) }
		eventually(t, "5 buffered during the lookup", func() bool { return buffered() == 5 })
		answer <- "someone_else"
		eventually(t, "mismatch", func() bool { return lb.LastError() != nil })
		if n := buffered(); n != 0 {
			t.Fatalf("%d events kept after the mismatch", n)
		}
	})
	t.Run("LiveBalances.Close cancels the fetch in flight", func(t *testing.T) {
		ws, _ := setupWS(t, WSOptions{})
		ctx := context.Background()
		if _, err := ws.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.Auth(ctx, "good"); err != nil {
			t.Fatal(err)
		}
		started := make(chan struct{})
		canceled := make(chan error, 1)
		lb, err := ws.LiveBalances(ctx, LiveBalancesOptions{
			AccountID: "u1",
			Snapshot: func(fctx context.Context) ([]Balance, error) {
				close(started)
				<-fctx.Done()
				canceled <- fctx.Err()
				return nil, fctx.Err()
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		<-started
		lb.Close()
		select {
		case err := <-canceled:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("fetch ended with %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not cancel the fetch")
		}
	})
}
