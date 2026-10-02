package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// mockWS is a WebSocket server that follows the protocol: welcome on connect, and every
// request with an id acknowledged with that id.
type mockWS struct {
	mu        sync.Mutex
	conns     []*websocket.Conn
	frames    []map[string]any // every client frame, all connections
	silent    bool             // no welcome, no acks (liveness tests)
	refuseSub atomic.Bool      // answer subscribe with UNAUTHENTICATED
	ownerID   atomic.Value     // string: GET /api/v1/account/id answer
	balCalls  atomic.Int32     // GET /api/v1/account/balances calls
	snapshots atomic.Int32
	seq       atomic.Int64 // sequence of the REST snapshot
}

func (m *mockWS) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/ws" {
			if r.URL.Path == "/api/v1/markets/BTC%2FUSDT/orderbook" || r.URL.EscapedPath() == "/api/v1/markets/BTC%2FUSDT/orderbook" {
				m.snapshots.Add(1)
				writeJSON(w, 200, dataEnv(map[string]any{"symbol": "BTC/USDT", "sequence": m.seq.Load(),
					"timestamp": "2026-09-27T10:00:00Z", "bids": [][]string{{"100", "1"}}, "asks": [][]string{{"101", "1"}}}))
				return
			}
			if r.URL.Path == "/api/v1/account/id" {
				owner, _ := m.ownerID.Load().(string)
				writeJSON(w, 200, dataEnv(map[string]any{"user_id": owner}))
				return
			}
			if r.URL.Path == "/api/v1/account/balances" {
				m.balCalls.Add(1)
				writeJSON(w, 200, dataEnv([]any{}))
				return
			}
			writeJSON(w, 404, apiErr("NOT_FOUND", "nothing here", false))
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		m.mu.Lock()
		m.conns = append(m.conns, c)
		silent := m.silent
		m.mu.Unlock()
		ctx := context.Background()
		if !silent {
			m.write(c, map[string]any{"type": "welcome", "protocol_version": 1, "heartbeat_interval_seconds": 30,
				"max_subscriptions": 100, "connection_id": "c1"})
		}
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			if json.Unmarshal(data, &f) != nil {
				continue
			}
			m.mu.Lock()
			m.frames = append(m.frames, f)
			silent := m.silent
			m.mu.Unlock()
			if silent {
				continue
			}
			id := f["id"]
			switch f["op"] {
			case "subscribe":
				if m.refuseSub.Load() {
					m.write(c, map[string]any{"type": "error", "code": "UNAUTHENTICATED", "message": "authentication required", "id": id})
					continue
				}
				m.write(c, map[string]any{"type": "subscribed", "channels": f["channels"], "id": id})
			case "unsubscribe":
				m.write(c, map[string]any{"type": "unsubscribed", "channels": f["channels"], "id": id})
			case "auth":
				if f["token"] == "good" {
					m.write(c, map[string]any{"type": "authenticated", "user_id": "u1", "id": id})
				} else {
					m.write(c, map[string]any{"type": "error", "code": "UNAUTHENTICATED", "message": "bad token", "id": id})
				}
			case "ping":
				if id != nil {
					m.write(c, map[string]any{"type": "pong", "id": id})
				}
			}
		}
	}
}

func (m *mockWS) write(c *websocket.Conn, v any) {
	b, _ := json.Marshal(v)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Write(ctx, websocket.MessageText, b)
}

func (m *mockWS) last() *websocket.Conn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conns[len(m.conns)-1]
}

func (m *mockWS) connCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.conns)
}

func (m *mockWS) push(v any) { m.write(m.last(), v) }

func (m *mockWS) sent(op string) []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []map[string]any
	for _, f := range m.frames {
		if f["op"] == op {
			out = append(out, f)
		}
	}
	return out
}

func setupWS(t *testing.T, opts WSOptions) (*WebSocket, *mockWS) {
	t.Helper()
	m := &mockWS{}
	m.seq.Store(10)
	srv := httptest.NewServer(m.handler(t))
	t.Cleanup(srv.Close)
	c, err := New(Options{BaseURL: srv.URL, AllowInsecure: true, DisableRateLimit: true, APIKey: "ak_test", APISecret: "test_secret_value"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.ReconnectBaseDelay == 0 {
		opts.ReconnectBaseDelay = 10 * time.Millisecond
	}
	if opts.AckTimeout == 0 {
		opts.AckTimeout = time.Second
	}
	ws, err := c.WebSocket(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws, m
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestWSRejectsInsecureURL(t *testing.T) {
	for _, u := range []string{"ws://api.example.com/ws", "http://api.example.com/ws"} {
		if _, err := NewWebSocket(WSOptions{URL: u, AllowInsecure: true}); err == nil {
			t.Errorf("%s accepted", u)
		}
	}
	if _, err := NewWebSocket(WSOptions{URL: "ws://127.0.0.1:9/ws"}); err == nil {
		t.Error("ws:// accepted without AllowInsecure")
	}
}

func TestWSSubscribePingUnsubscribe(t *testing.T) {
	ws, m := setupWS(t, WSOptions{})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := ws.Subscribe(ctx, "ticker:BTC/USDT", "trades:BTC/USDT", "ticker:BTC/USDT")
	if err != nil || len(res.Added) != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	again, _ := ws.Subscribe(ctx, "ticker:BTC/USDT")
	if len(again.AlreadySubscribed) != 1 || len(m.sent("subscribe")) != 1 {
		t.Fatalf("%+v", again)
	}
	if _, err := ws.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ws.Unsubscribe(ctx, "trades:BTC/USDT"); err != nil {
		t.Fatal(err)
	}
	if got := ws.Channels(); len(got) != 1 || got[0] != "ticker:BTC/USDT" {
		t.Fatalf("channels %v", got)
	}
	for _, f := range append(m.sent("subscribe"), m.sent("unsubscribe")...) {
		if f["id"] == nil {
			t.Fatalf("request without id: %v", f)
		}
	}
}

func TestWSLocalSubscriptionCap(t *testing.T) {
	ws, _ := setupWS(t, WSOptions{MaxSubscriptions: 2})
	if _, err := ws.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	res, err := ws.Subscribe(context.Background(), "a:1", "a:2", "a:3")
	if err != nil || len(res.Refused) != 1 || res.Refused[0] != "a:3" {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestWSAuth(t *testing.T) {
	ws, _ := setupWS(t, WSOptions{})
	ctx := context.Background()
	queued, _ := ws.Auth(ctx, "good")
	if !queued.Queued {
		t.Fatal("not queued before connect")
	}
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := ws.Auth(ctx, "good")
	if err != nil || res.UserID != "u1" {
		t.Fatalf("%+v %v", res, err)
	}
	_, err = ws.Auth(ctx, "bad")
	var we *WSError
	if !errors.As(err, &we) || !we.FromServer || we.Code != "UNAUTHENTICATED" {
		t.Fatalf("got %v", err)
	}
	ws.mu.Lock()
	token := ws.token
	ws.mu.Unlock()
	if token != "" {
		t.Fatal("refused token kept for reconnect")
	}
}

func TestWSReconnectRestoresAuthAndChannels(t *testing.T) {
	var reconnected, resynced atomic.Int32
	ws, m := setupWS(t, WSOptions{Handlers: WSHandlers{
		OnReconnected: func(Welcome) { reconnected.Add(1) },
		OnResync:      func(r ResyncReason) { resynced.Add(1) },
	}})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Subscribe(ctx, "ticker:BTC/USDT"); err != nil {
		t.Fatal(err)
	}
	m.last().CloseNow() // the server drops the connection
	eventually(t, "reconnect", func() bool { return m.connCount() == 2 && ws.Connected() })
	eventually(t, "re-auth and re-subscribe", func() bool { return len(m.sent("auth")) == 2 && len(m.sent("subscribe")) == 2 })
	eventually(t, "handlers", func() bool { return reconnected.Load() == 1 && resynced.Load() == 1 })
	last := m.sent("subscribe")[1]["channels"].([]any)
	if len(last) != 1 || last[0] != "ticker:BTC/USDT" {
		t.Fatalf("resubscribed %v", last)
	}
}

func TestWSLivenessTimeoutReconnects(t *testing.T) {
	var closes []CloseInfo
	var mu sync.Mutex
	ws, m := setupWS(t, WSOptions{LivenessTimeout: 150 * time.Millisecond, PingInterval: maxPingInterval,
		Handlers: WSHandlers{OnClose: func(ci CloseInfo) { mu.Lock(); closes = append(closes, ci); mu.Unlock() }}})
	if _, err := ws.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "liveness close", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(closes) > 0 && closes[0].Code == 4000 && closes[0].WillReconnect
	})
	eventually(t, "reconnect", func() bool { return m.connCount() >= 2 })
}

func TestWSPingLoop(t *testing.T) {
	ws, m := setupWS(t, WSOptions{PingInterval: 30 * time.Millisecond})
	if _, err := ws.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "pings", func() bool { return len(m.sent("ping")) >= 2 })
}

func TestWSLiveOrderBook(t *testing.T) {
	var stale, healed atomic.Int32
	ws, m := setupWS(t, WSOptions{})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	book, err := ws.OrderBook(ctx, "BTC/USDT", LiveOrderBookOptions{
		OnStale:  func(expected, received int64) { stale.Add(1) },
		OnHealed: func() { healed.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if s := book.Snapshot(); !s.Synced || s.Sequence != 10 || s.Bids[0].Price != "100" {
		t.Fatalf("%+v", s)
	}
	if len(m.sent("subscribe")) != 1 || m.snapshots.Load() != 1 {
		t.Fatal("expected one subscribe before one snapshot")
	}
	update := func(seq int64, bid string) {
		m.push(map[string]any{"type": "orderbook.update", "channel": "orderbook:BTC/USDT", "sequence": seq,
			"data": map[string]any{"symbol": "BTC/USDT", "full": true, "bids": [][]string{{bid, "2"}}, "asks": [][]string{{"105", "1"}}}})
	}
	update(9, "90") // at or below the snapshot: ignored
	update(11, "111")
	eventually(t, "update 11", func() bool { return book.Snapshot().Sequence == 11 })
	if s := book.Snapshot(); s.Bids[0].Price != "111" || s.Stale {
		t.Fatalf("%+v", s)
	}
	update(13, "113") // gap
	eventually(t, "stale", func() bool { return book.Snapshot().Stale && stale.Load() == 1 })
	update(14, "114") // in order again
	eventually(t, "healed", func() bool { s := book.Snapshot(); return !s.Stale && s.Sequence == 14 && healed.Load() == 1 })

	// CONCURRENT_MODIFICATION without an id: a fresh snapshot.
	m.seq.Store(50)
	m.push(map[string]any{"type": "error", "code": "CONCURRENT_MODIFICATION", "message": "12 messages dropped", "id": nil})
	eventually(t, "resync", func() bool { return m.snapshots.Load() == 2 && book.Snapshot().Sequence == 50 })

	book.Close()
	eventually(t, "unsubscribe", func() bool { return len(m.sent("unsubscribe")) == 1 })
}

func TestWSConformanceFrames(t *testing.T) {
	dir := specDir(t)
	var pongs, events, authLost, serverErrors, resyncs atomic.Int32
	ws, err := NewWebSocket(WSOptions{Handlers: WSHandlers{
		OnPong:        func(id string) { pongs.Add(1) },
		OnEvent:       func(Event) { events.Add(1) },
		OnAuthLost:    func(Event) { authLost.Add(1) },
		OnServerError: func(*WSError) { serverErrors.Add(1) },
		OnResync:      func(ResyncReason) { resyncs.Add(1) },
	}})
	if err != nil {
		t.Fatal(err)
	}
	ws.mu.Lock()
	ws.startDispatcherLocked()
	ws.channels = []string{"orders", "ticker:BTC/USDT"}
	ws.token = "tok"
	ws.mu.Unlock()
	files, _ := filepath.Glob(filepath.Join(dir, "conformance", "ws", "*.json"))
	for _, f := range files {
		if filepath.Base(f) == "private_signout.json" {
			continue // a script, not a frame (see TestWSPrivateSignoutConformance)
		}
		if filepath.Base(f) == "welcome.json" {
			var wel Welcome
			readJSON(t, f, &wel)
			if wel.ProtocolVersion != SupportedProtocolVersion || wel.HeartbeatIntervalSeconds != 30 {
				t.Errorf("welcome %+v", wel)
			}
			continue
		}
		b, _ := os.ReadFile(f)
		ws.onFrame(parseFrame(b))
	}
	eventually(t, "handlers", func() bool {
		return pongs.Load() == 1 && events.Load() == 2 && authLost.Load() == 1 && serverErrors.Load() == 1 && resyncs.Load() == 1
	})
	if got := ws.Channels(); len(got) != 1 || got[0] != "ticker:BTC/USDT" {
		t.Fatalf("private channels kept after session.revoked: %v", got)
	}
	var ob OrderBookUpdate
	var ev Event
	readJSON(t, filepath.Join(dir, "conformance", "ws", "orderbook_update.json"), &ev)
	if err := ev.Decode(&ob); err != nil || !ob.Full || Levels(ob.Bids)[0].Price != "61000.10" || *ev.Sequence != 1042 {
		t.Fatalf("%+v %v", ob, err)
	}
	ws.stopDispatcher()
}

func TestWSNotConnected(t *testing.T) {
	ws, _ := setupWS(t, WSOptions{})
	if _, err := ws.Ping(context.Background()); err == nil {
		t.Fatal("ping while disconnected succeeded")
	}
	res, err := ws.Subscribe(context.Background(), "ticker:BTC/USDT")
	if err != nil || len(res.Added) != 0 || len(ws.Channels()) != 1 {
		t.Fatalf("queued subscribe: %+v %v", res, err)
	}
}

func TestWSFailedReauthThenReconnectRestoresPrivateOnNextAuth(t *testing.T) {
	var mu sync.Mutex
	var changes []AuthChange
	var resyncs []ResyncReason
	ws, m := setupWS(t, WSOptions{Handlers: WSHandlers{
		OnAuthChanged: func(a AuthChange) { mu.Lock(); changes = append(changes, a); mu.Unlock() },
		OnResync:      func(r ResyncReason) { mu.Lock(); resyncs = append(resyncs, r); mu.Unlock() },
	}})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Subscribe(ctx, "orders", "ticker:BTC/USDT"); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "expired"); err == nil {
		t.Fatal("refused auth succeeded")
	}
	if got := ws.Channels(); len(got) != 1 || got[0] != "ticker:BTC/USDT" || ws.HasToken() {
		t.Fatalf("after failed auth: %v token=%v", got, ws.HasToken())
	}
	eventually(t, "auth_changed", func() bool { mu.Lock(); defer mu.Unlock(); return len(changes) == 1 })
	mu.Lock()
	c := changes[0]
	mu.Unlock()
	if c.Reason != AuthFailed || c.PreviousUserID != "u1" || c.Code != "UNAUTHENTICATED" || len(c.Dropped) != 1 || c.Dropped[0] != "orders" {
		t.Fatalf("change %+v", c)
	}
	// A private subscribe while signed out is not re-sent: it is pending.
	if res, _ := ws.Subscribe(ctx, "orders"); len(res.AlreadySubscribed) != 1 {
		t.Fatalf("subscribe while pending: %+v", res)
	}
	subsBefore := len(m.sent("subscribe"))
	_ = m.last().Close(websocket.StatusGoingAway, "restart")
	eventually(t, "reconnect", func() bool { return m.connCount() == 2 && ws.Connected() && len(m.sent("subscribe")) == subsBefore+1 })
	if len(m.sent("auth")) != 2 {
		t.Fatalf("refused token re-sent after reconnect: %d auth frames", len(m.sent("auth")))
	}
	if got := m.sent("subscribe")[subsBefore]["channels"]; !reflect.DeepEqual(stringList(got), []string{"ticker:BTC/USDT"}) {
		t.Fatalf("reconnect subscribe %v", got)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "private re-subscribe", func() bool { return len(m.sent("subscribe")) == subsBefore+2 })
	if got := stringList(m.sent("subscribe")[subsBefore+1]["channels"]); !reflect.DeepEqual(got, []string{"orders"}) {
		t.Fatalf("private re-subscribe %v", got)
	}
	eventually(t, "reauth resync", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(resyncs, ResyncReauth)
	})
}

func TestWSSessionRevokedNeedsCurrentTrue(t *testing.T) {
	ws, m := setupWS(t, WSOptions{})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Subscribe(ctx, "account", "orders"); err != nil {
		t.Fatal(err)
	}
	for _, current := range []any{false, "true", nil} {
		m.push(map[string]any{"type": "session.revoked", "channel": "account", "data": map[string]any{"session_id": nil, "reason": "logout", "current": current}})
	}
	for i := 0; i < 2; i++ {
		if _, err := ws.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := ws.Channels(); len(got) != 2 || !ws.HasToken() {
		t.Fatalf("non-current revoke acted: %v token=%v", got, ws.HasToken())
	}
}

func TestWSRefusedPrivateResubscribeGoesBackToPending(t *testing.T) {
	ws, m := setupWS(t, WSOptions{})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Subscribe(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	m.push(map[string]any{"type": "session.revoked", "channel": "account", "data": map[string]any{"session_id": nil, "reason": "logout_all", "current": true}})
	eventually(t, "privates dropped", func() bool { return len(ws.Channels()) == 0 })
	m.refuseSub.Store(true)
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "refused re-subscribe", func() bool { return len(m.sent("subscribe")) == 2 })
	for i := 0; i < 2; i++ {
		_, _ = ws.Ping(ctx)
	}
	eventually(t, "back to pending", func() bool { return len(ws.Channels()) == 0 })
	m.refuseSub.Store(false)
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "retry", func() bool { return len(m.sent("subscribe")) == 3 && len(ws.Channels()) == 1 })
}

func TestWSRefusedSubscribeIsNotHeld(t *testing.T) {
	ws, m := setupWS(t, WSOptions{})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	m.refuseSub.Store(true)
	if _, err := ws.Subscribe(ctx, "orders"); err == nil {
		t.Fatal("refused subscribe succeeded")
	}
	if got := ws.Channels(); len(got) != 0 {
		t.Fatalf("refused channel held: %v", got)
	}
}
