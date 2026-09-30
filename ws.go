package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// DefaultWebSocketURL is the production WebSocket endpoint.
const DefaultWebSocketURL = "wss://api.cexy.io/api/v1/ws"

// SupportedProtocolVersion is the WebSocket protocol version this SDK was written for.
const SupportedProtocolVersion = 1

const maxChannelLength = 64

// PrivateChannels need Auth with a session access token.
var PrivateChannels = map[string]bool{"orders": true, "balances": true, "deposits": true, "withdrawals": true, "account": true}

var knownEventTypes = map[string]bool{
	"ticker.update": true, "orderbook.update": true, "trade.new": true, "market.status": true,
	"order.created": true, "order.updated": true, "order.cancelled": true, "order.filled": true,
	"balance.updated": true, "deposit.detected": true, "deposit.updated": true, "deposit.completed": true,
	"withdrawal.updated": true, "session.revoked": true,
}

// WSError is a WebSocket protocol error, a server error frame (FromServer), or a local guard.
type WSError struct {
	Code    string
	Message string
	// True when it came from a server error frame (not a local guard or a disconnect).
	FromServer bool
}

func (e *WSError) Error() string { return "cexy websocket: [" + e.Code + "] " + e.Message }

// Welcome is the server's first frame on every connection.
type Welcome struct {
	ProtocolVersion int `json:"protocol_version"`
	// The server's own pong cadence (30), not a client deadline.
	HeartbeatIntervalSeconds int    `json:"heartbeat_interval_seconds"`
	MaxSubscriptions         int    `json:"max_subscriptions"`
	ConnectionID             string `json:"connection_id"`
}

// Event is a channel event such as ticker.update, orderbook.update or order.filled. Decode
// Data with Decode. Unknown event types are ignored (they may be added without notice).
type Event struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	// Per channel, +1 per update. Resets per connection and when the server restarts.
	Sequence  *int64          `json:"sequence,omitempty"`
	Timestamp string          `json:"timestamp,omitempty"`
	Data      json.RawMessage `json:"data"`
}

// Decode unmarshals Data into v (for example *OrderBookUpdate or *Order).
func (e Event) Decode(v any) error { return json.Unmarshal(e.Data, v) }

// OrderBookUpdate is the data of orderbook.update: always the complete top 50 levels of both
// sides (Full is always true). It replaces the previous book; there are no deltas.
type OrderBookUpdate struct {
	Symbol string     `json:"symbol"`
	Full   bool       `json:"full"`
	Bids   [][]Amount `json:"bids"`
	Asks   [][]Amount `json:"asks"`
}

// SessionRevoked is the data of session.revoked on the account channel.
type SessionRevoked struct {
	SessionID *string `json:"session_id"`
	Reason    string  `json:"reason"`
	Current   bool    `json:"current"`
}

// CloseInfo describes a closed connection.
type CloseInfo struct {
	Code          int
	Reason        string
	WillReconnect bool
}

// SubscribeResult is what Subscribe did.
type SubscribeResult struct {
	// Channels the server confirmed as newly added.
	Added []string
	// Channels refused locally because the subscription cap was reached.
	Refused []string
	// Channels already held (nothing was sent for them).
	AlreadySubscribed []string
}

// AuthResult is what Auth did.
type AuthResult struct {
	// From the authenticated acknowledgement; empty when queued.
	UserID string
	// True when not connected: the token is kept and sent (and acknowledged) on connect.
	Queued bool
}

// ResyncReason says why state may have been missed.
type ResyncReason string

const (
	ResyncConcurrentModification ResyncReason = "concurrent_modification"
	ResyncReconnect              ResyncReason = "reconnect"
	// ResyncReauth: private channels were re-subscribed after the server signed the
	// connection out or switched it to another account; refetch private state through REST.
	ResyncReauth ResyncReason = "reauth"
)

// AuthChangeReason says why the server ended the connection's private subscriptions.
type AuthChangeReason string

const (
	// AuthUserChanged: Auth succeeded as a different user.
	AuthUserChanged AuthChangeReason = "user_changed"
	// AuthFailed: an Auth failed; the server signs the connection out on any auth error.
	AuthFailed AuthChangeReason = "auth_failed"
	// AuthSessionRevoked: this connection's own session was revoked (session.revoked with
	// current true).
	AuthSessionRevoked AuthChangeReason = "session_revoked"
)

// AuthChange is passed to OnAuthChanged.
type AuthChange struct {
	Reason AuthChangeReason
	// The user of the last successful auth on this connection, or empty.
	PreviousUserID string
	// The new user (AuthUserChanged), otherwise empty: the connection is signed out.
	UserID string
	// The server's error code (AuthFailed only).
	Code string
	// Private channels the server dropped. They are re-subscribed automatically: at once for
	// AuthUserChanged, after the next successful Auth otherwise (then OnResync(ResyncReauth)).
	Dropped []string
}

// WSHandlers are optional callbacks. They run one at a time on a dedicated goroutine, in
// frame order, so they may call Subscribe, Auth and the other methods. Keep them quick:
// frames queue up while a handler runs.
type WSHandlers struct {
	OnWelcome func(Welcome)
	// Every known event (ticker.update, orderbook.update, order.*, ...).
	OnEvent        func(Event)
	OnSubscribed   func(channels []string)
	OnUnsubscribed func(channels []string)
	// The authenticated acknowledgement (after Auth or the automatic re-auth on reconnect).
	OnAuthenticated func(userID string)
	// id is empty for the server's own pongs.
	OnPong func(id string)
	// An error frame from the server.
	OnServerError func(*WSError)
	// Transport problems and failed automatic re-subscriptions or re-authentications.
	OnError        func(error)
	OnClose        func(CloseInfo)
	OnReconnecting func(attempt int, delay time.Duration)
	// Reconnected, re-authenticated (if a token was held) and re-subscribed.
	OnReconnected func(Welcome)
	// State may have been missed: refetch anything you keep from private or public channels.
	OnResync func(ResyncReason)
	// session.revoked arrived for this connection's own session (current true): private
	// channels are dead. The socket stays open and public channels keep working. Call Auth with
	// a new token to restore private channels.
	OnAuthLost func(Event)
	// The server ended this connection's private subscriptions: Auth succeeded as another
	// user, an Auth failed, or this connection's own session was revoked.
	OnAuthChanged func(AuthChange)
}

// WSOptions configures a WebSocket.
type WSOptions struct {
	// Default DefaultWebSocketURL. Must be wss:// (see AllowInsecure).
	URL string
	// Allow ws://, but ONLY for localhost, 127.0.0.1 or ::1 (local test servers).
	AllowInsecure bool
	// Client ping cadence, required by the server. Default 30 s.
	PingInterval time.Duration
	// Reconnect when no frame arrives for this long. Default 75 s.
	LivenessTimeout time.Duration
	// Default 10 s.
	WelcomeTimeout time.Duration
	// How long Subscribe, Unsubscribe, Auth and Ping wait for the acknowledgement. Default 5 s.
	AckTimeout time.Duration
	// Disables automatic reconnection.
	NoReconnect bool
	// Reconnect backoff: full jitter from ReconnectBaseDelay (default 1 s), doubling up to
	// ReconnectMaxDelay (default 30 s). MaxReconnectAttempts 0 means unlimited.
	ReconnectBaseDelay   time.Duration
	ReconnectMaxDelay    time.Duration
	MaxReconnectAttempts int
	// Local subscription cap. Default 100 (the server's limit).
	MaxSubscriptions int
	// Local cap on client messages per fixed minute. Default 200 (the server closes above 240).
	MaxMessagesPerMinute int
	// Sent as the User-Agent of the handshake. Client.WebSocket sets the SDK's.
	UserAgent string
	// Used for the handshake. Default http.DefaultClient. The SDK uses a copy with redirects
	// disabled; the client you pass is not modified.
	HTTPClient *http.Client
	// Default slog.Default().
	Logger   *slog.Logger
	Handlers WSHandlers

	snapshots snapshotSource
	random    func() float64
}

// snapshotSource provides order-book snapshots (a *Client).
type snapshotSource interface {
	orderBookSnapshot(ctx context.Context, symbol string, depth int) (OrderBook, error)
}

type wsPending struct {
	kind     string // auth, subscribe, unsubscribe, ping
	channels []string
	done     chan wsAck
}

type wsAck struct {
	frame map[string]any
	err   error
}

var ackType = map[string]string{"auth": "authenticated", "subscribe": "subscribed", "unsubscribe": "unsubscribed", "ping": "pong"}

// WebSocket is the CEXY.io WebSocket client: heartbeat, liveness, subscriptions with local
// limits, automatic reconnect with re-auth and re-subscribe, and live order books. It is safe
// for concurrent use.
//
// API-key authentication on the WebSocket is not available yet: Auth takes a session access
// token. Programs holding only an API key get public channels and poll REST for private state.
type WebSocket struct {
	url  string
	opts WSOptions
	log  *slog.Logger
	h    WSHandlers

	mu             sync.Mutex
	conn           *websocket.Conn
	connGen        int
	connCancel     context.CancelFunc
	welcome        *Welcome
	channels       []string
	token          string
	authUserID     string   // user of the last successful auth on the current connection
	pendingPrivate []string // dropped by a server sign-out; re-subscribed after the next successful auth
	pending        map[string]*wsPending
	nextID         int
	closedByUser   bool
	everConnected  bool
	reconnecting   bool
	windowStart    time.Time
	windowCount    int
	warnedVersion  bool
	books          map[string]*LiveOrderBook
	stop           chan struct{}
	dispatchQueue  []func()
	dispatchSignal chan struct{}
	dispatcherOn   bool // wanted
	dispatcherRun  bool // a dispatch goroutine is running
	connectMu      sync.Mutex
}

// NewWebSocket checks the options. Call Connect to open the connection.
func NewWebSocket(opts WSOptions) (*WebSocket, error) {
	if opts.URL == "" {
		opts.URL = DefaultWebSocketURL
	}
	u, err := url.Parse(opts.URL)
	if err != nil || u.Host == "" {
		return nil, &ConfigError{Msg: fmt.Sprintf("invalid WebSocket URL: %q", opts.URL)}
	}
	if err := checkSecureURL(u, "wss", "ws", opts.AllowInsecure, "WebSocket URL"); err != nil {
		return nil, err
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&opts.PingInterval, 30*time.Second)
	def(&opts.LivenessTimeout, 75*time.Second)
	def(&opts.WelcomeTimeout, 10*time.Second)
	def(&opts.AckTimeout, 5*time.Second)
	def(&opts.ReconnectBaseDelay, time.Second)
	def(&opts.ReconnectMaxDelay, 30*time.Second)
	if opts.MaxSubscriptions <= 0 {
		opts.MaxSubscriptions = 100
	}
	if opts.MaxMessagesPerMinute <= 0 {
		opts.MaxMessagesPerMinute = 200
	}
	opts.HTTPClient = noRedirectClient(opts.HTTPClient)
	if strings.ContainsAny(opts.UserAgent, "\r\n") {
		return nil, &ConfigError{Msg: "UserAgent must not contain line breaks"}
	}
	if opts.random == nil {
		opts.random = rand.Float64
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &WebSocket{
		url: opts.URL, opts: opts, log: logger, h: opts.Handlers,
		pending: map[string]*wsPending{}, nextID: 1, closedByUser: true,
		books: map[string]*LiveOrderBook{}, dispatchSignal: make(chan struct{}, 1),
	}, nil
}

// WebSocket returns a WebSocket client for the same deployment, wired to this client for
// order-book snapshots. An empty opts.URL is derived from BaseURL.
func (c *Client) WebSocket(opts WSOptions) (*WebSocket, error) {
	if opts.URL == "" {
		opts.URL = "ws" + strings.TrimPrefix(c.baseURL, "http") + "/api/v1/ws"
	}
	if !opts.AllowInsecure {
		opts.AllowInsecure = c.allowInsecure
	}
	if opts.UserAgent == "" {
		opts.UserAgent = c.t.userAgent
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = c.t.http
	}
	opts.snapshots = c
	return NewWebSocket(opts)
}

func (c *Client) orderBookSnapshot(ctx context.Context, symbol string, depth int) (OrderBook, error) {
	return c.Markets.OrderBook(ctx, symbol, &GetOrderBookParams{Depth: &depth})
}

// URL returns the WebSocket URL.
func (w *WebSocket) URL() string { return w.url }

// Welcome returns the last welcome frame; ok is false before the first connection.
func (w *WebSocket) Welcome() (Welcome, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.welcome == nil {
		return Welcome{}, false
	}
	return *w.welcome, true
}

// Connected reports whether the connection is open and welcomed.
func (w *WebSocket) Connected() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn != nil && w.welcome != nil
}

// Channels returns the channels currently held (restored after every reconnect).
func (w *WebSocket) Channels() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.channels...)
}

// Connect opens the connection and returns the server's welcome frame. After it succeeds,
// dropped connections are reopened automatically until Close (unless NoReconnect).
func (w *WebSocket) Connect(ctx context.Context) (Welcome, error) {
	w.connectMu.Lock()
	defer w.connectMu.Unlock()
	w.mu.Lock()
	if w.conn != nil && w.welcome != nil {
		wel := *w.welcome
		w.mu.Unlock()
		return wel, nil
	}
	w.closedByUser = false
	if w.stop == nil {
		w.stop = make(chan struct{})
	}
	w.startDispatcherLocked()
	w.mu.Unlock()
	return w.open(ctx)
}

func (w *WebSocket) open(ctx context.Context) (Welcome, error) {
	wctx, cancel := context.WithTimeout(ctx, w.opts.WelcomeTimeout)
	defer cancel()
	header := http.Header{}
	if w.opts.UserAgent != "" {
		header.Set("User-Agent", w.opts.UserAgent)
	}
	conn, _, err := websocket.Dial(wctx, w.url, &websocket.DialOptions{HTTPClient: w.opts.HTTPClient, HTTPHeader: header})
	if err != nil {
		if ctx.Err() != nil {
			return Welcome{}, ctx.Err()
		}
		return Welcome{}, &WSError{Code: "CONNECT_FAILED", Message: fmt.Sprintf("could not connect to %s: %v", w.url, err)}
	}
	conn.SetReadLimit(4 << 20)

	var welcome *Welcome
	var early []map[string]any
	for welcome == nil {
		_, data, err := conn.Read(wctx)
		if err != nil {
			conn.CloseNow()
			if ctx.Err() != nil {
				return Welcome{}, ctx.Err()
			}
			if errors.Is(wctx.Err(), context.DeadlineExceeded) {
				return Welcome{}, &WSError{Code: "TIMEOUT", Message: "no welcome frame from server"}
			}
			return Welcome{}, &WSError{Code: "CONNECT_FAILED", Message: fmt.Sprintf("connection closed before welcome: %v", err)}
		}
		frame := parseFrame(data)
		if frame == nil {
			continue
		}
		if frame["type"] == "welcome" {
			var wel Welcome
			_ = remarshal(frame, &wel)
			welcome = &wel
		} else {
			early = append(early, frame)
		}
	}

	w.mu.Lock()
	if w.closedByUser {
		w.mu.Unlock()
		conn.Close(websocket.StatusNormalClosure, "client closing")
		return Welcome{}, &WSError{Code: "CLOSED", Message: "connection closed by client"}
	}
	connCtx, connCancel := context.WithCancel(context.Background())
	w.conn = conn
	w.connGen++
	gen := w.connGen
	w.connCancel = connCancel
	w.welcome = welcome
	w.windowStart, w.windowCount = time.Time{}, 0
	w.mu.Unlock()

	go w.readLoop(connCtx, conn, gen)
	go w.pingLoop(connCtx, gen)
	for _, f := range early {
		w.onFrame(f)
	}
	w.onWelcome(*welcome)
	return *welcome, nil
}

func (w *WebSocket) onWelcome(welcome Welcome) {
	w.mu.Lock()
	if welcome.ProtocolVersion != SupportedProtocolVersion && !w.warnedVersion {
		w.warnedVersion = true
		w.log.Warn("cexy: server WebSocket protocol_version is newer than this SDK supports; continuing",
			"server", welcome.ProtocolVersion, "supported", SupportedProtocolVersion)
	}
	isReconnect := w.everConnected
	w.everConnected = true
	w.authUserID = "" // a new connection starts signed out
	token := w.token
	channels := append([]string(nil), w.channels...)
	books := w.bookList()
	w.mu.Unlock()

	w.dispatch(func() {
		if w.h.OnWelcome != nil {
			w.h.OnWelcome(welcome)
		}
	})
	if token != "" || len(channels) > 0 {
		go func() {
			ctx := context.Background()
			if token != "" {
				if _, err := w.auth(ctx, token); err != nil {
					w.emitError(err)
				}
			}
			if len(channels) > 0 {
				if _, err := w.sendSubscribe(ctx, channels); err != nil {
					w.emitError(err)
				}
			}
		}()
	}
	if isReconnect {
		w.dispatch(func() {
			if w.h.OnReconnected != nil {
				w.h.OnReconnected(welcome)
			}
			if w.h.OnResync != nil {
				w.h.OnResync(ResyncReconnect)
			}
		})
		for _, b := range books {
			go b.resync(0)
		}
	}
}

// Auth authenticates private channels with a session access token. It returns on the
// server's authenticated acknowledgement; it fails on an error frame with the request id (the
// token is then forgotten) or when no acknowledgement arrives within AckTimeout. The token is
// kept in memory and re-sent after each reconnect. When not connected, the token is queued
// and Queued is true. (API-key authentication is not available on the WebSocket yet.)
func (w *WebSocket) Auth(ctx context.Context, token string) (AuthResult, error) {
	if token == "" {
		return AuthResult{}, &WSError{Code: "CONFIG", Message: "Auth: token is required"}
	}
	w.mu.Lock()
	w.token = token
	connected := w.conn != nil && w.welcome != nil
	w.mu.Unlock()
	if !connected {
		return AuthResult{Queued: true}, nil
	}
	return w.auth(ctx, token)
}

func (w *WebSocket) auth(ctx context.Context, token string) (AuthResult, error) {
	ack, err := w.request(ctx, "auth", map[string]any{"token": token}, nil, true)
	if err != nil {
		var we *WSError
		if errors.As(err, &we) && we.FromServer {
			w.mu.Lock()
			if w.token == token {
				w.token = "" // the server refused it: do not resend on reconnect
			}
			w.mu.Unlock()
		}
		return AuthResult{}, err
	}
	uid, _ := ack["user_id"].(string)
	return AuthResult{UserID: uid}, nil
}

// HasToken reports whether a session token is kept for automatic re-authentication. A
// refused token and a revoked session are forgotten.
func (w *WebSocket) HasToken() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.token != ""
}

// Ping sends a ping with an id and returns the round-trip time.
func (w *WebSocket) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	if _, err := w.request(ctx, "ping", map[string]any{}, nil, true); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// Subscribe subscribes to channels such as "ticker:BTC/USDT" and "trades:BTC/USDT", and
// returns when the server confirms. Channels beyond MaxSubscriptions are refused locally.
// When not connected, channels are queued and subscribed on connect.
func (w *WebSocket) Subscribe(ctx context.Context, channels ...string) (SubscribeResult, error) {
	wanted := uniq(channels)
	for _, c := range wanted {
		if c == "" || len(c) > maxChannelLength {
			return SubscribeResult{}, &WSError{Code: "CONFIG", Message: fmt.Sprintf("invalid channel name: %q", c)}
		}
	}
	var res SubscribeResult
	w.mu.Lock()
	var fresh []string
	for _, c := range wanted {
		if w.holds(c) {
			res.AlreadySubscribed = append(res.AlreadySubscribed, c)
		} else {
			fresh = append(fresh, c)
		}
	}
	room := max(0, w.opts.MaxSubscriptions-len(w.channels)-len(w.pendingPrivate))
	accepted := fresh[:min(room, len(fresh))]
	res.Refused = fresh[len(accepted):]
	w.channels = append(w.channels, accepted...)
	connected := w.conn != nil && w.welcome != nil
	w.mu.Unlock()
	if len(res.Refused) > 0 {
		w.log.Warn("cexy: WebSocket subscription cap reached", "cap", w.opts.MaxSubscriptions, "refused", res.Refused)
	}
	if len(accepted) == 0 || !connected {
		return res, nil
	}
	added, err := w.sendSubscribe(ctx, accepted)
	var we *WSError
	if errors.As(err, &we) && we.FromServer {
		// Refused by the server (e.g. UNAUTHENTICATED for a private channel): not held.
		w.mu.Lock()
		for _, c := range accepted {
			w.drop(c)
		}
		w.mu.Unlock()
	}
	res.Added = added
	return res, err
}

// Unsubscribe unsubscribes and returns on the unsubscribed acknowledgement (or after
// AckTimeout without one); it fails on an error frame with the request id.
func (w *WebSocket) Unsubscribe(ctx context.Context, channels ...string) error {
	w.mu.Lock()
	var held []string
	for _, c := range uniq(channels) {
		w.pendingPrivate = remove(w.pendingPrivate, c)
		if w.holds(c) {
			held = append(held, c)
			w.drop(c)
		}
	}
	connected := w.conn != nil && w.welcome != nil
	w.mu.Unlock()
	if len(held) == 0 || !connected {
		return nil
	}
	_, err := w.request(ctx, "unsubscribe", map[string]any{"channels": held}, held, false)
	return err
}

// OrderBook returns a live order book for symbol that follows the sync rules: subscribe
// first, then take a REST snapshot (sequence S); drop updates with sequence <= S; each
// update replaces the top 50 levels; a gap marks the book stale until the next update; a
// fresh snapshot after every reconnect and after CONCURRENT_MODIFICATION. It returns after
// the first snapshot. It needs a WebSocket made by Client.WebSocket.
func (w *WebSocket) OrderBook(ctx context.Context, symbol string, opts LiveOrderBookOptions) (*LiveOrderBook, error) {
	if w.opts.snapshots == nil {
		return nil, &WSError{Code: "CONFIG", Message: "OrderBook needs a WebSocket made by Client.WebSocket"}
	}
	w.mu.Lock()
	if b, ok := w.books[symbol]; ok {
		w.mu.Unlock()
		return b, nil
	}
	b := newLiveOrderBook(symbol, w, opts)
	w.books[symbol] = b
	w.mu.Unlock()

	res, err := w.Subscribe(ctx, "orderbook:"+symbol) // BEFORE the snapshot; updates are buffered
	if err == nil && len(res.Refused) > 0 {
		err = &WSError{Code: "LOCAL_SUBSCRIPTION_LIMIT", Message: "cannot subscribe to orderbook:" + symbol + ": cap reached"}
	}
	if err == nil {
		err = b.initialSync(ctx)
	}
	if err != nil {
		w.mu.Lock()
		delete(w.books, symbol)
		w.mu.Unlock()
		b.closed.Store(true)
		_ = w.Unsubscribe(context.Background(), "orderbook:"+symbol)
		return nil, err
	}
	return b, nil
}

// Close closes the connection for good (no reconnect).
func (w *WebSocket) Close() error {
	w.mu.Lock()
	if w.closedByUser && w.conn == nil {
		w.mu.Unlock()
		return nil
	}
	w.closedByUser = true
	w.authUserID = ""
	if w.stop != nil {
		close(w.stop)
		w.stop = nil
	}
	conn := w.conn
	books := w.bookList()
	w.teardownLocked(&WSError{Code: "CLOSED", Message: "connection closed by client"})
	w.mu.Unlock()
	for _, b := range books {
		b.markDisconnected()
	}
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "client closing") // best effort, like any close
	}
	w.dispatch(func() {
		if w.h.OnClose != nil {
			w.h.OnClose(CloseInfo{Code: 1000, Reason: "client closing"})
		}
	})
	w.stopDispatcher()
	return nil
}

// ---------------------------------------------------------------------------------------

func (w *WebSocket) readLoop(ctx context.Context, conn *websocket.Conn, gen int) {
	for {
		rctx, cancel := context.WithTimeout(ctx, w.opts.LivenessTimeout)
		_, data, err := conn.Read(rctx)
		timedOut := errors.Is(rctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return // torn down by us
			}
			code, reason := int(websocket.CloseStatus(err)), ""
			if timedOut {
				w.log.Warn("cexy: no WebSocket frame from server; reconnecting", "timeout", w.opts.LivenessTimeout)
				code, reason = 4000, "liveness timeout"
			} else {
				var ce websocket.CloseError
				if errors.As(err, &ce) {
					reason = ce.Reason
				}
			}
			w.onDropped(gen, code, reason)
			return
		}
		if frame := parseFrame(data); frame != nil {
			w.onFrame(frame)
		}
	}
}

func (w *WebSocket) pingLoop(ctx context.Context, gen int) {
	t := time.NewTicker(w.opts.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = w.send(ctx, map[string]any{"op": "ping"}) // a dead socket is handled by the reader
		}
	}
}

func (w *WebSocket) onFrame(frame map[string]any) {
	typ, _ := frame["type"].(string)
	id, _ := frame["id"].(string)
	switch typ {
	case "welcome":
		var wel Welcome
		_ = remarshal(frame, &wel)
		w.mu.Lock()
		w.welcome = &wel
		w.mu.Unlock()
		w.onWelcome(wel)
		return
	case "pong":
		// Replies to our pings echo the id; the server's own pongs every 30 s have none.
		if id != "" {
			w.settle(id, "ping", frame, nil)
		}
		w.dispatch(func() {
			if w.h.OnPong != nil {
				w.h.OnPong(id)
			}
		})
		return
	case "authenticated":
		uid, _ := frame["user_id"].(string)
		w.dispatch(func() {
			if w.h.OnAuthenticated != nil {
				w.h.OnAuthenticated(uid)
			}
		})
		// Update the state before Auth returns, and before any later frame.
		w.onAuthenticated(uid)
		if id != "" {
			w.settle(id, "auth", frame, nil)
		}
		return
	case "subscribed", "unsubscribed":
		channels := stringList(frame["channels"])
		kind := "subscribe"
		if typ == "unsubscribed" {
			kind = "unsubscribe"
		}
		if id != "" {
			w.settle(id, kind, frame, nil)
		} else {
			w.settleByChannel(kind, channels, frame) // fallback for acks that carry no id
		}
		w.dispatch(func() {
			if typ == "subscribed" && w.h.OnSubscribed != nil {
				w.h.OnSubscribed(channels)
			} else if typ == "unsubscribed" && w.h.OnUnsubscribed != nil {
				w.h.OnUnsubscribed(channels)
			}
		})
		return
	case "error":
		code, _ := frame["code"].(string)
		if code == "" {
			code = "UNKNOWN"
		}
		msg, _ := frame["message"].(string)
		if msg == "" {
			msg = code
		}
		werr := &WSError{Code: code, Message: msg, FromServer: true}
		if id != "" {
			// Any error on an auth frame signs the connection out (an UNAUTHENTICATED error on a
			// subscribe is only a refused subscribe).
			w.mu.Lock()
			p, ok := w.pending[id]
			isAuth := ok && p.kind == "auth"
			w.mu.Unlock()
			if isAuth {
				w.signedOut(AuthFailed, code)
			}
			w.settle(id, "", nil, werr)
		}
		w.dispatch(func() {
			if w.h.OnServerError != nil {
				w.h.OnServerError(werr)
			}
		})
		if code == string(CodeConcurrentModification) && id == "" {
			// The server dropped messages: resynchronise every book and channel.
			w.dispatch(func() {
				if w.h.OnResync != nil {
					w.h.OnResync(ResyncConcurrentModification)
				}
			})
			w.mu.Lock()
			books := w.bookList()
			w.mu.Unlock()
			for _, b := range books {
				go b.resync(0)
			}
		}
		return
	}
	if !knownEventTypes[typ] {
		w.log.Debug("cexy: ignoring unknown WebSocket frame type", "type", typ)
		return
	}
	var ev Event
	if remarshal(frame, &ev) != nil {
		return
	}
	var book *LiveOrderBook
	if ev.Type == "orderbook.update" {
		symbol := strings.TrimPrefix(ev.Channel, "orderbook:")
		if !strings.HasPrefix(ev.Channel, "orderbook:") {
			var d OrderBookUpdate
			_ = ev.Decode(&d)
			symbol = d.Symbol
		}
		w.mu.Lock()
		book = w.books[symbol]
		w.mu.Unlock()
	}
	// Only this connection's own session signs it out (the server checks current == true
	// exactly); current false, missing or not a boolean changes nothing.
	if data, _ := frame["data"].(map[string]any); ev.Type == "session.revoked" && data["current"] == true {
		w.mu.Lock()
		w.token = "" // never re-auth with a revoked session
		w.mu.Unlock()
		w.signedOut(AuthSessionRevoked, "")
		w.dispatch(func() {
			if w.h.OnAuthLost != nil {
				w.h.OnAuthLost(ev)
			}
		})
	}
	w.dispatch(func() {
		if book != nil {
			book.onUpdate(ev)
		}
		if w.h.OnEvent != nil {
			w.h.OnEvent(ev)
		}
	})
}

// dropPrivateLocked moves the held private channels to the pending set and returns them.
func (w *WebSocket) dropPrivateLocked() []string {
	dropped := []string{}
	kept := w.channels[:0]
	for _, c := range w.channels {
		if PrivateChannels[c] {
			dropped = append(dropped, c)
		} else {
			kept = append(kept, c)
		}
	}
	w.channels = kept
	w.pendingPrivate = append(w.pendingPrivate, dropped...)
	return dropped
}

// signedOut: the server signed the connection out and ended every private subscription.
func (w *WebSocket) signedOut(reason AuthChangeReason, code string) {
	w.mu.Lock()
	change := AuthChange{Reason: reason, PreviousUserID: w.authUserID, Code: code}
	w.authUserID = ""
	change.Dropped = w.dropPrivateLocked()
	w.mu.Unlock()
	w.dispatch(func() {
		if w.h.OnAuthChanged != nil {
			w.h.OnAuthChanged(change)
		}
	})
}

// onAuthenticated: a successful auth. It detects an account switch, then restores the
// pending private channels.
func (w *WebSocket) onAuthenticated(userID string) {
	w.mu.Lock()
	previous := w.authUserID
	w.authUserID = userID
	var change *AuthChange
	if previous != "" && userID != previous {
		change = &AuthChange{Reason: AuthUserChanged, PreviousUserID: previous, UserID: userID, Dropped: w.dropPrivateLocked()}
	}
	channels := w.pendingPrivate
	w.pendingPrivate = nil
	w.channels = append(w.channels, channels...)
	w.mu.Unlock()
	if change != nil {
		w.dispatch(func() {
			if w.h.OnAuthChanged != nil {
				w.h.OnAuthChanged(*change)
			}
		})
	}
	if len(channels) == 0 {
		return
	}
	// Not on the read loop: the acknowledgement arrives through it.
	go func() {
		_, err := w.sendSubscribe(context.Background(), channels)
		var we *WSError
		if errors.As(err, &we) && we.FromServer {
			// Refused by the server (e.g. signed out again meanwhile): back to pending.
			w.mu.Lock()
			for _, c := range channels {
				if w.holds(c) {
					w.drop(c)
					w.pendingPrivate = append(w.pendingPrivate, c)
				}
			}
			w.mu.Unlock()
		}
		if err != nil {
			w.emitError(err)
		}
	}()
	w.dispatch(func() {
		if w.h.OnResync != nil {
			w.h.OnResync(ResyncReauth)
		}
	})
}

// settle completes pending request id if the acknowledgement matches its kind ("" = any, for
// error frames).
func (w *WebSocket) settle(id, kind string, frame map[string]any, err error) {
	w.mu.Lock()
	p, ok := w.pending[id]
	if !ok || (kind != "" && p.kind != kind) {
		w.mu.Unlock()
		return
	}
	delete(w.pending, id)
	w.mu.Unlock()
	p.done <- wsAck{frame: frame, err: err}
}

func (w *WebSocket) settleByChannel(kind string, channels []string, frame map[string]any) {
	w.mu.Lock()
	for id, p := range w.pending {
		if p.kind == kind && overlaps(p.channels, channels) {
			delete(w.pending, id)
			w.mu.Unlock()
			p.done <- wsAck{frame: frame}
			return
		}
	}
	w.mu.Unlock()
}

// request sends {op, id, ...payload} and waits for the acknowledgement with the same id, or
// an error frame with that id. With strict, no acknowledgement within AckTimeout is an error;
// otherwise it returns a nil frame.
func (w *WebSocket) request(ctx context.Context, kind string, payload map[string]any, channels []string, strict bool) (map[string]any, error) {
	w.mu.Lock()
	id := strconv.Itoa(w.nextID)
	w.nextID++
	p := &wsPending{kind: kind, channels: channels, done: make(chan wsAck, 1)}
	w.pending[id] = p
	w.mu.Unlock()

	payload["op"] = kind
	payload["id"] = id
	if err := w.send(ctx, payload); err != nil {
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
		return nil, err
	}
	t := time.NewTimer(w.opts.AckTimeout)
	defer t.Stop()
	select {
	case ack := <-p.done:
		return ack.frame, ack.err
	case <-t.C:
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
		if strict {
			return nil, &WSError{Code: "TIMEOUT", Message: fmt.Sprintf("no %s acknowledgement for %s (id %s)", ackType[kind], kind, id)}
		}
		return nil, nil
	case <-ctx.Done():
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
		return nil, ctx.Err()
	}
}

// sendSubscribe returns the channels the server confirmed. subscribed is sent only when
// something was added: silence means nothing new.
func (w *WebSocket) sendSubscribe(ctx context.Context, channels []string) ([]string, error) {
	ack, err := w.request(ctx, "subscribe", map[string]any{"channels": channels}, channels, false)
	if err != nil || ack == nil {
		return []string{}, err
	}
	return stringList(ack["channels"]), nil
}

func (w *WebSocket) send(ctx context.Context, frame map[string]any) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	w.mu.Lock()
	conn := w.conn
	if conn == nil {
		w.mu.Unlock()
		return &WSError{Code: "NOT_CONNECTED", Message: "WebSocket is not connected"}
	}
	now := time.Now()
	if now.Sub(w.windowStart) >= time.Minute {
		w.windowStart, w.windowCount = now, 0
	}
	// Pings are never refused locally: without them the server closes the connection.
	if frame["op"] != "ping" && w.windowCount >= w.opts.MaxMessagesPerMinute {
		w.mu.Unlock()
		return &WSError{Code: "LOCAL_RATE_LIMIT", Message: fmt.Sprintf(
			"more than %d messages this minute; the server closes the socket above 240", w.opts.MaxMessagesPerMinute)}
	}
	w.windowCount++
	w.mu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, data)
}

func (w *WebSocket) onDropped(gen, code int, reason string) {
	w.mu.Lock()
	if gen != w.connGen || w.conn == nil {
		w.mu.Unlock()
		return
	}
	conn := w.conn
	books := w.bookList()
	w.teardownLocked(&WSError{Code: "DISCONNECTED", Message: fmt.Sprintf("connection lost (code %d)", code)})
	willReconnect := !w.closedByUser && !w.opts.NoReconnect
	start := willReconnect && !w.reconnecting
	if start {
		w.reconnecting = true
	}
	stop := w.stop
	w.mu.Unlock()
	conn.CloseNow()
	for _, b := range books {
		b.markDisconnected() // sequences reset per connection
	}
	w.dispatch(func() {
		if w.h.OnClose != nil {
			w.h.OnClose(CloseInfo{Code: code, Reason: reason, WillReconnect: willReconnect})
		}
	})
	if start {
		go w.reconnectLoop(stop)
	}
}

func (w *WebSocket) reconnectLoop(stop chan struct{}) {
	defer func() {
		w.mu.Lock()
		w.reconnecting = false
		w.mu.Unlock()
	}()
	for attempt := 1; ; attempt++ {
		if w.opts.MaxReconnectAttempts > 0 && attempt > w.opts.MaxReconnectAttempts {
			w.emitError(&WSError{Code: "RECONNECT_FAILED", Message: fmt.Sprintf("gave up after %d reconnect attempts", w.opts.MaxReconnectAttempts)})
			return
		}
		capped := math.Min(float64(w.opts.ReconnectMaxDelay), float64(w.opts.ReconnectBaseDelay)*math.Pow(2, float64(attempt-1)))
		delay := time.Duration(w.opts.random() * capped) // full jitter
		w.dispatch(func() {
			if w.h.OnReconnecting != nil {
				w.h.OnReconnecting(attempt, delay)
			}
		})
		t := time.NewTimer(delay)
		select {
		case <-stop:
			t.Stop()
			return
		case <-t.C:
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			select {
			case <-stop:
				cancel()
			case <-ctx.Done():
			}
		}()
		_, err := w.open(ctx)
		cancel()
		if err == nil {
			return
		}
		select {
		case <-stop:
			return
		default:
		}
		w.emitError(err)
	}
}

func (w *WebSocket) teardownLocked(err error) {
	if w.connCancel != nil {
		w.connCancel()
		w.connCancel = nil
	}
	w.conn = nil
	w.welcome = nil
	for id, p := range w.pending {
		delete(w.pending, id)
		p.done <- wsAck{err: err}
	}
}

// emitError reports err, except for requests cut short by a disconnect (the reconnect redoes them).
func (w *WebSocket) emitError(err error) {
	var we *WSError
	if errors.As(err, &we) && (we.Code == "DISCONNECTED" || we.Code == "CLOSED") {
		return
	}
	w.dispatch(func() {
		if w.h.OnError != nil {
			w.h.OnError(err)
		}
	})
}

// holds reports whether c is held, or pending re-subscription after a sign-out.
func (w *WebSocket) holds(c string) bool {
	for _, x := range w.channels {
		if x == c {
			return true
		}
	}
	for _, x := range w.pendingPrivate {
		if x == c {
			return true
		}
	}
	return false
}

func remove(list []string, c string) []string {
	for i, x := range list {
		if x == c {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

func (w *WebSocket) drop(c string) {
	for i, x := range w.channels {
		if x == c {
			w.channels = append(w.channels[:i], w.channels[i+1:]...)
			return
		}
	}
}

func (w *WebSocket) bookList() []*LiveOrderBook {
	out := make([]*LiveOrderBook, 0, len(w.books))
	for _, b := range w.books {
		out = append(out, b)
	}
	return out
}

// --- handler dispatch: one goroutine, frame order, unbounded queue ---------------------

func (w *WebSocket) startDispatcherLocked() {
	w.dispatcherOn = true
	if !w.dispatcherRun {
		w.dispatcherRun = true
		go w.dispatchLoop()
	}
}

func (w *WebSocket) dispatch(f func()) {
	w.mu.Lock()
	w.dispatchQueue = append(w.dispatchQueue, f)
	w.mu.Unlock()
	select {
	case w.dispatchSignal <- struct{}{}:
	default:
	}
}

func (w *WebSocket) dispatchLoop() {
	for {
		w.mu.Lock()
		queue := w.dispatchQueue
		w.dispatchQueue = nil
		if len(queue) == 0 && !w.dispatcherOn {
			w.dispatcherRun = false
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
		for _, f := range queue {
			f()
		}
		if len(queue) == 0 {
			<-w.dispatchSignal
		}
	}
}

// stopDispatcher lets the dispatcher drain what is queued, then exit.
func (w *WebSocket) stopDispatcher() {
	w.mu.Lock()
	w.dispatcherOn = false
	w.mu.Unlock()
	select {
	case w.dispatchSignal <- struct{}{}:
	default:
	}
}

// --- helpers ---------------------------------------------------------------------------

func parseFrame(data []byte) map[string]any {
	var v map[string]any
	if json.Unmarshal(data, &v) != nil {
		return nil
	}
	return v
}

func remarshal(from map[string]any, to any) error {
	b, err := json.Marshal(from)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, to)
}

func stringList(v any) []string {
	arr, _ := v.([]any)
	out := []string{}
	for _, x := range arr {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func uniq(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func overlaps(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}
