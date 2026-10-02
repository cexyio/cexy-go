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
	"slices"
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

// maxPingInterval: the server closes a connection silent for 90 to 120 s.
const maxPingInterval = 60 * time.Second

// PrivateChannels need Auth with a session access token, or AuthKey.
var PrivateChannels = map[string]bool{"orders": true, "balances": true, "deposits": true, "withdrawals": true, "account": true,
	FuturesAccountChannel: true}

var knownEventTypes = map[string]bool{
	"ticker.update": true, "orderbook.update": true, "trade.new": true, "market.status": true,
	"order.created": true, "order.updated": true, "order.cancelled": true, "order.filled": true,
	"balance.updated": true, "deposit.detected": true, "deposit.updated": true, "deposit.completed": true,
	"withdrawal.updated": true, "session.revoked": true,
	"balances.resync": true, "deposits.resync": true, "withdrawals.resync": true,
	// Futures (see ws_futures.go).
	"futures.mids": true, "futures.orderbook.update": true, "futures.trades.new": true, "futures.candle.update": true,
	"futures.status": true, "futures.positions": true, "futures.orders": true, "futures.resync": true,
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
	// Single-use challenge for AuthKey (API-key authentication).
	Challenge string `json:"challenge,omitempty"`
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
	// Channels the server refused, each with its error frame. They are not held and not retried.
	RefusedByServer []SubscribeRefusal
}

// SubscribeRefusal is a channel the server refused (an error frame with the subscribe's id). It
// is also the error OnError reports when an automatic re-subscribe (after a reconnect, a re-auth
// or futures.resync on futures.account) is refused; errors.As finds the *WSError in it.
type SubscribeRefusal struct {
	Channel string
	Err     *WSError
}

func (r *SubscribeRefusal) Error() string {
	return fmt.Sprintf("cexy websocket: subscribe %s refused: [%s] %s", r.Channel, r.Err.Code, r.Err.Message)
}

func (r *SubscribeRefusal) Unwrap() error { return r.Err }

// AuthResult is what Auth did.
type AuthResult struct {
	// From the authenticated acknowledgement; empty when queued.
	UserID string
	// How the connection is authenticated ("api_key" or "session"), when the server says.
	Auth string
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
	// ResyncSequenceGap: a private channel skipped sequence numbers (see OnSequenceGap).
	ResyncSequenceGap ResyncReason = "sequence_gap"
	// ResyncBalancesResync: balances.resync, the server could not resume its balance change stream.
	ResyncBalancesResync ResyncReason = "balances_resync"
	// ResyncDepositsResync: deposits.resync (planned server frame), refetch the deposit list.
	ResyncDepositsResync ResyncReason = "deposits_resync"
	// ResyncWithdrawalsResync: withdrawals.resync (planned server frame), refetch the withdrawal list.
	ResyncWithdrawalsResync ResyncReason = "withdrawals_resync"
)

// NoReorderWindow, as WSOptions.ReorderWindow, reports a sequence gap at once instead of waiting
// for a swapped frame (0 selects the default).
const NoReorderWindow time.Duration = -1

// SequenceGap is passed to OnSequenceGap.
type SequenceGap struct {
	Channel  string
	Expected int64
	Received int64
}

// WSKeySigner signs a WebSocket auth_key challenge (an *HMACAuthenticator fits).
type WSKeySigner interface {
	SignWebSocketChallenge(connectionID, challenge string) (keyID, signature string)
}

// WSTimer is a stoppable timer (a *time.Timer fits).
type WSTimer interface{ Stop() bool }

// WSClock is a TEST-ONLY time source for the reorder-window timer and LiveBalances scheduling
// (minimum snapshot interval, retry backoff). Socket timeouts always use the real clock. Leave
// WSOptions.Clock nil in production.
type WSClock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) WSTimer
}

type realClock struct{}

func (realClock) Now() time.Time                              { return time.Now() }
func (realClock) AfterFunc(d time.Duration, f func()) WSTimer { return time.AfterFunc(d, f) }

type seqState struct {
	next  int64
	holes map[int64]bool
	first *SequenceGap
	timer WSTimer
}

// AuthChangeReason says why the server ended the connection's private subscriptions.
type AuthChangeReason string

const (
	// AuthUserChanged: Auth succeeded as a different user.
	AuthUserChanged AuthChangeReason = "user_changed"
	// AuthFailed: an Auth failed; the server signs the connection out on any auth error.
	AuthFailed AuthChangeReason = "auth_failed"
	// AuthSessionRevoked: this connection's own session was revoked (session.revoked with
	// current true, or the signed_out frame with reason "revoked").
	AuthSessionRevoked AuthChangeReason = "session_revoked"
	// AuthTokenExpired: the signed_out frame with reason "expired". Re-send Auth on every token
	// refresh to avoid it.
	AuthTokenExpired AuthChangeReason = "token_expired"
	// AuthKeyRevoked: the API key was revoked or deleted (key authentication).
	AuthKeyRevoked AuthChangeReason = "key_revoked"
	// AuthKeyExpired: the API key expired (key authentication).
	AuthKeyExpired AuthChangeReason = "key_expired"
	// AuthSignedOut: a server sign-out with a reason this SDK does not know (raw value in Code).
	// The set of reasons may grow.
	AuthSignedOut AuthChangeReason = "signed_out"
)

// AuthChange is passed to OnAuthChanged.
type AuthChange struct {
	Reason AuthChangeReason
	// The user of the last successful auth on this connection, or empty.
	PreviousUserID string
	// The new user (AuthUserChanged), otherwise empty: the connection is signed out.
	UserID string
	// The server's error code (AuthFailed), or the raw signed_out reason (AuthSignedOut).
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
	// A private channel skipped sequence numbers on this connection (after the reorder window):
	// events were lost. Followed by OnResync(ResyncSequenceGap); refetch that channel's state.
	OnSequenceGap func(SequenceGap)
	// futures.resync arrived on channel: data on it may have been missed; refetch it over REST
	// (the event itself also goes to OnEvent). Public futures channels stay subscribed. For
	// futures.account the client then unsubscribes and subscribes again by itself (the server's
	// poller stopped); a refusal of that subscribe is reported (OnServerError, and OnError as a
	// *SubscribeRefusal): UNAUTHENTICATED puts it back to pending, any other code drops it.
	OnFuturesResync func(channel string)
}

// WSOptions configures a WebSocket.
type WSOptions struct {
	// Default DefaultWebSocketURL. Must be wss:// (see AllowInsecure).
	URL string
	// Allow ws://, but ONLY for localhost, 127.0.0.1 or ::1 (local test servers).
	AllowInsecure bool
	// Client ping cadence, required by the server (it closes connections silent for 90 s).
	// Default 30 s; at most 60 s (NewWebSocket refuses a longer one with a *ConfigError).
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
	// Private channels with several publishers (orders, account) can deliver two adjacent frames
	// swapped: a missing sequence number gets this long to arrive before it counts as a gap.
	// 0 selects the default, 250 ms; NoReorderWindow reports a gap at once.
	ReorderWindow time.Duration
	// TEST-ONLY: see WSClock.
	Clock WSClock
	// Signs AuthKey challenges (API-key authentication). Client.WebSocket sets it when
	// the client uses Auth "hmac".
	KeySigner WSKeySigner

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
	// Error frames with this subscribe's id. The server sends one per refused channel, BEFORE
	// its single subscribed ack, and no ack at all when it accepted nothing.
	refusals []*WSError
}

type wsAck struct {
	frame    map[string]any
	err      error
	refusals []*WSError // subscribe: the error frames received before the ack
}

var ackType = map[string]string{"auth": "authenticated", "auth_key": "authenticated", "subscribe": "subscribed", "unsubscribe": "unsubscribed", "ping": "pong"}

// WebSocket is the CEXY.io WebSocket client: heartbeat, liveness, subscriptions with local
// limits, automatic reconnect with re-auth and re-subscribe, and live order books. It is safe
// for concurrent use.
//
// Private channels need Auth with a session access token, or AuthKey with an API key (on a
// client that signs requests: Options.Auth "hmac", the default).
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
	authUserID     string          // user of the last successful auth on the current connection
	challenge      string          // latest unused auth_key challenge (welcome or the last auth_key reply)
	keyAuth        bool            // AuthKey is the active credential (re-signed after reconnects)
	authKeyIDs     map[string]bool // auth_key request ids sent on the current connection
	seq            map[string]*seqState
	liveBalances   []*LiveBalances
	balancesByUs   bool     // balances was subscribed by LiveBalances
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
	if opts.PingInterval > maxPingInterval {
		return nil, &ConfigError{Msg: fmt.Sprintf("PingInterval %s is above 60s: the server closes connections silent for 90s", opts.PingInterval)}
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
	switch {
	case opts.ReorderWindow == 0:
		opts.ReorderWindow = 250 * time.Millisecond
	case opts.ReorderWindow < 0:
		opts.ReorderWindow = 0 // NoReorderWindow: the gap timer fires at once
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &WebSocket{
		url: opts.URL, opts: opts, log: logger, h: opts.Handlers,
		pending: map[string]*wsPending{}, nextID: 1, closedByUser: true,
		books: map[string]*LiveOrderBook{}, dispatchSignal: make(chan struct{}, 1),
		seq: map[string]*seqState{},
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
	if h, ok := c.t.auth.(*HMACAuthenticator); ok && opts.KeySigner == nil {
		opts.KeySigner = h
	}
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
	w.authUserID = ""    // a new connection starts signed out
	w.resetSeqLocked("") // sequences on a new connection are unrelated
	w.challenge = welcome.Challenge
	w.authKeyIDs = map[string]bool{}
	token := w.token
	keyAuth := w.keyAuth
	channels := append([]string(nil), w.channels...)
	books := w.bookList()
	w.mu.Unlock()

	w.dispatch(func() {
		if w.h.OnWelcome != nil {
			w.h.OnWelcome(welcome)
		}
	})
	if token != "" || keyAuth || len(channels) > 0 {
		go func() {
			ctx := context.Background()
			if token != "" {
				if _, err := w.auth(ctx, token); err != nil {
					w.emitError(err)
				}
			} else if keyAuth {
				if _, err := w.authKey(ctx); err != nil {
					w.emitError(err)
				}
			}
			if len(channels) > 0 {
				w.resubscribed(w.sendSubscribe(ctx, channels))
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
// and Queued is true. (For an API key, use AuthKey.)
func (w *WebSocket) Auth(ctx context.Context, token string) (AuthResult, error) {
	if token == "" {
		return AuthResult{}, &WSError{Code: "CONFIG", Message: "Auth: token is required"}
	}
	w.mu.Lock()
	w.token = token
	w.keyAuth = false
	connected := w.conn != nil && w.welcome != nil
	w.mu.Unlock()
	if !connected {
		return AuthResult{Queued: true}, nil
	}
	return w.auth(ctx, token)
}

// AuthKey authenticates with the client's API key.
// It signs the server's single-use challenge; the secret never leaves the process. After a
// reconnect it signs the new connection's challenge automatically. A refused auth_key stops the
// automatic re-auth (the server closes the socket after 5 failures). It needs WSOptions.KeySigner
// (Client.WebSocket sets it when the client uses Auth "hmac").
func (w *WebSocket) AuthKey(ctx context.Context) (AuthResult, error) {
	if w.opts.KeySigner == nil {
		return AuthResult{}, &ConfigError{Msg: `AuthKey needs a client created with Auth "hmac" (or WSOptions.KeySigner)`}
	}
	w.mu.Lock()
	w.token = ""
	w.keyAuth = true
	connected := w.conn != nil && w.welcome != nil
	w.mu.Unlock()
	if !connected {
		return AuthResult{Queued: true}, nil
	}
	return w.authKey(ctx)
}

func (w *WebSocket) authKey(ctx context.Context) (AuthResult, error) {
	w.mu.Lock()
	challenge := w.challenge
	w.challenge = "" // a challenge is signed at most once
	welcome := w.welcome
	connID := ""
	if welcome != nil {
		connID = welcome.ConnectionID
	}
	w.mu.Unlock()
	if challenge == "" || connID == "" || w.opts.KeySigner == nil {
		return AuthResult{}, &WSError{Code: "NO_CHALLENGE", Message: "AuthKey: the server has not issued a challenge on this connection"}
	}
	keyID, sig := w.opts.KeySigner.SignWebSocketChallenge(connID, challenge)
	// A custom signer may be slow (a KMS or HSM): if the connection changed meanwhile, the
	// signature is for the old one. Drop it; the new connection signs its own challenge.
	w.mu.Lock()
	stale := w.welcome != welcome
	w.mu.Unlock()
	if stale {
		return AuthResult{}, &WSError{Code: "STALE_CHALLENGE", Message: "AuthKey: the connection changed while signing; the new connection authenticates itself"}
	}
	ack, err := w.request(ctx, "auth_key", map[string]any{"key_id": keyID, "signature": sig}, nil, true)
	if err != nil {
		return AuthResult{}, err
	}
	uid, _ := ack["user_id"].(string)
	kind, _ := ack["auth"].(string)
	return AuthResult{UserID: uid, Auth: kind}, nil
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

// UserID is the user of the last successful Auth on the current connection ("" when signed out).
func (w *WebSocket) UserID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.authUserID
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
//
// Channels the server refuses are listed in RefusedByServer with their error frames; they are
// not held and not retried. The error is non-nil only when every channel sent was refused (the
// first refusal), or when no answer came within AckTimeout (TIMEOUT; the channels stay held and
// are re-subscribed after a reconnect) or the connection dropped. After a reconnect or a
// re-auth, the client re-subscribes by itself: a private channel refused with UNAUTHENTICATED
// waits for the next successful auth (PendingChannels); any other refusal drops the channel.
// Each such refusal goes to OnError as a *SubscribeRefusal.
func (w *WebSocket) Subscribe(ctx context.Context, channels ...string) (SubscribeResult, error) {
	// Two spellings of one channel (ticker:btc_usdt, ticker:BTC/USDT) are one subscription: send it once.
	wanted := uniqChannels(channels)
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
	var send []string
	for _, c := range accepted {
		if c == FuturesAccountChannel && w.authUserID == "" {
			// Held until Auth or AuthKey succeeds, then subscribed (see PendingChannels).
			w.pendingPrivate = addUnique(w.pendingPrivate, c)
			continue
		}
		w.channels = append(w.channels, c)
		send = append(send, c)
	}
	connected := w.conn != nil && w.welcome != nil
	w.mu.Unlock()
	if len(res.Refused) > 0 {
		w.log.Warn("cexy: WebSocket subscription cap reached", "cap", w.opts.MaxSubscriptions, "refused", res.Refused)
	}
	if len(send) == 0 || !connected {
		return res, nil
	}
	o := w.sendSubscribe(ctx, send)
	// Refused by the server (e.g. UNAUTHENTICATED for a private channel, RATE_LIMITED for a
	// futures feed): those channels are not held, and not retried.
	w.mu.Lock()
	for _, r := range o.refused {
		w.drop(r.Channel)
	}
	// Hold accepted channels by the name the server acknowledged (its canonical form).
	for _, a := range o.added {
		for i, x := range w.channels {
			if x != a && sameChannel(x, a) && slices.Contains(send, x) {
				w.channels[i] = a
				break
			}
		}
	}
	w.mu.Unlock()
	res.Added = o.added
	res.RefusedByServer = o.refused
	return res, o.err
}

// Unsubscribe unsubscribes and returns on the unsubscribed acknowledgement (or after
// AckTimeout without one); it fails on an error frame with the request id.
func (w *WebSocket) Unsubscribe(ctx context.Context, channels ...string) error {
	w.mu.Lock()
	var held []string
	for _, c := range uniq(channels) {
		w.pendingPrivate = remove(w.pendingPrivate, c)
		w.resetSeqLocked(c)
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
	w.resetSeqLocked("")
	if w.stop != nil {
		close(w.stop)
		w.stop = nil
	}
	conn := w.conn
	books := w.bookList()
	helpers := append([]*LiveBalances(nil), w.liveBalances...)
	w.teardownLocked(&WSError{Code: "CLOSED", Message: "connection closed by client"})
	w.mu.Unlock()
	for _, b := range books {
		b.markDisconnected()
	}
	for _, lb := range helpers {
		lb.markStale() // no connection: nothing is live any more
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
		// Every auth_key reply carries the next challenge: store it before anything else.
		if ch, ok := frame["challenge"].(string); ok {
			w.mu.Lock()
			w.challenge = ch
			w.mu.Unlock()
		}
		uid, _ := frame["user_id"].(string)
		w.dispatch(func() {
			if w.h.OnAuthenticated != nil {
				w.h.OnAuthenticated(uid)
			}
		})
		// Update the state before Auth returns, and before any later frame.
		w.onAuthenticated(uid)
		if id != "" {
			w.settle(id, "auth|auth_key", frame, nil)
		}
		return
	case "subscribed", "unsubscribed":
		channels := stringList(frame["channels"])
		kind := "subscribe"
		if typ == "unsubscribed" {
			kind = "unsubscribe"
		}
		if typ == "subscribed" {
			w.mu.Lock()
			for _, c := range channels {
				w.resetSeqLocked(c) // the next frame is the new baseline
			}
			helpers := append([]*LiveBalances(nil), w.liveBalances...)
			w.mu.Unlock()
			if slices.Contains(channels, "balances") {
				for _, lb := range helpers {
					lb.trigger("resubscribed")
				}
			}
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
	case "signed_out":
		// signed_out (a planned server frame): the server signed this connection out (token
		// expired, session revoked, or a future reason). Private subscriptions are gone; a fresh
		// Auth on this socket restores them.
		raw, _ := frame["reason"].(string)
		if raw == "" {
			raw = "unknown"
		}
		w.mu.Lock()
		w.token = ""
		w.mu.Unlock()
		switch raw {
		case "revoked":
			w.signedOut(AuthSessionRevoked, "")
			lost := Event{Type: "session.revoked", Channel: "account",
				Data: json.RawMessage(`{"session_id":null,"reason":"signed_out","current":true}`)}
			w.dispatch(func() {
				if w.h.OnAuthLost != nil {
					w.h.OnAuthLost(lost)
				}
			})
		case "expired":
			w.signedOut(AuthTokenExpired, "")
		case string(AuthKeyRevoked), string(AuthKeyExpired):
			w.mu.Lock()
			w.keyAuth = false // the key cannot sign in again
			w.mu.Unlock()
			w.signedOut(AuthChangeReason(raw), "")
		default:
			w.signedOut(AuthSignedOut, raw)
		}
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
		if ch, ok := frame["challenge"].(string); ok {
			w.mu.Lock()
			w.challenge = ch
			w.mu.Unlock()
		}
		if id != "" {
			// Any error on an auth frame signs the connection out (an UNAUTHENTICATED error on a
			// subscribe is only a refused subscribe).
			w.mu.Lock()
			p, ok := w.pending[id]
			late := !ok && w.authKeyIDs[id] // a refusal that arrived after the timeout
			isAuth := late || (ok && (p.kind == "auth" || p.kind == "auth_key"))
			if late || (ok && p.kind == "auth_key") {
				w.keyAuth = false // a refused key is not tried again automatically
			}
			// A subscribe gets one error per refused channel before its ack: it completes on the
			// ack, or here once every channel was refused (then no ack follows).
			waitAck := false
			if ok && p.kind == "subscribe" {
				p.refusals = append(p.refusals, werr)
				waitAck = len(p.refusals) < len(p.channels)
			}
			w.mu.Unlock()
			if isAuth {
				w.signedOut(AuthFailed, code)
			}
			if !waitAck {
				w.settle(id, "", nil, werr)
			}
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
			helpers := append([]*LiveBalances(nil), w.liveBalances...)
			w.mu.Unlock()
			for _, b := range books {
				go b.resync(0)
			}
			for _, lb := range helpers {
				lb.trigger(string(ResyncConcurrentModification))
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
	if PrivateChannels[ev.Channel] && ev.Sequence != nil {
		w.trackSeq(ev.Channel, *ev.Sequence)
	}
	switch ev.Type {
	case "futures.resync":
		w.onFuturesResync(ev)
		return
	case "balances.resync", "deposits.resync", "withdrawals.resync":
		reason := ResyncReason(strings.ReplaceAll(ev.Type, ".", "_"))
		w.dispatch(func() {
			if w.h.OnEvent != nil {
				w.h.OnEvent(ev)
			}
			if w.h.OnResync != nil {
				w.h.OnResync(reason)
			}
		})
		if ev.Type == "balances.resync" {
			for _, lb := range w.helpers() {
				lb.trigger(string(ResyncBalancesResync))
			}
		}
		return
	case "balance.updated":
		for _, lb := range w.helpers() {
			lb.onEvent(ev.Data)
		}
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

func (w *WebSocket) helpers() []*LiveBalances {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]*LiveBalances(nil), w.liveBalances...)
}

// resetSeqLocked forgets the sequence baseline of channel ("" for all channels).
func (w *WebSocket) resetSeqLocked(channel string) {
	for c, st := range w.seq {
		if channel == "" || c == channel {
			if st.timer != nil {
				st.timer.Stop()
			}
			delete(w.seq, c)
		}
	}
}

// trackSeq: the first frame of a private channel is the baseline, a lower number is late (never a
// gap), and a higher one opens holes that must fill within the reorder window.
func (w *WebSocket) trackSeq(channel string, n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.seq[channel]
	if st == nil {
		w.seq[channel] = &seqState{next: n + 1, holes: map[int64]bool{}}
		return
	}
	if n < st.next {
		if st.holes[n] {
			delete(st.holes, n)
			if len(st.holes) == 0 && st.timer != nil {
				st.timer.Stop()
				st.timer = nil
				st.first = nil
			}
		}
		return
	}
	if n > st.next && st.first == nil {
		st.first = &SequenceGap{Channel: channel, Expected: st.next, Received: n}
	}
	for m := st.next; m < n; m++ {
		st.holes[m] = true
	}
	st.next = n + 1
	if len(st.holes) > 0 && st.timer == nil {
		state := st
		st.timer = w.opts.Clock.AfterFunc(w.opts.ReorderWindow, func() {
			w.mu.Lock()
			state.timer = nil
			if len(state.holes) == 0 || w.seq[channel] != state {
				w.mu.Unlock()
				return
			}
			gap := *state.first
			state.holes = map[int64]bool{}
			state.first = nil
			helpers := append([]*LiveBalances(nil), w.liveBalances...)
			w.mu.Unlock()
			w.dispatch(func() {
				if w.h.OnSequenceGap != nil {
					w.h.OnSequenceGap(gap)
				}
				if w.h.OnResync != nil {
					w.h.OnResync(ResyncSequenceGap)
				}
			})
			if channel == "balances" {
				for _, lb := range helpers {
					lb.trigger(string(ResyncSequenceGap))
				}
			}
		})
	}
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
	for _, c := range dropped {
		w.pendingPrivate = addUnique(w.pendingPrivate, c)
	}
	return dropped
}

// signedOut: the server signed the connection out and ended every private subscription.
func (w *WebSocket) signedOut(reason AuthChangeReason, code string) {
	w.mu.Lock()
	change := AuthChange{Reason: reason, PreviousUserID: w.authUserID, Code: code}
	w.authUserID = ""
	for c := range PrivateChannels {
		w.resetSeqLocked(c)
	}
	change.Dropped = w.dropPrivateLocked()
	helpers := append([]*LiveBalances(nil), w.liveBalances...)
	w.mu.Unlock()
	for _, lb := range helpers {
		lb.onAuthChanged(reason)
	}
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
		for c := range PrivateChannels {
			w.resetSeqLocked(c)
		}
		change = &AuthChange{Reason: AuthUserChanged, PreviousUserID: previous, UserID: userID, Dropped: w.dropPrivateLocked()}
	}
	channels := w.pendingPrivate
	w.pendingPrivate = nil
	w.channels = append(w.channels, channels...)
	helpers := append([]*LiveBalances(nil), w.liveBalances...)
	w.mu.Unlock()
	if change != nil {
		for _, lb := range helpers {
			lb.onAuthChanged(AuthUserChanged)
		}
	}
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
		w.resubscribed(w.sendSubscribe(context.Background(), channels))
	}()
	w.dispatch(func() {
		if w.h.OnResync != nil {
			w.h.OnResync(ResyncReauth)
		}
	})
}

// settle completes pending request id if the acknowledgement matches its kind (kinds joined
// with "|"; "" = any, for error frames).
func (w *WebSocket) settle(id, kind string, frame map[string]any, err error) {
	w.mu.Lock()
	p, ok := w.pending[id]
	if !ok || (kind != "" && !slices.Contains(strings.Split(kind, "|"), p.kind)) {
		w.mu.Unlock()
		return
	}
	delete(w.pending, id)
	refusals := p.refusals
	w.mu.Unlock()
	p.done <- wsAck{frame: frame, err: err, refusals: refusals}
}

func (w *WebSocket) settleByChannel(kind string, channels []string, frame map[string]any) {
	w.mu.Lock()
	for id, p := range w.pending {
		if p.kind == kind && overlaps(p.channels, channels) {
			delete(w.pending, id)
			refusals := p.refusals
			w.mu.Unlock()
			p.done <- wsAck{frame: frame, refusals: refusals}
			return
		}
	}
	w.mu.Unlock()
}

// request sends {op, id, ...payload} and waits for the acknowledgement with the same id, or
// an error frame with that id. With strict, no acknowledgement within AckTimeout is an error;
// otherwise it returns a nil frame.
func (w *WebSocket) request(ctx context.Context, kind string, payload map[string]any, channels []string, strict bool) (map[string]any, error) {
	r, err := w.begin(ctx, kind, payload, channels)
	if err != nil {
		return nil, err
	}
	return w.wait(ctx, r, strict)
}

type wsRequest struct {
	id   string
	kind string
	p    *wsPending
}

// begin sends a request; wait collects its acknowledgement. Splitting them lets frames that
// must reach the server in order be written back to back.
func (w *WebSocket) begin(ctx context.Context, kind string, payload map[string]any, channels []string) (*wsRequest, error) {
	w.mu.Lock()
	id := strconv.Itoa(w.nextID)
	w.nextID++
	p := &wsPending{kind: kind, channels: channels, done: make(chan wsAck, 1)}
	w.pending[id] = p
	if kind == "auth_key" && w.authKeyIDs != nil {
		w.authKeyIDs[id] = true
	}
	w.mu.Unlock()

	payload["op"] = kind
	payload["id"] = id
	if err := w.send(ctx, payload); err != nil {
		w.mu.Lock()
		delete(w.pending, id)
		w.mu.Unlock()
		return nil, err
	}
	return &wsRequest{id: id, kind: kind, p: p}, nil
}

func (w *WebSocket) wait(ctx context.Context, r *wsRequest, strict bool) (map[string]any, error) {
	ack := w.waitAck(ctx, r, strict)
	return ack.frame, ack.err
}

func (w *WebSocket) waitAck(ctx context.Context, r *wsRequest, strict bool) wsAck {
	t := time.NewTimer(w.opts.AckTimeout)
	defer t.Stop()
	// abandon gives up on the acknowledgement; error frames already received for a subscribe
	// mean the server refused every channel it got to (no ack follows then).
	abandon := func() []*WSError {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.pending[r.id] == r.p {
			delete(w.pending, r.id)
		}
		return r.p.refusals
	}
	select {
	case ack := <-r.p.done:
		return ack
	case <-t.C:
		if refusals := abandon(); len(refusals) > 0 {
			return wsAck{err: refusals[0], refusals: refusals}
		}
		if strict {
			return wsAck{err: &WSError{Code: "TIMEOUT", Message: fmt.Sprintf("no %s acknowledgement for %s (id %s)", ackType[r.kind], r.kind, r.id)}}
		}
		return wsAck{}
	case <-ctx.Done():
		_ = abandon()
		return wsAck{err: ctx.Err()}
	}
}

// subscribeOutcome is what the server did with one subscribe request.
type subscribeOutcome struct {
	added   []string
	refused []SubscribeRefusal
	// Set only when every channel was refused (the first refusal), or when no answer came
	// (TIMEOUT, a disconnect, the context).
	err error
}

// sendSubscribe subscribes and waits for the outcome. The server sends one error frame per
// refused channel, in the order of the request, BEFORE its single subscribed ack, and no ack at
// all when it accepted nothing.
func (w *WebSocket) sendSubscribe(ctx context.Context, channels []string) subscribeOutcome {
	r, err := w.begin(ctx, "subscribe", map[string]any{"channels": channels}, channels)
	if err != nil {
		return subscribeOutcome{added: []string{}, err: err}
	}
	return subscribeResult(channels, w.waitAck(ctx, r, true))
}

// subscribeResult: the channels missing from the ack were refused, matched in order with the
// error frames (after the server's subscription cap it stops with one error for the rest).
func subscribeResult(channels []string, ack wsAck) subscribeOutcome {
	acked := subscribed(ack.frame)
	o := subscribeOutcome{added: uniq(acked)}
	if o.added == nil {
		o.added = []string{}
	}
	if len(ack.refusals) == 0 {
		o.err = ack.err
		return o
	}
	// The ack can repeat a name (a channel already held, or two spellings of one channel), so
	// its names are matched as a multiset: each acknowledges one channel sent.
	pool := append([]string(nil), acked...)
	i := 0
	for _, c := range channels {
		if j := slices.IndexFunc(pool, func(a string) bool { return sameChannel(c, a) }); j >= 0 {
			pool = slices.Delete(pool, j, j+1)
			continue
		}
		o.refused = append(o.refused, SubscribeRefusal{Channel: c, Err: ack.refusals[min(i, len(ack.refusals)-1)]})
		i++
	}
	if len(o.added) == 0 {
		o.err = ack.refusals[0]
	}
	return o
}

// sameChannel matches a channel sent with one named in an ack, with the server's
// canonicalisation (see channelKey).
func sameChannel(sent, acked string) bool {
	return channelKey(sent) == channelKey(acked)
}

// channelKey is the server's canonical form of a channel name: the whole name trimmed; the
// channel kind exactly (Ticker:BTC/USDT is not ticker:BTC/USDT); a spot market symbol trimmed,
// uppercased and with "_" read as "/" (ticker:btc_usdt is ticker:BTC/USDT); futures names
// exactly (coins are case-sensitive).
func channelKey(c string) string {
	c = strings.TrimSpace(c)
	if strings.HasPrefix(c, "futures.") {
		return c
	}
	kind, market, ok := strings.Cut(c, ":")
	if !ok {
		return c
	}
	return kind + ":" + strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(market), "_", "/"))
}

// resubscribed handles an automatic re-subscribe (after a reconnect, a re-auth, or futures.resync
// on futures.account): a private channel refused with UNAUTHENTICATED goes back to pending (the
// next successful auth restores it); any other refusal drops the channel. Every refusal is
// reported to OnError as a *SubscribeRefusal.
func (w *WebSocket) resubscribed(o subscribeOutcome) {
	w.mu.Lock()
	for _, r := range o.refused {
		w.drop(r.Channel)
		if PrivateChannels[r.Channel] && r.Err.Code == "UNAUTHENTICATED" {
			w.pendingPrivate = addUnique(w.pendingPrivate, r.Channel)
		} else {
			w.pendingPrivate = remove(w.pendingPrivate, r.Channel)
		}
	}
	w.mu.Unlock()
	for i := range o.refused {
		w.emitError(&o.refused[i])
	}
	if o.err != nil && len(o.refused) == 0 {
		w.emitError(o.err)
	}
}

func subscribed(ack map[string]any) []string {
	if ack == nil {
		return []string{}
	}
	return stringList(ack["channels"])
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
	helpers := append([]*LiveBalances(nil), w.liveBalances...)
	w.mu.Unlock()
	conn.CloseNow()
	for _, b := range books {
		b.markDisconnected() // sequences reset per connection
	}
	for _, lb := range helpers {
		lb.markStale() // the re-subscribe after the reconnect takes a new snapshot
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
		if sameChannel(x, c) {
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

func addUnique(list []string, c string) []string {
	for _, x := range list {
		if x == c {
			return list
		}
	}
	return append(list, c)
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
		if sameChannel(x, c) {
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

// uniqChannels drops channels that canonicalise alike (see channelKey), keeping the first spelling.
func uniqChannels(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if k := channelKey(s); !seen[k] {
			seen[k] = true
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
