package cexy

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultBaseURL is the production API.
const DefaultBaseURL = "https://api.cexy.io"

// Default client-side rate limits. The server allows about 120 requests a minute per IP
// without credentials and 600 a minute per API key; these keep a margin.
const (
	DefaultRPMAnonymous = 100
	DefaultRPMWithKey   = 300
)

// Options configures a Client. The zero value gives an anonymous client for public data.
type Options struct {
	// API key id (ak_…). Give it together with APISecret, or not at all.
	APIKey string
	// API key secret. Never logged, never put in a URL.
	APISecret string
	// A custom credentials scheme (for example request signing once the API supports it).
	// Mutually exclusive with APIKey/APISecret.
	Authenticator Authenticator
	// Default DefaultBaseURL. Must be https:// (see AllowInsecure).
	BaseURL string
	// Allow plain http:// (and ws:// for WebSocket), but ONLY for a loopback host
	// (localhost, 127.0.0.1, ::1), for example a local test server.
	AllowInsecure bool
	// Per-attempt timeout. Default 10 s.
	Timeout time.Duration
	// Retries after the first attempt for retryable failures. Default 3; use NoRetries for 0.
	MaxRetries int
	// Set to disable retries (MaxRetries 0 means "default").
	NoRetries bool
	// Default: an http.Client without a timeout of its own (Timeout applies per attempt).
	// The SDK uses a copy with redirects disabled; the client you pass is not modified.
	HTTPClient *http.Client
	// Client-side rate limit in requests per minute. Default DefaultRPMAnonymous without
	// credentials and DefaultRPMWithKey with them. It adapts downwards to the server's
	// X-RateLimit-* headers.
	RequestsPerMinute int
	// Disables the client-side rate limiter.
	DisableRateLimit bool
	// Appended to the User-Agent: "my-bot/1.2" gives "cexy-go/<Version> my-bot/1.2".
	UserAgentSuffix string
	// Called before each retry (for logging or metrics). Never receives credentials.
	OnRetry func(RetryInfo)

	// Test hooks (unexported: set only by this package's tests).
	sleep  func(context.Context, time.Duration) error
	random func() float64
	now    func() time.Time
}

// String keeps fmt (%v, %+v) from printing APISecret.
func (o Options) String() string {
	key := ""
	if o.APIKey != "" {
		key = redacted
	}
	return fmt.Sprintf("cexy.Options{BaseURL: %q, APIKey: %q, APISecret: %q}", o.BaseURL, key, key)
}

// GoString keeps %#v from printing APISecret.
func (o Options) GoString() string { return o.String() }

// Client is the CEXY.io REST client. It is safe for concurrent use.
type Client struct {
	Markets  *MarketsService
	Assets   *AssetsService
	Networks *NetworksService
	Fees     *FeesService
	Pools    *PoolsService
	Account  *AccountService
	Exports  *ExportsService
	Wallet   *WalletService
	Trading  *TradingService

	t             *transport
	baseURL       string
	allowInsecure bool
}

// New checks the options and returns a client. It sends nothing.
func New(opts Options) (*Client, error) {
	hasKey, hasSecret := opts.APIKey != "", opts.APISecret != ""
	if hasKey != hasSecret {
		return nil, &ConfigError{Msg: "APIKey and APISecret must be given together (got only one of them)"}
	}
	if hasKey && opts.Authenticator != nil {
		return nil, &ConfigError{Msg: "pass either APIKey/APISecret or Authenticator, not both"}
	}
	auth := opts.Authenticator
	if hasKey {
		a, err := NewAPIKeyAuthenticator(opts.APIKey, opts.APISecret)
		if err != nil {
			return nil, err
		}
		auth = a
	}

	base := strings.TrimRight(opts.BaseURL, "/")
	if base == "" {
		base = DefaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return nil, &ConfigError{Msg: fmt.Sprintf("BaseURL is not a valid URL: %q", base)}
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, &ConfigError{Msg: "BaseURL must not contain credentials, a query or a fragment"}
	}
	if err := checkSecureURL(u, "https", "http", opts.AllowInsecure, "BaseURL"); err != nil {
		return nil, err
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if timeout < 0 {
		return nil, &ConfigError{Msg: "Timeout must be > 0"}
	}
	maxRetries := opts.MaxRetries
	switch {
	case opts.NoRetries:
		maxRetries = 0
	case maxRetries == 0:
		maxRetries = 3
	case maxRetries < 0:
		return nil, &ConfigError{Msg: "MaxRetries must be >= 0"}
	}
	hc := noRedirectClient(opts.HTTPClient)
	sleep := opts.sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	random := opts.random
	if random == nil {
		random = rand.Float64
	}
	now := opts.now
	if now == nil {
		now = time.Now
	}
	var limiter *rateLimiter
	if !opts.DisableRateLimit {
		rpm := opts.RequestsPerMinute
		if rpm < 0 {
			return nil, &ConfigError{Msg: "RequestsPerMinute must be > 0"}
		}
		if rpm == 0 {
			rpm = DefaultRPMAnonymous
			if auth != nil {
				rpm = DefaultRPMWithKey
			}
		}
		limiter = newRateLimiter(rpm, now, sleep)
	}
	ua := UserAgent
	if s := strings.TrimSpace(opts.UserAgentSuffix); s != "" {
		if strings.ContainsAny(s, "\r\n") {
			return nil, &ConfigError{Msg: "UserAgentSuffix must not contain line breaks"}
		}
		ua += " " + s
	}

	t := &transport{baseURL: base, origin: origin(u), http: hc, auth: auth, limiter: limiter, userAgent: ua, timeout: timeout,
		maxRetries: maxRetries, sleep: sleep, random: random, onRetry: opts.OnRetry}
	return &Client{
		Markets:       &MarketsService{t},
		Assets:        &AssetsService{t},
		Networks:      &NetworksService{t},
		Fees:          &FeesService{t},
		Pools:         &PoolsService{t},
		Account:       &AccountService{t},
		Exports:       &ExportsService{t},
		Wallet:        &WalletService{t},
		Trading:       &TradingService{t},
		t:             t,
		baseURL:       base,
		allowInsecure: opts.AllowInsecure,
	}, nil
}

// HasCredentials reports whether private endpoints are available.
func (c *Client) HasCredentials() bool { return c.t.auth != nil }

// BaseURL returns the API base URL.
func (c *Client) BaseURL() string { return c.baseURL }

// UserAgent returns the User-Agent the client sends.
func (c *Client) UserAgent() string { return c.t.userAgent }

// RateLimit returns the client-side rate limiter state; ok is false when it is disabled.
func (c *Client) RateLimit() (state RateLimitState, ok bool) {
	if c.t.limiter == nil {
		return RateLimitState{}, false
	}
	return c.t.limiter.state(), true
}

// Time returns the server clock. Compare it with yours to detect skew.
func (c *Client) Time(ctx context.Context, opts ...CallOption) (ServerTime, error) {
	return getData[ServerTime](ctx, c.t, call{op: OpServerTime}, opts)
}

// Config returns the public exchange configuration (maintenance state, page sizes, ...).
func (c *Client) Config(ctx context.Context, opts ...CallOption) (ExchangeConfig, error) {
	return getData[ExchangeConfig](ctx, c.t, call{op: OpExchangeConfig}, opts)
}

func (c *Client) String() string {
	auth := "none"
	if c.t.auth != nil {
		auth = c.t.auth.Kind() + " " + redacted
	}
	return "cexy.Client(" + c.baseURL + ", auth=" + auth + ")"
}

// GoString keeps %#v from printing the credentials.
func (c *Client) GoString() string { return c.String() }

// noRedirectClient returns a copy of hc (or of a zero http.Client when hc is nil) that never
// follows redirects. Go's http.Client copies custom headers to the redirect target, so following
// one could send X-API-Key and X-API-Secret to another host, even over plain http, and a 307/308
// would re-send an order. The 3xx response is returned instead and becomes an error.
func noRedirectClient(hc *http.Client) *http.Client {
	c := &http.Client{}
	if hc != nil {
		cp := *hc
		c = &cp
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return c
}

// origin is the scheme and host of u, lower-cased: "https://api.cexy.io".
func origin(u *url.URL) string {
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host)
}

// IsLocalHost reports whether host is a loopback name or address, the only hosts where
// plain-text transport may be allowed.
func IsLocalHost(host string) bool {
	h := strings.ToLower(strings.Trim(host, "[]"))
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}

// checkSecureURL requires scheme secure; insecure is accepted only with allowInsecure and a
// loopback host. Credentials must never travel in clear text.
func checkSecureURL(u *url.URL, secure, insecure string, allowInsecure bool, what string) error {
	switch strings.ToLower(u.Scheme) {
	case secure:
		return nil
	case insecure:
		if !allowInsecure {
			return &ConfigError{Msg: fmt.Sprintf("%s must use %s:// (set AllowInsecure only for a local test server)", what, secure)}
		}
		host := u.Hostname()
		if h, _, err := net.SplitHostPort(u.Host); err == nil {
			host = h
		}
		if !IsLocalHost(host) {
			return &ConfigError{Msg: fmt.Sprintf("%s: %s:// is only allowed for localhost, 127.0.0.1 or ::1", what, insecure)}
		}
		return nil
	default:
		return &ConfigError{Msg: fmt.Sprintf("%s must use %s://", what, secure)}
	}
}
