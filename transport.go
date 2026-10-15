package cexy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"
)

const (
	backoffBase     = 500 * time.Millisecond
	backoffMax      = 10 * time.Second
	maxResponseBody = 64 << 20
)

// MaxServerWait is the longest the SDK waits because of a server hint (Retry-After,
// details.retry_after_seconds, X-RateLimit-Reset). A longer hint is not waited: the call fails
// at once with the rate-limit error, whose RetryAfter still holds the server's value, and the
// client-side rate limiter blocks for at most this long.
const MaxServerWait = 120 * time.Second

// CallOption changes one call: WithTimeout, WithMaxRetries, WithIdempotencyKey.
type CallOption func(*callOptions)

type callOptions struct {
	timeout        time.Duration
	maxRetries     int
	idempotencyKey string
	// noRetry, when set, names errors the request must return at once instead of retrying (the
	// futures history helpers handle a busy 503 themselves). Not part of the public options.
	noRetry func(error) bool
}

// WithTimeout overrides the client's per-attempt timeout for this call.
func WithTimeout(d time.Duration) CallOption { return func(o *callOptions) { o.timeout = d } }

// WithMaxRetries overrides the client's MaxRetries for this call (0 disables retries).
func WithMaxRetries(n int) CallOption { return func(o *callOptions) { o.maxRetries = n } }

// WithIdempotencyKey sets the Idempotency-Key of a pool join or exit (generated when absent),
// the only endpoints that honour it; set it yourself to make a retry across process restarts
// safe there. It is ignored on every other call: no Idempotency-Key is sent on orders or
// cancels, whose safety comes from client_order_id (see Trading.PlaceOrder).
func WithIdempotencyKey(key string) CallOption {
	return func(o *callOptions) { o.idempotencyKey = key }
}

// RetryInfo is passed to Options.OnRetry before the SDK waits and retries. It never holds
// credentials.
type RetryInfo struct {
	Operation      OperationID
	Method, Path   string
	Attempt        int // 1 for the first retry
	Delay          time.Duration
	Err            error
	IdempotencyKey string
}

// OperationInfo describes one operation of the SDK surface.
type OperationInfo struct {
	Method, Path string
	Auth         string // "none" or "api_key"
	Scope        string // "", "read" or "trade"
}

// Operations returns the SDK surface: the 55 operations with their method, path, auth and
// required key scope.
func Operations() map[OperationID]OperationInfo {
	out := make(map[OperationID]OperationInfo, len(operations))
	for k, v := range operations {
		out[k] = v
	}
	return out
}

type call struct {
	op             OperationID
	pathParams     map[string]string
	query          url.Values
	body           any
	text           bool
	idempotencyKey string
	// idempotent: the endpoint honours Idempotency-Key (pool join and exit), so one is sent and
	// reused on every retry. No other call sends one.
	idempotent bool
	// cost: request tokens this call takes from the client limiter (0 means 1; a batch of N
	// takes N).
	cost int
	// orderActions: placements in this call (PlaceOrder 1, PlaceOrders N, anything else 0). They
	// wait out an order-actions hold and spend Options.OrderActionsPerSecond.
	orderActions int
	// attempts, when set, gets one entry per failed attempt of request: whether the server may
	// have acted on it. The batch calls read it to tell a lost answer from a definite refusal.
	attempts *[]attemptKind
	// sent, when set, tells whether the latest attempt reached the HTTP client (anything that
	// fails before it, such as a config or credential error, sent nothing).
	sent *bool
}

// attemptKind is how one failed attempt of a mutation ended.
type attemptKind string

const (
	// attemptAmbiguous: the server may have acted (a network error after sending, a cancelled
	// or expired context, any 5xx except 501 and 503, or a 2xx the SDK cannot use).
	attemptAmbiguous attemptKind = "ambiguous"
	// attemptRefused: it did not (429, 501, 503, any other 4xx, or a request that was never sent).
	attemptRefused attemptKind = "refused"
)

// classifyAttempt tells an ambiguous failure from a definite refusal. sent is false when the
// attempt failed before reaching the HTTP client. An error it does not recognise is ambiguous:
// a wrong "maybe placed" costs a lookup, a wrong "not placed" invites a duplicate order.
func classifyAttempt(err error, sent bool) attemptKind {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return attemptAmbiguous // a cancelled or expired call may have been sent
	}
	if !sent {
		return attemptRefused
	}
	var ce *ConnectionError
	if errors.As(err, &ce) {
		if ce.notSent {
			return attemptRefused
		}
		return attemptAmbiguous
	}
	var ae *APIError
	if errors.As(err, &ae) {
		// Every 5xx except 501 and 503 (Cloudflare's 520 and 524 included). This changes only how
		// an attempt is read, not which statuses are retried.
		if ae.Status >= 500 && ae.Status != 501 && ae.Status != 503 {
			return attemptAmbiguous
		}
		return attemptRefused
	}
	// An unusable 2xx was acted on; anything else unrecognised may have been.
	return attemptAmbiguous
}

// afterAmbiguous reports whether an earlier attempt of the call may have been acted on.
func afterAmbiguous(attempts []attemptKind) bool {
	for _, a := range attempts {
		if a == attemptAmbiguous {
			return true
		}
	}
	return false
}

// repeatSafeMutations are the mutations that request retries without an Idempotency-Key:
// placements (a client_order_id makes the server refuse a repeat; PlaceOrders reads the
// attempts), cancels (cancelling twice changes nothing more) and cancel-all-after (a repeated
// arm leaves a deadline no earlier, a repeated disarm leaves it disarmed). PlaceOrder,
// CancelOrder and CancelOrderByClientID run their own retry loops with the same rule.
var repeatSafeMutations = map[OperationID]bool{
	OpPlaceOrder:            true,
	OpPlaceBatch:            true,
	OpCancelOrder:           true,
	OpCancelOrderByClientID: true,
	OpCancelBatch:           true,
	OpCancelAll:             true,
	OpCancelAllAfter:        true,
}

// orderActionsLimit is details.limit on a 429 that refused order actions (a market maker's
// per-second budget). It holds only placements, never reads or cancels.
const orderActionsLimit = "order_actions"

type rawResponse struct {
	status int
	header http.Header
	body   []byte
}

type transport struct {
	baseURL    string
	origin     string // scheme://host of baseURL; credentials are only sent there
	http       *http.Client
	auth       Authenticator
	limiter    *rateLimiter
	userAgent  string
	timeout    time.Duration
	maxRetries int
	sleep      func(context.Context, time.Duration) error
	now        func() time.Time
	random     func() float64
	onRetry    func(RetryInfo)

	// orderLimiter: the optional per-second budget for placements (Options.OrderActionsPerSecond).
	orderLimiter *rateLimiter
	// placementsHeldUntil: an order-actions 429 holds placements, and only them, until then.
	holdMu              sync.Mutex
	placementsHeldUntil time.Time
}

func (t *transport) options(opts []CallOption) callOptions {
	o := callOptions{timeout: t.timeout, maxRetries: t.maxRetries}
	for _, f := range opts {
		f(&o)
	}
	if o.maxRetries < 0 {
		o.maxRetries = 0
	}
	return o
}

// request sends c with the standard retry policy: retryable errors and connection failures
// are retried, except when the server asks to wait longer than MaxServerWait. Pool join and
// exit carry an Idempotency-Key reused on every attempt, which the server honours; the
// repeatSafeMutations are naturally repeatable. Any other mutation is sent once. PlaceOrder,
// CancelOrder and CancelOrderByClientID use attempt with their own policies.
func (t *transport) request(ctx context.Context, c call, opts []CallOption) (*rawResponse, error) {
	o := t.options(opts)
	if operations[c.op].Method != http.MethodGet && !c.idempotent && !repeatSafeMutations[c.op] {
		o.maxRetries = 0
	}
	if c.idempotent && c.idempotencyKey == "" {
		c.idempotencyKey = o.idempotencyKey
		if c.idempotencyKey == "" {
			c.idempotencyKey = newID()
		}
	}
	sent := new(bool)
	if c.sent != nil {
		sent = c.sent
	}
	c.sent = sent
	for attempt := 0; ; attempt++ {
		*sent = false
		res, err := t.attempt(ctx, c, o)
		if err == nil {
			return res, nil
		}
		if c.attempts != nil {
			*c.attempts = append(*c.attempts, classifyAttempt(err, *sent))
		}
		if attempt >= o.maxRetries || !isRetryable(err) || ctx.Err() != nil || (o.noRetry != nil && o.noRetry(err)) {
			return nil, err
		}
		if err := t.backoff(ctx, c.op, attempt, err, c.idempotencyKey); err != nil {
			return nil, err
		}
	}
}

// backoff waits before retry number attempt+1, honouring server hints. A server wait above
// MaxServerWait is not taken: backoff returns cause, which the caller returns as is.
func (t *transport) backoff(ctx context.Context, op OperationID, attempt int, cause error, idemKey string) error {
	if serverWaitTooLong(cause) {
		return cause
	}
	d := t.retryDelay(attempt, cause)
	if t.onRetry != nil {
		info := operations[op]
		t.onRetry(RetryInfo{Operation: op, Method: info.Method, Path: info.Path, Attempt: attempt + 1, Delay: d,
			Err: cause, IdempotencyKey: idemKey})
	}
	return t.sleep(ctx, d)
}

// backoffHinted is backoff with a wait the caller chose: hint, taken exactly (no jitter), or the
// normal backoff when hint is not positive. A hint above MaxServerWait is not taken: it returns
// cause, as backoff does.
func (t *transport) backoffHinted(ctx context.Context, op OperationID, attempt int, cause error, hint time.Duration) error {
	if hint <= 0 {
		return t.backoff(ctx, op, attempt, cause, "")
	}
	if hint > MaxServerWait {
		return cause
	}
	if t.onRetry != nil {
		info := operations[op]
		t.onRetry(RetryInfo{Operation: op, Method: info.Method, Path: info.Path, Attempt: attempt + 1, Delay: hint, Err: cause})
	}
	return t.sleep(ctx, hint)
}

// serverWaitTooLong: the server asked to wait longer than MaxServerWait.
func serverWaitTooLong(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.RetryAfter > MaxServerWait
}

// retryDelay: the server's hint (at most MaxServerWait, checked by backoff) plus up to 250 ms
// of jitter, or full-jitter exponential backoff (500 ms doubling, capped at 10 s).
func (t *transport) retryDelay(attempt int, cause error) time.Duration {
	var ae *APIError
	if errors.As(cause, &ae) && ae.RetryAfter > 0 {
		return ae.RetryAfter + time.Duration(t.random()*250)*time.Millisecond
	}
	capped := math.Min(float64(backoffMax), float64(backoffBase)*math.Pow(2, float64(attempt)))
	return time.Duration(math.Ceil(t.random() * capped))
}

// attempt is one try: rate limiter, credentials, timeout, error mapping. No retries, except ONE
// re-signed resend after SIGNATURE_EXPIRED once the client clock has been corrected.
func (t *transport) attempt(ctx context.Context, c call, o callOptions) (*rawResponse, error) {
	res, err := t.attemptOnce(ctx, c, o)
	var apiErr *APIError
	if err == nil || !errors.As(err, &apiErr) {
		return res, err
	}
	if apiErr.Code == "SIGNATURE_REQUIRED" {
		e := *apiErr
		e.Message = `this API key must sign its requests: use Auth "hmac" (the default) instead of "headers"`
		e.Retryable = false
		return nil, &e
	}
	if apiErr.Code == "KEY_NOT_SIGNABLE" {
		e := *apiErr
		e.Message = "create a new API key; keys issued before request signing can't sign"
		e.Retryable = false
		return nil, &e
	}
	h, ok := t.auth.(*HMACAuthenticator)
	if apiErr.Code != "SIGNATURE_EXPIRED" || !ok {
		return nil, err
	}
	serverMs, isNum := apiErr.Details["server_time_ms"].(int64)
	if f, isFloat := apiErr.Details["server_time_ms"].(float64); isFloat {
		serverMs, isNum = int64(f), true
	}
	if !isNum {
		return nil, err // no usable server clock: the error as the server sent it
	}
	if !h.adjustClock(serverMs) {
		e := *apiErr
		e.Message = "the local clock is more than 1 hour away from the server's: fix the system clock"
		e.Retryable = false
		return nil, &e
	}
	return t.attemptOnce(ctx, c, o) // re-signed with the corrected clock, once
}

func (t *transport) attemptOnce(ctx context.Context, c call, o callOptions) (*rawResponse, error) {
	info, ok := operations[c.op]
	if !ok {
		return nil, &ConfigError{Msg: fmt.Sprintf("unknown operation %q", c.op)}
	}
	u, err := t.buildURL(info, c)
	if err != nil {
		return nil, err
	}
	var body []byte
	if c.body != nil {
		if body, err = json.Marshal(c.body); err != nil {
			return nil, &ConfigError{Msg: fmt.Sprintf("%s %s: cannot encode the request body: %v", info.Method, info.Path, err)}
		}
	}
	if info.Auth == "api_key" && t.auth == nil {
		return nil, &ConfigError{Msg: fmt.Sprintf("%s %s needs an API key: create the client with Options.APIKey and APISecret",
			info.Method, info.Path)}
	}
	if t.limiter != nil {
		if err := t.limiter.acquire(ctx, float64(max(c.cost, 1))); err != nil {
			return nil, err
		}
	}
	if c.orderActions > 0 {
		if err := t.acquirePlacements(ctx, c.orderActions); err != nil {
			return nil, err
		}
	}

	actx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(actx, info.Method, u, rd)
	if err != nil {
		return nil, &ConfigError{Msg: err.Error()}
	}
	accept := "application/json"
	if c.text {
		accept = "text/csv, application/json"
	}
	req.Header.Set("Accept", accept)
	if t.userAgent != "" {
		req.Header.Set("User-Agent", t.userAgent)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.idempotent && c.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", c.idempotencyKey)
	}
	if info.Auth == "api_key" {
		// Defence in depth: requests are always built from baseURL, so this only fails on a bug.
		if origin(req.URL) != t.origin {
			return nil, &ConfigError{Msg: fmt.Sprintf("%s %s: refusing to send credentials to %s", info.Method, info.Path,
				origin(req.URL))}
		}
		if err := t.auth.Authenticate(req, body); err != nil {
			return nil, err
		}
	}

	if c.sent != nil {
		*c.sent = true
	}
	resp, err := t.http.Do(req)
	var data []byte
	notSent := err != nil && dialFailed(err)
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
		resp.Body.Close()
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		timeout := errors.Is(actx.Err(), context.DeadlineExceeded)
		return nil, &ConnectionError{Method: info.Method, Path: info.Path, Timeout: timeout,
			Err: errors.New(t.redact(err.Error())), notSent: notSent}
	}

	if t.limiter != nil {
		t.limiter.update(resp.Header)
	}
	if resp.StatusCode >= 300 && resp.StatusCode <= 399 {
		// Redirects are never followed (see noRedirectClient). Not retryable, not ambiguous.
		return nil, redirectError(resp.StatusCode, resp.Header, t.redact)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		apiErr := errorFromResponse(resp.StatusCode, data, resp.Header, t.redact)
		if errors.Is(apiErr, ErrRateLimited) && apiErr.RetryAfter > 0 && t.limiter != nil {
			if limit, _ := apiErr.Details["limit"].(string); limit == orderActionsLimit {
				t.holdPlacements(apiErr.RetryAfter)
			} else {
				t.limiter.blockFor(apiErr.RetryAfter)
			}
		}
		return nil, apiErr
	}
	if !c.text && len(data) > 0 && !json.Valid(data) {
		return nil, &badResponseError{msg: fmt.Sprintf("cexy: %s %s: expected JSON, got %q", info.Method, info.Path,
			resp.Header.Get("Content-Type"))}
	}
	return &rawResponse{status: resp.StatusCode, header: resp.Header, body: data}, nil
}

// holdPlacements holds placements, and only them, for d (an order-actions 429), at most
// MaxServerWait. A later hold is never shortened.
func (t *transport) holdPlacements(d time.Duration) {
	if d <= 0 {
		return
	}
	d = min(d, MaxServerWait) // server hints are untrusted
	t.holdMu.Lock()
	defer t.holdMu.Unlock()
	if until := t.now().Add(d); until.After(t.placementsHeldUntil) {
		t.placementsHeldUntil = until
	}
}

// acquirePlacements waits out an order-actions hold, then takes n from the optional order budget.
func (t *transport) acquirePlacements(ctx context.Context, n int) error {
	for {
		t.holdMu.Lock()
		wait := t.placementsHeldUntil.Sub(t.now())
		t.holdMu.Unlock()
		if wait <= 0 {
			break
		}
		if err := t.sleep(ctx, wait); err != nil {
			return err
		}
	}
	if t.orderLimiter != nil {
		return t.orderLimiter.acquire(ctx, float64(n))
	}
	return nil
}

// dialFailed: the connection was never made, so the request was not sent.
func dialFailed(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op == "dial"
}

var pathParamRE = regexp.MustCompile(`\{(\w+)\}`)

func (t *transport) buildURL(info OperationInfo, c call) (string, error) {
	var missing string
	path := pathParamRE.ReplaceAllStringFunc(info.Path, func(m string) string {
		name := m[1 : len(m)-1]
		v := c.pathParams[name]
		if !validPathValue(v) {
			missing = name
			return ""
		}
		return encodeRFC3986(v) // RFC 3986, uppercase hex: what is sent is what is signed
	})
	if missing != "" {
		return "", &ConfigError{Msg: fmt.Sprintf(`%s %s: %s is required (non-empty, not "." or "..", no CR/LF)`,
			info.Method, info.Path, missing)}
	}
	u := t.baseURL + path
	// RFC 3986 (%20 for a space), not url.Values.Encode (+ for a space): the query that is signed
	// is exactly the query that is sent.
	if q := encodeQuery(c.query); q != "" {
		u += "?" + q
	}
	return u, nil
}

func (t *transport) redact(s string) string {
	if t.auth == nil {
		return s
	}
	return t.auth.Redact(s)
}

// getData decodes the {"data": T} envelope.
func getData[T any](ctx context.Context, t *transport, c call, opts []CallOption) (T, error) {
	var zero T
	raw, err := t.request(ctx, c, opts)
	if err != nil {
		return zero, err
	}
	return decodeData[T](c.op, raw)
}

func decodeData[T any](op OperationID, raw *rawResponse) (T, error) {
	var env struct {
		Data T `json:"data"`
	}
	if err := json.Unmarshal(raw.body, &env); err != nil {
		return env.Data, fmt.Errorf("cexy: %s: cannot decode the response: %w", op, err)
	}
	return env.Data, nil
}

// getPage decodes one page of a cursor-paginated listing.
func getPage[T any](ctx context.Context, t *transport, c call, opts []CallOption) (*Page[T], error) {
	raw, err := t.request(ctx, c, opts)
	if err != nil {
		return nil, err
	}
	var p Page[T]
	if err := json.Unmarshal(raw.body, &p); err != nil {
		return nil, fmt.Errorf("cexy: %s: cannot decode the response: %w", c.op, err)
	}
	return &p, nil
}

// getText returns a text/csv body.
func getText(ctx context.Context, t *transport, c call, opts []CallOption) (string, error) {
	c.text = true
	raw, err := t.request(ctx, c, opts)
	if err != nil {
		return "", err
	}
	return string(raw.body), nil
}

// newID returns a random UUID v4.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("cexy: crypto/rand failed: " + err.Error())
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
