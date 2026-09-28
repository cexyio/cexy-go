package cexy

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateLimitState is a snapshot of the client-side rate limiter.
type RateLimitState struct {
	RequestsPerMinute float64
	Tokens            float64
	BlockedUntil      time.Time
}

// rateLimiter is a client-side token bucket. It adapts downwards to X-RateLimit-Limit,
// X-RateLimit-Remaining and X-RateLimit-Reset (seconds until the window resets), and to the
// Retry-After of a 429. It never raises the configured limit.
type rateLimiter struct {
	mu           sync.Mutex
	rpm          float64
	tokens       float64
	last         time.Time
	blockedUntil time.Time
	now          func() time.Time
	sleep        func(context.Context, time.Duration) error
}

func newRateLimiter(rpm int, now func() time.Time, sleep func(context.Context, time.Duration) error) *rateLimiter {
	return &rateLimiter{rpm: float64(rpm), tokens: float64(rpm), last: now(), now: now, sleep: sleep}
}

func (l *rateLimiter) state() RateLimitState {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	return RateLimitState{RequestsPerMinute: l.rpm, Tokens: l.tokens, BlockedUntil: l.blockedUntil}
}

// acquire waits until a request may be sent, then takes a token.
func (l *rateLimiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		l.refill()
		now := l.now()
		var wait time.Duration
		switch {
		case now.Before(l.blockedUntil):
			wait = l.blockedUntil.Sub(now)
		case l.tokens >= 1:
			l.tokens--
			l.mu.Unlock()
			return nil
		default:
			perToken := time.Minute.Seconds() / l.rpm
			wait = time.Duration(math.Ceil((1-l.tokens)*perToken*1000)) * time.Millisecond
		}
		l.mu.Unlock()
		if err := l.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// pendingWait is how long acquire would block right now: a server-imposed block, or the wait
// for the next token. It takes no token.
func (l *rateLimiter) pendingWait() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	now := l.now()
	if now.Before(l.blockedUntil) {
		return l.blockedUntil.Sub(now)
	}
	if l.tokens >= 1 {
		return 0
	}
	perToken := time.Minute.Seconds() / l.rpm
	return time.Duration(math.Ceil((1-l.tokens)*perToken*1000)) * time.Millisecond
}

// update adapts to the server's rate-limit headers.
func (l *rateLimiter) update(h http.Header) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	// A limit below 1 a minute is not plausible; ignoring it keeps every token wait under a minute.
	if limit, ok := headerNum(h, "X-RateLimit-Limit"); ok && limit >= 1 && limit < l.rpm {
		l.rpm = limit
		l.tokens = math.Min(l.tokens, limit)
	}
	remaining, ok := headerNum(h, "X-RateLimit-Remaining")
	if !ok {
		return
	}
	if remaining >= 0 && remaining < l.tokens {
		l.tokens = remaining
	}
	if remaining == 0 {
		if reset, ok := headerNum(h, "X-RateLimit-Reset"); ok {
			l.blockLocked(resetDuration(reset, l.now()))
		} else {
			l.blockLocked(time.Duration(float64(time.Minute) / l.rpm))
		}
	}
}

// blockFor blocks every request for d (a 429's Retry-After), at most MaxServerWait.
func (l *rateLimiter) blockFor(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.blockLocked(d)
}

func (l *rateLimiter) blockLocked(d time.Duration) {
	if d <= 0 {
		return
	}
	d = min(d, MaxServerWait) // server hints are untrusted
	if until := l.now().Add(d); until.After(l.blockedUntil) {
		l.blockedUntil = until
	}
}

func (l *rateLimiter) refill() {
	now := l.now()
	if elapsed := now.Sub(l.last); elapsed > 0 {
		l.tokens = math.Min(l.rpm, l.tokens+elapsed.Minutes()*l.rpm)
		l.last = now
	}
}

// resetDuration: X-RateLimit-Reset is seconds until the window resets. Values that can only
// be epoch timestamps (seconds or milliseconds) are tolerated. The result is at most
// MaxServerWait; absurd values never overflow.
func resetDuration(reset float64, now time.Time) time.Duration {
	var d time.Duration
	switch {
	case reset > 1e15:
		return MaxServerWait
	case reset > 1e12:
		d = time.UnixMilli(int64(reset)).Sub(now)
	case reset > 1e9:
		d = time.Unix(int64(reset), 0).Sub(now)
	default:
		d = secondsDuration(reset)
	}
	return min(max(d, 0), MaxServerWait)
}

func headerNum(h http.Header, name string) (float64, bool) {
	v := strings.TrimSpace(h.Get(name))
	if v == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// sleepCtx waits for d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
