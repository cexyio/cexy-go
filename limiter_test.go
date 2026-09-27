package cexy

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
	return nil
}

func TestLimiterBucket(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	l := newRateLimiter(60, clk.Now, clk.sleep)
	start := clk.Now()
	for i := 0; i < 61; i++ {
		if err := l.acquire(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 60 immediately, the 61st after about one second.
	if waited := clk.Now().Sub(start); waited < 900*time.Millisecond || waited > 1100*time.Millisecond {
		t.Fatalf("waited %v", waited)
	}
}

func TestLimiterAdaptsToHeaders(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	l := newRateLimiter(300, clk.Now, clk.sleep)
	l.update(http.Header{"X-Ratelimit-Limit": {"120"}, "X-Ratelimit-Remaining": {"0"}, "X-Ratelimit-Reset": {"7"}})
	st := l.state()
	if st.RequestsPerMinute != 120 || st.Tokens != 0 || st.BlockedUntil.Sub(clk.Now()) != 7*time.Second {
		t.Fatalf("%+v", st)
	}
	start := clk.Now()
	_ = l.acquire(context.Background())
	if clk.Now().Sub(start) < 7*time.Second {
		t.Fatal("did not wait for the reset")
	}
	// Never raised above the configured limit.
	l.update(http.Header{"X-Ratelimit-Limit": {"10000"}})
	if l.state().RequestsPerMinute != 120 {
		t.Fatal("limit raised")
	}
}

func TestResetDuration(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	if d := resetDuration(5, now); d != 5*time.Second {
		t.Fatal(d)
	}
	if d := resetDuration(float64(now.Unix()+9), now); d != 9*time.Second {
		t.Fatal(d)
	}
	if d := resetDuration(float64(now.UnixMilli()+3000), now); d != 3*time.Second {
		t.Fatal(d)
	}
}

func Test429BlocksTheLimiter(t *testing.T) {
	calls := 0
	c, _, fs := newTestClient(t, Options{NoRetries: true}, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Retry-After", "30")
		writeJSON(w, 429, apiErr("RATE_LIMITED", "slow", true))
	})
	_, _ = c.Markets.List(context.Background())
	st, ok := c.RateLimit()
	if !ok || st.BlockedUntil.Sub(fs.clock()) < 25*time.Second {
		t.Fatalf("%+v", st)
	}
}
