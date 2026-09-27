package cexy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// target is a second server that must never be reached by a redirect.
func redirectTarget(t *testing.T) (*httptest.Server, *recorder) {
	t.Helper()
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.add(r)
		writeJSON(w, 200, dataEnv([]any{}))
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func wantRedirectError(t *testing.T, err error, status int) {
	t.Helper()
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != status || ae.Code != CodeUnexpectedRedirect || ae.Retryable ||
		!errors.Is(err, ErrUnexpectedRedirect) {
		t.Fatalf("want an UNEXPECTED_REDIRECT APIError with status %d, got %#v", status, err)
	}
}

// An https API answering a private GET with a 302 to plain http: the SDK must not follow it, so
// the credentials never reach the second server (in clear text or at all).
func TestRedirectHTTPSToHTTPIsNotFollowed(t *testing.T) {
	target, got := redirectTarget(t)
	api := &recorder{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.add(r)
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	c, err := New(Options{APIKey: testKey, APISecret: testSecret, BaseURL: srv.URL, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Account.Balances(context.Background())
	wantRedirectError(t, err, http.StatusFound)
	if got.count() != 0 {
		req, _ := got.get(0)
		t.Fatalf("redirect followed: the target got %s with X-API-Secret %q", req.URL, req.Header.Get("X-API-Secret"))
	}
	if api.count() != 1 {
		t.Fatalf("the API got %d requests, want 1 (a redirect is not retried)", api.count())
	}
	var ae *APIError
	errors.As(err, &ae)
	if !strings.HasPrefix(fmt.Sprint(ae.Details["location"]), target.URL) {
		t.Errorf("details.location = %v", ae.Details["location"])
	}
}

// A 307 on PlaceOrder: no re-POST to the target, no retry, and no client_order_id lookup (a
// redirect is not an ambiguous failure).
func TestRedirectOnPlaceOrderIsNotFollowedOrResent(t *testing.T) {
	target, got := redirectTarget(t)
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	})
	_, err := c.Trading.PlaceOrder(context.Background(), PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("0.1")), ClientOrderID: Ptr("mine-1")})
	wantRedirectError(t, err, http.StatusTemporaryRedirect)
	var unknown *OrderStateUnknownError
	if errors.As(err, &unknown) {
		t.Fatalf("a redirect must not be treated as ambiguous: %v", err)
	}
	if got.count() != 0 {
		t.Fatalf("the order was re-posted to the redirect target")
	}
	if rec.count() != 1 {
		t.Fatalf("the API got %d requests, want 1 (no retry, no lookup)", rec.count())
	}
}

// A caller-supplied http.Client keeps its own redirect policy; the SDK uses a copy that never
// follows redirects.
func TestCallerHTTPClientIsNotMutatedAndNotFollowed(t *testing.T) {
	target, got := redirectTarget(t)
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret, HTTPClient: &http.Client{}},
		func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL+r.URL.Path, http.StatusMovedPermanently)
		})
	_, err := c.Account.Balances(context.Background())
	wantRedirectError(t, err, http.StatusMovedPermanently)
	if got.count() != 0 || rec.count() != 1 {
		t.Fatalf("target got %d, API got %d", got.count(), rec.count())
	}

	mine := &http.Client{}
	if _, err := New(Options{HTTPClient: mine}); err != nil {
		t.Fatal(err)
	}
	if mine.CheckRedirect != nil {
		t.Fatal("New modified the caller's http.Client")
	}
	ws, err := NewWebSocket(WSOptions{HTTPClient: mine})
	if err != nil {
		t.Fatal(err)
	}
	if mine.CheckRedirect != nil || ws.opts.HTTPClient == mine || ws.opts.HTTPClient.CheckRedirect == nil {
		t.Fatal("NewWebSocket must use its own no-redirect copy of the caller's http.Client")
	}
}

// Credentials are only attached for the base URL's scheme and host.
func TestCredentialsOnlyForBaseOrigin(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv([]any{}))
	})
	c.t.baseURL = "http://127.0.0.1:1" // simulate a bug that builds a URL on another origin
	_, err := c.Account.Balances(context.Background())
	var ce *ConfigError
	if !errors.As(err, &ce) || !strings.Contains(err.Error(), "refusing to send credentials") || rec.count() != 0 {
		t.Fatalf("err %v, requests %d", err, rec.count())
	}
}

// The server may echo request values in details or fields; credentials are redacted there too.
func TestErrorDetailsAndFieldsAreRedacted(t *testing.T) {
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 400, map[string]any{"error": map[string]any{
			"code": "VALIDATION_ERROR", "message": "invalid",
			"details": map[string]any{"echo": "secret=" + testSecret, "n": 3},
			"fields":  map[string]string{"api_secret": "got " + testSecret},
		}})
	})
	_, err := c.Account.Balances(context.Background())
	var ae *APIError
	if !errors.As(err, &ae) {
		t.Fatalf("got %v", err)
	}
	if ae.Details["n"] != int64(3) || ae.Details["echo"] != "secret="+redacted || ae.Fields["api_secret"] != "got "+redacted {
		t.Fatalf("details %v fields %v", ae.Details, ae.Fields)
	}
	if out := fmt.Sprintf("%v %+v %#v", ae, ae, *ae); strings.Contains(out, testSecret) {
		t.Fatalf("secret leaked: %s", out)
	}
}

// The redacting methods have value receivers, so a copied authenticator redacts too.
func TestAuthenticatorValueRedacts(t *testing.T) {
	a, err := NewAPIKeyAuthenticator(testKey, testSecret)
	if err != nil {
		t.Fatal(err)
	}
	v := *a
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("x", "auth", v)
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("x", "auth", v)
	out := fmt.Sprintf("%v %+v %#v %s", v, v, v, v) + logs.String()
	if strings.Contains(out, testSecret) || !strings.Contains(out, redacted) {
		t.Fatalf("value not redacted: %s", out)
	}
}
