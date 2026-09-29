package cexy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestNewRejectsBadConfig(t *testing.T) {
	cases := []Options{
		{APIKey: testKey},                                        // half pair
		{APISecret: testSecret},                                  // half pair
		{BaseURL: "http://api.example.com"},                      // plain text
		{BaseURL: "http://api.example.com", AllowInsecure: true}, // plain text to a remote host
		{BaseURL: "ftp://api.example.com"},
		{BaseURL: "https://user:pw@api.example.com"},
		{BaseURL: "https://api.example.com?x=1"},
		{APIKey: "ak_x\n", APISecret: "s"},
		{APIKey: testKey, APISecret: testSecret, Authenticator: &APIKeyAuthenticator{key: "a", secret: "b"}},
		{MaxRetries: -1},
		{RequestsPerMinute: -5},
	}
	for i, o := range cases {
		if _, err := New(o); err == nil {
			t.Errorf("case %d: expected a config error", i)
		} else if ce := new(ConfigError); !errors.As(err, &ce) {
			t.Errorf("case %d: got %T, want *ConfigError", i, err)
		}
	}
	for _, host := range []string{"http://localhost", "http://127.0.0.1", "http://[::1]"} {
		if _, err := New(Options{BaseURL: host + ":9", AllowInsecure: true}); err != nil {
			t.Errorf("%s: %v", host, err)
		}
	}
}

// Conformance: auth.json (half_pair_rejected_locally sends nothing).
func TestHalfPairSendsNothing(t *testing.T) {
	calls := 0
	_, _, _ = newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) { calls++ })
	if _, err := New(Options{APIKey: testKey}); err == nil {
		t.Fatal("expected an error")
	}
	if calls != 0 {
		t.Fatalf("requests sent: %d", calls)
	}
}

func TestAuthConformance(t *testing.T) {
	dir := specDir(t)
	var spec struct {
		Cases []struct {
			ID      string `json:"id"`
			Request *struct {
				Operation string `json:"operation"`
			} `json:"request"`
			Expect struct {
				HeadersPresent []string          `json:"headers_present"`
				HeadersAbsent  []string          `json:"headers_absent"`
				URLMustNot     []string          `json:"url_must_not_contain"`
				HeaderMatches  map[string]string `json:"header_matches"`
			} `json:"expect"`
		} `json:"cases"`
		ServerResponses []struct {
			ID     string         `json:"id"`
			Status int            `json:"status"`
			Body   map[string]any `json:"body"`
			Expect struct {
				ErrorClass string `json:"error_class"`
			} `json:"expect"`
		} `json:"server_responses"`
	}
	readJSON(t, filepath.Join(dir, "conformance", "auth.json"), &spec)

	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/time":
			writeJSON(w, 200, dataEnv(map[string]any{"epoch_ms": 1, "iso": "2026-09-27T10:00:00Z"}))
		default:
			writeJSON(w, 200, dataEnv([]any{}))
		}
	})
	ctx := context.Background()
	run := map[string]func() error{
		"GET /api/v1/account/balances": func() error { _, err := c.Account.Balances(ctx); return err },
		"GET /api/v1/markets":          func() error { _, err := c.Markets.List(ctx); return err },
		"GET /api/v1/time":             func() error { _, err := c.Time(ctx); return err },
	}
	for _, tc := range spec.Cases {
		if tc.Request == nil {
			continue // client-construction cases are covered above
		}
		t.Run(tc.ID, func(t *testing.T) {
			f, ok := run[tc.Request.Operation]
			if !ok {
				t.Fatalf("no runner for %s", tc.Request.Operation)
			}
			n := rec.count()
			if err := f(); err != nil {
				t.Fatal(err)
			}
			req, _ := rec.get(n)
			for _, h := range tc.Expect.HeadersPresent {
				if req.Header.Get(h) == "" {
					t.Errorf("missing header %s", h)
				}
			}
			for _, h := range tc.Expect.HeadersAbsent {
				if req.Header.Get(h) != "" {
					t.Errorf("unexpected header %s", h)
				}
			}
			for _, s := range tc.Expect.URLMustNot {
				if strings.Contains(req.URL.String(), s) {
					t.Errorf("URL contains %s", s)
				}
			}
			for h, pattern := range tc.Expect.HeaderMatches {
				if !regexp.MustCompile(pattern).MatchString(req.Header.Get(h)) {
					t.Errorf("%s = %q does not match %s", h, req.Header.Get(h), pattern)
				}
			}
		})
	}

	classes := map[string]error{"AuthenticationError": ErrAuthentication, "PermissionError": ErrForbidden}
	for _, sr := range spec.ServerResponses {
		t.Run(sr.ID, func(t *testing.T) {
			c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret},
				func(w http.ResponseWriter, r *http.Request) { writeJSON(w, sr.Status, sr.Body) })
			_, err := c.Account.Balances(ctx)
			if want := classes[sr.Expect.ErrorClass]; !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
		})
	}
}

func TestRedaction(t *testing.T) {
	c, _, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		// A server echoing the credentials back in its message must not leak them.
		writeJSON(w, 401, apiErr("INVALID_CREDENTIALS", "bad key "+testKey+" / "+testSecret, false))
	})
	_, err := c.Account.Balances(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	auth := c.t.auth.(*APIKeyAuthenticator)
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("x", "client", c, "auth", auth, "err", err)
	outputs := []string{
		err.Error(), fmt.Sprint(c), fmt.Sprintf("%v %+v %#v", c, c, c), fmt.Sprintf("%v %+v %#v", auth, auth, auth),
		fmt.Sprintf("%+v", *c), logs.String(),
		fmt.Sprintf("%v %+v %#v", Options{APIKey: testKey, APISecret: testSecret}, Options{APISecret: testSecret},
			&Options{APISecret: testSecret}),
	}
	for _, out := range outputs {
		if strings.Contains(out, testSecret) {
			t.Errorf("secret leaked: %s", out)
		}
	}
	if !strings.Contains(err.Error(), redacted) {
		t.Errorf("message not redacted: %s", err)
	}
}

func TestUserAgent(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{UserAgentSuffix: "my-bot/1.2"}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv(map[string]any{"epoch_ms": 1, "iso": "2026-09-27T10:00:00Z"}))
	})
	if _, err := c.Time(context.Background()); err != nil {
		t.Fatal(err)
	}
	req, _ := rec.get(0)
	if got, want := req.Header.Get("User-Agent"), "cexy-go/"+Version+" my-bot/1.2"; got != want {
		t.Fatalf("User-Agent %q, want %q", got, want)
	}
}

func TestPrivateCallWithoutKeySendsNothing(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {})
	_, err := c.Account.Balances(context.Background())
	var ce *ConfigError
	if !errors.As(err, &ce) || rec.count() != 0 {
		t.Fatalf("err %v, requests %d", err, rec.count())
	}
}

func TestPathParamsAreEscaped(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv(map[string]any{}))
	})
	if _, err := c.Markets.Get(context.Background(), "BTC/USDT"); err != nil {
		t.Fatal(err)
	}
	req, _ := rec.get(0)
	if req.URL.EscapedPath() != "/api/v1/markets/BTC%2FUSDT" {
		t.Fatalf("path %s", req.URL.EscapedPath())
	}
	if _, err := c.Markets.Get(context.Background(), ""); err == nil {
		t.Fatal("empty symbol accepted")
	}
}

func TestQueryEncoding(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv([]any{}))
	})
	_, err := c.Markets.Candles(context.Background(), "BTC/USDT", GetCandlesParams{Interval: CandleInterval1h, Limit: Ptr(10)})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := rec.get(0)
	if q := req.URL.Query(); q.Get("interval") != "1h" || q.Get("limit") != "10" || q.Has("start_time") {
		t.Fatalf("query %s", req.URL.RawQuery)
	}
	if _, err := c.Markets.Candles(context.Background(), "BTC/USDT", GetCandlesParams{}); err == nil {
		t.Fatal("missing interval accepted")
	}
}

func TestExportsReturnText(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: testKey, APISecret: testSecret}, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("id,amount\n1,0.5\n"))
	})
	csv, err := c.Exports.Deposits(context.Background(), nil)
	if err != nil || csv != "id,amount\n1,0.5\n" {
		t.Fatalf("%q %v", csv, err)
	}
	req, _ := rec.get(0)
	if !strings.Contains(req.Header.Get("Accept"), "text/csv") {
		t.Fatalf("Accept %q", req.Header.Get("Accept"))
	}
}

// The operation table must be exactly the SDK spec, with auth from surface.yaml.
func TestOperationsMatchSpec(t *testing.T) {
	dir := specDir(t)
	var spec struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
			Security    []map[string][]string
		} `json:"paths"`
	}
	readJSON(t, filepath.Join(dir, "spec", "openapi.sdk.json"), &spec)
	seen := 0
	for path, item := range spec.Paths {
		for method, op := range item {
			info, ok := operations[OperationID(op.OperationID)]
			if !ok {
				t.Errorf("%s missing", op.OperationID)
				continue
			}
			seen++
			if info.Path != path || info.Method != strings.ToUpper(method) {
				t.Errorf("%s: %s %s != %s %s", op.OperationID, info.Method, info.Path, method, path)
			}
		}
	}
	if seen != len(operations) || seen != 41 {
		t.Fatalf("spec has %d operations, table %d (want 41)", seen, len(operations))
	}
}

func TestVersionInChangelog(t *testing.T) {
	b, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), Version) {
		t.Fatalf("CHANGELOG.md does not mention %s", Version)
	}
}
