package cexy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Conformance: conformance/trading/batch_orders.json (PlaceOrders, CancelOrders,
// CancelOrderByClientID), run against a fake HTTP transport with a fake clock.

type batchExchange struct {
	NetworkError string            `json:"network_error"`
	HTTPStatus   int               `json:"http_status"`
	Data         map[string]any    `json:"data"`
	Error        map[string]any    `json:"error"`
	Headers      map[string]string `json:"headers"`
	EchoCids     bool              `json:"echo_cids"`
	Text         *string           `json:"text"` // a body that is not JSON, sent as is
	Body         json.RawMessage   `json:"body"` // a JSON body exactly as given, not wrapped
}

type batchCase struct {
	ID      string `json:"id"`
	Request struct {
		Symbol string            `json:"symbol"`
		Orders []json.RawMessage `json:"orders"`
	} `json:"request"`
	ClientOrderID string                   `json:"client_order_id"`
	Attempts      []batchExchange          `json:"attempts"`
	Lookups       map[string]batchExchange `json:"lookups"`
	Expect        struct {
		Calls               int              `json:"calls"`
		SameBody            bool             `json:"same_body"`
		Lookups             *[]string        `json:"lookups"`
		Hold                string           `json:"hold"`
		Results             []map[string]any `json:"results"`
		Placed              *int             `json:"placed"`
		Cancelled           *int             `json:"cancelled"`
		Failed              *int             `json:"failed"`
		Unknown             *int             `json:"unknown"`
		Error               string           `json:"error"`
		Cause               string           `json:"cause"`
		MaybePlaced         *bool            `json:"maybe_placed"`
		ErrorCids           string           `json:"error_cids"`
		ErrorCode           string           `json:"error_code"`
		ErrorDetailsLimit   any              `json:"error_details_limit"`
		GeneratedCidIndexes []int            `json:"generated_cid_indexes"`
		OrderID             string           `json:"order_id"`
		Status              string           `json:"status"`
	} `json:"expect"`
}

type batchFixture struct {
	OrderTemplate    map[string]any `json:"order_template"`
	Place            []batchCase    `json:"place"`
	Cancel           []batchCase    `json:"cancel"`
	CancelByClientID []batchCase    `json:"cancel_by_client_id"`
	Validation       []struct {
		ID      string `json:"id"`
		Call    string `json:"call"`
		Request struct {
			Symbol      string            `json:"symbol"`
			Orders      []json.RawMessage `json:"orders"`
			OrdersCount int               `json:"orders_count"`
		} `json:"request"`
	} `json:"validation"`
	Valid []struct {
		ID            string `json:"id"`
		Call          string `json:"call"`
		ClientOrderID string `json:"client_order_id"`
		OrdersCount   int    `json:"orders_count"`
	} `json:"valid"`
	CidPath []struct {
		ClientOrderID string `json:"client_order_id"`
		Path          string `json:"path"`
	} `json:"cid_path"`
	Cost struct {
		Calls map[string]struct {
			RequestTokens string `json:"request_tokens"`
			OrderActions  string `json:"order_actions"`
		} `json:"calls"`
	} `json:"cost"`
}

func loadBatchFixture(t *testing.T) batchFixture {
	t.Helper()
	var f batchFixture
	readJSON(t, filepath.Join(specDir(t), "conformance", "trading", "batch_orders.json"), &f)
	if len(f.Place) == 0 || len(f.Cancel) == 0 || len(f.CancelByClientID) == 0 || len(f.Validation) == 0 ||
		len(f.Valid) == 0 || len(f.CidPath) == 0 || len(f.Cost.Calls) == 0 {
		t.Fatal("conformance/trading/batch_orders.json: a section is empty")
	}
	return f
}

const (
	batchPath     = "/api/v1/trading/orders/batch"
	cancelPath    = "/api/v1/trading/orders/cancel-batch"
	byClientIDPfx = "/api/v1/trading/orders/by-client-id/"
	byIDPfx       = "/api/v1/trading/orders/"
)

// fakeBatchAPI answers the call under test from attempts and the lookups from lookups,
// recording both. It is an http.RoundTripper: a network_error is an error after sending.
type fakeBatchAPI struct {
	t        *testing.T
	tmpl     map[string]any
	method   string
	path     string
	attempts []batchExchange
	lookups  map[string]batchExchange
	// fallback answers any other request (tests outside the conformance cases).
	fallback func(*http.Request) (int, any)
	// answered, if set, runs before the call under test gets attempt n's answer (from 1).
	answered func(n int)

	mu      sync.Mutex
	bodies  [][]byte
	nonces  []string
	paths   []string
	seen    []string // lookup keys, in order
	reqs    []string // "METHOD path" of every request
	all     [][]byte // the body of every request
	unknown []string
}

func (f *fakeBatchAPI) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body)) // for fallback
	}
	path := req.URL.EscapedPath()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req.Method+" "+path)
	f.all = append(f.all, body)
	switch {
	case req.Method == f.method && path == f.path && f.attempts != nil:
		n := len(f.bodies)
		f.bodies = append(f.bodies, body)
		f.nonces = append(f.nonces, req.Header.Get("X-API-Nonce"))
		f.paths = append(f.paths, path)
		if n >= len(f.attempts) {
			f.unknown = append(f.unknown, fmt.Sprintf("attempt %d beyond the case", n+1))
			return response(req, 500, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": "x", "retryable": false}}), nil
		}
		item := f.attempts[n]
		if f.answered != nil {
			f.answered(n + 1)
		}
		var sent []string
		if item.EchoCids {
			var b struct {
				Orders []struct {
					ClientOrderID string `json:"client_order_id"`
				} `json:"orders"`
			}
			_ = json.Unmarshal(body, &b)
			for _, o := range b.Orders {
				sent = append(sent, o.ClientOrderID)
			}
		}
		return f.reply(req, item, false, sent)
	case req.Method == http.MethodGet && f.lookups != nil && strings.HasPrefix(path, byClientIDPfx):
		return f.lookup(req, "cid:"+unescape(path[len(byClientIDPfx):]))
	case req.Method == http.MethodGet && f.lookups != nil && strings.HasPrefix(path, byIDPfx):
		return f.lookup(req, "order:"+unescape(path[len(byIDPfx):]))
	}
	if f.fallback != nil {
		status, v := f.fallback(req)
		return response(req, status, v), nil
	}
	f.unknown = append(f.unknown, req.Method+" "+path)
	return response(req, 500, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": "x", "retryable": false}}), nil
}

func (f *fakeBatchAPI) lookup(req *http.Request, key string) (*http.Response, error) {
	f.seen = append(f.seen, key)
	item, ok := f.lookups[key]
	if !ok {
		f.unknown = append(f.unknown, "lookup "+key)
		return response(req, 404, map[string]any{"error": map[string]any{"code": "NOT_FOUND", "message": "x", "retryable": false}}), nil
	}
	return f.reply(req, item, true, nil)
}

func (f *fakeBatchAPI) reply(req *http.Request, item batchExchange, isLookup bool, sent []string) (*http.Response, error) {
	if item.NetworkError == "dial" { // never connected: nothing was sent
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	if item.NetworkError != "" {
		return nil, errors.New("lost response: " + item.NetworkError)
	}
	if item.Text != nil || item.Body != nil {
		b := []byte(item.Body)
		ct := "application/json"
		if item.Text != nil {
			b, ct = []byte(*item.Text), "text/html"
		}
		return &http.Response{StatusCode: item.HTTPStatus, Header: http.Header{"Content-Type": {ct}},
			Body: io.NopCloser(bytes.NewReader(b)), Request: req}, nil
	}
	if item.HTTPStatus == 200 {
		if isLookup {
			return response(req, 200, map[string]any{"data": overlay(f.tmpl, item.Data)}), nil
		}
		return response(req, 200, map[string]any{"data": withOrders(f.tmpl, item.Data, sent)}), nil
	}
	resp := response(req, item.HTTPStatus, map[string]any{"error": item.Error})
	for k, v := range item.Headers {
		resp.Header.Set(k, v)
	}
	return resp, nil
}

func response(req *http.Request, status int, v any) *http.Response {
	b, _ := json.Marshal(v)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(bytes.NewReader(b)), Request: req}
}

func unescape(s string) string {
	u, err := url.PathUnescape(s)
	if err != nil {
		return s
	}
	return u
}

func overlay(tmpl, stub map[string]any) map[string]any {
	out := make(map[string]any, len(tmpl)+len(stub))
	for k, v := range tmpl {
		out[k] = v
	}
	for k, v := range stub {
		out[k] = v
	}
	return out
}

// withOrders overlays every result's order on the template, and puts the sent client order
// ids in place of "<generated>".
func withOrders(tmpl, data map[string]any, sent []string) map[string]any {
	b, _ := json.Marshal(data)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	results, _ := out["results"].([]any)
	for _, r := range results {
		m := r.(map[string]any)
		if o, ok := m["order"].(map[string]any); ok {
			m["order"] = overlay(tmpl, o)
		}
		if m["client_order_id"] == "<generated>" {
			m["client_order_id"] = sent[int(m["index"].(float64))]
		}
	}
	return out
}

// newBatchClient returns a client whose requests go to api, with a fake clock and sleeps.
func newBatchClient(t *testing.T, api *fakeBatchAPI, opts Options) (*Client, *fakeSleep) {
	t.Helper()
	fs := &fakeSleep{now: time.Unix(1_800_000_000, 0)}
	opts.APIKey, opts.APISecret = testKey, testSecret
	opts.BaseURL = "https://api.test"
	opts.HTTPClient = &http.Client{Transport: api}
	opts.sleep = fs.sleep
	opts.now = fs.clock
	opts.random = func() float64 { return 0.5 }
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c, fs
}

func placeItems(t *testing.T, raw []json.RawMessage) []PlaceOrdersItem {
	t.Helper()
	items := make([]PlaceOrdersItem, len(raw))
	for i, r := range raw {
		if err := json.Unmarshal(r, &items[i]); err != nil {
			t.Fatal(err)
		}
	}
	return items
}

func cancelItems(t *testing.T, raw []json.RawMessage) []CancelBatchItem {
	t.Helper()
	items := make([]CancelBatchItem, len(raw))
	for i, r := range raw {
		if err := json.Unmarshal(r, &items[i]); err != nil {
			t.Fatal(err)
		}
	}
	return items
}

// checkBatchCommon checks calls, bodies, nonces, lookups, the hold and a returned error.
func checkBatchCommon(t *testing.T, cs batchCase, api *fakeBatchAPI, c *Client, err error) {
	t.Helper()
	exp := cs.Expect
	if len(api.unknown) > 0 {
		t.Errorf("unexpected requests: %v", api.unknown)
	}
	if len(api.bodies) != exp.Calls {
		t.Errorf("calls %d, want %d", len(api.bodies), exp.Calls)
	}
	if exp.SameBody {
		for i, b := range api.bodies {
			if !bytes.Equal(b, api.bodies[0]) {
				t.Errorf("attempt %d body differs:\n%s\n%s", i+1, b, api.bodies[0])
			}
		}
		// Every attempt is signed afresh.
		nonces := map[string]bool{}
		for _, n := range api.nonces {
			if n == "" || nonces[n] {
				t.Errorf("nonce %q missing or reused: %v", n, api.nonces)
			}
			nonces[n] = true
		}
	}
	if exp.Lookups != nil && !reflect.DeepEqual(append([]string{}, api.seen...), append([]string{}, *exp.Lookups...)) {
		t.Errorf("lookups %v, want %v", api.seen, *exp.Lookups)
	}
	if exp.Hold == "placements" {
		if c.t.placementsHeldUntil.IsZero() || !c.t.limiter.blockedUntil.IsZero() {
			t.Errorf("hold: placements until %v, client until %v", c.t.placementsHeldUntil, c.t.limiter.blockedUntil)
		}
	}
	switch {
	case exp.Error == "batch":
		var be *BatchPlaceError
		if !errors.As(err, &be) {
			t.Fatalf("err %v, want a *BatchPlaceError", err)
		}
		if exp.MaybePlaced == nil || be.MaybePlaced != *exp.MaybePlaced {
			t.Errorf("MaybePlaced %v, want %v", be.MaybePlaced, exp.MaybePlaced)
		}
		if exp.ErrorCids == "sent" && !reflect.DeepEqual(be.ClientOrderIDs, sentCids(t, api.bodies[0])) {
			t.Errorf("error cids %v, sent %v", be.ClientOrderIDs, sentCids(t, api.bodies[0]))
		}
		checkCause(t, err, exp.Cause)
		if exp.ErrorDetailsLimit != nil {
			var ae *APIError
			if !errors.As(err, &ae) || fmt.Sprint(ae.Details["limit"]) != fmt.Sprint(exp.ErrorDetailsLimit) {
				t.Errorf("details.limit %v, want %v", err, exp.ErrorDetailsLimit)
			}
		}
	case exp.Error == "connection":
		var ce *ConnectionError
		if !errors.As(err, &ce) {
			t.Errorf("err %v, want a *ConnectionError", err)
		}
	case exp.ErrorCode != "" && exp.Results == nil:
		var ae *APIError
		if !errors.As(err, &ae) || string(ae.Code) != exp.ErrorCode {
			t.Fatalf("err %v, want %s", err, exp.ErrorCode)
		}
		if exp.ErrorDetailsLimit != nil && fmt.Sprint(ae.Details["limit"]) != fmt.Sprint(exp.ErrorDetailsLimit) {
			t.Errorf("details.limit %v, want %v", ae.Details["limit"], exp.ErrorDetailsLimit)
		}
	default:
		if err != nil {
			t.Fatalf("err %v", err)
		}
	}
}

// checkCause maps a conformance `cause` onto Go: "connection" is a *ConnectionError,
// "bad_response" matches ErrBadResponse, anything else is the *APIError code (Go names an
// answer without an error envelope HTTP_<status>, as the conformance file does).
func checkCause(t *testing.T, err error, cause string) {
	t.Helper()
	var ce *ConnectionError
	var ae *APIError
	switch {
	case cause == "connection":
		if !errors.As(err, &ce) {
			t.Errorf("cause of %v, want a *ConnectionError", err)
		}
	case cause == "bad_response":
		if !errors.Is(err, ErrBadResponse) {
			t.Errorf("cause of %v, want ErrBadResponse", err)
		}
	case !errors.As(err, &ae) || string(ae.Code) != cause:
		t.Errorf("cause of %v, want %s", err, cause)
	}
}

func sentCids(t *testing.T, body []byte) []string {
	t.Helper()
	var b struct {
		Orders []struct {
			ClientOrderID string `json:"client_order_id"`
		} `json:"orders"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(b.Orders))
	for i, o := range b.Orders {
		out[i] = o.ClientOrderID
	}
	return out
}

func outcomeRow(index int, outcome BatchOutcome, orderID string, status OrderStatus, e *ErrorBody) map[string]any {
	row := map[string]any{"index": float64(index), "outcome": string(outcome)}
	switch outcome {
	case OutcomePlaced, OutcomeRecovered, OutcomeCancelled:
		row["order_id"], row["status"] = orderID, string(status)
	case OutcomeFailed:
		row["error_code"] = string(e.Code)
	}
	return row
}

func TestBatchPlaceConformance(t *testing.T) {
	f := loadBatchFixture(t)
	for _, cs := range f.Place {
		t.Run(cs.ID, func(t *testing.T) {
			api := &fakeBatchAPI{t: t, tmpl: f.OrderTemplate, method: http.MethodPost, path: batchPath,
				attempts: cs.Attempts, lookups: orEmpty(cs.Lookups)}
			c, _ := newBatchClient(t, api, Options{})
			res, err := c.Trading.PlaceOrders(context.Background(), cs.Request.Symbol, placeItems(t, cs.Request.Orders))
			checkBatchCommon(t, cs, api, c, err)
			exp := cs.Expect
			for _, i := range exp.GeneratedCidIndexes {
				sent := sentCids(t, api.bodies[0])[i]
				var got string
				var be *BatchPlaceError
				if errors.As(err, &be) {
					got = be.ClientOrderIDs[i]
				} else {
					got = res.Results[i].ClientOrderID
				}
				if !clientOrderIDRE.MatchString(sent) || got != sent {
					t.Errorf("generated cid %q, reported %q", sent, got)
				}
			}
			if exp.Results == nil {
				return
			}
			got := make([]map[string]any, 0, len(res.Results))
			for _, o := range res.Results {
				got = append(got, outcomeRow(o.Index, o.Outcome, o.OrderID, o.Status, o.Error))
			}
			if !reflect.DeepEqual(got, exp.Results) {
				t.Errorf("results\n got %v\nwant %v", got, exp.Results)
			}
			if res.Placed != *exp.Placed || res.Failed != *exp.Failed || res.Unknown != *exp.Unknown {
				t.Errorf("counts %d/%d/%d, want %d/%d/%d", res.Placed, res.Failed, res.Unknown, *exp.Placed, *exp.Failed, *exp.Unknown)
			}
		})
	}
}

func orEmpty(m map[string]batchExchange) map[string]batchExchange {
	if m == nil {
		return map[string]batchExchange{}
	}
	return m
}

func TestBatchCancelConformance(t *testing.T) {
	f := loadBatchFixture(t)
	for _, cs := range f.Cancel {
		t.Run(cs.ID, func(t *testing.T) {
			api := &fakeBatchAPI{t: t, tmpl: f.OrderTemplate, method: http.MethodPost, path: cancelPath,
				attempts: cs.Attempts, lookups: orEmpty(cs.Lookups)}
			c, _ := newBatchClient(t, api, Options{})
			res, err := c.Trading.CancelOrders(context.Background(), cancelItems(t, cs.Request.Orders))
			checkBatchCommon(t, cs, api, c, err)
			exp := cs.Expect
			got := make([]map[string]any, 0, len(res.Results))
			for _, o := range res.Results {
				got = append(got, outcomeRow(o.Index, o.Outcome, o.OrderID, o.Status, o.Error))
			}
			if !reflect.DeepEqual(got, exp.Results) {
				t.Errorf("results\n got %v\nwant %v", got, exp.Results)
			}
			if res.Cancelled != *exp.Cancelled || res.Failed != *exp.Failed || res.Unknown != *exp.Unknown {
				t.Errorf("counts %d/%d/%d, want %d/%d/%d", res.Cancelled, res.Failed, res.Unknown, *exp.Cancelled, *exp.Failed, *exp.Unknown)
			}
		})
	}
}

func TestCancelByClientIDConformance(t *testing.T) {
	f := loadBatchFixture(t)
	for _, cs := range f.CancelByClientID {
		t.Run(cs.ID, func(t *testing.T) {
			api := &fakeBatchAPI{t: t, tmpl: f.OrderTemplate, method: http.MethodDelete,
				path: byClientIDPfx + url.PathEscape(cs.ClientOrderID), attempts: cs.Attempts, lookups: orEmpty(cs.Lookups)}
			c, _ := newBatchClient(t, api, Options{})
			ord, err := c.Trading.CancelOrderByClientID(context.Background(), cs.ClientOrderID)
			exp := cs.Expect
			if len(api.unknown) > 0 {
				t.Errorf("unexpected requests: %v", api.unknown)
			}
			if len(api.bodies) != exp.Calls || !reflect.DeepEqual(append([]string{}, api.seen...), append([]string{}, *exp.Lookups...)) {
				t.Errorf("calls %d lookups %v, want %d %v", len(api.bodies), api.seen, exp.Calls, *exp.Lookups)
			}
			if exp.ErrorCode != "" {
				var ae *APIError
				if !errors.As(err, &ae) || string(ae.Code) != exp.ErrorCode {
					t.Fatalf("err %v, want %s", err, exp.ErrorCode)
				}
				return
			}
			if err != nil || ord.ID != exp.OrderID || string(ord.Status) != exp.Status {
				t.Fatalf("%s %s %v, want %s %s", ord.ID, ord.Status, err, exp.OrderID, exp.Status)
			}
		})
	}
}

func expandPlace(n int) []PlaceOrdersItem {
	out := make([]PlaceOrdersItem, n)
	for i := range out {
		out[i] = PlaceOrdersItem{Side: OrderSideBuy, Type: OrderTypeLimit, Price: Ptr(Amount("1")), Quantity: Ptr(Amount("1")),
			ClientOrderID: Ptr("c" + strconv.Itoa(i))}
	}
	return out
}

func expandCancel(n int) []CancelBatchItem {
	out := make([]CancelBatchItem, n)
	for i := range out {
		out[i] = CancelBatchItem{OrderID: Ptr("o" + strconv.Itoa(i))}
	}
	return out
}

// emptyBatches answers every batch item with success, echoing what was sent, and other calls
// with an order.
func emptyBatches(req *http.Request) (int, any) {
	var sent struct {
		Orders []map[string]any `json:"orders"`
	}
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		_ = json.Unmarshal(b, &sent)
	}
	results := make([]any, len(sent.Orders))
	switch req.URL.Path {
	case batchPath:
		for i, o := range sent.Orders {
			results[i] = map[string]any{"index": i, "client_order_id": o["client_order_id"], "fills": []any{},
				"order": overlay(testOrder, map[string]any{"id": fmt.Sprintf("o%d", i)}), "error": nil}
		}
		return 200, dataEnv(map[string]any{"placed": len(results), "failed": 0, "results": results})
	case cancelPath:
		for i, o := range sent.Orders {
			results[i] = map[string]any{"index": i, "order_id": o["order_id"], "client_order_id": o["client_order_id"],
				"order": overlay(testOrder, map[string]any{"status": "cancelled"}), "error": nil}
		}
		return 200, dataEnv(map[string]any{"cancelled": len(results), "failed": 0, "results": results})
	case "/api/v1/trading/orders":
		return 200, placeResponse()
	case "/api/v1/trading/orders/cancel-all":
		return 200, dataEnv(map[string]any{"cancelled": []any{}, "failed": []any{}, "has_more": false})
	}
	if req.Method == http.MethodDelete || req.Method == http.MethodGet {
		return 200, dataEnv(overlay(testOrder, map[string]any{"status": "cancelled"}))
	}
	return 500, apiErr("INTERNAL", "unexpected", false)
}

func TestBatchValidationRefusedLocally(t *testing.T) {
	f := loadBatchFixture(t)
	for _, cs := range f.Validation {
		t.Run(cs.ID, func(t *testing.T) {
			api := &fakeBatchAPI{t: t, fallback: emptyBatches}
			c, _ := newBatchClient(t, api, Options{})
			var err error
			n := cs.Request.OrdersCount
			if cs.Call == "place" {
				items := placeItems(t, cs.Request.Orders)
				if n > 0 {
					items = expandPlace(n)
				}
				_, err = c.Trading.PlaceOrders(context.Background(), cs.Request.Symbol, items)
			} else {
				items := cancelItems(t, cs.Request.Orders)
				if n > 0 {
					items = expandCancel(n)
				}
				_, err = c.Trading.CancelOrders(context.Background(), items)
			}
			var ce *ConfigError
			if !errors.As(err, &ce) || len(api.reqs) != 0 {
				t.Fatalf("err %v, requests %v", err, api.reqs)
			}
		})
	}
}

func TestBatchValidRequestsAreSent(t *testing.T) {
	f := loadBatchFixture(t)
	for _, cs := range f.Valid {
		t.Run(cs.ID, func(t *testing.T) {
			api := &fakeBatchAPI{t: t, fallback: emptyBatches}
			c, _ := newBatchClient(t, api, Options{})
			var err error
			switch {
			case cs.Call == "place" && cs.ClientOrderID != "":
				_, err = c.Trading.PlaceOrders(context.Background(), "BTC/USDT", []PlaceOrdersItem{{Side: OrderSideBuy,
					Type: OrderTypeMarket, Quantity: Ptr(Amount("1")), ClientOrderID: Ptr(cs.ClientOrderID)}})
			case cs.Call == "place":
				_, err = c.Trading.PlaceOrders(context.Background(), "BTC/USDT", expandPlace(cs.OrdersCount))
			default:
				_, err = c.Trading.CancelOrders(context.Background(), expandCancel(cs.OrdersCount))
			}
			if err != nil || len(api.reqs) != 1 {
				t.Fatalf("err %v, requests %v", err, api.reqs)
			}
		})
	}
}

func TestCancelByClientIDPathIsEncoded(t *testing.T) {
	f := loadBatchFixture(t)
	for _, cs := range f.CidPath {
		t.Run(cs.ClientOrderID, func(t *testing.T) {
			api := &fakeBatchAPI{t: t, fallback: emptyBatches}
			c, _ := newBatchClient(t, api, Options{})
			if _, err := c.Trading.CancelOrderByClientID(context.Background(), cs.ClientOrderID); err != nil {
				t.Fatal(err)
			}
			if want := "DELETE " + cs.Path; len(api.reqs) != 1 || api.reqs[0] != want {
				t.Fatalf("requests %v, want %s", api.reqs, want)
			}
		})
	}
}

// The cost table: request tokens from the client limiter, and order actions from the optional
// order budget, per call.
func TestBatchCostPerCall(t *testing.T) {
	f := loadBatchFixture(t)
	const n = 7
	api := &fakeBatchAPI{t: t, fallback: emptyBatches}
	c, _ := newBatchClient(t, api, Options{RequestsPerMinute: 600, OrderActionsPerSecond: 50})
	ctx := context.Background()
	place := make([]PlaceOrdersItem, n)
	for i := range place {
		place[i] = PlaceOrdersItem{Side: OrderSideBuy, Type: OrderTypeMarket, Quantity: Ptr(Amount("1"))}
	}
	calls := map[string]func() error{
		"place_order": func() error {
			_, err := c.Trading.PlaceOrder(ctx, PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy, Type: OrderTypeMarket,
				Quantity: Ptr(Amount("1"))})
			return err
		},
		"place_orders":  func() error { _, err := c.Trading.PlaceOrders(ctx, "BTC/USDT", place); return err },
		"cancel_order":  func() error { _, err := c.Trading.CancelOrder(ctx, "o1"); return err },
		"cancel_orders": func() error { _, err := c.Trading.CancelOrders(ctx, expandCancel(n)); return err },
		"cancel_order_by_client_id": func() error {
			_, err := c.Trading.CancelOrderByClientID(ctx, "c1")
			return err
		},
		"cancel_all": func() error { _, err := c.Trading.CancelAll(ctx, "BTC/USDT"); return err },
	}
	for name, want := range f.Cost.Calls {
		fn, ok := calls[name]
		if !ok {
			t.Errorf("cost table: no runner for %s", name)
			continue
		}
		tokens0, actions0 := c.t.limiter.state().Tokens, c.t.orderLimiter.state().Tokens
		if err := fn(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		tokens, actions := tokens0-c.t.limiter.state().Tokens, actions0-c.t.orderLimiter.state().Tokens
		wantTokens, _ := strconv.ParseFloat(strings.ReplaceAll(want.RequestTokens, "N", strconv.Itoa(n)), 64)
		wantActions, _ := strconv.ParseFloat(strings.ReplaceAll(want.OrderActions, "N", strconv.Itoa(n)), 64)
		if tokens != wantTokens || actions != wantActions {
			t.Errorf("%s: %v tokens and %v order actions, want %v and %v", name, tokens, actions, wantTokens, wantActions)
		}
	}
}

// An order-actions hold delays placements, not reads or cancels.
func TestOrderActionsHoldDelaysOnlyPlacements(t *testing.T) {
	api := &fakeBatchAPI{t: t, fallback: emptyBatches}
	c, fs := newBatchClient(t, api, Options{})
	ctx := context.Background()
	c.t.holdPlacements(5 * time.Second)
	if _, err := c.Trading.Order(ctx, "o1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Trading.CancelOrder(ctx, "o1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Trading.CancelOrders(ctx, expandCancel(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Trading.CancelOrderByClientID(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if w := fs.all(); len(w) != 0 {
		t.Fatalf("reads and cancels waited %v", w)
	}
	if _, err := c.Trading.PlaceOrders(ctx, "BTC/USDT", expandPlace(2)); err != nil {
		t.Fatal(err)
	}
	if w := fs.all(); len(w) != 1 || w[0] != 5*time.Second {
		t.Fatalf("placement waited %v, want [5s]", w)
	}
	c.t.holdPlacements(2 * time.Second)
	if _, err := c.Trading.PlaceOrder(ctx, PlaceOrderRequest{Symbol: "BTC/USDT", Side: OrderSideBuy, Type: OrderTypeMarket,
		Quantity: Ptr(Amount("1"))}); err != nil {
		t.Fatal(err)
	}
	if w := fs.all(); len(w) != 2 || w[1] != 2*time.Second {
		t.Fatalf("PlaceOrder waited %v, want a 2s hold", w)
	}
}

// A 429 for the request limit (no details.limit "order_actions") still holds the whole client.
func TestRequestLimit429StillHoldsEverything(t *testing.T) {
	var n int
	api := &fakeBatchAPI{t: t, fallback: func(req *http.Request) (int, any) {
		if n++; n == 1 {
			e := apiErr("RATE_LIMITED", "slow", true)
			e["error"].(map[string]any)["details"] = map[string]any{"retry_after_seconds": 3}
			return 429, e
		}
		return 200, dataEnv(testOrder)
	}}
	c, _ := newBatchClient(t, api, Options{})
	if _, err := c.Trading.Order(context.Background(), "o1"); err != nil {
		t.Fatal(err)
	}
	if c.t.limiter.blockedUntil.IsZero() || !c.t.placementsHeldUntil.IsZero() {
		t.Fatalf("client until %v, placements until %v", c.t.limiter.blockedUntil, c.t.placementsHeldUntil)
	}
}

// Every attempt of a batch is signed afresh (a new nonce) over the same body bytes.
func TestBatchEveryAttemptHasAFreshNonce(t *testing.T) {
	timeout := batchExchange{NetworkError: "timeout"}
	api := &fakeBatchAPI{t: t, method: http.MethodPost, path: batchPath, lookups: map[string]batchExchange{},
		attempts: []batchExchange{timeout, timeout, timeout, timeout}}
	c, _ := newBatchClient(t, api, Options{})
	_, err := c.Trading.PlaceOrders(context.Background(), "BTC/USDT", []PlaceOrdersItem{{Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("1"))}})
	var ce *ConnectionError
	if !errors.As(err, &ce) || len(api.nonces) != 4 {
		t.Fatalf("err %v, attempts %d", err, len(api.nonces))
	}
	seen := map[string]bool{}
	for i, nonce := range api.nonces {
		if nonce == "" || seen[nonce] || !bytes.Equal(api.bodies[i], api.bodies[0]) {
			t.Fatalf("attempt %d: nonce %q (seen %v) or body differs", i+1, nonce, seen[nonce])
		}
		seen[nonce] = true
	}
}

func TestPlaceOrdersWireBodyAndCallerSliceUntouched(t *testing.T) {
	api := &fakeBatchAPI{t: t, fallback: emptyBatches}
	c, _ := newBatchClient(t, api, Options{})
	orders := []PlaceOrdersItem{
		{Side: OrderSideSell, Type: OrderTypeLimit, Price: Ptr(Amount("100.50")), Quantity: Ptr(Amount("2")),
			TimeInForce: Ptr(TimeInForceGtc), ClientOrderID: Ptr("x1")},
		{Side: OrderSideBuy, Type: OrderTypeMarket, Quantity: Ptr(Amount("1"))},
	}
	if _, err := c.Trading.PlaceOrders(context.Background(), "BTC/USDT", orders); err != nil {
		t.Fatal(err)
	}
	if orders[1].ClientOrderID != nil {
		t.Fatal("the caller's slice was modified")
	}
	var sent map[string]any
	if len(api.all) != 1 || json.Unmarshal(api.all[0], &sent) != nil {
		t.Fatalf("bodies %q", api.all)
	}
	gen := sent["orders"].([]any)[1].(map[string]any)["client_order_id"].(string)
	want := map[string]any{"symbol": "BTC/USDT", "orders": []any{
		map[string]any{"side": "sell", "type": "limit", "price": "100.50", "quantity": "2", "time_in_force": "gtc", "client_order_id": "x1"},
		map[string]any{"side": "buy", "type": "market", "quantity": "1", "client_order_id": gen},
	}}
	if !reflect.DeepEqual(sent, want) || !clientOrderIDRE.MatchString(gen) {
		t.Fatalf("sent %s", api.all[0])
	}
}

func TestPlaceOrdersRejectsBadAmountsLocally(t *testing.T) {
	api := &fakeBatchAPI{t: t, fallback: emptyBatches}
	c, _ := newBatchClient(t, api, Options{})
	_, err := c.Trading.PlaceOrders(context.Background(), "BTC/USDT", []PlaceOrdersItem{{Side: OrderSideBuy,
		Type: OrderTypeMarket, Quantity: Ptr(Amount("1e3"))}})
	var ie *InvalidAmountError
	if !errors.As(err, &ie) || len(api.reqs) != 0 {
		t.Fatalf("err %v requests %v", err, api.reqs)
	}
}

func TestOrderActionsPerSecondMustBePositive(t *testing.T) {
	for _, bad := range []float64{-1, math.Inf(1), math.NaN()} {
		if _, err := New(Options{APIKey: testKey, APISecret: testSecret, OrderActionsPerSecond: bad}); err == nil {
			t.Errorf("OrderActionsPerSecond %v accepted", bad)
		}
	}
}

// Once the batch has answered, a lookup that cannot run (the context is done) makes that item
// unknown: no lookup is sent, the other results stand and the error is nil.
func TestBatchLookupAfterContextDoneIsUnknown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exists := func(cid string, details map[string]any) map[string]any {
		return map[string]any{"index": 0, "client_order_id": cid, "order": nil, "fills": []any{},
			"error": map[string]any{"code": "ALREADY_EXISTS", "message": "exists", "retryable": false, "details": details}}
	}
	r0 := exists("c0", map[string]any{"client_order_id": "c0"})
	r1 := exists("c1", map[string]any{"client_order_id": "c1", "order_id": "o2", "status": "open"})
	r1["index"] = 1
	r2 := map[string]any{"index": 2, "client_order_id": "c2", "order": map[string]any{"id": "o3", "status": "open"},
		"fills": []any{}, "error": nil}
	api := &fakeBatchAPI{t: t, tmpl: map[string]any{"symbol": "BTC/USDT", "side": "buy", "type": "limit"},
		method: http.MethodPost, path: batchPath, lookups: map[string]batchExchange{},
		attempts: []batchExchange{{NetworkError: "timeout"}, {HTTPStatus: 200,
			Data: map[string]any{"placed": 2, "failed": 1, "results": []any{r0, r1, r2}}}},
		answered: func(n int) {
			if n == 2 {
				cancel()
			}
		}}
	c, _ := newBatchClient(t, api, Options{})
	res, err := c.Trading.PlaceOrders(ctx, "BTC/USDT", expandPlace(3)[:3])
	if err != nil {
		t.Fatalf("err %v", err)
	}
	got := []BatchOutcome{res.Results[0].Outcome, res.Results[1].Outcome, res.Results[2].Outcome}
	if !reflect.DeepEqual(got, []BatchOutcome{OutcomeUnknown, OutcomeRecovered, OutcomePlaced}) || len(api.seen) != 0 ||
		res.Results[1].OrderID != "o2" || res.Results[2].OrderID != "o3" || res.Placed != 2 || res.Failed != 0 || res.Unknown != 1 {
		t.Fatalf("outcomes %v lookups %v result %+v", got, api.seen, res)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	invalid := map[string]any{"index": 0, "order_id": "o1", "client_order_id": nil, "order": nil,
		"error": map[string]any{"code": "INVALID_STATE", "message": "This order is no longer open", "retryable": false}}
	done := map[string]any{"index": 1, "order_id": "o2", "client_order_id": nil,
		"order": map[string]any{"id": "o2", "status": "cancelled"}, "error": nil}
	api2 := &fakeBatchAPI{t: t, tmpl: map[string]any{"symbol": "BTC/USDT"}, method: http.MethodPost, path: cancelPath,
		lookups: map[string]batchExchange{},
		attempts: []batchExchange{{HTTPStatus: 502, Error: map[string]any{"code": "SERVICE_UNAVAILABLE", "message": "x", "retryable": true}},
			{HTTPStatus: 200, Data: map[string]any{"cancelled": 1, "failed": 1, "results": []any{invalid, done}}}},
		answered: func(n int) {
			if n == 2 {
				cancel2()
			}
		}}
	c2, _ := newBatchClient(t, api2, Options{})
	cres, err := c2.Trading.CancelOrders(ctx2, expandCancel(3)[1:3])
	if err != nil {
		t.Fatalf("err %v", err)
	}
	if cres.Results[0].Outcome != OutcomeUnknown || cres.Results[1].Outcome != OutcomeCancelled || len(api2.seen) != 0 ||
		cres.Cancelled != 1 || cres.Unknown != 1 || cres.Failed != 0 {
		t.Fatalf("cancel result %+v lookups %v", cres, api2.seen)
	}
}

// Go counts a connection that was never made as a definite refusal (L-1): the error says
// nothing was placed, and a later ALREADY_EXISTS stays the caller's own.
func TestBatchNotSentIsARefusal(t *testing.T) {
	dial := batchExchange{NetworkError: "dial"}
	api := &fakeBatchAPI{t: t, method: http.MethodPost, path: batchPath, lookups: map[string]batchExchange{},
		attempts: []batchExchange{dial, dial, dial, dial}}
	c, _ := newBatchClient(t, api, Options{})
	_, err := c.Trading.PlaceOrders(context.Background(), "BTC/USDT", expandPlace(2))
	var be *BatchPlaceError
	var ce *ConnectionError
	if !errors.As(err, &be) || be.MaybePlaced || !errors.As(err, &ce) || len(api.bodies) != 4 ||
		!reflect.DeepEqual(be.ClientOrderIDs, []string{"c0", "c1"}) {
		t.Fatalf("err %v (%+v), attempts %d", err, be, len(api.bodies))
	}

	exists := map[string]any{"index": 0, "client_order_id": "c0", "order": nil, "fills": []any{},
		"error": map[string]any{"code": "ALREADY_EXISTS", "message": "exists", "retryable": false,
			"details": map[string]any{"client_order_id": "c0", "order_id": "o_old", "status": "filled"}}}
	api2 := &fakeBatchAPI{t: t, method: http.MethodPost, path: batchPath, lookups: map[string]batchExchange{},
		attempts: []batchExchange{dial, {HTTPStatus: 200, Data: map[string]any{"placed": 0, "failed": 1, "results": []any{exists}}}}}
	c2, _ := newBatchClient(t, api2, Options{})
	res, err := c2.Trading.PlaceOrders(context.Background(), "BTC/USDT", expandPlace(1))
	if err != nil || res.Results[0].Outcome != OutcomeFailed || res.Failed != 1 {
		t.Fatalf("%+v %v", res, err)
	}
}

// A context cancelled while the batch is in flight: the error carries the ids, says the orders
// may have been placed, and still matches context.Canceled.
func TestBatchCancelledContextCarriesIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api := &fakeBatchAPI{t: t, method: http.MethodPost, path: batchPath, lookups: map[string]batchExchange{},
		attempts: []batchExchange{{NetworkError: "timeout"}}, answered: func(int) { cancel() }}
	c, _ := newBatchClient(t, api, Options{})
	orders := expandPlace(2)
	orders[0].ClientOrderID = nil
	_, err := c.Trading.PlaceOrders(ctx, "BTC/USDT", orders)
	var be *BatchPlaceError
	if !errors.As(err, &be) || !be.MaybePlaced || !errors.Is(err, context.Canceled) ||
		!reflect.DeepEqual(be.ClientOrderIDs, sentCids(t, api.bodies[0])) || be.ClientOrderIDs[1] != "c1" {
		t.Fatalf("err %v (%+v)", err, be)
	}
}

func TestNewClientOrderID(t *testing.T) {
	a, b := NewClientOrderID(), NewClientOrderID()
	if a == b || !clientOrderIDRE.MatchString(a) || checkClientOrderID(a, "x") != nil {
		t.Fatalf("%q %q", a, b)
	}
}

// A cancel-batch 200 whose results do not match the entries sent is ErrBadResponse.
func TestCancelOrdersMismatchIsBadResponse(t *testing.T) {
	short := map[string]any{"cancelled": 1, "failed": 0, "results": []any{map[string]any{"index": 0, "order_id": "o0",
		"client_order_id": nil, "order": map[string]any{"id": "o0", "status": "cancelled"}, "error": nil}}}
	swapped := map[string]any{"cancelled": 2, "failed": 0, "results": []any{
		map[string]any{"index": 1, "order_id": "o1", "order": map[string]any{"id": "o1", "status": "cancelled"}, "error": nil},
		map[string]any{"index": 0, "order_id": "o0", "order": map[string]any{"id": "o0", "status": "cancelled"}, "error": nil}}}
	for name, data := range map[string]map[string]any{"count": short, "index": swapped} {
		api := &fakeBatchAPI{t: t, tmpl: map[string]any{"symbol": "BTC/USDT"}, method: http.MethodPost, path: cancelPath,
			lookups: map[string]batchExchange{}, attempts: []batchExchange{{HTTPStatus: 200, Data: data}}}
		c, _ := newBatchClient(t, api, Options{})
		_, err := c.Trading.CancelOrders(context.Background(), expandCancel(2))
		if !errors.Is(err, ErrBadResponse) || len(api.bodies) != 1 {
			t.Errorf("%s: err %v, calls %d", name, err, len(api.bodies))
		}
	}
}

type oddError struct{}

func (oddError) Error() string { return "something the SDK does not know" }

// oddTransport fails every request with an error type the SDK does not recognise.
type oddTransport struct{ calls int }

func (o *oddTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	o.calls++
	if req.Body != nil {
		req.Body.Close()
	}
	return nil, oddError{}
}

type failingAuth struct{}

func (failingAuth) Kind() string                             { return "failing" }
func (failingAuth) Authenticate(*http.Request, []byte) error { return oddError{} }
func (failingAuth) Redact(s string) string                   { return s }

// An unrecognised failure is "maybe placed"; only an error before anything is sent (here a
// credential error) is not.
func TestBatchUnrecognisedErrorMaybePlaced(t *testing.T) {
	if classifyAttempt(oddError{}, true) != attemptAmbiguous || classifyAttempt(oddError{}, false) != attemptRefused {
		t.Fatal("classifyAttempt: an unrecognised error after sending must be ambiguous")
	}
	rt := &oddTransport{}
	fs := &fakeSleep{now: time.Unix(1_800_000_000, 0)}
	c, err := New(Options{APIKey: testKey, APISecret: testSecret, BaseURL: "https://api.test", HTTPClient: &http.Client{Transport: rt},
		sleep: fs.sleep, now: fs.clock, random: func() float64 { return 0.5 }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Trading.PlaceOrders(context.Background(), "BTC/USDT", expandPlace(2))
	var be *BatchPlaceError
	if !errors.As(err, &be) || !be.MaybePlaced || rt.calls != 4 || !reflect.DeepEqual(be.ClientOrderIDs, []string{"c0", "c1"}) {
		t.Fatalf("err %v (%+v), calls %d", err, be, rt.calls)
	}

	rt2 := &oddTransport{}
	c2, err := New(Options{Authenticator: failingAuth{}, BaseURL: "https://api.test", HTTPClient: &http.Client{Transport: rt2},
		sleep: fs.sleep, now: fs.clock})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c2.Trading.PlaceOrders(context.Background(), "BTC/USDT", expandPlace(1))
	if !errors.As(err, &be) || be.MaybePlaced || rt2.calls != 0 || !errors.As(err, new(oddError)) {
		t.Fatalf("credential error: %v (%+v), calls %d", err, be, rt2.calls)
	}
}
