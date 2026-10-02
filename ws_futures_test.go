package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

type futuresWSSpec struct {
	Frames []struct {
		ID     string         `json:"id"`
		Frame  map[string]any `json:"frame"`
		Expect map[string]any `json:"expect"`
	} `json:"frames"`
	ChannelNames struct {
		Valid   [][]json.RawMessage `json:"valid"`
		Invalid [][]json.RawMessage `json:"invalid"`
	} `json:"channel_names"`
	Scenarios []struct {
		ID    string `json:"id"`
		Steps []struct {
			Client   string         `json:"client"`
			Channels []string       `json:"channels"`
			Server   map[string]any `json:"server"`
			ReplyTo  string         `json:"server_reply_to"`
			Frame    map[string]any `json:"frame"`
		} `json:"steps"`
		Expect map[string]json.RawMessage `json:"expect"`
	} `json:"scenarios"`
	PingMax int `json:"ping_interval_max_seconds"`
}

func readFuturesWSSpec(t *testing.T) futuresWSSpec {
	var spec futuresWSSpec
	readJSON(t, filepath.Join(specDir(t), "conformance", "ws", "futures.json"), &spec)
	if len(spec.Frames) == 0 || len(spec.Scenarios) == 0 || len(spec.ChannelNames.Valid) == 0 {
		t.Fatal("conformance/ws/futures.json: empty")
	}
	return spec
}

// futuresWSRun is a WebSocket on a scripted server, recording what the handlers saw.
type futuresWSRun struct {
	t       *testing.T
	s       *scriptedWS
	ws      *WebSocket
	mu      sync.Mutex
	events  []Event
	resyncs []string
	errors  []string
	errs    []error
}

func newFuturesWSRun(t *testing.T) *futuresWSRun {
	r := &futuresWSRun{t: t, s: &scriptedWS{challenge: "ch1"}}
	srv := httptest.NewServer(http.HandlerFunc(r.s.handler))
	t.Cleanup(srv.Close)
	signer, err := NewHMACAuthenticator(testKey, testSecret)
	if err != nil {
		t.Fatal(err)
	}
	r.ws, err = NewWebSocket(WSOptions{
		URL: "ws" + srv.URL[len("http"):] + "/api/v1/ws", AllowInsecure: true, NoReconnect: true, AckTimeout: 2 * time.Second,
		KeySigner: signer,
		Handlers: WSHandlers{
			OnEvent: func(e Event) { r.mu.Lock(); r.events = append(r.events, e); r.mu.Unlock() },
			OnFuturesResync: func(c string) {
				r.mu.Lock()
				r.resyncs = append(r.resyncs, c)
				r.mu.Unlock()
			},
			OnServerError: func(e *WSError) { r.mu.Lock(); r.errors = append(r.errors, e.Code); r.mu.Unlock() },
			OnError:       func(e error) { r.mu.Lock(); r.errs = append(r.errs, e); r.mu.Unlock() },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.ws.Close() })
	if _, err := r.ws.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

// settle: two ping round trips and a drained dispatcher, so every earlier frame was handled.
func (r *futuresWSRun) settle() {
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.ws.Ping(ctx); err != nil {
			r.t.Fatal(err)
		}
	}
	done := make(chan struct{})
	r.ws.dispatch(func() { close(done) })
	<-done
}

func (r *futuresWSRun) lastEvent() Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) == 0 {
		r.t.Fatal("no event")
	}
	return r.events[len(r.events)-1]
}

func TestWSFuturesChannelNames(t *testing.T) {
	spec := readFuturesWSSpec(t)
	build := func(kind string, args []string) (string, error) {
		switch kind {
		case "mids":
			return FuturesMidsChannel, nil
		case "status":
			return FuturesStatusChannel, nil
		case "account":
			return FuturesAccountChannel, nil
		case "orderbook":
			return FuturesOrderBookChannel(args[0])
		case "trades":
			return FuturesTradesChannel(args[0])
		case "candles":
			return FuturesCandlesChannel(args[0], args[1])
		}
		t.Fatalf("unknown kind %s", kind)
		return "", nil
	}
	parse := func(row []json.RawMessage) (string, []string) {
		var kind string
		var args []string
		_ = json.Unmarshal(row[0], &kind)
		_ = json.Unmarshal(row[1], &args)
		return kind, args
	}
	for _, row := range spec.ChannelNames.Valid {
		kind, args := parse(row)
		var want string
		_ = json.Unmarshal(row[2], &want)
		if got, err := build(kind, args); err != nil || got != want {
			t.Errorf("%s %v: %q %v, want %q", kind, args, got, err, want)
		}
	}
	for _, row := range spec.ChannelNames.Invalid {
		kind, args := parse(row)
		var ce *ConfigError
		if got, err := build(kind, args); !errors.As(err, &ce) || got != "" {
			t.Errorf("%s %v: %q %v, want a ConfigError", kind, args, got, err)
		}
	}
	if !PrivateChannels[FuturesAccountChannel] || PrivateChannels[FuturesMidsChannel] {
		t.Fatal("PrivateChannels")
	}
}

func TestWSFuturesFrames(t *testing.T) {
	spec := readFuturesWSSpec(t)
	r := newFuturesWSRun(t)
	for _, f := range spec.Frames {
		t.Run(f.ID, func(t *testing.T) {
			r.s.write(f.Frame)
			r.settle()
			ev := r.lastEvent()
			x := f.Expect
			if ev.Type != x["event_type"] || ev.Channel != x["channel"] || ev.Sequence == nil || *ev.Sequence != int64(f.Frame["sequence"].(float64)) {
				t.Fatalf("event %s %s %v", ev.Type, ev.Channel, ev.Sequence)
			}
			if seq, ok := x["sequence"].(float64); ok && *ev.Sequence != int64(seq) {
				t.Fatalf("sequence %d", *ev.Sequence)
			}
			check := func(ok bool) {
				if !ok {
					t.Fatalf("decoded data does not match %v: %s", x, ev.Data)
				}
			}
			switch ev.Type {
			case "futures.mids":
				var d FuturesMids
				check(ev.Decode(&d) == nil && d.Mids["kPEPE"] == "0.009871" && !d.AsOf.IsZero())
			case "futures.orderbook.update":
				var d FuturesBookUpdate
				check(ev.Decode(&d) == nil && d.Full && len(d.Bids) > 0 && len(d.Asks) > 0)
				bb, ba := x["best_bid"].([]any), x["best_ask"].([]any)
				check(string(d.Bids[0].Price) == bb[0] && string(d.Bids[0].Size) == bb[1] &&
					string(d.Asks[0].Price) == ba[0] && string(d.Asks[0].Size) == ba[1])
			case "futures.trades.new":
				var d FuturesTradesUpdate
				check(ev.Decode(&d) == nil && len(d.Trades) == int(x["trade_count"].(float64)) && d.Trades[0].Time > 0)
			case "futures.candle.update":
				var d FuturesCandleUpdate
				check(ev.Decode(&d) == nil && d.Candle.OpenTime == int64(x["open_time"].(float64)) && d.Candle.Trades == 412)
			case "futures.status":
				var d FuturesStatus
				check(ev.Decode(&d) == nil && d.State == x["state"] && !d.Since.IsZero())
			case "futures.positions":
				var d FuturesPositionsUpdate
				check(ev.Decode(&d) == nil && len(d.Positions.Positions) == int(x["position_count"].(float64)) &&
					d.Stale == x["stale"] && d.Positions.Positions[0].Leverage == 10)
			case "futures.orders":
				var d FuturesOrdersUpdate
				check(ev.Decode(&d) == nil && len(d.Orders) == int(x["order_count"].(float64)) && d.Orders[0].TriggerPrice == nil)
			default:
				t.Fatalf("no check for %s", ev.Type)
			}
		})
	}
}

var replyOp = map[string]string{"subscribed": "subscribe", "unsubscribed": "unsubscribe", "authenticated": "auth_key"}

func TestWSFuturesScenarios(t *testing.T) {
	spec := readFuturesWSSpec(t)
	for _, sc := range spec.Scenarios {
		t.Run(sc.ID, func(t *testing.T) {
			r := newFuturesWSRun(t)
			ctx := context.Background()
			answered := map[any]bool{}
			// unanswered waits for the client's most recent unanswered request of op.
			unanswered := func(op string) map[string]any {
				var req map[string]any
				eventually(t, "a "+op+" request", func() bool {
					reqs := r.s.requests()
					for j := len(reqs) - 1; j >= 0; j-- {
						if reqs[j]["op"] == op && !answered[reqs[j]["id"]] {
							req = reqs[j]
							return true
						}
					}
					return false
				})
				answered[req["id"]] = true
				return req
			}
			authenticated := false
			mark := 0
			for i, st := range sc.Steps {
				switch {
				case st.Client == "subscribe":
					before := len(r.s.requests())
					go func() { _, _ = r.ws.Subscribe(ctx, st.Channels...) }()
					heldBack := !authenticated && slices.Equal(st.Channels, []string{FuturesAccountChannel})
					if !heldBack {
						eventually(t, "subscribe frame", func() bool { return len(r.s.requests()) > before })
					}
					r.settle()
					mark = len(r.s.requests())
				case st.Client == "auth_key":
					before := len(r.s.requests())
					go func() { _, _ = r.ws.AuthKey(ctx) }()
					eventually(t, "auth_key frame", func() bool { return len(r.s.requests()) > before })
					r.settle()
					mark = len(r.s.requests())
				case st.Server != nil || st.Frame != nil:
					frame := map[string]any{}
					src, op := st.Frame, st.ReplyTo
					if st.Server != nil {
						typ, _ := st.Server["type"].(string)
						src, op = st.Server, replyOp[typ] // an ack: it carries the request's id
					}
					for k, v := range src {
						frame[k] = v
					}
					if op != "" {
						frame["id"] = unanswered(op)["id"]
					}
					if frame["type"] == "authenticated" {
						authenticated = true
					}
					r.s.write(frame)
					r.settle()
				default:
					t.Fatalf("step %d: unknown step", i)
				}
			}
			r.settle()
			x := sc.Expect
			strs := func(key string) []string {
				var v []string
				if err := json.Unmarshal(x[key], &v); err != nil {
					t.Fatalf("%s: %v", key, err)
				}
				if v == nil {
					v = []string{}
				}
				return v
			}
			if _, ok := x["client_sends_after"]; ok {
				var want []map[string]any
				_ = json.Unmarshal(x["client_sends_after"], &want)
				eventually(t, "client frames", func() bool { return len(r.s.requests())-mark >= len(want) })
				r.settle()
				got := []map[string]any{}
				for _, m := range r.s.requests()[mark:] {
					got = append(got, normSent(m))
				}
				wantN := []map[string]any{}
				for _, m := range want {
					wantN = append(wantN, normSent(m))
				}
				if !reflect.DeepEqual(got, wantN) {
					t.Fatalf("client sent %v, want %v", got, wantN)
				}
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if _, ok := x["events"]; ok {
				got := []string{}
				for _, e := range r.events {
					got = append(got, e.Type)
				}
				if want := strs("events"); !slices.Equal(got, want) {
					t.Errorf("events %v, want %v", got, want)
				}
			}
			if _, ok := x["resync_channels"]; ok {
				if want := strs("resync_channels"); !slices.Equal(append([]string{}, r.resyncs...), want) {
					t.Errorf("resyncs %v, want %v", r.resyncs, want)
				}
			}
			if _, ok := x["errors"]; ok {
				if want := strs("errors"); !slices.Equal(append([]string{}, r.errors...), want) {
					t.Errorf("errors %v, want %v", r.errors, want)
				}
			}
			if _, ok := x["held_channels_after"]; ok {
				if got, want := sortedStrings(r.ws.Channels()), sortedStrings(strs("held_channels_after")); !slices.Equal(got, want) {
					t.Errorf("held %v, want %v", got, want)
				}
			}
			if _, ok := x["held_pending_private"]; ok {
				if got, want := sortedStrings(r.ws.PendingChannels()), sortedStrings(strs("held_pending_private")); !slices.Equal(got, want) {
					t.Errorf("pending %v, want %v", got, want)
				}
				for _, f := range r.s.requests() {
					if f["op"] == "subscribe" {
						t.Errorf("subscribe sent before auth: %v", f)
					}
				}
			}
		})
	}
	if spec.PingMax <= 0 {
		t.Fatal("ping_interval_max_seconds missing")
	}
}

// After a held futures.account, a successful AuthKey subscribes it.
func TestWSFuturesAccountSubscribedAfterAuth(t *testing.T) {
	r := newFuturesWSRun(t)
	ctx := context.Background()
	if _, err := r.ws.Subscribe(ctx, FuturesAccountChannel); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = r.ws.AuthKey(ctx) }()
	eventually(t, "auth_key", func() bool { return len(r.s.requests()) == 1 })
	r.s.write(map[string]any{"type": "authenticated", "user_id": "u1", "auth": "api_key", "id": r.s.requests()[0]["id"]})
	eventually(t, "subscribe", func() bool { return len(r.s.requests()) == 2 })
	sub := r.s.requests()[1]
	if sub["op"] != "subscribe" || !slices.Equal(stringList(sub["channels"]), []string{FuturesAccountChannel}) {
		t.Fatalf("sent %v", sub)
	}
	r.s.write(map[string]any{"type": "subscribed", "channels": []string{FuturesAccountChannel}, "id": sub["id"]})
	r.settle()
	if got := r.ws.Channels(); !slices.Equal(got, []string{FuturesAccountChannel}) || len(r.ws.PendingChannels()) != 0 {
		t.Fatalf("held %v pending %v", got, r.ws.PendingChannels())
	}
}

// The server sends one error frame per refused channel BEFORE the single subscribed ack, and
// no ack when it accepted nothing.
func TestWSSubscribePartlyRefused(t *testing.T) {
	r := newFuturesWSRun(t)
	ctx := context.Background()
	type result struct {
		res SubscribeResult
		err error
	}
	subscribe := func(channels ...string) chan result {
		done := make(chan result, 1)
		before := len(r.s.requests())
		go func() {
			res, err := r.ws.Subscribe(ctx, channels...)
			done <- result{res, err}
		}()
		eventually(t, "subscribe", func() bool { return len(r.s.requests()) > before })
		return done
	}

	done := subscribe("futures.trades:BTC", "futures.trades:XYZ", "futures.mids")
	id := r.s.requests()[len(r.s.requests())-1]["id"]
	r.s.write(map[string]any{"type": "error", "code": "NOT_FOUND", "message": "Futures market not found", "id": id})
	r.s.write(map[string]any{"type": "subscribed", "channels": []string{"futures.trades:BTC", "futures.mids"}, "id": id})
	got := <-done
	if got.err != nil { // partly refused: not an error
		t.Fatalf("err %v", got.err)
	}
	if rf := got.res.RefusedByServer; len(rf) != 1 || rf[0].Channel != "futures.trades:XYZ" || rf[0].Err.Code != "NOT_FOUND" || !rf[0].Err.FromServer {
		t.Fatalf("refused %+v", rf)
	}
	if !slices.Equal(got.res.Added, []string{"futures.trades:BTC", "futures.mids"}) {
		t.Fatalf("added %v", got.res.Added)
	}
	if held := sortedStrings(r.ws.Channels()); !slices.Equal(held, []string{"futures.mids", "futures.trades:BTC"}) {
		t.Fatalf("held %v", held)
	}

	start := time.Now()
	done = subscribe("futures.trades:ETH", "futures.trades:SOL")
	id = r.s.requests()[len(r.s.requests())-1]["id"]
	for i := 0; i < 2; i++ {
		r.s.write(map[string]any{"type": "error", "code": "RATE_LIMITED", "message": "Too many new futures market subscriptions.", "id": id})
	}
	got = <-done
	var we *WSError
	if !errors.As(got.err, &we) || we.Code != "RATE_LIMITED" || len(got.res.Added) != 0 || len(got.res.RefusedByServer) != 2 ||
		time.Since(start) > time.Second {
		t.Fatalf("%+v %v after %s", got.res, got.err, time.Since(start))
	}
	if held := sortedStrings(r.ws.Channels()); !slices.Equal(held, []string{"futures.mids", "futures.trades:BTC"}) {
		t.Fatalf("held %v", held)
	}
	r.settle()
	if n := len(r.s.requests()); n != 2 {
		t.Fatalf("%d requests, want the 2 subscribes (a refused one is not retried)", n)
	}
}

func TestWSPingIntervalAbove60sRefused(t *testing.T) {
	spec := readFuturesWSSpec(t)
	limit := time.Duration(spec.PingMax) * time.Second
	for _, in := range []time.Duration{0, 45 * time.Second, limit} {
		ws, err := NewWebSocket(WSOptions{PingInterval: in})
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if want := cmpOr(in, 30*time.Second); ws.opts.PingInterval != want {
			t.Fatalf("PingInterval %s from %s, want %s", ws.opts.PingInterval, in, want)
		}
	}
	for _, in := range []time.Duration{limit + time.Millisecond, 5 * time.Minute} {
		var ce *ConfigError
		if _, err := NewWebSocket(WSOptions{PingInterval: in}); !errors.As(err, &ce) {
			t.Fatalf("%s: err %v, want a ConfigError", in, err)
		}
	}
}

func cmpOr(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

func refusalChannels(errs []error) []string {
	out := []string{}
	for _, e := range errs {
		var r *SubscribeRefusal
		if errors.As(e, &r) {
			out = append(out, r.Channel+" "+r.Err.Code)
		}
	}
	slices.Sort(out)
	return out
}

// Re-subscribe after a re-auth: a private channel refused with UNAUTHENTICATED goes back to
// pending; any other refusal drops the channel. Both are reported.
func TestWSResubscribeAfterReauthRefused(t *testing.T) {
	r := newFuturesWSRun(t)
	ctx := context.Background()
	reply := func(op string, frame map[string]any) {
		reqs := r.s.requests()
		frame["id"] = reqs[len(reqs)-1]["id"]
		if reqs[len(reqs)-1]["op"] != op {
			t.Fatalf("last request %v, want %s", reqs[len(reqs)-1], op)
		}
		r.s.write(frame)
	}
	go func() { _, _ = r.ws.AuthKey(ctx) }()
	eventually(t, "auth_key", func() bool { return len(r.s.requests()) == 1 })
	reply("auth_key", map[string]any{"type": "authenticated", "user_id": "u1", "auth": "api_key", "challenge": "ch2"})
	r.settle()
	all := []string{"orders", FuturesAccountChannel, "futures.trades:BTC"}
	go func() { _, _ = r.ws.Subscribe(ctx, all...) }()
	eventually(t, "subscribe", func() bool { return len(r.s.requests()) == 2 })
	reply("subscribe", map[string]any{"type": "subscribed", "channels": all})
	r.settle()

	r.s.write(map[string]any{"type": "signed_out", "reason": "expired"})
	r.settle()
	if got := sortedStrings(r.ws.PendingChannels()); !slices.Equal(got, []string{FuturesAccountChannel, "orders"}) {
		t.Fatalf("pending %v", got)
	}
	go func() { _, _ = r.ws.AuthKey(ctx) }()
	eventually(t, "auth_key 2", func() bool { return len(r.s.requests()) == 3 })
	reply("auth_key", map[string]any{"type": "authenticated", "user_id": "u1", "auth": "api_key"})
	eventually(t, "re-subscribe", func() bool { return len(r.s.requests()) == 4 })
	sub := r.s.requests()[3]
	if sub["op"] != "subscribe" || !slices.Equal(sortedStrings(sub["channels"]), []string{FuturesAccountChannel, "orders"}) {
		t.Fatalf("re-subscribe %v", sub)
	}
	// One error per channel, in the order sent; nothing accepted, so no ack.
	codes := map[string]string{"orders": "UNAUTHENTICATED", FuturesAccountChannel: "NOT_FOUND"}
	for _, c := range stringList(sub["channels"]) {
		r.s.write(map[string]any{"type": "error", "code": codes[c], "message": "refused", "id": sub["id"]})
	}
	eventually(t, "refusals reported", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(refusalChannels(r.errs)) == 2
	})
	r.mu.Lock()
	got := refusalChannels(r.errs)
	r.mu.Unlock()
	if !slices.Equal(got, []string{"futures.account NOT_FOUND", "orders UNAUTHENTICATED"}) {
		t.Fatalf("reported %v", got)
	}
	if p := r.ws.PendingChannels(); !slices.Equal(p, []string{"orders"}) {
		t.Fatalf("pending %v", p)
	}
	if held := r.ws.Channels(); !slices.Equal(held, []string{"futures.trades:BTC"}) {
		t.Fatalf("held %v", held)
	}
}

// Re-subscribe after a reconnect: the same rule, for spot and futures channels.
func TestWSResubscribeAfterReconnectRefused(t *testing.T) {
	var mu sync.Mutex
	var errs []error
	ws, m := setupWS(t, WSOptions{Handlers: WSHandlers{OnError: func(e error) { mu.Lock(); errs = append(errs, e); mu.Unlock() }}})
	ctx := context.Background()
	if _, err := ws.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Auth(ctx, "good"); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Subscribe(ctx, "orders", "ticker:BTC/USDT", FuturesMidsChannel); err != nil {
		t.Fatal(err)
	}
	m.refuseSub.Store(true) // one UNAUTHENTICATED error per subscribe, and no ack
	m.last().CloseNow()
	eventually(t, "refusals reported", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(refusalChannels(errs)) == 3
	})
	if p := ws.PendingChannels(); !slices.Equal(p, []string{"orders"}) {
		t.Fatalf("pending %v", p)
	}
	if held := ws.Channels(); len(held) != 0 {
		t.Fatalf("held %v", held)
	}
}
