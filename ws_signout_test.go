package cexy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// signoutStep is one step of conformance/ws/private_signout.json.
type signoutStep struct {
	Client       string           `json:"client"`
	Token        string           `json:"token"`
	Channels     []string         `json:"channels"`
	Server       map[string]any   `json:"server"`
	ReplyTo      string           `json:"reply_to"`
	ExpectSent   []map[string]any `json:"expect_sent"`
	ExpectEvents []map[string]any `json:"expect_events"`
	ExpectHeld   []string         `json:"expect_held"`
	ExpectToken  *bool            `json:"expect_token"`
	raw          map[string]json.RawMessage
}

// scriptedWS answers only pings; the script sends every other frame.
type scriptedWS struct {
	mu        sync.Mutex
	conn      *websocket.Conn
	received  []map[string]any
	challenge string // sent in the welcome when set (AuthKey)
}

func (s *scriptedWS) handler(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conn = c
	s.mu.Unlock()
	welcome := map[string]any{"type": "welcome", "protocol_version": 1, "heartbeat_interval_seconds": 30,
		"max_subscriptions": 100, "connection_id": "c1"}
	if s.challenge != "" {
		welcome["challenge"] = s.challenge
	}
	s.write(welcome)
	for {
		_, data, err := c.Read(context.Background())
		if err != nil {
			return
		}
		var f map[string]any
		if json.Unmarshal(data, &f) != nil {
			continue
		}
		s.mu.Lock()
		s.received = append(s.received, f)
		s.mu.Unlock()
		if f["op"] == "ping" {
			s.write(map[string]any{"type": "pong", "id": f["id"]})
		}
	}
}

func (s *scriptedWS) write(v any) {
	b, _ := json.Marshal(v)
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Write(ctx, websocket.MessageText, b)
}

func (s *scriptedWS) requests() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, f := range s.received {
		if f["op"] != "ping" {
			out = append(out, f)
		}
	}
	return out
}

func sortedStrings(v any) []string {
	var out []string
	if ss, ok := v.([]string); ok {
		out = append([]string(nil), ss...)
	} else {
		out = stringList(v)
	}
	if out == nil {
		out = []string{}
	}
	slices.Sort(out)
	return out
}

func normSent(m map[string]any) map[string]any {
	out := map[string]any{"op": m["op"]}
	if t, ok := m["token"]; ok {
		out["token"] = t
	}
	if c, ok := m["channels"]; ok {
		out["channels"] = sortedStrings(c)
	}
	return out
}

// TestWSPrivateSignoutConformance runs conformance/ws/private_signout.json step by step.
func TestWSPrivateSignoutConformance(t *testing.T) {
	dir := specDir(t)
	var spec struct {
		Cases []struct {
			ID    string        `json:"id"`
			Steps []signoutStep `json:"steps"`
		} `json:"cases"`
	}
	for _, file := range []string{"private_signout.json", "server_signout.json"} {
		spec.Cases = nil
		readJSON(t, filepath.Join(dir, "conformance", "ws", file), &spec)
		if len(spec.Cases) == 0 {
			t.Fatalf("%s: no cases", file)
		}
		runSignoutCases(t, file, spec.Cases)
	}
}

func runSignoutCases(t *testing.T, file string, cases []struct {
	ID    string        `json:"id"`
	Steps []signoutStep `json:"steps"`
}) {
	for _, c := range cases {
		t.Run(file+"/"+c.ID, func(t *testing.T) {
			s := &scriptedWS{}
			srv := httptest.NewServer(http.HandlerFunc(s.handler))
			defer srv.Close()
			var evMu sync.Mutex
			var events []map[string]any
			record := func(e map[string]any) {
				evMu.Lock()
				events = append(events, e)
				evMu.Unlock()
			}
			ws, err := NewWebSocket(WSOptions{
				URL: "ws" + srv.URL[len("http"):] + "/api/v1/ws", AllowInsecure: true, NoReconnect: true, AckTimeout: 2 * time.Second,
				Handlers: WSHandlers{
					OnAuthChanged: func(a AuthChange) {
						e := map[string]any{"type": "auth_changed", "reason": string(a.Reason), "previous_user_id": nilIfEmpty(a.PreviousUserID),
							"user_id": nilIfEmpty(a.UserID), "dropped": sortedStrings(a.Dropped)}
						if a.Code != "" {
							e["code"] = a.Code
						}
						record(e)
					},
					OnResync:   func(r ResyncReason) { record(map[string]any{"type": "resync", "reason": string(r)}) },
					OnAuthLost: func(Event) { record(map[string]any{"type": "auth_lost"}) },
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer ws.Close()
			ctx := context.Background()
			if _, err := ws.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			// Two ping round trips: the client has handled every earlier frame (and its handlers
			// have run), and the server has recorded everything the client sent in reaction.
			settle := func() {
				for i := 0; i < 2; i++ {
					if _, err := ws.Ping(ctx); err != nil {
						t.Fatal(err)
					}
				}
				done := make(chan struct{})
				ws.dispatch(func() { close(done) })
				<-done
			}
			answered := map[any]bool{}
			sentMark := 0
			for i, st := range c.Steps {
				switch {
				case st.Client != "":
					before := len(s.requests())
					switch st.Client {
					case "auth":
						go func() { _, _ = ws.Auth(ctx, st.Token) }()
					case "subscribe":
						go func() { _, _ = ws.Subscribe(ctx, st.Channels...) }()
					}
					eventually(t, st.Client+" frame", func() bool { return len(s.requests()) > before })
				case st.Server != nil:
					frame := map[string]any{}
					for k, v := range st.Server {
						frame[k] = v
					}
					if st.ReplyTo != "" {
						reqs := s.requests()
						var req map[string]any
						for j := len(reqs) - 1; j >= 0; j-- {
							if reqs[j]["op"] == st.ReplyTo && !answered[reqs[j]["id"]] {
								req = reqs[j]
								break
							}
						}
						if req == nil {
							t.Fatalf("step %d: no unanswered %s request", i, st.ReplyTo)
						}
						answered[req["id"]] = true
						frame["id"] = req["id"]
					}
					s.write(frame)
					settle()
				case st.ExpectSent != nil:
					settle()
					reqs := s.requests()
					got := []map[string]any{}
					for _, m := range reqs[sentMark:] {
						got = append(got, normSent(m))
					}
					sentMark = len(reqs)
					want := []map[string]any{}
					for _, m := range st.ExpectSent {
						want = append(want, normSent(m))
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("step %d: sent %v, want %v", i, got, want)
					}
				case st.ExpectEvents != nil:
					settle()
					evMu.Lock()
					got := events
					events = nil
					evMu.Unlock()
					if len(got) != len(st.ExpectEvents) {
						t.Fatalf("step %d: events %v, want %v", i, got, st.ExpectEvents)
					}
					for _, want := range st.ExpectEvents {
						if !slices.ContainsFunc(got, func(g map[string]any) bool { return matchEvent(g, want) }) {
							t.Fatalf("step %d: no event %v in %v", i, want, got)
						}
					}
				case st.ExpectHeld != nil:
					settle()
					got := ws.Channels()
					slices.Sort(got)
					want := sortedStrings(st.ExpectHeld)
					if len(got) == 0 {
						got = []string{}
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("step %d: held %v, want %v", i, got, want)
					}
				case st.ExpectToken != nil:
					settle()
					if ws.HasToken() != *st.ExpectToken {
						t.Fatalf("step %d: HasToken %v, want %v", i, ws.HasToken(), *st.ExpectToken)
					}
				default:
					t.Fatalf("step %d: unknown step", i)
				}
			}
		})
	}
}

// ExpectHeld and ExpectEvents need to tell an empty list from a missing key.
func (s *signoutStep) UnmarshalJSON(b []byte) error {
	type plain signoutStep
	if err := json.Unmarshal(b, (*plain)(s)); err != nil {
		return err
	}
	if err := json.Unmarshal(b, &s.raw); err != nil {
		return err
	}
	if _, ok := s.raw["expect_held"]; ok && s.ExpectHeld == nil {
		s.ExpectHeld = []string{}
	}
	if _, ok := s.raw["expect_events"]; ok && s.ExpectEvents == nil {
		s.ExpectEvents = []map[string]any{}
	}
	if _, ok := s.raw["expect_sent"]; ok && s.ExpectSent == nil {
		s.ExpectSent = []map[string]any{}
	}
	return nil
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func matchEvent(got, want map[string]any) bool {
	for k, v := range want {
		if k == "dropped" {
			if !reflect.DeepEqual(got[k], sortedStrings(v)) {
				return false
			}
			continue
		}
		if !reflect.DeepEqual(got[k], v) {
			return false
		}
	}
	return true
}
