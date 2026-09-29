# cexy-go

The official Go SDK for the [CEXY.io](https://cexy.io) REST and WebSocket API.

- Typed models generated from the public OpenAPI spec ([cexy-api-spec](https://github.com/cexyio/cexy-api-spec)).
- Safe by default: retries with backoff, order placement designed to avoid duplicate orders (via `client_order_id`), a client-side rate limiter, and no HTTP redirects followed.
- A WebSocket client with heartbeat, reconnect and a live order book that applies the sync rules for you.
- `context.Context` on every call, range-over-func iterators (`iter.Seq2`) for pagination, one dependency ([coder/websocket](https://github.com/coder/websocket)).
- Go 1.26 or newer.

> **Status: 0.x, pre-release.** The API is not yet frozen. It stays 0.x until the exchange ships HMAC
> request signing, which will change how credentials are sent.

## Install

```bash
go get github.com/cexyio/cexy-go@v0.1.0-dev.5
```

```go
import cexy "github.com/cexyio/cexy-go"
```

## Quick start: public data

```go
ctx := context.Background()
c, err := cexy.New(cexy.Options{}) // no key needed for market data
if err != nil {
	log.Fatal(err)
}

markets, err := c.Markets.List(ctx)
btc, err := c.Markets.Get(ctx, "BTC/USDT")
book, err := c.Markets.OrderBook(ctx, "BTC/USDT", &cexy.GetOrderBookParams{Depth: cexy.Ptr(10)})
candles, err := c.Markets.Candles(ctx, "BTC/USDT", cexy.GetCandlesParams{Interval: cexy.CandleInterval1h, Limit: cexy.Ptr(24)})
now, err := c.Time(ctx)
```

Also: `Assets.List/Get`, `Networks.List`, `Fees.List`, `Config`, `Pools.List/Get`, `Markets.Trades`.
Optional parameters are pointers; `cexy.Ptr(v)` makes one.

## Quick start: your account

```go
c, err := cexy.New(cexy.Options{
	APIKey:    os.Getenv("CEXY_API_KEY"),    // ak_your_key_here
	APISecret: os.Getenv("CEXY_API_SECRET"), // your_secret_here
})

balances, err := c.Account.Balances(ctx)
// A sub-account's balances (parent account only; same shape, incl. HeldIncoming):
subBalances, err := c.Account.SubAccountBalances(ctx, "sub-account-id")
open, err := c.Trading.OpenOrders(ctx, &cexy.ListOpenOrdersParams{Symbol: cexy.Ptr("BTC/USDT")})

placed, err := c.Trading.PlaceOrder(ctx, cexy.PlaceOrderRequest{
	Symbol:   "BTC/USDT",
	Side:     cexy.OrderSideBuy,
	Type:     cexy.OrderTypeLimit,
	Price:    cexy.Ptr(cexy.Amount("60000.00")), // decimal STRINGS, never floats
	Quantity: cexy.Ptr(cexy.Amount("0.0010")),
})
_, err = c.Trading.CancelOrder(ctx, placed.Order.ID)
_, err = c.Trading.CancelAll(ctx, "BTC/USDT") // CancelAllMarkets(ctx) = every market, explicitly
```

`CancelAll` requires a symbol: the server treats a missing symbol as "every market", so that is a
separate method, `CancelAllMarkets`. The server allows cancel-all 30 times a minute per account.
Cancel-all also cancels stop orders that have not triggered yet (status `pending_trigger`) and
releases their reservations, so nothing fires into the market after the call.

One cancel-all call handles at most 500 orders. Each order it handled is in exactly one of
`Cancelled`, `AlreadyClosed` (it closed on its own first; not an error) and `Failed`, with the
reason in `Failures` (`INVALID_STATE` means the order was still being placed). `HasMore` means
there are more. To repeat until nothing is left, use `CancelAllUntilDone`:

```go
sum, err := c.Trading.CancelAllUntilDone(ctx, cexy.CancelAllOptions{Symbol: "BTC/USDT"}) // or AllMarkets: true
// sum.Cancelled, sum.AlreadyClosed, sum.Failed + sum.Failures, sum.Rounds, sum.Stopped
```

It calls again while `HasMore` is set or an order failed with `INVALID_STATE` or
`SERVICE_UNAVAILABLE`. Each round is exactly one HTTP request (the loop owns the retries), so it
never sends more than `MaxRounds` requests (default 20). After a round with no progress it waits
1, 2, 4, 8, then 15 s. A round that fails with a retryable error counts as a round: after a 429 it
waits the server's `Retry-After` exactly, after other retryable errors the next back-off step. It
never takes a wait that would reach `TimeBudget` (default 120 s): it stops with `Stopped:
"time_budget"` and `LastErrorCode` set. A non-retryable error is returned with the summary so far.
Rounds are merged by order id: the latest state wins.

Give both `APIKey` and `APISecret`, or neither: `New` returns a `*ConfigError` for only one.

| Service | Methods | Scope |
|---|---|---|
| `Markets` | `List`, `Get`, `OrderBook`, `Trades`, `AllTrades`, `Candles` | public |
| `Assets`, `Networks`, `Fees`, `Pools` | `List`, `Get` / `List` / `List` / `List`, `Get` | public |
| `Time`, `Config` (on `Client`) | | public |
| `Account` | `Balances`, `Balance`, `Ledger`, `Notifications`, `SubAccounts`, `SubAccountBalances`, `APIKeys` (+ `All…` iterators) | read |
| `Exports` | `Deposits`, `Ledger`, `Orders`, `Trades`, `Withdrawals` (CSV text) | read |
| `Wallet` | `Deposits`, `Deposit`, `Withdrawals`, `Withdrawal`, `WithdrawalAddresses`, `DepositAddress` (+ iterators) | read |
| `Trading` | `OpenOrders`, `Order`, `OrderByClientID`, `OrderHistory`, `Trades` (+ iterators) | read |
| `Trading` | `PlaceOrder`, `CancelOrder`, `CancelAll`, `CancelAllMarkets`, `CancelAllUntilDone` | trade |
| `Pools` | `Join`, `Exit` | trade |

`Wallet.DepositAddress` **creates** the address on the first call for that asset and network (later
calls return the same one). Always use the memo too when one is returned.

There are no withdrawal or transfer methods: API keys cannot withdraw or transfer funds.

## Held incoming transfers

`Balance.HeldIncoming` lists incoming internal transfers still held, each with `TransferID`,
`Amount` and `AvailableAt`. Their sum is **already included in `Locked`**, so never add it to
`Locked` or `Total` again. There are at most 100 entries, soonest `AvailableAt` first
(millisecond precision), with no sender identity. An entry disappears once the transfer is
released (the amount moves to `Available`) or cancelled by the exchange. It is never nil: an
empty slice when none, including from servers that predate the field.

## Amounts

Every amount is an exact decimal string (`cexy.Amount("0.00150000")`), in responses and requests.
Floats cannot hold most decimals exactly. The SDK checks every amount field before sending and
returns an `*InvalidAmountError` for anything but a plain decimal (no exponents, no spaces). Do
arithmetic with a decimal package such as `github.com/shopspring/decimal`:

```go
total := decimal.RequireFromString(string(*order.Price)).Mul(decimal.RequireFromString(string(order.Quantity)))
```

## Errors

Every API failure is an `*cexy.APIError` with `Status`, `Code`, `Message`, `Details`, `Fields`,
`RequestID`, `Retryable` and `RetryAfter`. Branch on `Code`, never on `Message`. Match the category
with `errors.Is`:

| Sentinel | When |
|---|---|
| `ErrAuthentication` | 401: missing or invalid credentials |
| `ErrForbidden` | 403: the key lacks a scope (`FORBIDDEN`), or the route is session-only (`API_KEY_NOT_ALLOWED`); also matches 451 |
| `ErrJurisdictionBlocked` | 451: `JURISDICTION_BLOCKED` (not available in the caller's jurisdiction) |
| `ErrValidation` | 400: see `Fields` |
| `ErrNotFound` | 404 |
| `ErrConflict` | 409: `ALREADY_EXISTS`, `IDEMPOTENCY_KEY_CONFLICT`, `CONCURRENT_MODIFICATION` |
| `ErrUnprocessable` | 422: `INSUFFICIENT_FUNDS`, `MARKET_UNAVAILABLE`, ... |
| `ErrRateLimited` | 429, with `RetryAfter` |
| `ErrServer` | 5xx |
| (none) | a code this SDK version does not know yet: check `Code` |

Local problems are `*ConfigError`, `*InvalidAmountError`, `*ConnectionError` (with `Timeout` for
the per-attempt timeout) and `*OrderStateUnknownError`. A cancelled context returns `ctx.Err()`.
`ErrorCode` is an open string type with a constant for every known code (`cexy.CodeInsufficientFunds`,
...): keep a default branch, because new codes can appear.

```go
_, err := c.Trading.PlaceOrder(ctx, order)
var apiErr *cexy.APIError
switch {
case errors.As(err, &apiErr) && apiErr.Code == cexy.CodeInsufficientFunds:
	log.Println("not enough funds:", apiErr.Details)
case errors.Is(err, cexy.ErrRateLimited):
	log.Println("slow down for", apiErr.RetryAfter)
case err != nil:
	return err
}
```

## Retries and idempotency

- Timeout per attempt: `Options.Timeout` (default 10 s). Retries: `Options.MaxRetries` (default 3; `NoRetries` turns them off), exponential backoff with full jitter.
- Retried: connection errors, timeouts and responses with `retryable: true` (and 409
  `CONCURRENT_MODIFICATION`). A 4xx is never retried except 429 and 409 `CONCURRENT_MODIFICATION`,
  whatever its body says.
- A 429 waits `Retry-After` / `details.retry_after_seconds` (plus up to 250 ms of jitter). Server
  wait hints are untrusted: unparseable or absurd values are ignored, and a hint above
  `cexy.MaxServerWait` (120 s) is not waited. The call fails at once with the rate-limit
  `APIError`, whose `RetryAfter` still holds the server's value. The client-side rate limiter
  never blocks longer than `MaxServerWait` because of a server hint.
- GETs retry freely.
- **Idempotency-Key** is sent only on pool join and exit, the only endpoints that honour it. Orders,
  order cancels and cancel-all send none.
- **Orders:** safety rests on `client_order_id`. `PlaceOrder` always sends a `client_order_id`
  (a UUID if you do not set one); it is unique per account and a repeat is refused before any funds
  move. After an ambiguous failure (connection error, timeout, 5xx) the SDK first looks the order up by
  that id. If the order exists it is returned with `Recovered: true`; only if it does not exist is it
  sent again, with the same id. If even the lookup fails you get `*OrderStateUnknownError`: check
  `Trading.OrderByClientID` before placing the order again.
- **Cancels:** `CancelOrder` is retried like a read: on connection errors, timeouts and retryable
  responses (such as a 503, a 429, or a 409 `CONCURRENT_MODIFICATION`). If a *retry* gets
  `INVALID_STATE`, the first attempt already cancelled the order, so the SDK fetches and returns it. Cancel-all is naturally
  repeatable and is retried the same way (a retry reports only what it cancelled).
- **Pool join and exit** send a generated `Idempotency-Key`, reused on every retry; the server honours
  it there, so they execute once. A 409 `CONCURRENT_MODIFICATION` (the same key still in flight) is
  retried with the same key. `cexy.WithIdempotencyKey(k)` sets it yourself (ignored on other calls).
- `Options.OnRetry` lets you log retries.

Every method takes a context and optional `CallOption`s: `cexy.WithTimeout(d)`,
`cexy.WithMaxRetries(n)`, `cexy.WithIdempotencyKey(k)`.

## Pagination

Histories use opaque cursors. Each listing returns one page (`Items`, `HasMore`, `NextCursor`), and an
`All…` method returns an iterator that fetches pages lazily:

```go
for order, err := range c.Trading.AllOrderHistory(ctx, &cexy.OrderHistoryParams{Symbol: cexy.Ptr("BTC/USDT")}) {
	if err != nil {
		return err
	}
	fmt.Println(order.ID, order.Status)
}
// cap the total: c.Account.AllLedger(ctx, nil, cexy.WithMaxItems(500))
```

## Rate limits

The client has a token-bucket limiter: **100 requests a minute without a key** (the server allows
120 a minute per IP for anonymous calls) and **300 a minute with an API key** (the server allows 600 a
minute per key). Cancel-all is limited separately to 30 a minute per account. It adapts downwards to
`X-RateLimit-Limit` / `X-RateLimit-Remaining` / `X-RateLimit-Reset` (seconds until the window resets),
and pauses after a 429. Change it with `Options.RequestsPerMinute`, or turn it off with
`DisableRateLimit`. The limiter belongs to one `Client`, which is safe for concurrent use: share it.

## WebSocket

```go
ws, err := c.WebSocket(cexy.WSOptions{ // wss://api.cexy.io/api/v1/ws
	Handlers: cexy.WSHandlers{
		OnEvent: func(e cexy.Event) {
			if e.Type == "ticker.update" {
				fmt.Println(e.Channel, string(e.Data))
			}
		},
		OnReconnected: func(cexy.Welcome) { log.Println("reconnected and re-subscribed") },
		OnResync:      func(cexy.ResyncReason) { /* refetch anything you derive from events */ },
	},
})
if _, err := ws.Connect(ctx); err != nil {
	log.Fatal(err)
}
defer ws.Close()
_, err = ws.Subscribe(ctx, "ticker:BTC/USDT", "trades:BTC/USDT")

book, err := ws.OrderBook(ctx, "BTC/USDT", cexy.LiveOrderBookOptions{
	OnUpdate: func(b cexy.BookSnapshot) {
		bid, _, ask, _ := b.Best()
		fmt.Println(bid.Price, ask.Price, b.Stale)
	},
})
```

Handlers run one at a time on a dedicated goroutine, in frame order, so they may call `Subscribe`,
`Auth` and the other methods. Keep them quick: frames queue up while a handler runs.

What the client does for you:

- Sends `{"op":"ping"}` every 30 s (required: the server closes idle connections) and accepts the server's
  unsolicited pongs. No frame for 75 s means a dead connection and a reconnect.
- Reconnects with exponential backoff and full jitter, then re-authenticates and re-subscribes everything.
- Correlates every request with its acknowledgement by `id`: `Auth` returns on `authenticated`
  (and fails on an `error` with its id, or on timeout), `Subscribe` on `subscribed`, `Unsubscribe` on
  `unsubscribed`, `Ping` on `pong`.
- Guards locally: at most 100 subscriptions (extras are returned in `Refused`) and 200 messages a minute.
- Warns once if the server speaks a newer `protocol_version`, and ignores unknown event types.

**Order-book rules** (applied by `ws.OrderBook`; follow them if you build your own):

1. Subscribe to `orderbook:{symbol}` first, then take the REST snapshot (its `sequence` is S).
2. Drop updates with `sequence <= S`.
3. Every update carries the complete top 50 of both sides (`"full": true`) and replaces the previous state.
   There are no deltas. Never merge REST levels deeper than 50 into WebSocket state.
4. A sequence gap marks the book stale until the next update, which heals it. There is no forced resync.
5. Sequences reset when the server restarts: take a fresh snapshot after every reconnect.
6. An `error` frame `CONCURRENT_MODIFICATION` with a null `id` means messages were dropped: resync every
   book and channel (the client calls `OnResync`).

**Private channels** (`orders`, `balances`, `deposits`, `withdrawals`, `account`) need
`ws.Auth(ctx, token)` with a session access token (it returns the `user_id` from `authenticated`).
**API-key authentication on the WebSocket is not available yet**: with an API key, use public channels
and poll REST for private state. If the session is revoked, the client calls `OnAuthLost`; public
channels keep working.

## Security

- API keys **cannot withdraw or transfer funds**, whatever their scopes.
- Use a **read-only** key unless you need to trade, and restrict keys to your IPs (`allowed_ips`).
- Credentials go only in the `X-API-Key` / `X-API-Secret` headers and only on private endpoints; never in URLs.
- The SDK **never follows HTTP redirects**, so credentials and orders are never re-sent to another URL.
  A 3xx response becomes an `*APIError` with code `UNEXPECTED_REDIRECT` (`errors.Is(err,
  cexy.ErrUnexpectedRedirect)`); it is not retried. The SDK uses its own copy of `Options.HTTPClient` and
  leaves yours unchanged.
- Only `https://` base URLs and `wss://` WebSocket URLs are accepted. `AllowInsecure` permits
  `http://` / `ws://` solely for `localhost`, `127.0.0.1` or `::1` (local test servers).
- The SDK redacts the secret from `fmt` output (`%v`, `%+v`, `%#v`), `log/slog` and error messages,
  including anything the server echoes back in `APIError.Details` and `Fields`: nested values and
  object keys too.
- Keep keys in environment variables or a secret manager, not in code.

Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## Trading risk

- **Orders are real** and irreversible once filled. Test your code with a read-only key and small
  amounts first, and add your own limits (maximum order size, allowed markets) before automating trades.
- Retries and the `client_order_id` lookup are designed to avoid duplicate orders, but when a failure
  is ambiguous and even the lookup fails you get `*OrderStateUnknownError`: check the order before
  placing it again.
- Markets move between reading data and placing an order; prices and balances you read may be stale.
- This SDK is provided under the MIT licence, without warranty. Nothing in it is investment advice.

## For tool builders

`cexy.Operations()` returns the table of the 40 operations (method, path, auth and scope). The package
also exports every model type, the error types, the `Authenticator` interface (HMAC signing will plug
in here) and `Options.UserAgentSuffix` to identify your tool.

## Development

```bash
pip install pyyaml
python3 tools/generate.py          # regenerate models_gen.go and operations_gen.go from ../cexy-api-spec
python3 tools/generate.py --check  # CI: the generated files match the spec
go vet ./... && go test -race ./...
CEXY_LIVE_TESTS=1 go test -run TestLive ./...   # optional: anonymous GETs against api.cexy.io
```

Tests read the shared conformance cases from a `cexy-api-spec` checkout next to this repo
(override with `CEXY_API_SPEC`; `CEXY_REQUIRE_SPEC=1` fails instead of skipping when it is missing).

## License

MIT, see [LICENSE](LICENSE).
