# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Versions stay 0.x until the API ships request signing.

## [Unreleased]

## [0.1.0-dev.7] (2026-09-30)

### Added
- `Account.ID`: the account id of the API key (`GET /api/v1/account/id`, read scope).
- `WebSocket.LiveBalances` / `LiveBalances`: live balances from a REST snapshot plus
  `balance.updated` events. An event applies only when its `sequence` is greater than the stored
  one (a total of 0 removes the row, and an older snapshot row cannot bring it back); a refetch
  happens on a missed event, `balances.resync`, `CONCURRENT_MODIFICATION`, a reconnect or an account
  change, at most every `MinSnapshotInterval` (default 2 s; negative: none), with retry backoff.
  Before every merge the REST key's account (`Account.ID`) must be the WebSocket's user, otherwise
  nothing is merged (`*AccountMismatchError`). Events without `sequence` (older servers) always
  apply and log one warning. `Stale`, `LastError`, `Get`, `All`, `Close`; callbacks `OnUpdate`,
  `OnSnapshot`, `OnError`.
- WebSocket: frame-sequence tracking on private channels. A gap that is not filled within
  `WSOptions.ReorderWindow` (default 250 ms) calls `OnSequenceGap` and `OnResync(ResyncSequenceGap)`.
- WebSocket: `balances.resync` (and the planned `deposits.resync` / `withdrawals.resync`) are known
  events and call `OnResync` with `ResyncBalancesResync`, `ResyncDepositsResync` or
  `ResyncWithdrawalsResync`.
- WebSocket: the planned `signed_out` server frame is handled as a server sign-out:
  `AuthTokenExpired`, `AuthSessionRevoked` plus `OnAuthLost` (synthetic `session.revoked` event with
  `data.reason` `"signed_out"`), or `AuthSignedOut` with the raw reason in `Code`. The token is
  forgotten.
- `Balance.Sequence`, `WebSocket.UserID`, `WSClock` / `WSOptions.Clock` (test-only time source).

## [0.1.0-dev.6] (2026-09-30)

### Fixed
- WebSocket: private channels no longer go silent after a server-side sign-out. The server ends
  every private subscription (without a frame) when `Auth` succeeds as another user, when an
  `Auth` fails, or when this connection's own session is revoked. The client kept those channels
  as held (after a failed auth) or never noticed the switch, so `Subscribe` for them sent
  nothing. It now drops them, calls the new `OnAuthChanged` handler (`AuthChange`: `Reason`,
  `PreviousUserID`, `UserID`, `Code`, `Dropped`) and re-subscribes them: at once after a switch
  to another user, after the next successful `Auth` otherwise, followed by
  `OnResync(ResyncReauth)`. Re-authenticating as the same user changes nothing.
- WebSocket: a `Subscribe` refused by the server (e.g. `UNAUTHENTICATED` for a private channel)
  no longer leaves the channels in `Channels()`.

### Changed
- WebSocket: `session.revoked` acts only when `data.current` is exactly `true` (this connection's
  own session). Another session's revocation (`current: false`) no longer calls `OnAuthLost`,
  drops private channels or forgets the token. **Behaviour change.**

### Added
- `WebSocket.HasToken`, `WSHandlers.OnAuthChanged`, `AuthChange`, `AuthChangeReason`
  (`AuthUserChanged`, `AuthFailed`, `AuthSessionRevoked`) and `ResyncReauth`.
- Conformance: runs `cexy-api-spec/conformance/ws/private_signout.json` against a scripted server.

## [0.1.0-dev.5] (2026-09-29)

### CI
- New `vuln-min-go` job (also weekly): runs govulncheck with the minimum Go that `go.mod` allows, so
  a stale security floor is caught when a new standard-library advisory appears. When the weekly run
  fails, a separate `security-floor-issue` job opens (or updates) one issue labelled
  `security-floor`; only that job has `issues: write` (`vuln-min-go` itself is `contents: read`), and
  it gets the govulncheck output as an artifact.

### Added
- `Balance.HeldIncoming` (`[]HeldIncoming`: `TransferID`, `Amount`, `AvailableAt`): incoming internal
  transfers still held, at most 100, soonest first. Their sum is already included in `Locked`: never
  add it again. `Balances`/`Balance` never return a nil slice (empty when the server omits it).
- `Account.SubAccountBalances(ctx, id)`: a sub-account's balances, read by its parent account
  (`GET /account/sub-accounts/{id}/balances`, read scope). Same `[]Balance` as `Balances`, including
  `HeldIncoming`. An id that is not the caller's sub-account matches `ErrNotFound` (not retried); an
  empty id is a `*ConfigError` before any request.

### Changed
- A 4xx response is never retried except 429 and 409 `CONCURRENT_MODIFICATION`, even when its body
  says `retryable: true`. A 408 is no longer retried either, and its default `Retryable` (no field in
  the body) is now false. 409 `CONCURRENT_MODIFICATION` and 429
  are still retried only where they were before. A mutation sent through the shared retry loop is
  retried only when it is repeat-safe (pool join/exit with their `Idempotency-Key`, cancel-all);
  `PlaceOrder` and `CancelOrder` keep their own policies.

### Security
- Path values `"."` and `".."` are rejected with a `*ConfigError`: previously they were sent as a
  literal dot segment, which the server's router or a proxy may resolve, so e.g.
  `SubAccountBalances("..")` could return the parent's own balances and `OrderByClientID("..")` the
  open-orders list. A write request could at most be redirected to a route that does not exist and
  is refused by the server; no write could reach a different operation.

## [0.1.0-dev.4] (2026-09-28)

Sync with API release H-1.

### Security
- Minimum Go is now **1.26.6** (`go` directive), a security floor for the standard-library
  vulnerabilities GO-2026-6218 (net/url), GO-2026-6090 (crypto/tls) and GO-2026-5972
  (encoding/asn1), all fixed in Go 1.26.6. The SDK uses `net/url` and `crypto/tls`, so building
  with Go 1.26.0–1.26.5 was exposed through the standard library.

### Added
- `LedgerEntry.Reference` is a typed `LedgerReference` (it was `json.RawMessage`): `Type` says the
  variant (`deposit`, `withdrawal`, `order`, `trade`, `transfer`, `adjustment`, `pool`,
  `futures_transfer`, `system`) and only that variant's id fields are set. An unknown variant never
  fails decoding: `Type` holds it, `Raw` keeps the object, `Known()` reports false.
- Id aliases `DepositID`, `FuturesTransferID`, `OrderID`, `PoolID`, `TradeID`, `UserID`, `WithdrawalID`
  (all `= string`, not validated).
- Error code `CodePriceUnavailable` (`PRICE_UNAVAILABLE`, HTTP 422: matches `ErrUnprocessable`).
- `WithdrawalStatusReverted` (a failed on-chain transaction, refunded by the exchange) and the new
  `LedgerEntryKind` values (`transfer_in_held`, `transfer_release`, `transfer_reversal`,
  `withdrawal_fee_revenue_reversal`, `withdrawal_refund`).

### Changed
- `JoinPoolRequest.MaxRatioDeviationPercent` is `*Amount` (was `*string`), and still validated.
- Cancel-all also cancels stop orders that have not triggered yet (`pending_trigger`); documented.

### Fixed
- `CancelAllUntilDone` counts a pending client-side rate-limiter block (for example after
  `X-RateLimit-Remaining: 0` with a `X-RateLimit-Reset`) against its time budget before each round. If
  the block would reach the budget, the loop stops with `time_budget` and `LastErrorCode` `RATE_LIMITED`
  without calling, instead of stalling past the budget inside the limiter.

### CI
- New `consumer` job: builds and runs `ci/consumer`, a separate module that depends on the SDK the way
  a user's program does (a `replace` to the checkout), so test-only code cannot mask a missing piece.

## [0.1.0-dev.3]

Patch round from the cexy-rust pre-publication review, whose findings apply to every SDK.

### Fixed
- **Server-controlled waits are bounded.** `Retry-After` (seconds or HTTP-date),
  `details.retry_after_seconds` and `X-RateLimit-Reset` are untrusted: unparseable, negative, NaN or
  infinite values are ignored, huge values saturate instead of overflowing, and a hint above the new
  `MaxServerWait` (120 s) is not waited. The call fails at once with the rate-limit `APIError`,
  whose `RetryAfter` keeps the server's value. The rate limiter never blocks longer than 120 s
  because of a server hint, and ignores an `X-RateLimit-Limit` below 1.
- **`CancelAllUntilDone` owns its retries.** Each round is exactly one HTTP request, so the loop
  sends at most `MaxRounds` requests even when rounds fail. Previously each round could retry 3
  times. A 429 waits `Retry-After` exactly, other retryable errors take the next back-off step, and
  a wait that would reach the budget is never taken: the loop stops with `time_budget` and the new
  `CancelAllSummary.LastErrorCode`.
- Credentials echoed back by the server are redacted in nested `APIError.Details` values and in
  object keys (of `Details` and `Fields`), not only in top-level strings.

### Changed
- `Idempotency-Key` is sent only on pool join and exit, the only endpoints that honour it. Orders
  and order cancels no longer send one; `WithIdempotencyKey` is ignored on other calls.

## [0.1.0-dev.2]

Cancel-all v2 (the API change of 2026-09-27).

### Added
- `CancelAllResult` has `AlreadyClosed` (orders that closed on their own first: not failures),
  `Failures` (`CancelFailure`: order id, code, message) and `HasMore` (more than the 500 orders one
  call handles).
- `Trading.CancelAllUntilDone(ctx, CancelAllOptions{Symbol | AllMarkets, MaxRounds, TimeBudget})`
  repeats cancel-all while `HasMore` is set or orders fail with `INVALID_STATE` or
  `SERVICE_UNAVAILABLE`. It backs off 1-2-4-8-15 s after rounds without progress, is bounded by
  `MaxRounds` (default 20) and `TimeBudget` (default 120 s), and merges rounds by order id.
  `CancelAll` and `CancelAllMarkets` stay a single request.

### Changed
- Cancel-all no longer sends an `Idempotency-Key`; the server does not honour one there.
- The unknown-symbol 404 (`ErrNotFound`) and the 30-a-minute 429 (`ErrRateLimited`) are documented.

## [0.1.0-dev.1]

First pre-release. Built from `openapi.sdk.json` (spec `info.version` 1.0.0).

### Added
- `Client` covering the 40 operations of the SDK surface: public market data, account, exports,
  wallet reads, trading and liquidity pools.
- Models and the operation table generated from the shared spec by `tools/generate.py`, with a
  `--check` mode for CI. Amounts are `Amount` decimal strings, checked before sending.
- `*APIError` with `errors.Is` categories (`ErrNotFound`, `ErrRateLimited`, ...), including
  `ErrJurisdictionBlocked` for `JURISDICTION_BLOCKED` / HTTP 451, which also matches `ErrForbidden`.
- Retries with exponential backoff and full jitter, `Retry-After` handling, idempotency keys on pool
  join and exit, duplicate-safe `PlaceOrder` (lookup by `client_order_id` after an ambiguous failure),
  and `CancelOrder` treating `INVALID_STATE` on a retry as already cancelled.
- A client-side rate limiter (100 a minute anonymous, 300 with a key) that adapts to `X-RateLimit-*`.
- Cursor pagination as `iter.Seq2` iterators.
- `WebSocket`: heartbeat, liveness, acknowledgements correlated by id, local limits, reconnect with
  re-auth and re-subscribe, and `LiveOrderBook` implementing the order-book sync rules.
- `Authenticator` interface so request signing can be added without breaking callers.
- `https://` and `wss://` only; `AllowInsecure` allows plain text for loopback hosts only.
- HTTP redirects are never followed: a 3xx is an `*APIError` with code `UNEXPECTED_REDIRECT`
  (`ErrUnexpectedRedirect`), so credentials and orders are never re-sent to another URL.
- Credentials are redacted from `fmt` and `log/slog` output and from error messages, details and fields.
