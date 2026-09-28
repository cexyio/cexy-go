# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Versions stay 0.x until the API ships request signing.

## [Unreleased]

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
