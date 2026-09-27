# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Versions stay 0.x until the API ships request signing.

## [Unreleased]

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
