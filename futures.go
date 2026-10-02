package cexy

import (
	"context"
	"fmt"
	"iter"
)

// FuturesService: futures data, read only. Markets, OrderBook, Candles and Trades are public;
// Positions, OpenOrders, Fills and Funding need an API key with the read scope. Every market
// response carries AsOf and Stale (for books and trades, Stale is the live feed's health, not
// the data's age). When nothing usable is cached the API answers 503 SERVICE_UNAVAILABLE
// (retryable, with Retry-After), which the client retries like any other.
//
// Without a futures account the account reads succeed with HasAccount false.
type FuturesService struct{ t *transport }

// Markets returns every listed futures market and its current figures.
func (s *FuturesService) Markets(ctx context.Context, opts ...CallOption) (FuturesMarkets, error) {
	return getData[FuturesMarkets](ctx, s.t, call{op: OpMarkets}, opts)
}

// Market returns one futures market, for example "BTC" (the coin name, not a spot symbol).
func (s *FuturesService) Market(ctx context.Context, coin string, opts ...CallOption) (FuturesMarket, error) {
	return getData[FuturesMarket](ctx, s.t, call{op: OpMarket, pathParams: map[string]string{"coin": coin}}, opts)
}

// OrderBook returns a futures market's book, depth levels a side (1 to 20; 0 sends no depth
// and the server's default, 20, applies).
func (s *FuturesService) OrderBook(ctx context.Context, coin string, depth int, opts ...CallOption) (FuturesBook, error) {
	p := OrderbookParams{}
	if depth != 0 {
		p.Depth = &depth
	}
	return getData[FuturesBook](ctx, s.t, call{op: OpOrderbook, pathParams: map[string]string{"coin": coin},
		query: p.values()}, opts)
}

// Candles returns 500 candles of a futures market, oldest first: the latest, or the window
// holding params.Before (unix milliseconds). params.Interval is required (1m, 5m, 15m, 1h,
// 4h or 1d).
func (s *FuturesService) Candles(ctx context.Context, coin string, params CandlesParams, opts ...CallOption) (FuturesCandles, error) {
	if params.Interval == "" {
		return FuturesCandles{}, &ConfigError{Msg: "Futures.Candles: Interval is required"}
	}
	return getData[FuturesCandles](ctx, s.t, call{op: OpCandles, pathParams: map[string]string{"coin": coin},
		query: params.values()}, opts)
}

// Trades returns a futures market's recent public trades, newest first: limit of them (1 to
// 100; 0 sends no limit and the server's default, 50, applies).
func (s *FuturesService) Trades(ctx context.Context, coin string, limit int, opts ...CallOption) (FuturesTrades, error) {
	p := TradesParams{}
	if limit != 0 {
		p.Limit = &limit
	}
	return getData[FuturesTrades](ctx, s.t, call{op: OpTrades, pathParams: map[string]string{"coin": coin},
		query: p.values()}, opts)
}

// Positions returns the account's margin summary and open positions.
func (s *FuturesService) Positions(ctx context.Context, opts ...CallOption) (FuturesPositions, error) {
	return getData[FuturesPositions](ctx, s.t, call{op: OpPositions}, opts)
}

// OpenOrders returns the account's open futures orders.
func (s *FuturesService) OpenOrders(ctx context.Context, opts ...CallOption) (FuturesOpenOrders, error) {
	return getData[FuturesOpenOrders](ctx, s.t, call{op: OpOpenOrders}, opts)
}

// Fills returns one page of the account's fills, newest first, 30 days back. Pass "" for the
// newest page, then each NextCursor exactly as given until it is nil. A page may be short,
// even empty, and still have a NextCursor. AllFills does the paging.
func (s *FuturesService) Fills(ctx context.Context, cursor string, opts ...CallOption) (FuturesFills, error) {
	return getData[FuturesFills](ctx, s.t, call{op: OpFills, query: (&FillsParams{Cursor: optCursor(cursor)}).values()}, opts)
}

// Funding returns one page of the account's funding payments, paged like Fills.
func (s *FuturesService) Funding(ctx context.Context, cursor string, opts ...CallOption) (FuturesFunding, error) {
	return getData[FuturesFunding](ctx, s.t, call{op: OpFunding, query: (&FundingParams{Cursor: optCursor(cursor)}).values()}, opts)
}

// AllFills iterates over the account's fills, newest first, fetching pages as needed. It ends
// at the last page, or at once (no rows) when the account has no futures account.
//
// While the futures provider is busy the server answers an empty page that hands back the
// cursor it was given: the iterator waits (the client's backoff, reported to Options.OnRetry
// with the PAGING_STALLED error as Err) and asks again, at most WithMaxBusyRetries times in a
// row (default 3, independent of the client's retry count), then yields a retryable *APIError
// with code CodePagingStalled. A page with rows that hands back the cursor it was given is a
// server error: its rows are yielded, then a non-retryable *APIError with code
// CodePagingCursorRepeated. Both carry Details["cursor"], which resumes the listing through Fills.
func (s *FuturesService) AllFills(ctx context.Context, opts ...IterOption) iter.Seq2[FuturesFill, error] {
	o := iterOpts(opts)
	return pageHistory(ctx, s.t, OpFills, o, func(ctx context.Context, cursor string) ([]FuturesFill, bool, *string, error) {
		p, err := s.Fills(ctx, cursor, o.call...)
		return p.Fills, p.HasAccount, p.NextCursor, err
	})
}

// AllFunding iterates over the account's funding payments, newest first, like AllFills.
func (s *FuturesService) AllFunding(ctx context.Context, opts ...IterOption) iter.Seq2[Funding, error] {
	o := iterOpts(opts)
	return pageHistory(ctx, s.t, OpFunding, o, func(ctx context.Context, cursor string) ([]Funding, bool, *string, error) {
		p, err := s.Funding(ctx, cursor, o.call...)
		return p.Funding, p.HasAccount, p.NextCursor, err
	})
}

func optCursor(c string) *string {
	if c == "" {
		return nil
	}
	return &c
}

// DefaultMaxBusyRetries is how many times in a row AllFills and AllFunding ask again for a page
// while the futures provider is busy (see WithMaxBusyRetries).
const DefaultMaxBusyRetries = 3

// pageHistory walks a futures history listing (conformance/futures/history_paging.json): the
// first request sends no cursor, each next_cursor is sent back verbatim until it is null; an
// empty page whose next_cursor is the cursor just sent means the provider is busy: back off and
// ask again, at most maxBusyRetries times in a row, then fail with PAGING_STALLED; a page with
// rows whose next_cursor is the cursor just sent yields its rows, then fails with
// PAGING_CURSOR_REPEATED (never loop).
func pageHistory[T any](ctx context.Context, t *transport, op OperationID, o iterOptions,
	fetch func(ctx context.Context, cursor string) (rows []T, hasAccount bool, next *string, err error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		maxBusy := DefaultMaxBusyRetries
		if o.maxBusyRetries != nil {
			maxBusy = max(0, *o.maxBusyRetries)
		}
		cursor := ""
		stalls, n := 0, 0
		for {
			rows, hasAccount, next, err := fetch(ctx, cursor)
			if err != nil {
				yield(zero, err)
				return
			}
			if !hasAccount {
				return
			}
			repeated := cursor != "" && next != nil && *next == cursor
			if repeated && len(rows) == 0 {
				stalled := pagingError(CodePagingStalled, op, cursor, stalls+1)
				if stalls >= maxBusy {
					yield(zero, stalled)
					return
				}
				if err := t.backoff(ctx, op, stalls, stalled, ""); err != nil {
					yield(zero, err)
					return
				}
				stalls++
				continue
			}
			stalls = 0
			for _, row := range rows {
				if o.maxItems > 0 && n >= o.maxItems {
					return
				}
				if !yield(row, nil) {
					return
				}
				n++
			}
			if repeated {
				yield(zero, pagingError(CodePagingCursorRepeated, op, cursor, 1))
				return
			}
			if next == nil || *next == "" || (o.maxItems > 0 && n >= o.maxItems) {
				return
			}
			cursor = *next
		}
	}
}

// CodePagingStalled is the code of the *APIError that AllFills and AllFunding yield when the
// futures provider stays busy: an empty page handed back the cursor just sent more times in a
// row than WithMaxBusyRetries allows. It is made by the SDK, not sent by the API (Status 0). It
// is retryable: the rows yielded so far are not the whole history; resume later from
// Details["cursor"] (a string) with Fills or Funding.
const CodePagingStalled ErrorCode = "PAGING_STALLED"

// CodePagingCursorRepeated is the code of the *APIError that AllFills and AllFunding yield after
// a page WITH rows handed back the cursor just sent: a server error, since paging on would repeat
// rows forever. Made by the SDK (Status 0), not retryable. Details["cursor"] is that cursor.
const CodePagingCursorRepeated ErrorCode = "PAGING_CURSOR_REPEATED"

func pagingError(code ErrorCode, op OperationID, cursor string, attempts int) *APIError {
	e := &APIError{Code: code, Details: map[string]any{"cursor": cursor}}
	if code == CodePagingStalled {
		e.Retryable = true
		e.Message = fmt.Sprintf("%s: the futures provider is busy (an empty page returned the same cursor %d time(s) in a row); "+
			"retry later from Details[\"cursor\"]", operations[op].Path, attempts)
	} else {
		e.Message = fmt.Sprintf("%s: a page with rows returned the cursor it was sent; stopped so as not to repeat rows",
			operations[op].Path)
	}
	return e
}
