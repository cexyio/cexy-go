package cexy

import (
	"context"
	"errors"
	"iter"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------------------
// Public market data (no credentials)
// ---------------------------------------------------------------------------------------

// MarketsService: markets, tickers, order books, public trades and candles.
type MarketsService struct{ t *transport }

// List returns every market with its current ticker.
func (s *MarketsService) List(ctx context.Context, opts ...CallOption) ([]Market, error) {
	return getData[[]Market](ctx, s.t, call{op: OpListMarkets}, opts)
}

// Get returns one market, for example "BTC/USDT".
func (s *MarketsService) Get(ctx context.Context, symbol string, opts ...CallOption) (Market, error) {
	return getData[Market](ctx, s.t, call{op: OpGetMarket, pathParams: map[string]string{"symbol": symbol}}, opts)
}

// OrderBook returns an order-book snapshot aggregated by price, with the realtime sequence it
// is current as of. To follow a book live, use WebSocket.OrderBook, which applies the sync rules.
func (s *MarketsService) OrderBook(ctx context.Context, symbol string, params *GetOrderBookParams, opts ...CallOption) (OrderBook, error) {
	return getData[OrderBook](ctx, s.t, call{op: OpGetOrderBook, pathParams: map[string]string{"symbol": symbol},
		query: params.values()}, opts)
}

// Trades returns one page of recent public trades.
func (s *MarketsService) Trades(ctx context.Context, symbol string, params *GetMarketTradesParams, opts ...CallOption) (*Page[PublicTrade], error) {
	return getPage[PublicTrade](ctx, s.t, call{op: OpGetMarketTrades, pathParams: map[string]string{"symbol": symbol},
		query: params.values()}, opts)
}

// AllTrades iterates over every public trade, fetching pages as needed.
func (s *MarketsService) AllTrades(ctx context.Context, symbol string, params *GetMarketTradesParams, opts ...IterOption) iter.Seq2[PublicTrade, error] {
	o := iterOpts(opts)
	p := derefOr(params)
	return paginate(ctx, p.Cursor, o.maxItems, func(ctx context.Context, cursor *string) (*Page[PublicTrade], error) {
		p.Cursor = cursor
		return s.Trades(ctx, symbol, &p, o.call...)
	})
}

// Candles returns OHLCV candles. params.Interval is required.
func (s *MarketsService) Candles(ctx context.Context, symbol string, params GetCandlesParams, opts ...CallOption) ([]Candle, error) {
	if params.Interval == "" {
		return nil, &ConfigError{Msg: "Markets.Candles: Interval is required"}
	}
	return getData[[]Candle](ctx, s.t, call{op: OpGetCandles, pathParams: map[string]string{"symbol": symbol},
		query: params.values()}, opts)
}

// AssetsService: the asset catalogue.
type AssetsService struct{ t *transport }

// List returns every asset.
func (s *AssetsService) List(ctx context.Context, opts ...CallOption) ([]Asset, error) {
	return getData[[]Asset](ctx, s.t, call{op: OpListAssets}, opts)
}

// Get returns one asset.
func (s *AssetsService) Get(ctx context.Context, symbol string, opts ...CallOption) (Asset, error) {
	return getData[Asset](ctx, s.t, call{op: OpGetAsset, pathParams: map[string]string{"symbol": symbol}}, opts)
}

// NetworksService: blockchain networks.
type NetworksService struct{ t *transport }

// List returns every network.
func (s *NetworksService) List(ctx context.Context, opts ...CallOption) ([]Network, error) {
	return getData[[]Network](ctx, s.t, call{op: OpListNetworks}, opts)
}

// FeesService: fee schedules.
type FeesService struct{ t *transport }

// List returns the fee schedules (maker and taker rates by tier).
func (s *FeesService) List(ctx context.Context, opts ...CallOption) ([]FeeSchedule, error) {
	return getData[[]FeeSchedule](ctx, s.t, call{op: OpListFeeSchedules}, opts)
}

// PoolsService: liquidity pools. Join and Exit need an API key with the trade scope.
type PoolsService struct{ t *transport }

// List returns every pool.
func (s *PoolsService) List(ctx context.Context, opts ...CallOption) ([]Pool, error) {
	return getData[[]Pool](ctx, s.t, call{op: OpListPools}, opts)
}

// Get returns one pool.
func (s *PoolsService) Get(ctx context.Context, symbol string, opts ...CallOption) (Pool, error) {
	return getData[Pool](ctx, s.t, call{op: OpGetPool, pathParams: map[string]string{"symbol": symbol}}, opts)
}

// Join adds liquidity. Amounts are decimal strings. The Idempotency-Key (generated, or set
// with WithIdempotencyKey) makes retries safe.
func (s *PoolsService) Join(ctx context.Context, symbol string, req JoinPoolRequest, opts ...CallOption) (JoinPoolResult, error) {
	if err := checkAmounts("Pools.Join", amountField{"base_amount", &req.BaseAmount},
		amountField{"quote_amount", &req.QuoteAmount},
		amountField{"max_ratio_deviation_percent", req.MaxRatioDeviationPercent}); err != nil {
		return JoinPoolResult{}, err
	}
	return getData[JoinPoolResult](ctx, s.t, call{op: OpJoinPool, pathParams: map[string]string{"symbol": symbol}, body: req,
		idempotent: true}, opts)
}

// Exit removes liquidity. The Idempotency-Key makes retries safe.
func (s *PoolsService) Exit(ctx context.Context, symbol string, req ExitPoolRequest, opts ...CallOption) (ExitPoolResult, error) {
	if err := checkAmounts("Pools.Exit", amountField{"shares", &req.Shares}); err != nil {
		return ExitPoolResult{}, err
	}
	return getData[ExitPoolResult](ctx, s.t, call{op: OpExitPool, pathParams: map[string]string{"symbol": symbol}, body: req,
		idempotent: true}, opts)
}

// ---------------------------------------------------------------------------------------
// Private (API key; read scope unless stated)
// ---------------------------------------------------------------------------------------

// AccountService: balances, ledger, notifications, sub-accounts and API keys.
type AccountService struct{ t *transport }

// Balances returns every balance.
//
// Balance.HeldIncoming lists incoming internal transfers still held. Their sum is ALREADY
// INCLUDED in Locked: never add them to Locked or Total again. At most 100 entries, soonest
// AvailableAt first (millisecond precision), with no sender identity. An entry disappears once
// the transfer is released (its amount moves to Available) or cancelled by the exchange.
// HeldIncoming is never nil: a server that omits the field decodes as an empty slice.
func (s *AccountService) Balances(ctx context.Context, opts ...CallOption) ([]Balance, error) {
	return s.balances(ctx, opts)
}

// ID returns the id of the account this API key belongs to (the same hex as the WebSocket's
// authenticated user id). WebSocket.LiveBalances uses it to check that REST snapshots and
// WebSocket events belong to the same account.
func (s *AccountService) ID(ctx context.Context, opts ...CallOption) (string, error) {
	r, err := getData[AccountId](ctx, s.t, call{op: OpGetAccountID}, opts)
	return r.UserID, err
}

func (s *AccountService) balances(ctx context.Context, opts []CallOption) ([]Balance, error) {
	bs, err := getData[[]Balance](ctx, s.t, call{op: OpListBalances}, opts)
	for i := range bs {
		bs[i].HeldIncoming = nonNilHeld(bs[i].HeldIncoming)
	}
	return bs, err
}

// Balance returns the balance of one asset. See Balances for HeldIncoming.
func (s *AccountService) Balance(ctx context.Context, asset string, opts ...CallOption) (Balance, error) {
	b, err := getData[Balance](ctx, s.t, call{op: OpGetBalance, pathParams: map[string]string{"asset": asset}}, opts)
	b.HeldIncoming = nonNilHeld(b.HeldIncoming)
	return b, err
}

func nonNilHeld(h []HeldIncoming) []HeldIncoming {
	if h == nil {
		return []HeldIncoming{}
	}
	return h
}

// Ledger returns one page of ledger entries.
func (s *AccountService) Ledger(ctx context.Context, params *GetLedgerParams, opts ...CallOption) (*Page[LedgerEntry], error) {
	return getPage[LedgerEntry](ctx, s.t, call{op: OpGetLedger, query: params.values()}, opts)
}

// AllLedger iterates over every ledger entry.
func (s *AccountService) AllLedger(ctx context.Context, params *GetLedgerParams, opts ...IterOption) iter.Seq2[LedgerEntry, error] {
	o := iterOpts(opts)
	p := derefOr(params)
	return paginate(ctx, p.Cursor, o.maxItems, func(ctx context.Context, cursor *string) (*Page[LedgerEntry], error) {
		p.Cursor = cursor
		return s.Ledger(ctx, &p, o.call...)
	})
}

// Notifications returns one page of notifications.
func (s *AccountService) Notifications(ctx context.Context, params *ListNotificationsParams, opts ...CallOption) (*Page[Notification], error) {
	return getPage[Notification](ctx, s.t, call{op: OpListNotifications, query: params.values()}, opts)
}

// AllNotifications iterates over every notification.
func (s *AccountService) AllNotifications(ctx context.Context, params *ListNotificationsParams, opts ...IterOption) iter.Seq2[Notification, error] {
	o := iterOpts(opts)
	p := derefOr(params)
	return paginate(ctx, p.Cursor, o.maxItems, func(ctx context.Context, cursor *string) (*Page[Notification], error) {
		p.Cursor = cursor
		return s.Notifications(ctx, &p, o.call...)
	})
}

// SubAccounts returns the sub-accounts.
func (s *AccountService) SubAccounts(ctx context.Context, opts ...CallOption) ([]SubAccount, error) {
	return getData[[]SubAccount](ctx, s.t, call{op: OpListSubAccounts}, opts)
}

// SubAccountBalances returns a sub-account's balances, read by its PARENT account: the same
// shape as Balances (zero balances omitted), including HeldIncoming, whose sum is already
// inside Locked; HeldIncoming is never nil. The server currently returns them ordered by asset
// symbol; don't rely on the order. An id that is not one of the caller's sub-accounts (or a
// call made with the sub-account's own key) returns an *APIError matching ErrNotFound, and it
// is not retried; a sub-account's own key reads its balances with Balances. A malformed id gets
// 400 (ErrValidation); a key without the read scope gets 403 FORBIDDEN (ErrForbidden). id must
// be non-empty and not "." or ".." (a *ConfigError before any request); it is sent as one URL
// path segment.
func (s *AccountService) SubAccountBalances(ctx context.Context, id string, opts ...CallOption) ([]Balance, error) {
	bs, err := getData[[]Balance](ctx, s.t, call{op: OpSubAccountBalances, pathParams: map[string]string{"id": id}}, opts)
	for i := range bs {
		bs[i].HeldIncoming = nonNilHeld(bs[i].HeldIncoming)
	}
	return bs, err
}

// APIKeys returns your API keys (metadata only; secrets are never returned).
func (s *AccountService) APIKeys(ctx context.Context, opts ...CallOption) ([]APIKey, error) {
	return getData[[]APIKey](ctx, s.t, call{op: OpListAPIKeys}, opts)
}

// ExportsService: CSV exports. Each method returns the CSV text.
type ExportsService struct{ t *transport }

// Deposits exports deposits as CSV.
func (s *ExportsService) Deposits(ctx context.Context, params *ExportParams, opts ...CallOption) (string, error) {
	return getText(ctx, s.t, call{op: OpExportDeposits, query: params.values()}, opts)
}

// Ledger exports the ledger as CSV.
func (s *ExportsService) Ledger(ctx context.Context, params *ExportParams, opts ...CallOption) (string, error) {
	return getText(ctx, s.t, call{op: OpExportLedger, query: params.values()}, opts)
}

// Orders exports orders as CSV.
func (s *ExportsService) Orders(ctx context.Context, params *ExportParams, opts ...CallOption) (string, error) {
	return getText(ctx, s.t, call{op: OpExportOrders, query: params.values()}, opts)
}

// Trades exports your trades as CSV.
func (s *ExportsService) Trades(ctx context.Context, params *ExportParams, opts ...CallOption) (string, error) {
	return getText(ctx, s.t, call{op: OpExportTrades, query: params.values()}, opts)
}

// Withdrawals exports withdrawals as CSV.
func (s *ExportsService) Withdrawals(ctx context.Context, params *ExportParams, opts ...CallOption) (string, error) {
	return getText(ctx, s.t, call{op: OpExportWithdrawals, query: params.values()}, opts)
}

// WalletService: wallet reads. API keys can never withdraw or transfer; there are no such methods.
type WalletService struct{ t *transport }

// Deposits returns one page of deposits.
func (s *WalletService) Deposits(ctx context.Context, params *ListDepositsParams, opts ...CallOption) (*Page[Deposit], error) {
	return getPage[Deposit](ctx, s.t, call{op: OpListDeposits, query: params.values()}, opts)
}

// AllDeposits iterates over every deposit.
func (s *WalletService) AllDeposits(ctx context.Context, params *ListDepositsParams, opts ...IterOption) iter.Seq2[Deposit, error] {
	o := iterOpts(opts)
	p := derefOr(params)
	return paginate(ctx, p.Cursor, o.maxItems, func(ctx context.Context, cursor *string) (*Page[Deposit], error) {
		p.Cursor = cursor
		return s.Deposits(ctx, &p, o.call...)
	})
}

// Deposit returns one deposit.
func (s *WalletService) Deposit(ctx context.Context, depositID string, opts ...CallOption) (Deposit, error) {
	return getData[Deposit](ctx, s.t, call{op: OpGetDeposit, pathParams: map[string]string{"deposit_id": depositID}}, opts)
}

// Withdrawals returns one page of withdrawals.
func (s *WalletService) Withdrawals(ctx context.Context, params *ListWithdrawalsParams, opts ...CallOption) (*Page[Withdrawal], error) {
	return getPage[Withdrawal](ctx, s.t, call{op: OpListWithdrawals, query: params.values()}, opts)
}

// AllWithdrawals iterates over every withdrawal.
func (s *WalletService) AllWithdrawals(ctx context.Context, params *ListWithdrawalsParams, opts ...IterOption) iter.Seq2[Withdrawal, error] {
	o := iterOpts(opts)
	p := derefOr(params)
	return paginate(ctx, p.Cursor, o.maxItems, func(ctx context.Context, cursor *string) (*Page[Withdrawal], error) {
		p.Cursor = cursor
		return s.Withdrawals(ctx, &p, o.call...)
	})
}

// Withdrawal returns one withdrawal.
func (s *WalletService) Withdrawal(ctx context.Context, withdrawalID string, opts ...CallOption) (Withdrawal, error) {
	return getData[Withdrawal](ctx, s.t, call{op: OpGetWithdrawal, pathParams: map[string]string{"withdrawal_id": withdrawalID}}, opts)
}

// WithdrawalAddresses returns the saved withdrawal addresses.
func (s *WalletService) WithdrawalAddresses(ctx context.Context, opts ...CallOption) ([]WithdrawalAddress, error) {
	return getData[[]WithdrawalAddress](ctx, s.t, call{op: OpListWithdrawalAddresses}, opts)
}

// DepositAddress returns your deposit address for an asset on a network.
//
// Side effect: the first call for an asset and network CREATES the address, and it is
// permanent; later calls return the same address. Always send the memo too when the response
// has one, or the deposit may be unrecoverable.
func (s *WalletService) DepositAddress(ctx context.Context, params DepositAddressParams, opts ...CallOption) (DepositAddress, error) {
	if params.Asset == "" || params.Network == "" {
		return DepositAddress{}, &ConfigError{Msg: "Wallet.DepositAddress: Asset and Network are required"}
	}
	return getData[DepositAddress](ctx, s.t, call{op: OpDepositAddress, query: params.values()}, opts)
}

// ---------------------------------------------------------------------------------------
// Trading
// ---------------------------------------------------------------------------------------

// TradingService: orders and your trades. Placing and cancelling need the trade scope.
type TradingService struct{ t *transport }

// OpenOrders returns open orders, optionally filtered by market or status.
func (s *TradingService) OpenOrders(ctx context.Context, params *ListOpenOrdersParams, opts ...CallOption) ([]Order, error) {
	return getData[[]Order](ctx, s.t, call{op: OpListOpenOrders, query: params.values()}, opts)
}

// Order returns one order.
func (s *TradingService) Order(ctx context.Context, orderID string, opts ...CallOption) (Order, error) {
	return getData[Order](ctx, s.t, call{op: OpGetOrder, pathParams: map[string]string{"order_id": orderID}}, opts)
}

// OrderByClientID returns the order with your client_order_id.
func (s *TradingService) OrderByClientID(ctx context.Context, clientOrderID string, opts ...CallOption) (Order, error) {
	return getData[Order](ctx, s.t, call{op: OpGetOrderByClientID, pathParams: map[string]string{"client_order_id": clientOrderID}}, opts)
}

// OrderHistory returns one page of past orders.
func (s *TradingService) OrderHistory(ctx context.Context, params *OrderHistoryParams, opts ...CallOption) (*Page[Order], error) {
	return getPage[Order](ctx, s.t, call{op: OpOrderHistory, query: params.values()}, opts)
}

// AllOrderHistory iterates over every past order.
func (s *TradingService) AllOrderHistory(ctx context.Context, params *OrderHistoryParams, opts ...IterOption) iter.Seq2[Order, error] {
	o := iterOpts(opts)
	p := derefOr(params)
	return paginate(ctx, p.Cursor, o.maxItems, func(ctx context.Context, cursor *string) (*Page[Order], error) {
		p.Cursor = cursor
		return s.OrderHistory(ctx, &p, o.call...)
	})
}

// Trades returns one page of your executions.
func (s *TradingService) Trades(ctx context.Context, params *TradeHistoryParams, opts ...CallOption) (*Page[Fill], error) {
	return getPage[Fill](ctx, s.t, call{op: OpTradeHistory, query: params.values()}, opts)
}

// AllTrades iterates over every execution.
func (s *TradingService) AllTrades(ctx context.Context, params *TradeHistoryParams, opts ...IterOption) iter.Seq2[Fill, error] {
	o := iterOpts(opts)
	p := derefOr(params)
	return paginate(ctx, p.Cursor, o.maxItems, func(ctx context.Context, cursor *string) (*Page[Fill], error) {
		p.Cursor = cursor
		return s.Trades(ctx, &p, o.call...)
	})
}

// PlaceOrderResult is what PlaceOrder returns.
type PlaceOrderResult struct {
	PlaceOrderResponse
	// The client_order_id that was sent (generated when you did not set one).
	ClientOrderID string
	// True when the POST failed ambiguously and the order was then found by ClientOrderID.
	// Fills is empty then; use Trades for the executions.
	Recovered bool
}

// PlaceOrder places a REAL order (needs the trade scope). Amounts are decimal strings.
//
// Retry safety rests on client_order_id (a UUID is generated when absent): it is unique per
// account, and the server refuses a repeat before any funds move. No Idempotency-Key is sent:
// the server does not honour one on orders. After an ambiguous failure (connection error, timeout or 5xx) the
// SDK first looks the order up by client_order_id and returns it if it exists (Recovered);
// only if it does not exist is the order sent again, with the same client_order_id, so a late
// first attempt makes the resend fail as a duplicate, which the lookup resolves again.
// It returns *OrderStateUnknownError when even the lookup fails.
func (s *TradingService) PlaceOrder(ctx context.Context, order PlaceOrderRequest, opts ...CallOption) (PlaceOrderResult, error) {
	if order.Symbol == "" || order.Side == "" || order.Type == "" {
		return PlaceOrderResult{}, &ConfigError{Msg: "Trading.PlaceOrder: Symbol, Side and Type are required"}
	}
	if err := checkAmounts("Trading.PlaceOrder", amountField{"price", order.Price}, amountField{"quantity", order.Quantity},
		amountField{"quote_quantity", order.QuoteQuantity}, amountField{"stop_price", order.StopPrice}); err != nil {
		return PlaceOrderResult{}, err
	}
	clientOrderID := newID()
	if order.ClientOrderID != nil && *order.ClientOrderID != "" {
		clientOrderID = *order.ClientOrderID
	}
	order.ClientOrderID = &clientOrderID
	o := s.t.options(opts)
	c := call{op: OpPlaceOrder, body: order}

	for attempt := 0; ; attempt++ {
		raw, err := s.t.attempt(ctx, c, o)
		if err == nil {
			res, derr := decodeData[PlaceOrderResponse](OpPlaceOrder, raw)
			return PlaceOrderResult{PlaceOrderResponse: res, ClientOrderID: clientOrderID}, derr
		}
		if ctx.Err() != nil {
			return PlaceOrderResult{}, err
		}
		var ae *APIError
		duplicateAfterRetry := attempt > 0 && errors.As(err, &ae) && errors.Is(err, ErrConflict) &&
			(ae.Code == CodeAlreadyExists || ae.Code == CodeIdempotencyKeyConflict)
		if isAmbiguous(err) || duplicateAfterRetry {
			existing, found, lerr := s.lookup(ctx, clientOrderID, err, o)
			if lerr != nil {
				return PlaceOrderResult{}, lerr
			}
			if found {
				return PlaceOrderResult{PlaceOrderResponse: PlaceOrderResponse{Order: existing, Fills: []Fill{}},
					ClientOrderID: clientOrderID, Recovered: true}, nil
			}
			if duplicateAfterRetry || attempt >= o.maxRetries {
				return PlaceOrderResult{}, err
			}
		} else if !isRetryable(err) || attempt >= o.maxRetries {
			// Definitive refusals that did not execute (such as 429) are resent below.
			return PlaceOrderResult{}, err
		}
		if berr := s.t.backoff(ctx, OpPlaceOrder, attempt, err, ""); berr != nil {
			return PlaceOrderResult{}, berr
		}
	}
}

func (s *TradingService) lookup(ctx context.Context, clientOrderID string, original error, o callOptions) (Order, bool, error) {
	ord, err := s.OrderByClientID(ctx, clientOrderID, WithTimeout(o.timeout))
	if err == nil {
		return ord, true, nil
	}
	if errors.Is(err, ErrNotFound) {
		return Order{}, false, nil
	}
	if ctx.Err() != nil {
		return Order{}, false, ctx.Err()
	}
	return Order{}, false, &OrderStateUnknownError{ClientOrderID: clientOrderID, Err: original}
}

// CancelOrder cancels one order (needs the trade scope). It is retried on connection errors
// and retryable responses. If a RETRY gets INVALID_STATE (the order is no longer open,
// typically because the first attempt did cancel it), the cancel is treated as done and the
// order is fetched and returned. INVALID_STATE on the first attempt is returned as an error
// (for example, the order was already filled).
func (s *TradingService) CancelOrder(ctx context.Context, orderID string, opts ...CallOption) (Order, error) {
	o := s.t.options(opts)
	c := call{op: OpCancelOrder, pathParams: map[string]string{"order_id": orderID}}
	for attempt := 0; ; attempt++ {
		raw, err := s.t.attempt(ctx, c, o)
		if err == nil {
			return decodeData[Order](OpCancelOrder, raw)
		}
		if ctx.Err() != nil {
			return Order{}, err
		}
		var ae *APIError
		if attempt > 0 && errors.As(err, &ae) && ae.Code == CodeInvalidState {
			return s.Order(ctx, orderID, WithTimeout(o.timeout))
		}
		if !isRetryable(err) || attempt >= o.maxRetries {
			return Order{}, err
		}
		if berr := s.t.backoff(ctx, OpCancelOrder, attempt, err, ""); berr != nil {
			return Order{}, berr
		}
	}
}

// CancelAll cancels every open order in one market, such as "BTC/USDT", in one request,
// including stop orders that have not triggered yet (status pending_trigger). An
// empty symbol is an error, so an account-wide cancel never happens by accident; use
// CancelAllMarkets for that. An unknown symbol is an APIError matching ErrNotFound.
//
// One call handles at most 500 orders. Every order it handled is in exactly one of Cancelled,
// AlreadyClosed (it closed on its own first: not a failure) and Failed (with the reason in
// Failures; INVALID_STATE means it was still being placed). HasMore means there are more:
// call again, or use CancelAllUntilDone.
//
// The server limits cancel-all to 30 calls a minute per account (ErrRateLimited, retried after
// the server's wait by the normal retry policy). It is naturally repeatable, so it is retried
// after connection errors; a retry reports only what that retry did. No Idempotency-Key is
// sent: the server does not honour one here.
func (s *TradingService) CancelAll(ctx context.Context, symbol string, opts ...CallOption) (CancelAllResult, error) {
	if symbol == "" {
		return CancelAllResult{}, &ConfigError{Msg: `Trading.CancelAll: symbol is required ("BASE/QUOTE"); use CancelAllMarkets to cancel in every market`}
	}
	return s.cancelAllOnce(ctx, &symbol, opts)
}

// CancelAllMarkets cancels every open order in EVERY market, in one request. See CancelAll.
func (s *TradingService) CancelAllMarkets(ctx context.Context, opts ...CallOption) (CancelAllResult, error) {
	return s.cancelAllOnce(ctx, nil, opts)
}

// CancelAllAfter arms, re-arms or disarms the dead-man switch for one market, such as
// "BTC/USDT": if it is not armed again within timeout, the server cancels every open order in
// that market. A timeout of exactly 0 disarms the switch. An empty or blank symbol is a
// ConfigError (the server would read it as "every market"); use CancelAllAfterMarkets for that.
//
// timeout must be a whole number of milliseconds: a negative one, a non-zero one below 1 ms and
// a fractional one are a ConfigError, so a non-zero timeout never turns into 0 (which would
// disarm). The SDK checks nothing else: the server owns the range (5000 to 600000 ms, else
// ErrValidation).
//
// A market maker should arm about every 2 seconds with a 10 second timeout. Take the local
// deadline from the moment the call started (not when it returned) and never compare the local
// clock with the returned Deadline: use ServerTime to measure it. When the switch fires it is
// cleared, so quoting again needs a new arm. The per-market switch and the all-markets switch
// are separate and each fires on its own. A disarm (0) only switches off the scope it names.
//
// Repeating an arm is harmless, so it is retried after connection errors like CancelAll. An
// arm still in flight can land after a later disarm and re-arm it: if the switch must be off,
// disarm once more after any retried arm. No endpoint reads the switch.
//
// A house account that places an order without an armed switch gets DEAD_MAN_NOT_ARMED
// (ErrDeadManNotArmed, also ErrConflict, never retryable) from PlaceOrder: stop quoting, arm,
// and do not retry the placement blindly.
func (s *TradingService) CancelAllAfter(ctx context.Context, symbol string, timeout time.Duration, opts ...CallOption) (CancelAllAfter, error) {
	if strings.TrimSpace(symbol) == "" {
		return CancelAllAfter{}, &ConfigError{Msg: `Trading.CancelAllAfter: symbol is required ("BASE/QUOTE"); use CancelAllAfterMarkets for every market`}
	}
	return s.cancelAllAfterOnce(ctx, &symbol, timeout, opts)
}

// CancelAllAfterMarkets is CancelAllAfter for the all-markets switch: it sends an explicit
// "symbol": null. It is separate from the per-market switches. See CancelAllAfter.
func (s *TradingService) CancelAllAfterMarkets(ctx context.Context, timeout time.Duration, opts ...CallOption) (CancelAllAfter, error) {
	return s.cancelAllAfterOnce(ctx, nil, timeout, opts)
}

// cancelAllAfterBody always carries "symbol", as null for every market: the generated request
// omits a nil symbol, and the server reads a missing one as every market too, but the SDK says so.
type cancelAllAfterBody struct {
	Symbol    *string `json:"symbol"`
	TimeoutMs int64   `json:"timeout_ms"`
}

func (s *TradingService) cancelAllAfterOnce(ctx context.Context, symbol *string, timeout time.Duration, opts []CallOption) (CancelAllAfter, error) {
	switch {
	case timeout < 0:
		return CancelAllAfter{}, &ConfigError{Msg: "Trading.CancelAllAfter: timeout is negative; use 0 to disarm"}
	case timeout > 0 && timeout < time.Millisecond:
		return CancelAllAfter{}, &ConfigError{Msg: "Trading.CancelAllAfter: timeout is below 1 ms; use 0 to disarm"}
	case timeout%time.Millisecond != 0:
		return CancelAllAfter{}, &ConfigError{Msg: "Trading.CancelAllAfter: timeout must be a whole number of milliseconds"}
	}
	return getData[CancelAllAfter](ctx, s.t, call{op: OpCancelAllAfter,
		body: cancelAllAfterBody{Symbol: symbol, TimeoutMs: int64(timeout / time.Millisecond)}}, opts)
}

func (s *TradingService) cancelAllOnce(ctx context.Context, symbol *string, opts []CallOption) (CancelAllResult, error) {
	res, err := getData[CancelAllResult](ctx, s.t, call{op: OpCancelAll, body: CancelAllRequest{Symbol: symbol}}, opts)
	if err != nil {
		return res, err
	}
	return withV2Slices(res), nil
}

// cancelAllRound is one round of CancelAllUntilDone: exactly one HTTP request, no retries (the
// loop owns them).
func (s *TradingService) cancelAllRound(ctx context.Context, symbol *string, o callOptions) (CancelAllResult, error) {
	o.maxRetries = 0
	raw, err := s.t.attempt(ctx, call{op: OpCancelAll, body: CancelAllRequest{Symbol: symbol}}, o)
	if err != nil {
		return CancelAllResult{}, err
	}
	res, err := decodeData[CancelAllResult](OpCancelAll, raw)
	if err != nil {
		return res, err
	}
	return withV2Slices(res), nil
}

// withV2Slices keeps the v2 slices non-nil (older servers omit them).
func withV2Slices(res CancelAllResult) CancelAllResult {
	// Older servers omit the v2 fields; keep the slices non-nil either way.
	if res.Cancelled == nil {
		res.Cancelled = []string{}
	}
	if res.AlreadyClosed == nil {
		res.AlreadyClosed = []string{}
	}
	if res.Failed == nil {
		res.Failed = []string{}
	}
	if res.Failures == nil {
		res.Failures = []CancelFailure{}
	}
	return res
}

// CancelAllOptions selects the target of CancelAllUntilDone and bounds its loop. Exactly one of
// Symbol and AllMarkets must be set, so an account-wide cancel is always explicit.
type CancelAllOptions struct {
	Symbol     string        // one market, such as "BTC/USDT"
	AllMarkets bool          // every market
	MaxRounds  int           // calls at most; default 20
	TimeBudget time.Duration // stop before a wait would reach this; default 120 s
}

// Reasons CancelAllUntilDone stopped.
const (
	CancelStoppedDone       = "done"
	CancelStoppedMaxRounds  = "max_rounds"
	CancelStoppedTimeBudget = "time_budget"
)

// CancelAllSummary merges the rounds of CancelAllUntilDone by order id; an order's latest state
// wins (an order that failed in one round and was cancelled in a later one is only in Cancelled).
type CancelAllSummary struct {
	Cancelled     []string
	AlreadyClosed []string
	Failed        []string        // still failed after the last round
	Failures      []CancelFailure // the latest reason for each order in Failed
	HasMore       bool            // the last successful round's has_more
	Rounds        int             // HTTP requests sent (one per round)
	Stopped       string          // CancelStoppedDone, CancelStoppedMaxRounds or CancelStoppedTimeBudget
	LastErrorCode string          // set when the last round was a retryable error (e.g. RATE_LIMITED)
}

var cancelBackoff = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second}

// CancelAllUntilDone repeats cancel-all until nothing is left to do: while HasMore is set, or
// while an order failed with INVALID_STATE (still being placed) or SERVICE_UNAVAILABLE.
//
// Each round is exactly one HTTP request: the loop owns the retries, so it never sends more
// than MaxRounds requests. After a round with no progress (nothing cancelled or already closed)
// it waits 1, 2, 4, 8, then 15 s before the next call; any progress resets the wait. A round
// that fails with a retryable error counts as a round without progress: after a 429 the loop
// waits the server's Retry-After exactly (without advancing the back-off), after any other
// retryable error the next back-off step. It never takes a wait that would bring the elapsed
// time to or past TimeBudget, or a server wait above MaxServerWait: it stops with
// CancelStoppedTimeBudget (and LastErrorCode when the last round failed). It also stops after
// MaxRounds requests. The server closes an order stuck being placed within about 90 s.
// Failure codes other than INVALID_STATE and SERVICE_UNAVAILABLE are returned in the summary,
// never retried.
//
// A non-retryable error (or a cancelled ctx) is returned with the summary so far.
func (s *TradingService) CancelAllUntilDone(ctx context.Context, o CancelAllOptions, opts ...CallOption) (CancelAllSummary, error) {
	var symbol *string
	switch {
	case o.Symbol != "" && o.AllMarkets:
		return CancelAllSummary{}, &ConfigError{Msg: "Trading.CancelAllUntilDone: set Symbol or AllMarkets, not both"}
	case o.Symbol != "":
		sym := o.Symbol
		symbol = &sym
	case !o.AllMarkets:
		return CancelAllSummary{}, &ConfigError{Msg: `Trading.CancelAllUntilDone: set Symbol ("BASE/QUOTE") or AllMarkets: true`}
	}
	maxRounds := o.MaxRounds
	if maxRounds <= 0 {
		maxRounds = 20
	}
	budget := o.TimeBudget
	if budget <= 0 {
		budget = 120 * time.Second
	}
	co := s.t.options(opts)

	m := newCancelMerge()
	start := s.t.now()
	step := 0
	hasMore := false
	for round := 1; ; round++ {
		// A limiter block (e.g. X-RateLimit-Remaining 0 with a Reset) would stall the next request
		// inside acquire, invisible to this loop: count it against the budget before calling.
		if round > 1 && s.t.limiter != nil {
			if w := s.t.limiter.pendingWait(); w > 0 && s.t.now().Sub(start)+w >= budget {
				return m.summary(round-1, CancelStoppedTimeBudget, hasMore, string(CodeRateLimited)), nil
			}
		}
		res, err := s.cancelAllRound(ctx, symbol, co)
		lastCode := ""
		var d time.Duration
		if err != nil {
			if ctx.Err() != nil || !isRetryable(err) {
				return m.summary(round, "", hasMore, ""), err
			}
			var ae *APIError
			if errors.As(err, &ae) {
				lastCode = string(ae.Code)
			} else {
				lastCode = "CONNECTION_ERROR"
			}
			if round >= maxRounds {
				return m.summary(round, CancelStoppedMaxRounds, hasMore, lastCode), nil
			}
			if errors.As(err, &ae) && errors.Is(err, ErrRateLimited) && ae.RetryAfter > 0 {
				d = ae.RetryAfter // the server's wait, exactly; the back-off does not advance
			} else {
				d = cancelBackoff[min(step, len(cancelBackoff)-1)]
				step++
			}
		} else {
			m.add(res)
			hasMore = res.HasMore
			retry := false
			for _, f := range res.Failures {
				if f.Code == string(CodeInvalidState) || f.Code == string(CodeServiceUnavailable) {
					retry = true
				}
			}
			if !res.HasMore && !retry {
				return m.summary(round, CancelStoppedDone, hasMore, ""), nil
			}
			if round >= maxRounds {
				return m.summary(round, CancelStoppedMaxRounds, hasMore, ""), nil
			}
			if len(res.Cancelled)+len(res.AlreadyClosed) > 0 {
				step = 0
				if s.t.now().Sub(start) >= budget {
					return m.summary(round, CancelStoppedTimeBudget, hasMore, ""), nil
				}
				continue
			}
			d = cancelBackoff[min(step, len(cancelBackoff)-1)]
			step++
		}
		if d > MaxServerWait || s.t.now().Sub(start)+d >= budget {
			return m.summary(round, CancelStoppedTimeBudget, hasMore, lastCode), nil
		}
		if err := s.t.sleep(ctx, d); err != nil {
			return m.summary(round, "", hasMore, lastCode), err
		}
	}
}

type cancelMerge struct {
	order    []string
	state    map[string]string
	failures map[string]CancelFailure
}

func newCancelMerge() *cancelMerge {
	return &cancelMerge{state: map[string]string{}, failures: map[string]CancelFailure{}}
}

func (m *cancelMerge) set(id, st string) {
	if _, seen := m.state[id]; !seen {
		m.order = append(m.order, id)
	}
	m.state[id] = st
}

func (m *cancelMerge) add(r CancelAllResult) {
	for _, id := range r.Cancelled {
		m.set(id, "cancelled")
	}
	for _, id := range r.AlreadyClosed {
		m.set(id, "already_closed")
	}
	reasons := map[string]CancelFailure{}
	for _, f := range r.Failures {
		reasons[f.OrderID] = f
	}
	for _, id := range r.Failed {
		m.set(id, "failed")
		if f, ok := reasons[id]; ok {
			m.failures[id] = f
		} else {
			m.failures[id] = CancelFailure{OrderID: id}
		}
	}
}

func (m *cancelMerge) summary(rounds int, stopped string, hasMore bool, lastErrorCode string) CancelAllSummary {
	out := CancelAllSummary{Cancelled: []string{}, AlreadyClosed: []string{}, Failed: []string{},
		Failures: []CancelFailure{}, HasMore: hasMore, Rounds: rounds, Stopped: stopped, LastErrorCode: lastErrorCode}
	for _, id := range m.order {
		switch m.state[id] {
		case "cancelled":
			out.Cancelled = append(out.Cancelled, id)
		case "already_closed":
			out.AlreadyClosed = append(out.AlreadyClosed, id)
		default:
			out.Failed = append(out.Failed, id)
			out.Failures = append(out.Failures, m.failures[id])
		}
	}
	return out
}

// derefOr copies *p, or returns the zero value for nil.
func derefOr[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}
