package cexy

import (
	"context"
	"errors"
	"iter"
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
	var maxDev *Amount
	if req.MaxRatioDeviationPercent != nil {
		maxDev = Ptr(Amount(*req.MaxRatioDeviationPercent))
	}
	if err := checkAmounts("Pools.Join", amountField{"base_amount", &req.BaseAmount},
		amountField{"quote_amount", &req.QuoteAmount}, amountField{"max_ratio_deviation_percent", maxDev}); err != nil {
		return JoinPoolResult{}, err
	}
	return getData[JoinPoolResult](ctx, s.t, call{op: OpJoinPool, pathParams: map[string]string{"symbol": symbol}, body: req}, opts)
}

// Exit removes liquidity. The Idempotency-Key makes retries safe.
func (s *PoolsService) Exit(ctx context.Context, symbol string, req ExitPoolRequest, opts ...CallOption) (ExitPoolResult, error) {
	if err := checkAmounts("Pools.Exit", amountField{"shares", &req.Shares}); err != nil {
		return ExitPoolResult{}, err
	}
	return getData[ExitPoolResult](ctx, s.t, call{op: OpExitPool, pathParams: map[string]string{"symbol": symbol}, body: req}, opts)
}

// ---------------------------------------------------------------------------------------
// Private (API key; read scope unless stated)
// ---------------------------------------------------------------------------------------

// AccountService: balances, ledger, notifications, sub-accounts and API keys.
type AccountService struct{ t *transport }

// Balances returns every balance.
func (s *AccountService) Balances(ctx context.Context, opts ...CallOption) ([]Balance, error) {
	return getData[[]Balance](ctx, s.t, call{op: OpListBalances}, opts)
}

// Balance returns the balance of one asset.
func (s *AccountService) Balance(ctx context.Context, asset string, opts ...CallOption) (Balance, error) {
	return getData[Balance](ctx, s.t, call{op: OpGetBalance, pathParams: map[string]string{"asset": asset}}, opts)
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
// account, and the server refuses a repeat before any funds move. The server does NOT honour
// Idempotency-Key on orders. After an ambiguous failure (connection error, timeout or 5xx) the
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
	idemKey := o.idempotencyKey
	if idemKey == "" {
		idemKey = newID() // sent, but not honoured for orders
	}
	c := call{op: OpPlaceOrder, body: order, idempotencyKey: idemKey}

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
		if berr := s.t.backoff(ctx, OpPlaceOrder, attempt, err, idemKey); berr != nil {
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
	idemKey := o.idempotencyKey
	if idemKey == "" {
		idemKey = newID() // sent, but not honoured for cancels
	}
	c := call{op: OpCancelOrder, pathParams: map[string]string{"order_id": orderID}, idempotencyKey: idemKey}
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
		if berr := s.t.backoff(ctx, OpCancelOrder, attempt, err, idemKey); berr != nil {
			return Order{}, berr
		}
	}
}

// CancelAll cancels every open order in one market, such as "BTC/USDT". An empty symbol is
// an error, so an account-wide cancel never happens by accident; use CancelAllMarkets for that.
//
// The server limits cancel-all to 30 calls a minute per account. It is naturally repeatable,
// so it is retried after connection errors; a retry reports only what that retry cancelled.
func (s *TradingService) CancelAll(ctx context.Context, symbol string, opts ...CallOption) (CancelAllResult, error) {
	if symbol == "" {
		return CancelAllResult{}, &ConfigError{Msg: `Trading.CancelAll: symbol is required ("BASE/QUOTE"); use CancelAllMarkets to cancel in every market`}
	}
	return getData[CancelAllResult](ctx, s.t, call{op: OpCancelAll, body: CancelAllRequest{Symbol: &symbol}}, opts)
}

// CancelAllMarkets cancels every open order in EVERY market.
func (s *TradingService) CancelAllMarkets(ctx context.Context, opts ...CallOption) (CancelAllResult, error) {
	return getData[CancelAllResult](ctx, s.t, call{op: OpCancelAll, body: CancelAllRequest{}}, opts)
}

// derefOr copies *p, or returns the zero value for nil.
func derefOr[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}
