package cexy

// Names from before the server's futures_ operationId prefix (API spec 2026-10-14). They are
// aliases of the new names, so existing code keeps compiling.

// Deprecated: use FuturesCandlesParams.
type CandlesParams = FuturesCandlesParams

// Deprecated: use FuturesFillsParams.
type FillsParams = FuturesFillsParams

// Deprecated: use FuturesFundingParams.
type FundingParams = FuturesFundingParams

// Deprecated: use FuturesOrderbookParams.
type OrderbookParams = FuturesOrderbookParams

// Deprecated: use FuturesTradesParams.
type TradesParams = FuturesTradesParams

// Deprecated: the futures operation ids now start with futures_; use the OpFutures* constants.
const (
	OpCandles    = OpFuturesCandles
	OpFills      = OpFuturesFills
	OpFunding    = OpFuturesFunding
	OpMarket     = OpFuturesMarket
	OpMarkets    = OpFuturesMarkets
	OpOpenOrders = OpFuturesOpenOrders
	OpOrderbook  = OpFuturesOrderbook
	OpPositions  = OpFuturesPositions
	OpTrades     = OpFuturesTrades
)
