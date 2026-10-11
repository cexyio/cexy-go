package cexy

import (
	"context"
	"fmt"
	"regexp"
)

// Batch place and batch cancel. The rules are the shared conformance cases in
// cexy-api-spec/conformance/trading/batch_orders.json.

// Batch limits.
const (
	MaxBatchOrders  = 20 // orders in one PlaceOrders call
	MaxCancelOrders = 50 // entries in one CancelOrders call
)

// clientOrderIDRE is the server's rule for a client order id (after it trims surrounding
// whitespace, which the SDK refuses instead, so the id in a result is always the one sent).
var clientOrderIDRE = regexp.MustCompile(`^[A-Za-z0-9_\-.:]{1,64}$`)

// PlaceOrdersItem is one order of PlaceOrders: a PlaceOrderRequest without the symbol. A nil
// ClientOrderID is generated (a UUID); one you set must be 1 to 64 characters of
// A-Z a-z 0-9 _ - . : (an empty one is refused).
type PlaceOrdersItem struct {
	ClientOrderID    *string           `json:"client_order_id,omitempty"`
	Side             OrderSide         `json:"side"`
	Type             OrderType         `json:"type"`
	Quantity         *Amount           `json:"quantity,omitempty"`
	Price            *Amount           `json:"price,omitempty"`
	QuoteQuantity    *Amount           `json:"quote_quantity,omitempty"`
	StopPrice        *Amount           `json:"stop_price,omitempty"`
	TimeInForce      *TimeInForce      `json:"time_in_force,omitempty"`
	TriggerDirection *TriggerDirection `json:"trigger_direction,omitempty"`
}

// BatchOutcome is what became of one order or entry of a batch call.
type BatchOutcome string

const (
	// OutcomePlaced: PlaceOrders placed it now; Order and Fills are set.
	OutcomePlaced BatchOutcome = "placed"
	// OutcomeCancelled: CancelOrders cancelled it now; Order is the cancelled order.
	OutcomeCancelled BatchOutcome = "cancelled"
	// OutcomeFailed: refused; Error says why. ALREADY_EXISTS here means the client_order_id was
	// used before this call.
	OutcomeFailed BatchOutcome = "failed"
	// OutcomeRecovered: an earlier attempt of this call acted on it and its answer was lost, so
	// the retry was refused (ALREADY_EXISTS for a placement, INVALID_STATE for a cancel).
	// OrderID and Status say what became of it.
	OutcomeRecovered BatchOutcome = "recovered"
	// OutcomeUnknown: as recovered, but its state could not be read. A placement was not placed
	// again: look it up with Trading.OrderByClientID.
	OutcomeUnknown BatchOutcome = "unknown"
)

// BatchOrderOutcome is one order of PlaceOrders, in request order.
//
// A recovered order's Status may be OrderStatusRejected: the server refused it for funds, its
// client_order_id is spent, and it must not be placed again under that id. Order is set for a
// placed order, and for a recovered one only when the SDK had to look it up.
type BatchOrderOutcome struct {
	Index         int
	ClientOrderID string
	Outcome       BatchOutcome
	OrderID       string
	Status        OrderStatus
	Order         *Order
	Fills         []Fill
	Error         *ErrorBody
}

// BatchPlaceResult is what PlaceOrders did. The counts are computed from Results, never copied
// from the server (whose counts describe only the last attempt): Placed counts placed and
// recovered orders that were not rejected, Failed the refused ones (a recovered rejected one
// included), Unknown those whose state could not be read.
type BatchPlaceResult struct {
	Results []BatchOrderOutcome
	Placed  int
	Failed  int
	Unknown int
}

// CancelEntryOutcome is one entry of CancelOrders, in request order. OrderID and
// ClientOrderID are as sent (OrderID is the order's id once it is known). A recovered entry's
// Status says cancelled or, for example, filled.
type CancelEntryOutcome struct {
	Index         int
	Outcome       BatchOutcome
	OrderID       string
	ClientOrderID string
	Status        OrderStatus
	Order         *Order
	Error         *ErrorBody
}

// BatchCancelResult is what CancelOrders did. Cancelled counts entries now cancelled
// (recovered ones whose status is cancelled included), Failed the rest whose state is known,
// Unknown those whose state could not be read.
type BatchCancelResult struct {
	Results   []CancelEntryOutcome
	Cancelled int
	Failed    int
	Unknown   int
}

// PlaceOrders places 1 to 20 REAL orders in one market (needs the trade scope), each on its own
// and in order: the batch is not atomic, and one refusal does not stop the rest. Amounts are
// decimal strings.
//
// A ClientOrderID (a UUID) is generated for each order without one (nil); your own ids must
// be 1 to 64 characters of A-Z a-z 0-9 _ - . : and distinct within the batch (a *ConfigError
// before anything is sent). The ids sent are in the results.
//
// Retry safety: the body, ids included, is built once and every retry resends it unchanged.
// The server refuses an id it already placed, so nothing is placed twice: after a lost answer
// (a connection error after sending, or 500, 502 or 504) such an order comes back
// OutcomeRecovered with its OrderID and Status, read from the refusal's details or, when they
// are missing, looked up once by client_order_id (OutcomeUnknown if that fails). A refusal of
// the whole batch (400, 403, 404) is returned and never retried; a batch larger than the key's
// whole budget (400 with a number in Details["limit"]) must be split by the caller. A 429 is
// waited out and the same body resent. Results are in request order.
func (s *TradingService) PlaceOrders(ctx context.Context, symbol string, orders []PlaceOrdersItem, opts ...CallOption) (BatchPlaceResult, error) {
	items, err := checkPlaceOrders(symbol, orders)
	if err != nil {
		return BatchPlaceResult{}, err
	}
	var attempts []attemptKind
	c := call{op: OpPlaceBatch, body: BatchOrderRequest{Symbol: symbol, Orders: items}, cost: len(items),
		orderActions: len(items), attempts: &attempts}
	data, err := getData[BatchOrder](ctx, s.t, c, opts)
	if err != nil {
		return BatchPlaceResult{}, err
	}
	ambiguous := afterAmbiguous(attempts)
	o := s.t.options(opts)
	results := make([]BatchOrderOutcome, 0, len(data.Results))
	for _, r := range data.Results {
		out := placeOutcome(r, ambiguous)
		if out.Outcome == OutcomeRecovered && (out.OrderID == "" || out.Status == "") {
			out = s.recoverPlaced(ctx, out, o)
		}
		results = append(results, out)
	}
	return placeResult(results), nil
}

// CancelOrders cancels 1 to 50 orders (needs the trade scope), each named by exactly one of
// OrderID and ClientOrderID, one after another in order; a failure does not stop the rest.
//
// It is retried like CancelOrder: after a lost answer, an entry refused with INVALID_STATE (an
// earlier attempt may have cancelled it) is looked up once and comes back OutcomeRecovered
// with its current Status, or OutcomeUnknown if the lookup fails. Results are in request order.
func (s *TradingService) CancelOrders(ctx context.Context, entries []CancelBatchItem, opts ...CallOption) (BatchCancelResult, error) {
	items, err := checkCancelOrders(entries)
	if err != nil {
		return BatchCancelResult{}, err
	}
	var attempts []attemptKind
	c := call{op: OpCancelBatch, body: CancelBatchRequest{Orders: items}, cost: len(items), attempts: &attempts}
	data, err := getData[CancelBatch](ctx, s.t, c, opts)
	if err != nil {
		return BatchCancelResult{}, err
	}
	ambiguous := afterAmbiguous(attempts)
	o := s.t.options(opts)
	results := make([]CancelEntryOutcome, 0, len(data.Results))
	for _, r := range data.Results {
		out := cancelOutcome(r, ambiguous)
		if out.Outcome == OutcomeRecovered {
			out = s.recoverCancelled(ctx, out, o)
		}
		results = append(results, out)
	}
	return cancelResult(results), nil
}

// recoverPlaced reads an order the server could not describe, once and without retries. It is
// never placed again. A lookup that fails for any reason, a done ctx included (then nothing is
// sent), makes it OutcomeUnknown: the batch has answered, so the other results still count.
func (s *TradingService) recoverPlaced(ctx context.Context, out BatchOrderOutcome, o callOptions) BatchOrderOutcome {
	var ord Order
	err := ctx.Err()
	if err == nil {
		ord, err = s.OrderByClientID(ctx, out.ClientOrderID, WithTimeout(o.timeout), WithMaxRetries(0))
	}
	if err != nil {
		return BatchOrderOutcome{Index: out.Index, ClientOrderID: out.ClientOrderID, Outcome: OutcomeUnknown,
			Fills: []Fill{}, Error: out.Error}
	}
	out.OrderID, out.Status, out.Order = ord.ID, ord.Status, &ord
	return out
}

// recoverCancelled reads the current state of an entry a retry found closed, once and without
// retries. A lookup that fails for any reason, a done ctx included (then nothing is sent), makes
// it OutcomeUnknown.
func (s *TradingService) recoverCancelled(ctx context.Context, out CancelEntryOutcome, o callOptions) CancelEntryOutcome {
	var ord Order
	err := ctx.Err()
	switch {
	case err != nil:
	case out.OrderID != "":
		ord, err = s.Order(ctx, out.OrderID, WithTimeout(o.timeout), WithMaxRetries(0))
	default:
		ord, err = s.OrderByClientID(ctx, out.ClientOrderID, WithTimeout(o.timeout), WithMaxRetries(0))
	}
	if err != nil {
		out.Outcome = OutcomeUnknown
		return out
	}
	out.OrderID, out.Status, out.Order = ord.ID, ord.Status, &ord
	return out
}

func checkClientOrderID(cid, where string) error {
	if !clientOrderIDRE.MatchString(cid) {
		return &ConfigError{Msg: fmt.Sprintf("%s: client_order_id must be 1 to 64 characters of A-Z a-z 0-9 _ - . :", where)}
	}
	return nil
}

// checkPlaceOrders checks the batch and returns the wire items, with every missing
// client_order_id generated (once: every attempt sends these).
func checkPlaceOrders(symbol string, orders []PlaceOrdersItem) ([]BatchOrderItem, error) {
	if symbol == "" {
		return nil, &ConfigError{Msg: "Trading.PlaceOrders: symbol is required"}
	}
	if len(orders) < 1 || len(orders) > MaxBatchOrders {
		return nil, &ConfigError{Msg: fmt.Sprintf("Trading.PlaceOrders takes 1 to %d orders, got %d", MaxBatchOrders, len(orders))}
	}
	items := make([]BatchOrderItem, len(orders))
	seen := make(map[string]bool, len(orders))
	for i, o := range orders {
		where := fmt.Sprintf("Trading.PlaceOrders: orders[%d]", i)
		if o.Side == "" || o.Type == "" {
			return nil, &ConfigError{Msg: where + ": Side and Type are required"}
		}
		if err := checkAmounts(where, amountField{"price", o.Price}, amountField{"quantity", o.Quantity},
			amountField{"quote_quantity", o.QuoteQuantity}, amountField{"stop_price", o.StopPrice}); err != nil {
			return nil, err
		}
		cid := newID()
		if o.ClientOrderID != nil {
			cid = *o.ClientOrderID
			if err := checkClientOrderID(cid, where); err != nil {
				return nil, err
			}
			if seen[cid] {
				return nil, &ConfigError{Msg: fmt.Sprintf("%s: client_order_id %q is used twice in this batch", where, cid)}
			}
			seen[cid] = true
		}
		items[i] = BatchOrderItem{ClientOrderID: cid, Side: o.Side, Type: o.Type, Quantity: o.Quantity, Price: o.Price,
			QuoteQuantity: o.QuoteQuantity, StopPrice: o.StopPrice, TimeInForce: o.TimeInForce, TriggerDirection: o.TriggerDirection}
	}
	return items, nil
}

func checkCancelOrders(entries []CancelBatchItem) ([]CancelBatchItem, error) {
	if len(entries) < 1 || len(entries) > MaxCancelOrders {
		return nil, &ConfigError{Msg: fmt.Sprintf("Trading.CancelOrders takes 1 to %d entries, got %d", MaxCancelOrders, len(entries))}
	}
	items := make([]CancelBatchItem, len(entries))
	for i, e := range entries {
		oid, cid := deref(e.OrderID), deref(e.ClientOrderID)
		if (oid == "") == (cid == "") {
			return nil, &ConfigError{Msg: fmt.Sprintf("Trading.CancelOrders: orders[%d]: give exactly one of OrderID and ClientOrderID", i)}
		}
		if oid != "" {
			items[i] = CancelBatchItem{OrderID: &oid}
		} else {
			items[i] = CancelBatchItem{ClientOrderID: &cid}
		}
	}
	return items, nil
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// detail is a non-empty string in an error's details, or "".
func detail(e *ErrorBody, name string) string {
	v, _ := e.Details[name].(string)
	return v
}

// placeOutcome reads one result. A recovered result still needing a lookup has an empty
// OrderID or Status.
func placeOutcome(r BatchOrderResult, afterAmbiguous bool) BatchOrderOutcome {
	fills := r.Fills
	if fills == nil {
		fills = []Fill{}
	}
	out := BatchOrderOutcome{Index: r.Index, ClientOrderID: r.ClientOrderID, Fills: fills, Error: r.Error}
	switch {
	case r.Error == nil && r.Order != nil:
		out.Outcome, out.OrderID, out.Status, out.Order = OutcomePlaced, r.Order.ID, r.Order.Status, r.Order
	case r.Error != nil && afterAmbiguous && r.Error.Code == CodeAlreadyExists:
		out.Outcome, out.OrderID, out.Status = OutcomeRecovered, detail(r.Error, "order_id"), OrderStatus(detail(r.Error, "status"))
		out.Fills = []Fill{}
	default:
		out.Outcome = OutcomeFailed
	}
	return out
}

func cancelOutcome(r CancelBatchResult, afterAmbiguous bool) CancelEntryOutcome {
	out := CancelEntryOutcome{Index: r.Index, OrderID: deref(r.OrderID), ClientOrderID: deref(r.ClientOrderID), Error: r.Error}
	switch {
	case r.Error == nil && r.Order != nil:
		out.Outcome, out.OrderID, out.Status, out.Order = OutcomeCancelled, r.Order.ID, r.Order.Status, r.Order
	case r.Error != nil && afterAmbiguous && r.Error.Code == CodeInvalidState:
		out.Outcome = OutcomeRecovered
	default:
		out.Outcome = OutcomeFailed
	}
	return out
}

func placeResult(results []BatchOrderOutcome) BatchPlaceResult {
	res := BatchPlaceResult{Results: results}
	for _, o := range results {
		switch {
		case o.Outcome == OutcomeUnknown:
			res.Unknown++
		case o.Outcome == OutcomePlaced || (o.Outcome == OutcomeRecovered && o.Status != OrderStatusRejected):
			res.Placed++
		default:
			res.Failed++
		}
	}
	return res
}

func cancelResult(results []CancelEntryOutcome) BatchCancelResult {
	res := BatchCancelResult{Results: results}
	for _, o := range results {
		switch {
		case o.Outcome == OutcomeUnknown:
			res.Unknown++
		case o.Outcome == OutcomeCancelled || (o.Outcome == OutcomeRecovered && o.Status == OrderStatusCancelled):
			res.Cancelled++
		default:
			res.Failed++
		}
	}
	return res
}
