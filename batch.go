package cexy

import (
	"context"
	"encoding/json"
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

// NewClientOrderID returns a new client order id (a random UUID), as PlaceOrder and PlaceOrders
// generate for an order without one. Create and save ids with it before a call when you must
// be able to reconcile after any failure.
func NewClientOrderID() string { return newID() }

// PlaceOrders places 1 to 20 REAL orders in one market (needs the trade scope), each on its own
// and in order: the batch is not atomic, and one refusal does not stop the rest. Amounts are
// decimal strings.
//
// A ClientOrderID (a UUID) is generated for each order without one (nil); your own ids must
// be 1 to 64 characters of A-Z a-z 0-9 _ - . :, not "." or "..", and distinct within the batch
// (a *ConfigError before anything is sent). The ids sent are in the results. To reconcile
// after any failure, even a process crash, create and save your own ids first
// (NewClientOrderID).
//
// Retry safety: the body, ids included, is built once and every retry resends it unchanged.
// The server refuses an id it already placed, so nothing is placed twice: after a lost answer
// (a connection error after sending, or any 5xx except 501 and 503) such an order comes back
// OutcomeRecovered with its OrderID and Status, read from the refusal's details or, when they
// are missing, looked up once by client_order_id (OutcomeUnknown if that fails). An item
// refused with an order id in its details (MARKET_UNAVAILABLE when the market stopped while
// matching, for example) exists, so it comes back OutcomeRecovered too; an item INTERNAL error
// is looked up once. Results are in request order. Res.Unknown > 0 means some orders' state
// could not be read: look them up with Trading.OrderByClientID before placing anything again.
//
// Errors: once the body is built, every failure is a *BatchPlaceError with all the ids sent
// and MaybePlaced (false only if no attempt can have been acted on). A refusal of the whole
// batch (400, 403, 404) is not retried; it placed nothing in that attempt, but an earlier
// ambiguous attempt may have. A batch larger than the key's whole budget (400 with a number in
// Details["limit"]) must be split by the caller. A 429 is waited out and the same body resent.
func (s *TradingService) PlaceOrders(ctx context.Context, symbol string, orders []PlaceOrdersItem, opts ...CallOption) (BatchPlaceResult, error) {
	items, err := checkPlaceOrders(symbol, orders)
	if err != nil {
		return BatchPlaceResult{}, err
	}
	var attempts []attemptKind
	var sent bool
	c := call{op: OpPlaceBatch, body: BatchOrderRequest{Symbol: symbol, Orders: items}, cost: len(items),
		orderActions: len(items), attempts: &attempts, sent: &sent}
	fail := func(err error) (BatchPlaceResult, error) {
		cids := make([]string, len(items))
		for i, it := range items {
			cids[i] = it.ClientOrderID
		}
		// The failing attempt is already in attempts when it was an attempt; reading err again
		// adds what was not (a decode failure, a wait cancelled between attempts). sent is false
		// only when the last attempt failed before the HTTP client (config, credentials).
		maybe := afterAmbiguous(attempts) || classifyAttempt(err, sent) == attemptAmbiguous
		return BatchPlaceResult{}, &BatchPlaceError{ClientOrderIDs: cids, MaybePlaced: maybe, Err: err}
	}
	raw, err := s.t.request(ctx, c, opts)
	if err != nil {
		return fail(err)
	}
	data, err := decodeBatch[BatchOrder](OpPlaceBatch, raw)
	if err == nil {
		err = matchPlaceResults(items, data.Results)
	}
	if err != nil {
		return fail(err)
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
// A 2xx answer that is not usable (no data, undecodable, or results whose count, index, order
// id or client order id do not match the entries) is an error matching ErrBadResponse.
func (s *TradingService) CancelOrders(ctx context.Context, entries []CancelBatchItem, opts ...CallOption) (BatchCancelResult, error) {
	items, err := checkCancelOrders(entries)
	if err != nil {
		return BatchCancelResult{}, err
	}
	var attempts []attemptKind
	c := call{op: OpCancelBatch, body: CancelBatchRequest{Orders: items}, cost: len(items), attempts: &attempts}
	raw, err := s.t.request(ctx, c, opts)
	if err != nil {
		return BatchCancelResult{}, err
	}
	data, err := decodeBatch[CancelBatch](OpCancelBatch, raw)
	if err == nil {
		err = matchCancelResults(items, data.Results)
	}
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

// decodeBatch decodes the {"data": T} envelope of a batch answer; a missing or undecodable
// "data" is ErrBadResponse.
func decodeBatch[T any](op OperationID, raw *rawResponse) (T, error) {
	var env struct {
		Data *T `json:"data"`
	}
	var zero T
	if err := json.Unmarshal(raw.body, &env); err != nil {
		return zero, &badResponseError{msg: fmt.Sprintf("cexy: %s: cannot decode the response: %v", op, err)}
	}
	if env.Data == nil {
		return zero, &badResponseError{msg: fmt.Sprintf("cexy: %s: the response has no data", op)}
	}
	return *env.Data, nil
}

// matchPlaceResults: one result per order sent, at its index, with its client_order_id.
func matchPlaceResults(items []BatchOrderItem, results []BatchOrderResult) error {
	if len(results) != len(items) {
		return &badResponseError{msg: fmt.Sprintf("cexy: %s: %d results for %d orders", OpPlaceBatch, len(results), len(items))}
	}
	for i, r := range results {
		if r.Index != i || r.ClientOrderID != items[i].ClientOrderID {
			return &badResponseError{msg: fmt.Sprintf("cexy: %s: result %d is for index %d, client_order_id %q, not %q",
				OpPlaceBatch, i, r.Index, r.ClientOrderID, items[i].ClientOrderID)}
		}
	}
	return nil
}

// matchCancelResults: one result per entry sent, at its index, echoing the id the entry was
// sent with (an absent echo is tolerated; the other id may name the order found).
func matchCancelResults(items []CancelBatchItem, results []CancelBatchResult) error {
	if len(results) != len(items) {
		return &badResponseError{msg: fmt.Sprintf("cexy: %s: %d results for %d entries", OpCancelBatch, len(results), len(items))}
	}
	same := func(echo, sent *string) bool { return sent == nil || echo == nil || *echo == *sent }
	for i, r := range results {
		if r.Index != i || !same(r.OrderID, items[i].OrderID) || !same(r.ClientOrderID, items[i].ClientOrderID) {
			return &badResponseError{msg: fmt.Sprintf("cexy: %s: result %d (index %d, order_id %q, client_order_id %q) does not "+
				"match the entry sent", OpCancelBatch, i, r.Index, deref(r.OrderID), deref(r.ClientOrderID))}
		}
	}
	return nil
}

// recoverPlaced reads an order the server could not describe, once and without retries. It is
// never placed again. A lookup that fails for any reason (an error, a done ctx, in which case
// nothing is sent, or an answer that is not an order) makes it OutcomeUnknown: the batch has answered, so the other results still count.
func (s *TradingService) recoverPlaced(ctx context.Context, out BatchOrderOutcome, o callOptions) BatchOrderOutcome {
	var ord Order
	err := ctx.Err()
	if err == nil {
		ord, err = s.OrderByClientID(ctx, out.ClientOrderID, WithTimeout(o.timeout), WithMaxRetries(0))
	}
	if err != nil || !isOrder(ord) {
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
	if err != nil || !isOrder(ord) {
		out.Outcome = OutcomeUnknown
		return out
	}
	out.OrderID, out.Status, out.Order = ord.ID, ord.Status, &ord
	return out
}

// isOrder: a lookup answer that names an order (rule 12: anything else is a failed lookup).
func isOrder(o Order) bool { return o.ID != "" && o.Status != "" }

func checkClientOrderID(cid, where string) error {
	if !clientOrderIDRE.MatchString(cid) || cid == "." || cid == ".." {
		return &ConfigError{Msg: fmt.Sprintf(`%s: client_order_id must be 1 to 64 characters of A-Z a-z 0-9 _ - . :, not "." or ".."`, where)}
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
		if cid == "." || cid == ".." {
			return nil, &ConfigError{Msg: fmt.Sprintf(`Trading.CancelOrders: orders[%d]: client_order_id must not be "." or ".."`, i)}
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

// detailStatus is details.status, or "" when it is missing or "unknown" (rule 11: then the
// order is looked up).
func detailStatus(e *ErrorBody) OrderStatus {
	if st := detail(e, "status"); st != "unknown" {
		return OrderStatus(st)
	}
	return ""
}

// placeOutcome reads one result. A recovered result still needing a lookup has an empty
// OrderID or Status. Rules 3 and 3b of the conformance file.
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
		out.Outcome, out.OrderID, out.Status = OutcomeRecovered, detail(r.Error, "order_id"), detailStatus(r.Error)
		out.Fills = []Fill{}
	case r.Error != nil && r.Error.Code != CodeAlreadyExists && detail(r.Error, "order_id") != "":
		// The order exists (MARKET_UNAVAILABLE when the market stopped while matching, for
		// example), on any attempt.
		out.Outcome, out.OrderID, out.Status = OutcomeRecovered, detail(r.Error, "order_id"), detailStatus(r.Error)
	case r.Error != nil && r.Error.Code == CodeInternal:
		// It may follow a committed order: looked up by client_order_id.
		out.Outcome = OutcomeRecovered
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
