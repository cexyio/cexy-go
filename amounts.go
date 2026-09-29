package cexy

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Amount is an exact decimal amount as a string, such as "0.00150000". The API never uses
// JSON numbers for money. Do arithmetic with a decimal package (for example
// github.com/shopspring/decimal or math/big), never with float64.
type Amount string

var amountRE = regexp.MustCompile(`^-?\d+(\.\d+)?$`)

// Valid reports whether a is a plain decimal string: digits, an optional "." and sign.
func (a Amount) Valid() bool { return amountRE.MatchString(string(a)) }

// String returns the decimal text.
func (a Amount) String() string { return string(a) }

// Ptr returns a pointer to v, for optional fields: cexy.Ptr(cexy.Amount("0.5")).
func Ptr[T any](v T) *T { return &v }

type amountField struct {
	name  string
	value *Amount
}

// checkAmounts returns an *InvalidAmountError for the first malformed amount. Nil pointers
// (optional fields) pass.
func checkAmounts(context string, fields ...amountField) error {
	for _, f := range fields {
		if f.value == nil {
			continue
		}
		if !f.value.Valid() {
			return &InvalidAmountError{Field: f.name, Msg: fmt.Sprintf(
				"%s: %s must be a plain decimal string such as \"0.5\" (got %q)", context, f.name, string(*f.value))}
		}
	}
	return nil
}

// Time parses the server's RFC 3339 time.
func (t ServerTime) Time() (time.Time, error) { return time.Parse(time.RFC3339Nano, t.Iso) }

// BookLevel is one [price, quantity] level of an order book.
type BookLevel struct {
	Price, Quantity Amount
}

// Levels converts the raw [price, quantity] pairs of an order book side. Malformed entries
// (fewer than two values) are skipped.
func Levels(raw [][]Amount) []BookLevel {
	out := make([]BookLevel, 0, len(raw))
	for _, l := range raw {
		if len(l) >= 2 {
			out = append(out, BookLevel{Price: l[0], Quantity: l[1]})
		}
	}
	return out
}

// validPathValue: non-empty, no CR/LF, and not "." or "..": a dot segment would be resolved by
// the URL layer, a proxy or the server's router (even as %2E) and reach a different route.
func validPathValue(v string) bool {
	return v != "" && v != "." && v != ".." && !strings.ContainsAny(v, "\r\n")
}
