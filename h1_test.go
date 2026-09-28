package cexy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestLedgerReferenceVariants(t *testing.T) {
	cases := []struct {
		raw  string
		want LedgerReferenceType
		chk  func(LedgerReference) bool
	}{
		{`{"type":"deposit","deposit_id":"d1"}`, LedgerReferenceTypeDeposit, func(r LedgerReference) bool { return r.DepositID != nil && *r.DepositID == "d1" }},
		{`{"type":"trade","trade_id":"t1","order_id":"o1"}`, LedgerReferenceTypeTrade, func(r LedgerReference) bool {
			return *r.TradeID == "t1" && *r.OrderID == "o1" && r.DepositID == nil
		}},
		{`{"type":"transfer","counterparty_user_id":"u1","transfer_ref":"x"}`, LedgerReferenceTypeTransfer, func(r LedgerReference) bool {
			return *r.CounterpartyUserID == "u1" && *r.TransferRef == "x"
		}},
		{`{"type":"system","cause":"rebate"}`, LedgerReferenceTypeSystem, func(r LedgerReference) bool { return *r.Cause == "rebate" }},
	}
	for _, tc := range cases {
		var r LedgerReference
		if err := json.Unmarshal([]byte(tc.raw), &r); err != nil {
			t.Fatal(err)
		}
		if r.Type != tc.want || !r.Known() || !tc.chk(r) || string(r.Raw) != tc.raw {
			t.Errorf("%s: %+v", tc.raw, r)
		}
	}
}

func TestLedgerReferenceUnknownAndMalformedNeverFail(t *testing.T) {
	var e LedgerEntry
	raw := `{"reference":{"type":"airdrop","campaign":"c9"}}`
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("unknown variant must decode: %v", err)
	}
	if e.Reference.Type != "airdrop" || e.Reference.Known() || string(e.Reference.Raw) != `{"type":"airdrop","campaign":"c9"}` {
		t.Fatalf("%+v", e.Reference)
	}
	for _, bad := range []string{`"just a string"`, `42`, `null`, `{"type":7}`} {
		var r LedgerReference
		if err := json.Unmarshal([]byte(bad), &r); err != nil {
			t.Fatalf("%s: %v", bad, err)
		}
		if string(r.Raw) != bad || r.Known() {
			t.Fatalf("%s: %+v", bad, r)
		}
	}
}

func TestIDsAreStringAliasesAndReverted(t *testing.T) {
	var id OrderID = "anything-goes" // no format check: a future id format keeps working
	var s string = id
	if s != "anything-goes" {
		t.Fatal(s)
	}
	var w struct{ Status WithdrawalStatus }
	if err := json.Unmarshal([]byte(`{"Status":"reverted"}`), &w); err != nil || w.Status != WithdrawalStatusReverted {
		t.Fatal(w, err)
	}
}

func TestJoinPoolValidatesMaxRatioDeviation(t *testing.T) {
	c, rec, _ := newTestClient(t, Options{APIKey: "ak_test", APISecret: "secret_test"}, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, dataEnv(map[string]any{}))
	})
	_, err := c.Pools.Join(context.Background(), "BTC/USDT", JoinPoolRequest{
		BaseAmount: "1", QuoteAmount: "2", MaxRatioDeviationPercent: Ptr(Amount("1e3")),
	})
	var ia *InvalidAmountError
	if !errors.As(err, &ia) || rec.count() != 0 {
		t.Fatalf("want InvalidAmountError before sending, got %v (%d requests)", err, rec.count())
	}
}
