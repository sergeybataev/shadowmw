package decimal_test

import (
	"testing"

	"github.com/sergeybataev/shadowmw/internal/decimal"
)

func TestParseJSONNormalizesExactDecimalValues(t *testing.T) {
	for _, test := range []struct {
		input, digits, exponent string
		negative                bool
	}{
		{input: "1.00", digits: "1", exponent: "0"},
		{input: "100e-2", digits: "1", exponent: "0"},
		{input: "-0.00e999999999999999999999", digits: "0", exponent: "0"},
		{input: "-12.300", digits: "123", exponent: "-1", negative: true},
		{input: "123e999999999999999999999", digits: "123", exponent: "999999999999999999999"},
		{input: "1200e-999999999999999999999", digits: "12", exponent: "-999999999999999999997"},
	} {
		t.Run(test.input, func(t *testing.T) {
			got := decimal.ParseJSON(test.input)
			if got.Digits != test.digits || got.Exponent.String() != test.exponent || got.Negative != test.negative {
				t.Fatalf("normalized value = sign %v digits %q exponent %s", got.Negative, got.Digits, got.Exponent.String())
			}
		})
	}
}

func TestEqualComparesWithoutFloatPrecisionLoss(t *testing.T) {
	for _, test := range []struct {
		left, right string
		equal       bool
	}{
		{left: "1", right: "1e0", equal: true},
		{left: "-0", right: "0.00", equal: true},
		{left: "9007199254740992", right: "9007199254740993"},
		{left: "1e999999999999999999999", right: "10e999999999999999999998", equal: true},
		{left: "1e-999999999999999999999", right: "1e-999999999999999999998"},
	} {
		if got := decimal.ParseJSON(test.left).Equal(decimal.ParseJSON(test.right)); got != test.equal {
			t.Errorf("Equal(%s, %s) = %v, want %v", test.left, test.right, got, test.equal)
		}
	}
}
