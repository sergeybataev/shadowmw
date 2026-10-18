// Package decimal normalizes JSON numbers without expanding their exponents.
package decimal

import (
	"math/big"
	"strings"
)

// Number represents a sign, significant digits, and a base-ten exponent.
// Zero has digits "0", a non-negative sign, and exponent zero.
type Number struct {
	Negative bool
	Digits   string
	Exponent big.Int
}

// ParseJSON normalizes a syntactically valid JSON number. Callers must validate
// the JSON syntax before calling it.
func ParseJSON(value string) Number {
	var number Number
	if value[0] == '-' {
		number.Negative = true
		value = value[1:]
	}
	if index := strings.IndexAny(value, "eE"); index >= 0 {
		if _, ok := number.Exponent.SetString(value[index+1:], 10); !ok {
			panic("decimal: invalid JSON number exponent")
		}
		value = value[:index]
	}
	fractionDigits := 0
	if index := strings.IndexByte(value, '.'); index >= 0 {
		fractionDigits = len(value) - index - 1
		value = value[:index] + value[index+1:]
	}
	value = strings.TrimLeft(value, "0")
	if value == "" {
		number.Negative = false
		number.Digits = "0"
		number.Exponent.SetInt64(0)
		return number
	}
	trailingZeros := len(value) - len(strings.TrimRight(value, "0"))
	number.Digits = value[:len(value)-trailingZeros]
	number.Exponent.Add(&number.Exponent, big.NewInt(int64(trailingZeros-fractionDigits)))
	return number
}

// Equal compares exact mathematical values.
func (n Number) Equal(other Number) bool {
	return n.Negative == other.Negative && n.Digits == other.Digits && n.Exponent.Cmp(&other.Exponent) == 0
}
