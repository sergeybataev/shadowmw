package bodycmp

import (
	"bytes"
	"strconv"
)

type exactComparator struct {
	limits Limits
}

// NewExact constructs an exact byte comparator.
func NewExact(limits Limits) (Comparator, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	return exactComparator{limits: limits}, nil
}

func (c exactComparator) Compare(expected, actual []byte) Result {
	if bytes.Equal(expected, actual) {
		return Result{}
	}
	diffs := differences{limits: c.limits}
	diffs.add("", "value", strconv.Quote(string(expected)), strconv.Quote(string(actual)))
	return diffs.result()
}
