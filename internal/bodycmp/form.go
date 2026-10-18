package bodycmp

import (
	"net/url"
	"slices"
	"sort"
	"strconv"
)

// FormOptions configures application/x-www-form-urlencoded comparison.
type FormOptions struct {
	Limits Limits
	// SortValuesForKeys makes repeated values order-insensitive only for the
	// listed keys.
	SortValuesForKeys []string
}

// NewForm constructs an application/x-www-form-urlencoded comparator.
func NewForm(options FormOptions) (Comparator, error) {
	if err := options.Limits.validate(); err != nil {
		return nil, err
	}
	sortKeys := make(map[string]struct{}, len(options.SortValuesForKeys))
	for _, key := range options.SortValuesForKeys {
		sortKeys[key] = struct{}{}
	}
	return formComparator{limits: options.Limits, sortKeys: sortKeys}, nil
}

// NormalizeForm parses and re-encodes an application/x-www-form-urlencoded body
// in deterministic key order.
func NormalizeForm(body []byte) (string, bool) {
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return "", false
	}
	return values.Encode(), true
}

type formComparator struct {
	limits   Limits
	sortKeys map[string]struct{}
}

func (c formComparator) Compare(expected, actual []byte) Result {
	expectedValues, expectedErr := url.ParseQuery(string(expected))
	actualValues, actualErr := url.ParseQuery(string(actual))
	diffs := differences{limits: c.limits}
	if expectedErr != nil || actualErr != nil {
		diffs.add("", "invalid", formValidity(expectedErr), formValidity(actualErr))
		return diffs.result()
	}

	keys := make([]string, 0, len(expectedValues)+len(actualValues))
	seen := make(map[string]struct{}, len(expectedValues)+len(actualValues))
	for key := range expectedValues {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range actualValues {
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		expectedList, expectedOK := expectedValues[key]
		actualList, actualOK := actualValues[key]
		path := appendPointer("", key)
		switch {
		case !expectedOK:
			diffs.add(path, "unexpected", "<missing>", renderStrings(actualList))
			continue
		case !actualOK:
			diffs.add(path, "missing", renderStrings(expectedList), "<missing>")
			continue
		}

		if _, ok := c.sortKeys[key]; ok {
			expectedList = slices.Clone(expectedList)
			actualList = slices.Clone(actualList)
			sort.Strings(expectedList)
			sort.Strings(actualList)
		}
		c.compareValues(&diffs, path, expectedList, actualList)
	}
	return diffs.result()
}

func (c formComparator) compareValues(diffs *differences, path string, expected, actual []string) {
	count := max(len(expected), len(actual))
	for i := range count {
		itemPath := appendPointer(path, strconv.Itoa(i))
		switch {
		case i >= len(expected):
			diffs.add(itemPath, "unexpected", "<missing>", strconv.Quote(actual[i]))
		case i >= len(actual):
			diffs.add(itemPath, "missing", strconv.Quote(expected[i]), "<missing>")
		case expected[i] != actual[i]:
			diffs.add(itemPath, "value", strconv.Quote(expected[i]), strconv.Quote(actual[i]))
		}
	}
}

func formValidity(err error) string {
	if err != nil {
		return "<invalid form>"
	}
	return "<valid form>"
}

func renderStrings(values []string) string {
	rendered := "["
	for i, value := range values {
		if i > 0 {
			rendered += ","
		}
		rendered += strconv.Quote(value)
	}
	return rendered + "]"
}
