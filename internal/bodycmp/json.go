package bodycmp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/sergeybataev/shadowmw/internal/decimal"
)

// JSONOptions configures structural JSON comparison.
type JSONOptions struct {
	Limits Limits
	// IgnorePointers contains RFC 6901 pointers. A segment containing exactly
	// "*" matches any single object key or array index.
	IgnorePointers []string
	// IgnoreFieldNames ignores matching object field names at every depth.
	IgnoreFieldNames []string
	// MissingEmptyObjectPointers contains pointers at which a missing value and
	// an empty JSON object are equivalent. The same one-segment wildcard syntax
	// as IgnorePointers is supported.
	MissingEmptyObjectPointers []string
}

// NewJSON constructs a structural JSON comparator.
func NewJSON(options JSONOptions) (Comparator, error) {
	if err := options.Limits.validate(); err != nil {
		return nil, err
	}
	ignore, err := parsePointers("IgnorePointers", options.IgnorePointers)
	if err != nil {
		return nil, err
	}
	emptyObject, err := parsePointers("MissingEmptyObjectPointers", options.MissingEmptyObjectPointers)
	if err != nil {
		return nil, err
	}
	ignoreFields := make(map[string]struct{}, len(options.IgnoreFieldNames))
	for _, field := range options.IgnoreFieldNames {
		ignoreFields[field] = struct{}{}
	}
	return jsonComparator{
		limits:       options.Limits,
		ignore:       ignore,
		ignoreFields: ignoreFields,
		emptyObject:  emptyObject,
	}, nil
}

// NormalizeComparedJSON canonicalizes a valid JSON document into compact UTF-8
// bytes with exact decimal numbers.
func NormalizeComparedJSON(body []byte) ([]byte, bool) {
	value, ok := decodeNormalizedJSON(body)
	if !ok {
		return nil, false
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	return normalized, true
}

// NormalizeRenderedJSON canonicalizes a valid JSON document into indented text
// with exact decimal numbers.
func NormalizeRenderedJSON(body []byte) (string, bool) {
	value, ok := decodeNormalizedJSON(body)
	if !ok {
		return "", false
	}
	normalized, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", false
	}
	return string(normalized), true
}

type pointerPattern []string

type jsonComparator struct {
	limits       Limits
	ignore       []pointerPattern
	ignoreFields map[string]struct{}
	emptyObject  []pointerPattern
}

func (c jsonComparator) Compare(expected, actual []byte) Result {
	expectedValue, expectedErr := decodeJSON(expected)
	actualValue, actualErr := decodeJSON(actual)
	diffs := differences{limits: c.limits}
	if expectedErr != nil || actualErr != nil {
		diffs.add("", "invalid", jsonValidity(expectedErr), jsonValidity(actualErr))
		return diffs.result()
	}
	if matchesPointer(c.ignore, nil) {
		return Result{}
	}
	c.compareValue(&diffs, nil, "", expectedValue, actualValue)
	return diffs.result()
}

func (c jsonComparator) compareValue(
	diffs *differences,
	segments []string,
	path string,
	expected, actual any,
) {
	expectedKind := jsonKind(expected)
	actualKind := jsonKind(actual)
	if expectedKind != actualKind {
		diffs.add(path, "type", renderJSON(expected), renderJSON(actual))
		return
	}

	switch expectedValue := expected.(type) {
	case map[string]any:
		c.compareObjects(diffs, segments, path, expectedValue, actual.(map[string]any))
	case []any:
		c.compareArrays(diffs, segments, path, expectedValue, actual.([]any))
	case json.Number:
		if !equalJSONNumbers(expectedValue.String(), actual.(json.Number).String()) {
			diffs.add(path, "value", expectedValue.String(), actual.(json.Number).String())
		}
	case string:
		if expectedValue != actual.(string) {
			diffs.add(path, "value", strconv.Quote(expectedValue), strconv.Quote(actual.(string)))
		}
	case bool:
		if expectedValue != actual.(bool) {
			diffs.add(path, "value", strconv.FormatBool(expectedValue), strconv.FormatBool(actual.(bool)))
		}
	case nil:
	}
}

func (c jsonComparator) compareObjects(
	diffs *differences,
	segments []string,
	path string,
	expected, actual map[string]any,
) {
	keys := make([]string, 0, len(expected)+len(actual))
	seen := make(map[string]struct{}, len(expected)+len(actual))
	for key := range expected {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range actual {
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	for _, key := range keys {
		if _, ignored := c.ignoreFields[key]; ignored {
			continue
		}
		childSegments := appendSegment(segments, key)
		if matchesPointer(c.ignore, childSegments) {
			continue
		}
		childPath := appendPointer(path, key)
		expectedValue, expectedOK := expected[key]
		actualValue, actualOK := actual[key]
		switch {
		case !expectedOK:
			if c.missingEqualsEmptyObject(childSegments, actualValue) {
				continue
			}
			diffs.add(childPath, "unexpected", "<missing>", renderJSON(actualValue))
		case !actualOK:
			if c.missingEqualsEmptyObject(childSegments, expectedValue) {
				continue
			}
			diffs.add(childPath, "missing", renderJSON(expectedValue), "<missing>")
		default:
			c.compareValue(diffs, childSegments, childPath, expectedValue, actualValue)
		}
	}
}

func (c jsonComparator) compareArrays(
	diffs *differences,
	segments []string,
	path string,
	expected, actual []any,
) {
	count := max(len(expected), len(actual))
	for i := range count {
		index := strconv.Itoa(i)
		childSegments := appendSegment(segments, index)
		if matchesPointer(c.ignore, childSegments) {
			continue
		}
		childPath := appendPointer(path, index)
		switch {
		case i >= len(expected):
			if c.missingEqualsEmptyObject(childSegments, actual[i]) {
				continue
			}
			diffs.add(childPath, "unexpected", "<missing>", renderJSON(actual[i]))
		case i >= len(actual):
			if c.missingEqualsEmptyObject(childSegments, expected[i]) {
				continue
			}
			diffs.add(childPath, "missing", renderJSON(expected[i]), "<missing>")
		default:
			c.compareValue(diffs, childSegments, childPath, expected[i], actual[i])
		}
	}
}

func (c jsonComparator) missingEqualsEmptyObject(segments []string, value any) bool {
	if !matchesPointer(c.emptyObject, segments) {
		return false
	}
	object, ok := value.(map[string]any)
	return ok && len(object) == 0
}

func decodeJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func decodeNormalizedJSON(body []byte) (any, bool) {
	value, err := decodeJSON(body)
	if err != nil {
		return nil, false
	}
	return canonicalizeRenderedJSONNumbers(value), true
}

type renderedJSONNumber string

func (number renderedJSONNumber) MarshalJSON() ([]byte, error) {
	return []byte(number), nil
}

func canonicalizeRenderedJSONNumbers(value any) any {
	switch typed := value.(type) {
	case json.Number:
		return renderedJSONNumber(canonicalRenderedJSONNumber(typed.String()))
	case []any:
		for i := range typed {
			typed[i] = canonicalizeRenderedJSONNumbers(typed[i])
		}
	case map[string]any:
		for key := range typed {
			typed[key] = canonicalizeRenderedJSONNumbers(typed[key])
		}
	}
	return value
}

func canonicalRenderedJSONNumber(value string) string {
	number := decimal.ParseJSON(value)
	if number.Digits == "0" {
		return "0"
	}
	sign := ""
	if number.Negative {
		sign = "-"
	}
	mantissa := number.Digits
	exponent := &number.Exponent
	if exponent.IsInt64() {
		const maximumExpandedNumberBytes = 1024
		power := exponent.Int64()
		digits := int64(len(mantissa))
		// Establish the expansion bound before doing arithmetic on the exponent.
		switch {
		case power >= 0 && power <= maximumExpandedNumberBytes-digits:
			return sign + mantissa + strings.Repeat("0", int(power))
		case power < 0 && power > -digits && digits < maximumExpandedNumberBytes:
			index := int(digits + power)
			return sign + mantissa[:index] + "." + mantissa[index:]
		case power <= -digits && power >= 2-maximumExpandedNumberBytes:
			return sign + "0." +
				strings.Repeat("0", int(-digits-power)) +
				mantissa
		}
	}
	return sign + mantissa + "e" + exponent.String()
}

func jsonValidity(err error) string {
	if err != nil {
		return "<invalid JSON>"
	}
	return "<valid JSON>"
}

func jsonKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case json.Number:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		panic("bodycmp: unexpected decoded JSON type")
	}
}

func renderJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic("bodycmp: cannot render decoded JSON: " + err.Error())
	}
	return string(encoded)
}

func parsePointers(optionName string, pointers []string) ([]pointerPattern, error) {
	patterns := make([]pointerPattern, 0, len(pointers))
	for _, pointer := range pointers {
		pattern, err := parsePointer(pointer)
		if err != nil {
			return nil, fmt.Errorf("bodycmp: %s contains invalid pointer %q: %w", optionName, pointer, err)
		}
		patterns = append(patterns, pattern)
	}
	return patterns, nil
}

func parsePointer(pointer string) (pointerPattern, error) {
	if pointer == "" {
		return pointerPattern{}, nil
	}
	if pointer[0] != '/' {
		return nil, fmt.Errorf("must be empty or start with '/'")
	}
	encodedSegments := strings.Split(pointer[1:], "/")
	segments := make(pointerPattern, len(encodedSegments))
	for i, encoded := range encodedSegments {
		var decoded strings.Builder
		for j := 0; j < len(encoded); j++ {
			if encoded[j] != '~' {
				decoded.WriteByte(encoded[j])
				continue
			}
			if j+1 == len(encoded) {
				return nil, fmt.Errorf("incomplete '~' escape")
			}
			j++
			switch encoded[j] {
			case '0':
				decoded.WriteByte('~')
			case '1':
				decoded.WriteByte('/')
			default:
				return nil, fmt.Errorf("invalid '~%c' escape", encoded[j])
			}
		}
		segments[i] = decoded.String()
		if segments[i] == "**" {
			return nil, fmt.Errorf("recursive wildcard '**' is unsupported")
		}
	}
	return segments, nil
}

func matchesPointer(patterns []pointerPattern, segments []string) bool {
	for _, pattern := range patterns {
		if len(pattern) != len(segments) {
			continue
		}
		matches := true
		for i, segment := range pattern {
			if segment != "*" && segment != segments[i] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func appendSegment(segments []string, segment string) []string {
	child := make([]string, len(segments)+1)
	copy(child, segments)
	child[len(segments)] = segment
	return child
}

func equalJSONNumbers(left, right string) bool {
	return decimal.ParseJSON(left).Equal(decimal.ParseJSON(right))
}
