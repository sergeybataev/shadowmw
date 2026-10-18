package bodycmp_test

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sergeybataev/shadowmw/internal/bodycmp"
)

func TestJSONStructuralComparison(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{Limits: testLimits()})

	tests := []struct {
		name     string
		expected string
		actual   string
		matched  bool
	}{
		{name: "object order", expected: `{"a":1,"b":2}`, actual: `{"b":2,"a":1}`, matched: true},
		{name: "number decimal", expected: `1`, actual: `1.0`, matched: true},
		{name: "number exponent", expected: `1`, actual: `1e0`, matched: true},
		{name: "number normalized", expected: `1000.00`, actual: `1e3`, matched: true},
		{name: "negative zero", expected: `-0`, actual: `0.000e99`, matched: true},
		{name: "huge exponent", expected: `10e999999999999999999999`, actual: `1e1000000000000000000000`, matched: true},
		{name: "beyond float precision equal", expected: `900719925474099300000`, actual: `9.007199254740993e20`, matched: true},
		{name: "beyond float precision different", expected: `9007199254740992`, actual: `9007199254740993`, matched: false},
		{name: "different number", expected: `1.01`, actual: `1.0101`, matched: false},
		{name: "strict array order", expected: `[1,2]`, actual: `[2,1]`, matched: false},
		{name: "different types", expected: `1`, actual: `"1"`, matched: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := comparator.Compare([]byte(test.expected), []byte(test.actual))
			if got := result.Matched(); got != test.matched {
				t.Fatalf("Matched() = %v, want %v; differences = %#v", got, test.matched, result.Differences)
			}
		})
	}
}

func TestJSONInvalidNeverMatches(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{Limits: testLimits()})

	for _, test := range []struct {
		name     string
		expected string
		actual   string
	}{
		{name: "both invalid with same bytes", expected: `{`, actual: `{`},
		{name: "expected invalid", expected: `{"a":`, actual: `{"a":1}`},
		{name: "actual invalid", expected: `null`, actual: `null null`},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := comparator.Compare([]byte(test.expected), []byte(test.actual))
			if result.Matched() {
				t.Fatal("Matched() = true, want false")
			}
			want := []bodycmp.Difference{{
				Path:     "",
				Kind:     "invalid",
				Expected: validity(test.expected),
				Actual:   validity(test.actual),
			}}
			if !reflect.DeepEqual(result.Differences, want) {
				t.Fatalf("Differences = %#v, want %#v", result.Differences, want)
			}
		})
	}
}

func TestJSONIgnoreRules(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{
		Limits:           testLimits(),
		IgnorePointers:   []string{"/items/*/stamp", "/a~1b/~0field"},
		IgnoreFieldNames: []string{"request_id"},
	})

	expected := `{
		"request_id":"outer-a",
		"a/b":{"~field":"a"},
		"items":[
			{"stamp":1,"value":"same","request_id":"inner-a"},
			{"stamp":2,"value":"same"}
		]
	}`
	actual := `{
		"request_id":"outer-b",
		"a/b":{"~field":"b"},
		"items":[
			{"stamp":9,"value":"same","request_id":"inner-b"},
			{"stamp":8,"value":"same"}
		]
	}`
	if result := comparator.Compare([]byte(expected), []byte(actual)); !result.Matched() {
		t.Fatalf("Matched() = false; differences = %#v", result.Differences)
	}
}

func TestJSONPointerWildcardMatchesExactlyOneSegment(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{
		Limits:         testLimits(),
		IgnorePointers: []string{"/items/*/ignored"},
	})
	result := comparator.Compare(
		[]byte(`{"items":[{"nested":{"ignored":1}}]}`),
		[]byte(`{"items":[{"nested":{"ignored":2}}]}`),
	)
	want := []bodycmp.Difference{{
		Path:     "/items/0/nested/ignored",
		Kind:     "value",
		Expected: "1",
		Actual:   "2",
	}}
	if !reflect.DeepEqual(result.Differences, want) {
		t.Fatalf("Differences = %#v, want %#v", result.Differences, want)
	}
}

func TestJSONMissingEmptyObjectEquivalence(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{
		Limits:                     testLimits(),
		MissingEmptyObjectPointers: []string{"/items/*/metadata", "/top"},
	})

	for _, test := range []struct {
		name     string
		expected string
		actual   string
		matched  bool
	}{
		{
			name:     "missing expected",
			expected: `{"items":[{}]}`,
			actual:   `{"items":[{"metadata":{}}]}`,
			matched:  true,
		},
		{
			name:     "missing actual",
			expected: `{"top":{}}`,
			actual:   `{}`,
			matched:  true,
		},
		{
			name:     "non-empty object",
			expected: `{}`,
			actual:   `{"top":{"value":1}}`,
			matched:  false,
		},
		{
			name:     "unconfigured path",
			expected: `{"other":{}}`,
			actual:   `{}`,
			matched:  false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := comparator.Compare([]byte(test.expected), []byte(test.actual))
			if got := result.Matched(); got != test.matched {
				t.Fatalf("Matched() = %v, want %v; differences = %#v", got, test.matched, result.Differences)
			}
		})
	}
}

func TestJSONDifferencesAreDeterministicAndBounded(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{
		Limits: bodycmp.Limits{MaxDifferences: 2, MaxValueBytes: 5},
	})
	expected := []byte(`{"z":"abcdefgh","a":1,"b":2}`)
	actual := []byte(`{"z":"ijklmnop","a":9,"b":8}`)

	first := comparator.Compare(expected, actual)
	second := comparator.Compare(expected, actual)
	want := bodycmp.Result{
		Differences: []bodycmp.Difference{
			{Path: "/a", Kind: "value", Expected: "1", Actual: "9"},
			{Path: "/b", Kind: "value", Expected: "2", Actual: "8"},
		},
		Truncated: true,
	}
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("first result = %#v, want %#v", first, want)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("second result = %#v, want deterministic %#v", second, first)
	}
	for _, difference := range first.Differences {
		if len(difference.Expected) > 5 || len(difference.Actual) > 5 {
			t.Fatalf("unbounded difference: %#v", difference)
		}
	}
}

func TestJSONDifferenceValuesAreBounded(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{
		Limits: bodycmp.Limits{MaxDifferences: 1, MaxValueBytes: 6},
	})
	result := comparator.Compare([]byte(`"abcdefgh"`), []byte(`"ijklmnop"`))
	want := []bodycmp.Difference{{
		Path:              "",
		Kind:              "value",
		Expected:          `"abcde`,
		Actual:            `"ijklm`,
		ExpectedTruncated: true,
		ActualTruncated:   true,
	}}
	if !reflect.DeepEqual(result.Differences, want) {
		t.Fatalf("Differences = %#v, want %#v", result.Differences, want)
	}
}

func TestDifferenceBoundsPreserveUTF8(t *testing.T) {
	comparator := mustJSON(t, bodycmp.JSONOptions{
		Limits: bodycmp.Limits{MaxDifferences: 1, MaxValueBytes: 3},
	})
	result := comparator.Compare([]byte(`"éé"`), []byte(`"üü"`))
	difference := result.Differences[0]
	if !utf8.ValidString(difference.Expected) {
		t.Fatalf("Expected is not valid UTF-8: %q", difference.Expected)
	}
	if !utf8.ValidString(difference.Actual) {
		t.Fatalf("Actual is not valid UTF-8: %q", difference.Actual)
	}
	if len(difference.Expected) > 3 || len(difference.Actual) > 3 {
		t.Fatalf("unbounded values: %#v", difference)
	}
}

func TestDifferenceValueBoundsReportEachTruncatedSide(t *testing.T) {
	tests := []struct {
		name              string
		maxValueBytes     int
		expected          string
		actual            string
		expectedTruncated bool
		actualTruncated   bool
	}{
		{
			name:          "both exact at limit",
			maxValueBytes: 3,
			expected:      "a",
			actual:        "b",
		},
		{
			name:              "expected over limit",
			maxValueBytes:     3,
			expected:          "ab",
			actual:            "b",
			expectedTruncated: true,
		},
		{
			name:            "actual over limit",
			maxValueBytes:   3,
			expected:        "a",
			actual:          "bc",
			actualTruncated: true,
		},
		{
			name:              "UTF-8 boundary",
			maxValueBytes:     4,
			expected:          "éé",
			actual:            "ü",
			expectedTruncated: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			comparator, err := bodycmp.NewExact(bodycmp.Limits{
				MaxDifferences: 1,
				MaxValueBytes:  test.maxValueBytes,
			})
			if err != nil {
				t.Fatalf("NewExact() error = %v", err)
			}
			result := comparator.Compare(
				[]byte(test.expected),
				[]byte(test.actual),
			)
			difference := result.Differences[0]
			if difference.ExpectedTruncated != test.expectedTruncated ||
				difference.ActualTruncated != test.actualTruncated {
				t.Fatalf(
					"truncation = expected:%v actual:%v, want expected:%v actual:%v",
					difference.ExpectedTruncated,
					difference.ActualTruncated,
					test.expectedTruncated,
					test.actualTruncated,
				)
			}
			if len(difference.Expected) > test.maxValueBytes ||
				len(difference.Actual) > test.maxValueBytes {
				t.Fatalf("unbounded difference: %#v", difference)
			}
			if !utf8.ValidString(difference.Expected) ||
				!utf8.ValidString(difference.Actual) {
				t.Fatalf("invalid UTF-8 difference: %#v", difference)
			}
		})
	}
}

func TestJSONOptionsAreValidatedAndCopied(t *testing.T) {
	for _, options := range []bodycmp.JSONOptions{
		{},
		{Limits: bodycmp.Limits{MaxDifferences: 1}},
		{Limits: testLimits(), IgnorePointers: []string{"not-a-pointer"}},
		{Limits: testLimits(), IgnorePointers: []string{"/bad~2escape"}},
		{Limits: testLimits(), IgnorePointers: []string{"/items/**/stamp"}},
		{Limits: testLimits(), MissingEmptyObjectPointers: []string{"/bad~"}},
		{Limits: testLimits(), MissingEmptyObjectPointers: []string{"/items/**/metadata"}},
	} {
		if _, err := bodycmp.NewJSON(options); err == nil {
			t.Fatalf("NewJSON(%#v) error = nil, want error", options)
		}
	}

	pointers := []string{"/ignored"}
	fields := []string{"field"}
	empty := []string{"/empty"}
	comparator := mustJSON(t, bodycmp.JSONOptions{
		Limits:                     testLimits(),
		IgnorePointers:             pointers,
		IgnoreFieldNames:           fields,
		MissingEmptyObjectPointers: empty,
	})
	pointers[0] = "/other"
	fields[0] = "other"
	empty[0] = "/other"
	result := comparator.Compare(
		[]byte(`{"ignored":1,"field":1}`),
		[]byte(`{"ignored":2,"field":2,"empty":{}}`),
	)
	if !result.Matched() {
		t.Fatalf("options were not copied; differences = %#v", result.Differences)
	}
}

func TestFormSemanticComparison(t *testing.T) {
	strict := mustForm(t, bodycmp.FormOptions{Limits: testLimits()})
	sorted := mustForm(t, bodycmp.FormOptions{
		Limits:            testLimits(),
		SortValuesForKeys: []string{"tag"},
	})

	if result := strict.Compare([]byte("b=2&a=1"), []byte("a=1&b=2")); !result.Matched() {
		t.Fatalf("key-order comparison differences = %#v", result.Differences)
	}
	if result := strict.Compare([]byte("tag=a&tag=b"), []byte("tag=b&tag=a")); result.Matched() {
		t.Fatal("strict repeated values unexpectedly matched")
	}
	if result := sorted.Compare([]byte("tag=a&tag=b&ordered=1&ordered=2"), []byte("tag=b&tag=a&ordered=1&ordered=2")); !result.Matched() {
		t.Fatalf("selected-key sorting differences = %#v", result.Differences)
	}
	if result := sorted.Compare([]byte("ordered=1&ordered=2"), []byte("ordered=2&ordered=1")); result.Matched() {
		t.Fatal("unselected repeated values unexpectedly matched")
	}
	if result := strict.Compare([]byte("name=hello+world"), []byte("name=hello%20world")); !result.Matched() {
		t.Fatalf("equivalent encodings differences = %#v", result.Differences)
	}
}

func TestFormDifferencesAreDeterministicBoundedAndEscaped(t *testing.T) {
	comparator := mustForm(t, bodycmp.FormOptions{
		Limits: bodycmp.Limits{MaxDifferences: 2, MaxValueBytes: 4},
	})
	expected := []byte("z=abcdefgh&a=1&b=2&x%2Fy=left")
	actual := []byte("z=ijklmnop&a=9&b=8&x%2Fy=right")
	result := comparator.Compare(expected, actual)
	want := bodycmp.Result{
		Differences: []bodycmp.Difference{
			{Path: "/a/0", Kind: "value", Expected: `"1"`, Actual: `"9"`},
			{Path: "/b/0", Kind: "value", Expected: `"2"`, Actual: `"8"`},
		},
		Truncated: true,
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("result = %#v, want %#v", result, want)
	}

	escaped := mustForm(t, bodycmp.FormOptions{Limits: testLimits()}).Compare(
		[]byte("x%2Fy=left"),
		[]byte("x%2Fy=right"),
	)
	if got := escaped.Differences[0].Path; got != "/x~1y/0" {
		t.Fatalf("Path = %q, want %q", got, "/x~1y/0")
	}
}

func TestFormInvalidNeverMatches(t *testing.T) {
	comparator := mustForm(t, bodycmp.FormOptions{Limits: testLimits()})
	result := comparator.Compare([]byte("bad=%zz"), []byte("bad=%zz"))
	if result.Matched() {
		t.Fatal("Matched() = true, want false")
	}
	if got := result.Differences[0].Kind; got != "invalid" {
		t.Fatalf("Kind = %q, want invalid", got)
	}
}

func TestFormOptionsAreValidatedAndCopied(t *testing.T) {
	if _, err := bodycmp.NewForm(bodycmp.FormOptions{}); err == nil {
		t.Fatal("NewForm() error = nil, want error")
	}
	keys := []string{"tag"}
	comparator := mustForm(t, bodycmp.FormOptions{
		Limits:            testLimits(),
		SortValuesForKeys: keys,
	})
	keys[0] = "other"
	if result := comparator.Compare([]byte("tag=a&tag=b"), []byte("tag=b&tag=a")); !result.Matched() {
		t.Fatalf("options were not copied; differences = %#v", result.Differences)
	}
}

func TestExactBytes(t *testing.T) {
	comparator, err := bodycmp.NewExact(bodycmp.Limits{MaxDifferences: 1, MaxValueBytes: 8})
	if err != nil {
		t.Fatalf("NewExact() error = %v", err)
	}
	if result := comparator.Compare([]byte{0, 1, 2}, []byte{0, 1, 2}); !result.Matched() {
		t.Fatalf("equal bytes differences = %#v", result.Differences)
	}
	result := comparator.Compare([]byte{0, 1, 2}, []byte{0, 1, 3})
	if result.Matched() {
		t.Fatal("different bytes matched")
	}
	if len(result.Differences) != 1 {
		t.Fatalf("len(Differences) = %d, want 1", len(result.Differences))
	}
	difference := result.Differences[0]
	if difference.Path != "" || difference.Kind != "value" {
		t.Fatalf("Difference = %#v", difference)
	}
	if len(difference.Expected) > 8 || len(difference.Actual) > 8 {
		t.Fatalf("unbounded Difference = %#v", difference)
	}
}

func TestExactRequiresPositiveLimits(t *testing.T) {
	for _, limits := range []bodycmp.Limits{
		{},
		{MaxDifferences: -1, MaxValueBytes: 1},
		{MaxDifferences: 1, MaxValueBytes: -1},
	} {
		if _, err := bodycmp.NewExact(limits); err == nil {
			t.Fatalf("NewExact(%#v) error = nil, want error", limits)
		}
	}
}

func testLimits() bodycmp.Limits {
	return bodycmp.Limits{MaxDifferences: 10, MaxValueBytes: 128}
}

func mustJSON(t *testing.T, options bodycmp.JSONOptions) bodycmp.Comparator {
	t.Helper()
	comparator, err := bodycmp.NewJSON(options)
	if err != nil {
		t.Fatalf("NewJSON() error = %v", err)
	}
	return comparator
}

func mustForm(t *testing.T, options bodycmp.FormOptions) bodycmp.Comparator {
	t.Helper()
	comparator, err := bodycmp.NewForm(options)
	if err != nil {
		t.Fatalf("NewForm() error = %v", err)
	}
	return comparator
}

func validity(value string) string {
	trimmed := strings.TrimSpace(value)
	switch trimmed {
	case "{", `{"a":`, "null null":
		return "<invalid JSON>"
	default:
		return "<valid JSON>"
	}
}
