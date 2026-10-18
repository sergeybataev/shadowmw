// Package bodycmp provides deterministic, bounded comparisons of encoded
// HTTP bodies.
package bodycmp

import (
	"fmt"
	"mime"
	"strings"
	"unicode/utf8"
)

// Limits bounds the details retained by a comparison.
type Limits struct {
	// MaxDifferences is the maximum number of differences returned.
	MaxDifferences int
	// MaxValueBytes is the maximum byte length of each Expected and Actual
	// value in a Difference.
	MaxValueBytes int
}

// Comparator compares an expected body with an actual body.
type Comparator interface {
	Compare(expected, actual []byte) Result
}

// Result is the bounded result of a body comparison.
type Result struct {
	Differences []Difference
	// Truncated reports that additional differences were omitted because
	// MaxDifferences was reached.
	Truncated bool
}

// Matched reports whether the bodies have semantic parity.
func (r Result) Matched() bool {
	return len(r.Differences) == 0
}

// Difference identifies one deterministic semantic deviation.
type Difference struct {
	// Path is an RFC 6901 JSON pointer. The empty string identifies the root.
	Path string
	// Kind classifies the deviation.
	Kind string
	// Expected and Actual are deterministic representations bounded by
	// Limits.MaxValueBytes.
	Expected string
	Actual   string
	// ExpectedTruncated and ActualTruncated independently report values
	// shortened to Limits.MaxValueBytes.
	ExpectedTruncated bool
	ActualTruncated   bool
}

// BoundUTF8 truncates value to at most max bytes without splitting a valid
// UTF-8 rune, reporting whether bytes were omitted.
func BoundUTF8(value string, max int) (string, bool) {
	if len(value) <= max {
		return value, false
	}
	end := max
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end], true
}

// ParseMediaType extracts and lowercases the MIME media type from contentType.
func ParseMediaType(contentType string) (string, bool) {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", false
	}
	return strings.ToLower(mediaType), true
}

// IsJSONMediaType reports whether mediaType is application/json or an
// application/*+json structured syntax suffix type.
func IsJSONMediaType(mediaType string) bool {
	return mediaType == "application/json" ||
		strings.HasPrefix(mediaType, "application/") &&
			strings.HasSuffix(mediaType, "+json")
}

// IsFormMediaType reports whether mediaType is application/x-www-form-urlencoded.
func IsFormMediaType(mediaType string) bool {
	return mediaType == "application/x-www-form-urlencoded"
}

func (l Limits) validate() error {
	if l.MaxDifferences <= 0 {
		return fmt.Errorf("bodycmp: MaxDifferences must be positive")
	}
	if l.MaxValueBytes <= 0 {
		return fmt.Errorf("bodycmp: MaxValueBytes must be positive")
	}
	return nil
}

type differences struct {
	limits    Limits
	items     []Difference
	truncated bool
}

func (d *differences) add(path, kind, expected, actual string) {
	if len(d.items) == d.limits.MaxDifferences {
		d.truncated = true
		return
	}
	expected, expectedTruncated := BoundUTF8(expected, d.limits.MaxValueBytes)
	actual, actualTruncated := BoundUTF8(actual, d.limits.MaxValueBytes)
	d.items = append(d.items, Difference{
		Path:              path,
		Kind:              kind,
		Expected:          expected,
		Actual:            actual,
		ExpectedTruncated: expectedTruncated,
		ActualTruncated:   actualTruncated,
	})
}

func (d *differences) result() Result {
	return Result{Differences: d.items, Truncated: d.truncated}
}

func appendPointer(path, segment string) string {
	escaped := make([]byte, 0, len(segment))
	for i := range len(segment) {
		switch segment[i] {
		case '~':
			escaped = append(escaped, '~', '0')
		case '/':
			escaped = append(escaped, '~', '1')
		default:
			escaped = append(escaped, segment[i])
		}
	}
	return path + "/" + string(escaped)
}
