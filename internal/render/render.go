// Package render formats bounded unified response diffs for shadowmw observer
// delivery.
package render

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/sergeybataev/shadowmw/internal/bodycmp"
)

// ResponseInput describes one branch response and its compared body bytes for
// unified diff rendering.
type ResponseInput struct {
	StatusCode int
	Header     http.Header
	Trailer    http.Header
	Body       []byte
	Compared   []byte
	Truncated  bool
}

// OutcomeDiff renders a bounded unified diff between expected and actual
// responses with up to contextLines of surrounding context and at most maxBytes
// of valid UTF-8 output.
func OutcomeDiff(
	expected ResponseInput,
	actual ResponseInput,
	contextLines int,
	maxBytes int64,
) []byte {
	expectedLines := renderResponse(expected)
	actualLines := renderResponse(actual)
	rendered := unifiedDifference(expectedLines, actualLines, contextLines, maxBytes)
	return append([]byte(nil), rendered...)
}

func renderResponse(input ResponseInput) []string {
	lines := []string{fmt.Sprintf("HTTP %d", input.StatusCode)}
	lines = append(lines, renderHeaderLines(input.Header)...)
	if len(input.Trailer) > 0 {
		lines = append(lines, "Trailers:")
		lines = append(lines, renderHeaderLines(input.Trailer)...)
	}
	if input.Truncated {
		lines = append(lines, "Capture-Truncated: true")
	}
	lines = append(lines, "")
	body := input.Compared
	if body == nil {
		body = input.Body
	}
	lines = append(lines, strings.Split(normalizeRenderedBody(
		body,
		input.Header.Get("Content-Type"),
	), "\n")...)
	return lines
}

func renderHeaderLines(header http.Header) []string {
	names := make([]string, 0, len(header))
	for name := range header {
		names = append(names, name)
	}
	sort.Strings(names)
	var lines []string
	for _, name := range names {
		for _, value := range header[name] {
			lines = append(lines, http.CanonicalHeaderKey(name)+": "+value)
		}
	}
	return lines
}

func normalizeRenderedBody(body []byte, contentType string) string {
	mediaType, _ := bodycmp.ParseMediaType(contentType)
	switch {
	case bodycmp.IsJSONMediaType(mediaType):
		if normalized, ok := bodycmp.NormalizeRenderedJSON(body); ok {
			return normalized
		}
	case bodycmp.IsFormMediaType(mediaType):
		if encoded, ok := bodycmp.NormalizeForm(body); ok {
			return encoded
		}
	}
	if utf8.Valid(body) && !binaryMediaType(mediaType) {
		return string(body)
	}
	return summarizeBinaryBody(body)
}

func binaryMediaType(mediaType string) bool {
	switch mediaType {
	case "application/octet-stream",
		"application/pdf",
		"application/zip",
		"application/gzip",
		"application/x-gzip",
		"application/protobuf",
		"application/x-protobuf":
		return true
	}
	return strings.HasPrefix(mediaType, "image/") ||
		strings.HasPrefix(mediaType, "audio/") ||
		strings.HasPrefix(mediaType, "video/") ||
		strings.HasPrefix(mediaType, "font/")
}

func summarizeBinaryBody(body []byte) string {
	const maximumPreviewBytes = 32
	preview := body
	suffix := ""
	if len(preview) > maximumPreviewBytes {
		preview = preview[:maximumPreviewBytes]
		suffix = "..."
	}
	return fmt.Sprintf(
		"<binary length=%d hex=%s%s>",
		len(body),
		hex.EncodeToString(preview),
		suffix,
	)
}

func unifiedDifference(
	expected,
	actual []string,
	contextLines int,
	maxBytes int64,
) []byte {
	if slicesEqual(expected, actual) {
		return nil
	}
	prefix := 0
	for prefix < len(expected) && prefix < len(actual) &&
		expected[prefix] == actual[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(expected)-prefix && suffix < len(actual)-prefix &&
		expected[len(expected)-1-suffix] == actual[len(actual)-1-suffix] {
		suffix++
	}
	start := max(0, prefix-contextLines)
	expectedEnd := min(len(expected), len(expected)-suffix+contextLines)
	actualEnd := min(len(actual), len(actual)-suffix+contextLines)

	output := newBoundedRender(maxBytes)
	output.write("--- authoritative\n")
	output.write("+++ shadow\n")
	fmt.Fprintf(
		output,
		"@@ -%d,%d +%d,%d @@\n",
		start+1,
		expectedEnd-start,
		start+1,
		actualEnd-start,
	)
	for _, line := range expected[start:prefix] {
		writeUnifiedLine(output, " ", line)
	}
	for _, line := range expected[prefix : len(expected)-suffix] {
		writeUnifiedLine(output, "-", line)
	}
	for _, line := range actual[prefix : len(actual)-suffix] {
		writeUnifiedLine(output, "+", line)
	}
	commonSuffixStart := len(expected) - suffix
	for _, line := range expected[commonSuffixStart:expectedEnd] {
		writeUnifiedLine(output, " ", line)
	}
	return output.bytes()
}

func writeUnifiedLine(output *boundedRender, prefix, line string) {
	if prefix == " " && line == "" {
		output.write("\n")
		return
	}
	output.write(prefix + line + "\n")
}

type boundedRender struct {
	buffer    bytes.Buffer
	limit     int64
	truncated bool
}

func (b *boundedRender) Write(value []byte) (int, error) {
	b.write(string(value))
	return len(value), nil
}

func (b *boundedRender) write(value string) {
	if b.truncated {
		return
	}
	value = strings.ToValidUTF8(value, "\uFFFD")
	remaining := b.limit - int64(b.buffer.Len())
	if remaining <= 0 {
		return
	}
	if int64(len(value)) > remaining {
		value, b.truncated = bodycmp.BoundUTF8(value, int(remaining))
	}
	b.buffer.WriteString(value)
}

func (b *boundedRender) bytes() []byte {
	return b.buffer.Bytes()
}

func newBoundedRender(limit int64) *boundedRender {
	if limit < 0 {
		limit = 0
	}
	output := &boundedRender{limit: limit}
	output.buffer.Grow(int(min(limit, 64<<10)))
	return output
}

func slicesEqual(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	for i := range first {
		if first[i] != second[i] {
			return false
		}
	}
	return true
}
