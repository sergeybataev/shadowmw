package core

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/sergeybataev/shadowmw/internal/bodycmp"
)

const (
	// defaultDecodedBodyLimit bounds decompressed response bodies to 4 MiB per branch.
	defaultDecodedBodyLimit = int64(4 << 20)
	// defaultMaxDifferences bounds retained differences per comparison outcome.
	defaultMaxDifferences = 100
	// defaultMaxValueBytes bounds each Expected and Actual value string to 4 KiB.
	defaultMaxValueBytes = 4096
)

// ContentDecoder constructs a bounded-decoding stream for a custom
// Content-Encoding token.
type ContentDecoder func(io.Reader) (io.ReadCloser, error)

// BodyComparisonInput contains detached decoded bodies for a custom comparator.
type BodyComparisonInput struct {
	AuthoritativeContentType string
	ShadowContentType        string
	AuthoritativeBody        []byte
	ShadowBody               []byte
}

// BodyComparisonFunc compares decoded bodies. It must honor ctx cancellation
// and is treated as trusted concurrent service code.
type BodyComparisonFunc func(
	context.Context,
	BodyComparisonInput,
) ([]Difference, error)

// ResponseParityOption configures strict response comparison.
type ResponseParityOption func(*responseParityConfig)

// ExpectedTruncated reports whether Expected was shortened to the configured
// maximum difference-value byte length.
func (d Difference) ExpectedTruncated() bool {
	return d.truncation&differenceExpectedTruncated != 0
}

// ActualTruncated reports whether Actual was shortened to the configured
// maximum difference-value byte length.
func (d Difference) ActualTruncated() bool {
	return d.truncation&differenceActualTruncated != 0
}

// DifferencesTruncated reports whether additional differences were omitted
// after the configured maximum difference count was reached.
func (d Difference) DifferencesTruncated() bool {
	return d.truncation&differenceCountTruncated != 0
}

// ResponseParity creates strict status, header, trailer, and body comparison.
func ResponseParity(options ...ResponseParityOption) Policy {
	cfg := responseParityConfig{
		headerMode:       headersAll,
		sortHeaderValues: make(map[string]struct{}),
		bodyMode:         bodyAuto,
		maxDifferences:   defaultMaxDifferences,
		maxValueBytes:    defaultMaxValueBytes,
		maxDecodedBytes:  defaultDecodedBodyLimit,
		decoders:         make(map[string]ContentDecoder),
	}
	for _, option := range options {
		if option == nil {
			cfg.err = fmt.Errorf("nil response parity option")
			continue
		}
		option(&cfg)
	}
	return &responseParityPolicy{cfg: cfg}
}

// DisableHeaderComparison omits headers and trailers from parity.
func DisableHeaderComparison() ResponseParityOption {
	return setHeaderMode(headersDisabled, nil)
}

// IgnoreHeaders compares every header except names.
func IgnoreHeaders(names ...string) ResponseParityOption {
	return setHeaderMode(headersIgnore, names)
}

// CompareOnlyHeaders compares only names.
func CompareOnlyHeaders(names ...string) ResponseParityOption {
	return setHeaderMode(headersOnly, names)
}

// SortHeaderValues makes selected repeated-header values order-insensitive.
func SortHeaderValues(names ...string) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		for name := range normalizedHeaderNameSet(names, &cfg.err) {
			cfg.sortHeaderValues[name] = struct{}{}
		}
	}
}

// CompareBodyAsJSON forces structural JSON comparison regardless of Content-Type.
func CompareBodyAsJSON() ResponseParityOption {
	return func(cfg *responseParityConfig) { cfg.bodyMode = bodyJSON }
}

// CompareBodyAsForm forces URL-encoded form comparison regardless of Content-Type.
func CompareBodyAsForm() ResponseParityOption {
	return func(cfg *responseParityConfig) { cfg.bodyMode = bodyForm }
}

// CompareBodyAsBytes forces exact byte-by-byte body comparison.
func CompareBodyAsBytes() ResponseParityOption {
	return func(cfg *responseParityConfig) { cfg.bodyMode = bodyBytes }
}

// DisableBodyComparison omits response bodies from parity evaluation.
func DisableBodyComparison() ResponseParityOption {
	return func(cfg *responseParityConfig) { cfg.bodyMode = bodyDisabled }
}

// IgnoreJSONPointers excludes matching RFC 6901 JSON pointers from structural comparison.
func IgnoreJSONPointers(pointers ...string) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		cfg.jsonIgnore = append([]string(nil), pointers...)
	}
}

// IgnoreJSONFieldNames excludes matching JSON object field names at every depth.
func IgnoreJSONFieldNames(names ...string) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		cfg.jsonIgnoreFields = append([]string(nil), names...)
	}
}

// EquateMissingWithEmptyObject treats a missing JSON field and an empty JSON
// object as equivalent at the specified RFC 6901 pointers.
func EquateMissingWithEmptyObject(pointers ...string) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		cfg.jsonEmptyObjects = append([]string(nil), pointers...)
	}
}

// SortFormValues makes repeated form values order-insensitive for the listed keys.
func SortFormValues(keys ...string) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		cfg.formSortKeys = append([]string(nil), keys...)
	}
}

// WithMaxDifferences sets the maximum number of differences retained per comparison.
func WithMaxDifferences(maximum int) ResponseParityOption {
	return func(cfg *responseParityConfig) { cfg.maxDifferences = maximum }
}

// WithMaxDecodedBodyBytes sets the maximum decoded response body size in bytes.
func WithMaxDecodedBodyBytes(maximum int64) ResponseParityOption {
	return func(cfg *responseParityConfig) { cfg.maxDecodedBytes = maximum }
}

// WithMaxDifferenceValueBytes sets the maximum byte length of each Expected and
// Actual value retained in a Difference.
func WithMaxDifferenceValueBytes(maximum int) ResponseParityOption {
	return func(cfg *responseParityConfig) { cfg.maxValueBytes = maximum }
}

// WithContentDecoder adds one custom Content-Encoding decoder.
func WithContentDecoder(name string, decoder ContentDecoder) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		name = strings.ToLower(strings.TrimSpace(name))
		switch name {
		case "", "identity", "gzip", "deflate":
			cfg.err = fmt.Errorf("invalid custom content decoder name %q", name)
			return
		}
		if !validToken(name) {
			cfg.err = fmt.Errorf("invalid custom content decoder name %q", name)
			return
		}
		if decoder == nil {
			cfg.err = fmt.Errorf("custom content decoder %q is nil", name)
			return
		}
		cfg.decoders[name] = decoder
	}
}

// CompareBodyWith delegates decoded-body comparison to trusted service code.
func CompareBodyWith(comparator BodyComparisonFunc) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		if comparator == nil {
			cfg.err = fmt.Errorf("custom body comparator is nil")
			return
		}
		cfg.bodyMode = bodyCustom
		cfg.customBody = comparator
	}
}

type headerMode uint8

const (
	headersAll headerMode = iota
	headersDisabled
	headersIgnore
	headersOnly
)

type bodyMode uint8

const (
	bodyAuto bodyMode = iota
	bodyJSON
	bodyForm
	bodyBytes
	bodyDisabled
	bodyCustom
)

type responseParityConfig struct {
	headerMode       headerMode
	headers          map[string]struct{}
	sortHeaderValues map[string]struct{}
	bodyMode         bodyMode
	maxDifferences   int
	maxValueBytes    int
	maxDecodedBytes  int64
	jsonIgnore       []string
	jsonIgnoreFields []string
	jsonEmptyObjects []string
	formSortKeys     []string
	decoders         map[string]ContentDecoder
	customBody       BodyComparisonFunc
	err              error
}

type responseParityPolicy struct {
	cfg responseParityConfig
}

func (p *responseParityPolicy) Compare(
	ctx context.Context,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
) (Comparison, error) {
	return p.compare(ctx, authoritative, shadow, true)
}

func (p *responseParityPolicy) validationError() error {
	if p.cfg.err != nil {
		return p.cfg.err
	}
	if p.cfg.maxDifferences <= 0 {
		return fmt.Errorf("maximum differences must be positive")
	}
	if p.cfg.maxValueBytes <= 0 {
		return fmt.Errorf("maximum difference value bytes must be positive")
	}
	if p.cfg.maxDecodedBytes <= 0 {
		return fmt.Errorf("maximum decoded body bytes must be positive")
	}
	_, err := p.bodyComparator(bodyJSON)
	return err
}

func (p *responseParityPolicy) compare(
	ctx context.Context,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
	compareStatus bool,
) (Comparison, error) {
	differences := make([]Difference, 0, min(p.cfg.maxDifferences, 8))
	add := func(difference Difference) {
		differences = appendBoundedDifference(
			differences,
			difference,
			p.cfg.maxDifferences,
			p.cfg.maxValueBytes,
		)
	}

	if compareStatus && authoritative.statusCode != shadow.statusCode {
		add(Difference{
			Dimension: "status",
			Kind:      "different",
			Expected:  fmt.Sprintf("%d", authoritative.statusCode),
			Actual:    fmt.Sprintf("%d", shadow.statusCode),
		})
	}
	if p.cfg.headerMode != headersDisabled {
		compareHeaders(add, "header", authoritative.header, shadow.header, p.cfg)
		compareHeaders(add, "trailer", authoritative.trailer, shadow.trailer, p.cfg)
	}
	if authoritative.truncated || shadow.truncated {
		add(Difference{Dimension: "body", Kind: "comparison_incomplete"})
		return Comparison{Differences: differences}, nil
	}
	if p.cfg.bodyMode == bodyDisabled {
		return Comparison{Differences: differences}, nil
	}

	return p.compareBodies(ctx, add, &differences, authoritative, shadow)
}

func (p *responseParityPolicy) compareBodies(
	ctx context.Context,
	add func(Difference),
	differences *[]Difference,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
) (Comparison, error) {
	authoritativeMode := p.codecForHeader(authoritative.header)
	shadowMode := p.codecForHeader(shadow.header)
	authoritativeBody, authoritativeErr := decodeBody(
		authoritative.body,
		strings.Join(authoritative.header.Values("Content-Encoding"), ","),
		p.cfg.maxDecodedBytes,
		p.cfg.decoders,
	)
	shadowBody, shadowErr := decodeBody(
		shadow.body,
		strings.Join(shadow.header.Values("Content-Encoding"), ","),
		p.cfg.maxDecodedBytes,
		p.cfg.decoders,
	)
	authoritativeRepresentation := bodyRepresentationForComparison(
		authoritativeBody,
		authoritativeErr,
		authoritativeMode,
		p.cfg.maxDecodedBytes,
	)
	shadowRepresentation := bodyRepresentationForComparison(
		shadowBody,
		shadowErr,
		shadowMode,
		p.cfg.maxDecodedBytes,
	)
	if authoritativeErr != nil || shadowErr != nil {
		add(Difference{
			Dimension: "body",
			Kind:      "decode_error",
			Expected:  validity(authoritativeErr),
			Actual:    validity(shadowErr),
		})
		return comparedBodies(*differences, authoritativeRepresentation, shadowRepresentation), nil
	}
	if authoritativeMode != shadowMode {
		add(Difference{Dimension: "body", Kind: "codec_different"})
		return comparedBodies(*differences, authoritativeRepresentation, shadowRepresentation), nil
	}
	if authoritativeMode == bodyCustom {
		p.appendCustomBodyDifferences(
			ctx,
			add,
			differences,
			authoritative,
			shadow,
			authoritativeBody,
			shadowBody,
		)
		return comparedBodies(*differences, authoritativeRepresentation, shadowRepresentation), nil
	}
	if err := p.appendCodecBodyDifferences(
		add,
		differences,
		authoritativeMode,
		authoritativeBody,
		shadowBody,
	); err != nil {
		return Comparison{}, err
	}
	return comparedBodies(*differences, authoritativeRepresentation, shadowRepresentation), nil
}

func (p *responseParityPolicy) appendCustomBodyDifferences(
	ctx context.Context,
	add func(Difference),
	differences *[]Difference,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
	authoritativeBody []byte,
	shadowBody []byte,
) {
	customDifferences, failureKind := callBodyComparator(
		ctx,
		p.cfg.customBody,
		BodyComparisonInput{
			AuthoritativeContentType: authoritative.header.Get("Content-Type"),
			ShadowContentType:        shadow.header.Get("Content-Type"),
			AuthoritativeBody:        append([]byte(nil), authoritativeBody...),
			ShadowBody:               append([]byte(nil), shadowBody...),
		},
	)
	if failureKind != "" {
		add(Difference{Dimension: "body", Kind: failureKind})
		return
	}
	remaining := p.cfg.maxDifferences - len(*differences)
	truncated := len(customDifferences) > remaining
	for _, difference := range customDifferences[:min(len(customDifferences), remaining)] {
		difference.Dimension = "body"
		add(difference)
	}
	if truncated {
		markLastDifferenceTruncated(*differences)
	}
}

func (p *responseParityPolicy) appendCodecBodyDifferences(
	add func(Difference),
	differences *[]Difference,
	mode bodyMode,
	authoritativeBody []byte,
	shadowBody []byte,
) error {
	comparator, err := p.bodyComparator(mode)
	if err != nil {
		return err
	}
	result := comparator.Compare(authoritativeBody, shadowBody)
	remaining := p.cfg.maxDifferences - len(*differences)
	truncated := result.Truncated || len(result.Differences) > remaining
	for _, bodyDifference := range result.Differences[:min(len(result.Differences), remaining)] {
		add(Difference{
			Dimension: "body",
			Path:      bodyDifference.Path,
			Kind:      bodyDifference.Kind,
			Expected:  bodyDifference.Expected,
			Actual:    bodyDifference.Actual,
			truncation: differenceValueTruncation(
				bodyDifference.ExpectedTruncated,
				bodyDifference.ActualTruncated,
			),
		})
	}
	if truncated {
		markLastDifferenceTruncated(*differences)
	}
	return nil
}

func appendBoundedDifference(
	differences []Difference,
	difference Difference,
	maxDifferences int,
	maxValueBytes int,
) []Difference {
	if len(differences) >= maxDifferences {
		markLastDifferenceTruncated(differences)
		return differences
	}
	var expectedTruncated, actualTruncated bool
	difference.Expected, expectedTruncated = boundDifferenceValueWithTruncation(
		difference.Expected,
		maxValueBytes,
	)
	difference.Actual, actualTruncated = boundDifferenceValueWithTruncation(
		difference.Actual,
		maxValueBytes,
	)
	difference.truncation |= differenceValueTruncation(
		expectedTruncated,
		actualTruncated,
	)
	return append(differences, difference)
}

func differenceValueTruncation(
	expected bool,
	actual bool,
) differenceTruncation {
	var truncation differenceTruncation
	if expected {
		truncation |= differenceExpectedTruncated
	}
	if actual {
		truncation |= differenceActualTruncated
	}
	return truncation
}

func markLastDifferenceTruncated(differences []Difference) {
	if len(differences) == 0 ||
		differences[len(differences)-1].DifferencesTruncated() {
		return
	}
	differences[len(differences)-1].truncation |= differenceCountTruncated
}

func callBodyComparator(
	ctx context.Context,
	comparator BodyComparisonFunc,
	input BodyComparisonInput,
) ([]Difference, string) {
	var (
		differences []Difference
		failureKind string
	)
	func() {
		defer func() {
			if recover() != nil {
				differences = nil
				failureKind = "custom_comparator_panic"
			}
		}()
		var err error
		differences, err = comparator(ctx, input)
		if err != nil {
			differences = nil
			failureKind = "custom_comparator_error"
			return
		}
		if ctx.Err() != nil {
			differences = nil
			failureKind = "custom_comparator_canceled"
		}
	}()
	return differences, failureKind
}

func comparedBodies(
	differences []Difference,
	authoritative BodyRepresentation,
	shadow BodyRepresentation,
) Comparison {
	return Comparison{
		Differences:           differences,
		authoritativeCompared: authoritative.clone(),
		shadowCompared:        shadow.clone(),
	}
}

func (c Comparison) authoritativeBodyRepresentation() BodyRepresentation {
	representation := c.authoritativeCompared.clone()
	if representation.decodingState == BodyDecodingUnavailable {
		representation.decodingState = BodyDecodingNotAttempted
	}
	return representation
}

func (c Comparison) shadowBodyRepresentation() BodyRepresentation {
	representation := c.shadowCompared.clone()
	if representation.decodingState == BodyDecodingUnavailable {
		representation.decodingState = BodyDecodingNotAttempted
	}
	return representation
}

func bodyRepresentationForComparison(
	decoded []byte,
	decodeErr error,
	mode bodyMode,
	maximumBytes int64,
) BodyRepresentation {
	if decodeErr != nil {
		return failedBodyRepresentation()
	}
	switch mode {
	case bodyJSON:
		if normalized, ok := bodycmp.NormalizeComparedJSON(decoded); ok &&
			int64(len(normalized)) <= maximumBytes {
			return decodedBodyRepresentation(
				normalized,
				BodyRepresentationNormalized,
			)
		}
	case bodyForm:
		if encoded, ok := bodycmp.NormalizeForm(decoded); ok &&
			int64(len(encoded)) <= maximumBytes {
			return decodedBodyRepresentation(
				[]byte(encoded),
				BodyRepresentationNormalized,
			)
		}
	}
	return decodedBodyRepresentation(decoded, BodyRepresentationDecoded)
}

func (p *responseParityPolicy) codecForHeader(header http.Header) bodyMode {
	if p.cfg.bodyMode != bodyAuto {
		return p.cfg.bodyMode
	}
	return autoBodyMode(header.Get("Content-Type"))
}

func autoBodyMode(contentType string) bodyMode {
	mediaType, ok := bodycmp.ParseMediaType(contentType)
	if !ok {
		return bodyBytes
	}
	switch {
	case bodycmp.IsJSONMediaType(mediaType):
		return bodyJSON
	case bodycmp.IsFormMediaType(mediaType):
		return bodyForm
	default:
		return bodyBytes
	}
}

func (p *responseParityPolicy) bodyComparator(mode bodyMode) (bodycmp.Comparator, error) {
	limits := bodycmp.Limits{
		MaxDifferences: p.cfg.maxDifferences,
		MaxValueBytes:  p.cfg.maxValueBytes,
	}
	switch mode {
	case bodyJSON:
		return bodycmp.NewJSON(bodycmp.JSONOptions{
			Limits:                     limits,
			IgnorePointers:             p.cfg.jsonIgnore,
			IgnoreFieldNames:           p.cfg.jsonIgnoreFields,
			MissingEmptyObjectPointers: p.cfg.jsonEmptyObjects,
		})
	case bodyForm:
		return bodycmp.NewForm(bodycmp.FormOptions{
			Limits:            limits,
			SortValuesForKeys: p.cfg.formSortKeys,
		})
	case bodyBytes:
		return bodycmp.NewExact(limits)
	default:
		return nil, fmt.Errorf("unsupported body mode %d", mode)
	}
}

func compareHeaders(
	add func(Difference),
	dimension string,
	authoritative http.Header,
	shadow http.Header,
	cfg responseParityConfig,
) {
	expected := selectedHeaders(authoritative, cfg)
	actual := selectedHeaders(shadow, cfg)
	names := make([]string, 0, len(expected)+len(actual))
	seen := make(map[string]struct{}, len(expected)+len(actual))
	for name := range expected {
		seen[name] = struct{}{}
		names = append(names, keyOrName(name))
	}
	for name := range actual {
		if _, ok := seen[name]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		expectedValues, expectedOK := expected[name]
		actualValues, actualOK := actual[name]
		switch {
		case !expectedOK:
			add(Difference{Dimension: dimension, Path: name, Kind: "unexpected"})
		case !actualOK:
			add(Difference{Dimension: dimension, Path: name, Kind: "missing"})
		default:
			if _, ok := cfg.sortHeaderValues[name]; ok {
				expectedValues = slices.Clone(expectedValues)
				actualValues = slices.Clone(actualValues)
				sort.Strings(expectedValues)
				sort.Strings(actualValues)
			}
			if !slices.Equal(expectedValues, actualValues) {
				add(Difference{
					Dimension: dimension,
					Path:      name,
					Kind:      "different",
					Expected:  fmt.Sprintf("%q", expectedValues),
					Actual:    fmt.Sprintf("%q", actualValues),
				})
			}
		}
	}
}

func keyOrName(name string) string { return name }

func selectedHeaders(header http.Header, cfg responseParityConfig) http.Header {
	normalized := make(http.Header)
	names := make([]string, 0, len(header))
	for name := range header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, original := range names {
		name := normalizedHeaderName(original)
		_, selected := cfg.headers[name]
		switch cfg.headerMode {
		case headersIgnore:
			if selected {
				continue
			}
		case headersOnly:
			if !selected {
				continue
			}
		}
		normalized[name] = append(normalized[name], header[original]...)
	}
	return normalized
}

func decodeBody(
	body []byte,
	encoding string,
	maxBytes int64,
	custom map[string]ContentDecoder,
) ([]byte, error) {
	openCustom := func(name string, reader io.Reader) (io.ReadCloser, bool, error) {
		decoder, ok := custom[name]
		if !ok {
			return nil, false, nil
		}
		rc, err := decoder(reader)
		return rc, true, err
	}
	return bodycmp.DecodeBody(body, encoding, maxBytes, openCustom, validToken)
}

func validity(err error) string {
	if err == nil {
		return "valid"
	}
	return "invalid"
}

func setHeaderMode(mode headerMode, names []string) ResponseParityOption {
	return func(cfg *responseParityConfig) {
		if cfg.headerMode != headersAll && cfg.headerMode != mode {
			cfg.err = fmt.Errorf("header comparison modes are mutually exclusive")
			return
		}
		cfg.headerMode = mode
		cfg.headers = normalizedHeaderNameSet(names, &cfg.err)
	}
}

func normalizedHeaderNameSet(names []string, targetErr *error) map[string]struct{} {
	result := make(map[string]struct{}, len(names))
	for _, name := range names {
		if !validToken(name) {
			*targetErr = fmt.Errorf("invalid header name %q", name)
			continue
		}
		normalized := normalizedHeaderName(name)
		result[normalized] = struct{}{}
	}
	return result
}

func normalizedHeaderName(name string) string {
	if !validToken(name) {
		return name
	}
	normalized := []byte(strings.ToLower(name))
	upperNext := true
	for i := range len(normalized) {
		if upperNext && normalized[i] >= 'a' && normalized[i] <= 'z' {
			normalized[i] -= 'a' - 'A'
		}
		upperNext = normalized[i] == '-'
	}
	return string(normalized)
}

func validToken(value string) bool {
	if value == "" {
		return false
	}
	for i := range len(value) {
		character := value[i]
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		default:
			return false
		}
	}
	return true
}

func boundDifferenceValue(value string, maximum int) string {
	bounded, _ := boundDifferenceValueWithTruncation(value, maximum)
	return bounded
}

func boundDifferenceValueWithTruncation(value string, maximum int) (string, bool) {
	return bodycmp.BoundUTF8(strings.ToValidUTF8(value, "\uFFFD"), maximum)
}
