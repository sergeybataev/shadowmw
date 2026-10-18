package core_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	shadowmw "github.com/sergeybataev/shadowmw/internal/core"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestResponseParityRejectsInvalidHeaderNames(t *testing.T) {
	for _, option := range []shadowmw.ResponseParityOption{
		shadowmw.IgnoreHeaders("bad header"),
		shadowmw.CompareOnlyHeaders("bad\nheader"),
		shadowmw.SortHeaderValues("bad:header"),
	} {
		if _, err := shadowmw.NewRule(
			"headers",
			shadowmw.ResponseParity(option),
			shadowmw.MatchAll(),
			shadowmw.SampleAll(),
		); err == nil {
			t.Errorf("NewRule(ResponseParity(invalid header option)) error = nil")
		}
	}
}

func TestResponseParityComparesAllEnabledDimensions(t *testing.T) {
	policy := shadowmw.ResponseParity(shadowmw.CompareBodyAsBytes(), shadowmw.WithMaxDifferences(3))
	comparison, err := policy.Compare(
		context.Background(),
		responseSnapshot(t, http.StatusOK, http.Header{"X-Test": {"a"}}, nil, "left"),
		responseSnapshot(t, http.StatusCreated, http.Header{"X-Test": {"b"}}, nil, "right"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	wantDimensions := []string{"status", "header", "body"}
	if len(comparison.Differences) != len(wantDimensions) {
		t.Fatalf("Differences = %#v, want dimensions %v", comparison.Differences, wantDimensions)
	}
	for i, want := range wantDimensions {
		if got := comparison.Differences[i].Dimension; got != want {
			t.Errorf("Differences[%d].Dimension = %q, want %q", i, got, want)
		}
	}
}

func TestResponseParityCustomBodyPreservesMetadataAndBoundsResults(t *testing.T) {
	policy := shadowmw.ResponseParity(
		shadowmw.WithMaxDifferences(3),
		shadowmw.WithMaxDifferenceValueBytes(4),
		shadowmw.CompareBodyWith(func(context.Context, shadowmw.BodyComparisonInput) ([]shadowmw.Difference, error) {
			return []shadowmw.Difference{
				{Dimension: "service", Path: "/z", Kind: "domain", Expected: "ééé", Actual: string([]byte{0xff, 0xfe})},
				{Dimension: "service", Path: "/a", Kind: "domain", Expected: "extra", Actual: "extra"},
			}, nil
		}),
	)
	comparison, err := policy.Compare(
		context.Background(),
		responseSnapshot(t, http.StatusOK, http.Header{"X-Test": {"a"}}, nil, "left"),
		responseSnapshot(t, http.StatusCreated, http.Header{"X-Test": {"b"}}, nil, "right"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if len(comparison.Differences) != 3 {
		t.Fatalf("len(Differences) = %d, want 3: %#v", len(comparison.Differences), comparison.Differences)
	}
	if comparison.Differences[0].Dimension != "status" ||
		comparison.Differences[1].Dimension != "header" {
		t.Fatalf("metadata differences were lost: %#v", comparison.Differences)
	}
	custom := comparison.Differences[2]
	if custom.Dimension != "body" || custom.Path != "/z" ||
		custom.Kind != "domain" {
		t.Fatalf("custom difference = %#v", custom)
	}
	if !custom.ExpectedTruncated() || custom.ActualTruncated() ||
		!custom.DifferencesTruncated() {
		t.Fatalf("custom truncation evidence = %#v", custom)
	}
	if len(custom.Expected) > 4 || len(custom.Actual) > 4 ||
		!utf8.ValidString(custom.Expected) || !utf8.ValidString(custom.Actual) {
		t.Fatalf("custom values are not independently UTF-8 bounded: %#v", custom)
	}
}

func TestResponseParityClassifiesCustomComparatorFailures(t *testing.T) {
	tests := []struct {
		name       string
		comparator shadowmw.BodyComparisonFunc
		wantKind   string
	}{
		{
			name: "error",
			comparator: func(context.Context, shadowmw.BodyComparisonInput) ([]shadowmw.Difference, error) {
				return nil, errors.New("sensitive service error")
			},
			wantKind: "custom_comparator_error",
		},
		{
			name: "panic",
			comparator: func(context.Context, shadowmw.BodyComparisonInput) ([]shadowmw.Difference, error) {
				panic("sensitive service panic")
			},
			wantKind: "custom_comparator_panic",
		},
		{
			name: "ignored cancellation",
			comparator: func(context.Context, shadowmw.BodyComparisonInput) ([]shadowmw.Difference, error) {
				return nil, nil
			},
			wantKind: "custom_comparator_canceled",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.name == "ignored cancellation" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			comparison, err := shadowmw.ResponseParity(shadowmw.CompareBodyWith(test.comparator)).Compare(
				ctx,
				responseSnapshot(t, http.StatusOK, nil, nil, "left"),
				responseSnapshot(t, http.StatusOK, nil, nil, "right"),
			)
			if err != nil {
				t.Fatalf("Compare() error = %v", err)
			}
			if len(comparison.Differences) != 1 ||
				comparison.Differences[0].Kind != test.wantKind {
				t.Fatalf("Differences = %#v, want kind %q", comparison.Differences, test.wantKind)
			}
		})
	}
}

func TestResponseParityClassifiesCustomDecoderFailures(t *testing.T) {
	tests := []struct {
		name    string
		decoder shadowmw.ContentDecoder
	}{
		{
			name: "error",
			decoder: func(io.Reader) (io.ReadCloser, error) {
				return nil, errors.New("sensitive decoder error")
			},
		},
		{
			name: "nil reader",
			decoder: func(io.Reader) (io.ReadCloser, error) {
				return nil, nil
			},
		},
		{
			name: "panic",
			decoder: func(io.Reader) (io.ReadCloser, error) {
				panic("sensitive decoder panic")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := shadowmw.ResponseParity(shadowmw.WithContentDecoder("br", test.decoder))
			headers := http.Header{"Content-Encoding": {"br"}}
			comparison, err := policy.Compare(
				context.Background(),
				responseSnapshot(t, http.StatusOK, headers, nil, "left"),
				responseSnapshot(t, http.StatusOK, headers, nil, "right"),
			)
			if err != nil {
				t.Fatalf("Compare() error = %v", err)
			}
			if len(comparison.Differences) != 1 ||
				comparison.Differences[0].Kind != "decode_error" {
				t.Fatalf("Differences = %#v, want decode_error", comparison.Differences)
			}
		})
	}
}

func TestResponseParityRejectsInvalidContentDecoderToken(t *testing.T) {
	if _, err := shadowmw.NewRule(
		"decoder",
		shadowmw.ResponseParity(shadowmw.WithContentDecoder("bad encoding", func(reader io.Reader) (io.ReadCloser, error) {
			return io.NopCloser(reader), nil
		})),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	); err == nil {
		t.Fatal("NewRule() error = nil, want invalid decoder token error")
	}
}

func TestResponseParityMergesDifferentlyCasedHeaderNames(t *testing.T) {
	policy := shadowmw.ResponseParity(shadowmw.DisableBodyComparison())
	comparison, err := policy.Compare(
		context.Background(),
		responseSnapshot(t, http.StatusOK, http.Header{
			"X-Test": {"first"},
			"x-test": {"second"},
		}, nil, ""),
		responseSnapshot(t, http.StatusOK, http.Header{
			"X-Test": {"first", "second"},
		}, nil, ""),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if !comparison.Matched() {
		t.Fatalf("Differences = %#v, want parity", comparison.Differences)
	}
}

func TestResponseParityRejectsMalformedContentEncodingList(t *testing.T) {
	headers := http.Header{"Content-Encoding": {"gzip,,identity"}}
	comparison, err := shadowmw.ResponseParity().Compare(
		context.Background(),
		responseSnapshot(t, http.StatusOK, headers, nil, strings.Repeat("x", 4)),
		responseSnapshot(t, http.StatusOK, headers, nil, strings.Repeat("x", 4)),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if len(comparison.Differences) != 1 ||
		comparison.Differences[0].Kind != "decode_error" {
		t.Fatalf("Differences = %#v, want decode_error", comparison.Differences)
	}
}

func TestResponseParityDecodesBoundedContentWithoutHidingEncodingMetadata(t *testing.T) {
	body := []byte(`{"value":1}`)
	compressed := gzipResponseBody(t, body)
	authoritative := responseSnapshot(t, http.StatusOK, http.Header{
		"Content-Encoding": {"gzip"},
		"Content-Type":     {"application/json"},
	}, nil, string(compressed))
	shadow := responseSnapshot(t, http.StatusOK, http.Header{"Content-Type": {"application/json"}}, nil, string(body))

	strict, err := shadowmw.ResponseParity().Compare(context.Background(), authoritative, shadow)
	if err != nil {
		t.Fatalf("strict Compare() error = %v", err)
	}
	if len(strict.Differences) != 1 ||
		strict.Differences[0].Dimension != "header" ||
		strict.Differences[0].Path != "Content-Encoding" {
		t.Fatalf("strict Differences = %#v, want encoding header difference only", strict.Differences)
	}

	semantic, err := shadowmw.ResponseParity(
		shadowmw.IgnoreHeaders("Content-Encoding"),
	).Compare(context.Background(), authoritative, shadow)
	if err != nil {
		t.Fatalf("semantic Compare() error = %v", err)
	}
	if !semantic.Matched() {
		t.Fatalf("semantic Differences = %#v, want parity", semantic.Differences)
	}
}

func TestResponseParityRejectsMalformedAndOversizedDecodedBodies(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		body   string
		option shadowmw.ResponseParityOption
	}{
		{
			name:   "malformed gzip",
			header: http.Header{"Content-Encoding": {"gzip"}},
			body:   "not gzip",
		},
		{
			name:   "oversized identity",
			body:   "12345",
			option: shadowmw.WithMaxDecodedBodyBytes(4),
		},
		{
			name:   "unsupported",
			header: http.Header{"Content-Encoding": {"unknown"}},
			body:   "same",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var options []shadowmw.ResponseParityOption
			if test.option != nil {
				options = append(options, test.option)
			}
			comparison, err := shadowmw.ResponseParity(options...).Compare(
				context.Background(),
				responseSnapshot(t, http.StatusOK, test.header, nil, test.body),
				responseSnapshot(t, http.StatusOK, test.header, nil, test.body),
			)
			if err != nil {
				t.Fatalf("Compare() error = %v", err)
			}
			if len(comparison.Differences) != 1 ||
				comparison.Differences[0].Kind != "decode_error" {
				t.Fatalf("Differences = %#v, want decode_error", comparison.Differences)
			}
		})
	}
}

func gzipResponseBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	return compressed.Bytes()
}

func TestJSONParityNormalizesExtremeExponentsWithoutExpansion(t *testing.T) {
	for _, test := range []struct{ name, authoritative, shadow, normalized string }{
		{name: "maximum int64", authoritative: `{"n":1e9223372036854775807}`, shadow: `{"n":10e9223372036854775806}`, normalized: `{"n":1e9223372036854775807}`},
		{name: "minimum int64", authoritative: `{"n":1e-9223372036854775808}`, shadow: `{"n":0.1e-9223372036854775807}`, normalized: `{"n":1e-9223372036854775808}`},
		{name: "beyond int64", authoritative: `{"n":1e9223372036854775808}`, shadow: `{"n":10e9223372036854775807}`, normalized: `{"n":1e9223372036854775808}`},
		{name: "below int64", authoritative: `{"n":1e-9223372036854775809}`, shadow: `{"n":0.1e-9223372036854775808}`, normalized: `{"n":1e-9223372036854775809}`},
		{name: "positive expansion exceeds bound", authoritative: `{"n":1e1024}`, shadow: `{"n":10e1023}`, normalized: `{"n":1e1024}`},
		{name: "negative expansion exceeds bound", authoritative: `{"n":1e-1023}`, shadow: `{"n":0.1e-1022}`, normalized: `{"n":1e-1023}`},
		{name: "decimal point within digits", authoritative: `{"n":12300e-3}`, shadow: `{"n":12.3}`, normalized: `{"n":12.3}`},
		{name: "decimal point before digits", authoritative: `{"n":1e-3}`, shadow: `{"n":0.001}`, normalized: `{"n":0.001}`},
		{name: "negative zero", authoritative: `{"n":-0e-9223372036854775808}`, shadow: `{"n":0}`, normalized: `{"n":0}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 1)
			engine := newAcceptanceEngine(t, 1, outcomes)
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			middleware := newAcceptanceMiddleware(t, engine, statusHandler(http.StatusOK, test.authoritative), cfg)
			middleware.Wrap(statusHandler(http.StatusOK, test.shadow)).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			outcome := waitForOutcome(t, outcomes)
			if outcome.Classification() != shadowmw.OutcomeMatch {
				t.Fatalf("outcome = %s/%s, want exact JSON parity", outcome.Classification(), outcome.Reason())
			}
			for _, representation := range []shadowmw.BodyRepresentation{outcome.AuthoritativeBodyRepresentation(), outcome.ShadowBodyRepresentation()} {
				if !representation.Present() || representation.Kind() != shadowmw.BodyRepresentationNormalized || string(representation.Body()) != test.normalized {
					t.Errorf("normalized representation = %q, want %q", representation.Body(), test.normalized)
				}
			}
		})
	}
}

// Policy tests obtain immutable evidence at the HTTP/observer boundary rather
// than depending on snapshot storage fields.
func responseSnapshot(t *testing.T, status int, header, trailer http.Header, body string) shadowmw.ResponseSnapshot {
	t.Helper()
	outcomes := make(chan shadowmw.Outcome, 1)
	engine := newAcceptanceEngine(t, 1, outcomes)
	rule, err := shadowmw.NewRule("snapshot.fixture", shadowmw.ResponseParity(shadowmw.DisableHeaderComparison(), shadowmw.DisableBodyComparison()), shadowmw.MatchAll(), shadowmw.SampleAll())
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for name, values := range header {
			w.Header()[name] = append([]string(nil), values...)
		}
		if len(trailer) > 0 {
			names := make([]string, 0, len(trailer))
			for name := range trailer {
				names = append(names, name)
			}
			sort.Strings(names)
			w.Header().Set("Trailer", strings.Join(names, ", "))
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
		for name, values := range trailer {
			w.Header()[name] = append([]string(nil), values...)
		}
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	cfg.MaxResponseBodyBytes = int64(len(body)) + 1
	middleware, err := shadowmw.NewMiddleware(engine, handler, cfg, rule)
	if err != nil {
		t.Fatal(err)
	}
	middleware.Wrap(handler).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	return waitForOutcome(t, outcomes).AuthoritativeResponse()
}

func TestDifferenceOverflowMarksLastRetainedDifferenceAcrossDimensions(t *testing.T) {
	tests := []struct {
		name          string
		policy        shadowmw.Policy
		authoritative publicParityResponse
		shadow        publicParityResponse
		wantDimension string
		wantExpected  bool
	}{
		{
			name: "header overflow after status",
			policy: shadowmw.ResponseParity(
				shadowmw.DisableBodyComparison(),
				shadowmw.WithMaxDifferences(1),
			),
			authoritative: publicParityResponse{
				status: http.StatusOK,
				header: http.Header{"X-Test": {"authoritative"}},
			},
			shadow: publicParityResponse{
				status: http.StatusCreated,
				header: http.Header{"X-Test": {"shadow"}},
			},
			wantDimension: "status",
		},
		{
			name: "trailer overflow after header",
			policy: shadowmw.ResponseParity(
				shadowmw.DisableBodyComparison(),
				shadowmw.WithMaxDifferences(1),
			),
			authoritative: publicParityResponse{
				status:  http.StatusOK,
				header:  http.Header{"X-Test": {"authoritative"}},
				trailer: http.Header{"X-Trailer": {"authoritative"}},
			},
			shadow: publicParityResponse{
				status:  http.StatusOK,
				header:  http.Header{"X-Test": {"shadow"}},
				trailer: http.Header{"X-Trailer": {"shadow"}},
			},
			wantDimension: "header",
		},
		{
			name: "body overflow after trailer",
			policy: shadowmw.ResponseParity(
				shadowmw.CompareBodyAsBytes(),
				shadowmw.WithMaxDifferences(1),
			),
			authoritative: publicParityResponse{
				status:  http.StatusOK,
				trailer: http.Header{"X-Trailer": {"authoritative"}},
				body:    "authoritative",
			},
			shadow: publicParityResponse{
				status:  http.StatusOK,
				trailer: http.Header{"X-Trailer": {"shadow"}},
				body:    "shadow",
			},
			wantDimension: "trailer",
		},
		{
			name: "body comparator overflow",
			policy: shadowmw.ResponseParity(
				shadowmw.CompareBodyAsJSON(),
				shadowmw.WithMaxDifferences(1),
			),
			authoritative: publicParityResponse{
				status: http.StatusOK,
				body:   `{"a":1,"b":2}`,
			},
			shadow: publicParityResponse{
				status: http.StatusOK,
				body:   `{"a":9,"b":8}`,
			},
			wantDimension: "body",
		},
		{
			name: "custom comparator overflow",
			policy: shadowmw.ResponseParity(
				shadowmw.WithMaxDifferences(1),
				shadowmw.WithMaxDifferenceValueBytes(4),
				shadowmw.CompareBodyWith(func(
					context.Context,
					shadowmw.BodyComparisonInput,
				) ([]shadowmw.Difference, error) {
					return []shadowmw.Difference{
						{
							Path:     "/first",
							Kind:     "domain",
							Expected: "abcde",
							Actual:   "1234",
						},
						{Path: "/second", Kind: "domain"},
					}, nil
				}),
			),
			authoritative: publicParityResponse{status: http.StatusOK},
			shadow:        publicParityResponse{status: http.StatusOK},
			wantDimension: "body",
			wantExpected:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := observePublicParity(
				t,
				test.policy,
				test.authoritative,
				test.shadow,
			)
			if len(outcome.Differences()) != 1 {
				t.Fatalf("Differences = %#v, want one retained difference", outcome.Differences())
			}
			difference := outcome.Differences()[0]
			if difference.Dimension != test.wantDimension ||
				!difference.DifferencesTruncated() ||
				difference.ExpectedTruncated() != test.wantExpected {
				t.Fatalf(
					"last retained difference = %#v, want %s with omitted-difference evidence",
					difference,
					test.wantDimension,
				)
			}
		})
	}
}

func TestObservedDifferenceReportsEachValueTruncation(t *testing.T) {
	tests := []struct {
		name              string
		expected          string
		actual            string
		expectedTruncated bool
		actualTruncated   bool
	}{
		{
			name:     "exact at limit",
			expected: "abcd",
			actual:   "1234",
		},
		{
			name:              "expected over limit",
			expected:          "abcde",
			actual:            "1234",
			expectedTruncated: true,
		},
		{
			name:            "actual over limit",
			expected:        "abcd",
			actual:          "12345",
			actualTruncated: true,
		},
		{
			name:              "UTF-8 boundary",
			expected:          "ééé",
			actual:            "üüü",
			expectedTruncated: true,
			actualTruncated:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := observePublicParity(
				t,
				shadowmw.ResponseParity(
					shadowmw.WithMaxDifferenceValueBytes(4),
					shadowmw.CompareBodyWith(func(
						context.Context,
						shadowmw.BodyComparisonInput,
					) ([]shadowmw.Difference, error) {
						return []shadowmw.Difference{{
							Kind:     "domain",
							Expected: test.expected,
							Actual:   test.actual,
						}}, nil
					}),
				),
				publicParityResponse{status: http.StatusOK},
				publicParityResponse{status: http.StatusOK},
			)
			difference := outcome.Differences()[0]
			if difference.ExpectedTruncated() != test.expectedTruncated ||
				difference.ActualTruncated() != test.actualTruncated {
				t.Fatalf(
					"truncation = expected:%v actual:%v, want expected:%v actual:%v",
					difference.ExpectedTruncated(),
					difference.ActualTruncated(),
					test.expectedTruncated,
					test.actualTruncated,
				)
			}
			if len(difference.Expected) > 4 || len(difference.Actual) > 4 ||
				!utf8.ValidString(difference.Expected) ||
				!utf8.ValidString(difference.Actual) {
				t.Fatalf("difference values are not independently bounded: %#v", difference)
			}
		})
	}
}

func TestObservedBodyDifferenceCarriesValueTruncation(t *testing.T) {
	outcome := observePublicParity(
		t,
		shadowmw.ResponseParity(
			shadowmw.CompareBodyAsBytes(),
			shadowmw.WithMaxDifferenceValueBytes(3),
		),
		publicParityResponse{status: http.StatusOK, body: "ab"},
		publicParityResponse{status: http.StatusOK, body: "b"},
	)
	difference := outcome.Differences()[0]
	if difference.Kind != "value" ||
		!difference.ExpectedTruncated() || difference.ActualTruncated() {
		t.Fatalf("body difference truncation = %#v", difference)
	}
}

type publicParityResponse struct {
	status  int
	header  http.Header
	trailer http.Header
	body    string
}

func observePublicParity(
	t *testing.T,
	policy shadowmw.Policy,
	authoritative publicParityResponse,
	shadow publicParityResponse,
) shadowmw.Outcome {
	t.Helper()
	outcomes := make(chan shadowmw.Outcome, 2)
	engine := newAcceptanceEngine(t, 1, outcomes)
	rule, err := shadowmw.NewRule(
		"public.truncation",
		policy,
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		publicParityHandler(authoritative),
		cfg,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	middleware.Wrap(publicParityHandler(shadow)).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	return waitForOutcome(t, outcomes)
}

func publicParityHandler(response publicParityResponse) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		for name, values := range response.header {
			for _, value := range values {
				writer.Header().Add(name, value)
			}
		}
		for name := range response.trailer {
			writer.Header().Add("Trailer", name)
		}
		status := response.status
		if status == 0 {
			status = http.StatusOK
		}
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(response.body))
		for name, values := range response.trailer {
			for _, value := range values {
				writer.Header().Add(name, value)
			}
		}
	})
}
