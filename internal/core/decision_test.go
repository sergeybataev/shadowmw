package core_test

import (
	"context"
	"errors"
	shadowmw "github.com/sergeybataev/shadowmw/internal/core"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"unicode/utf8"
)

// --- merged from decision_test.go ---

func TestDecisionParityStatusClasses(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		leftBody    string
		rightBody   string
		wantMatched bool
	}{
		{name: "equal success ignores representation", status: 201, leftBody: "a", rightBody: "b", wantMatched: true},
		{name: "equal redirect uses full parity", status: 302, leftBody: "a", rightBody: "b"},
		{name: "equal rejection uses full parity", status: 422, leftBody: `{"error":"a"}`, rightBody: `{"error":"b"}`},
		{name: "equal server error uses full parity", status: 503, leftBody: "a", rightBody: "b"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			comparison, err := shadowmw.DecisionParity(shadowmw.WithDecisionResponseOptions(
				shadowmw.CompareBodyAsBytes(),
			)).Compare(
				context.Background(),
				responseSnapshot(t, test.status, nil, nil, test.leftBody),
				responseSnapshot(t, test.status, nil, nil, test.rightBody),
			)
			if err != nil {
				t.Fatalf("Compare() error = %v", err)
			}
			if got := comparison.Matched(); got != test.wantMatched {
				t.Fatalf("Matched() = %v, want %v; Differences = %#v", got, test.wantMatched, comparison.Differences)
			}
		})
	}
}

func TestDecisionParitySwitchingProtocolsUsesResponseParity(t *testing.T) {
	comparison, err := shadowmw.DecisionParity().Compare(
		context.Background(),
		responseSnapshot(t, http.StatusSwitchingProtocols, http.Header{"X-Test": {"a"}}, nil, ""),
		responseSnapshot(t, http.StatusSwitchingProtocols, http.Header{"X-Test": {"b"}}, nil, ""),
	)
	if err != nil {
		t.Fatal(err)
	}
	if comparison.Matched() || len(comparison.Differences) != 1 || comparison.Differences[0].Dimension != "header" {
		t.Fatalf("Differences = %#v, want header mismatch", comparison.Differences)
	}
}

func TestDecisionParityDifferentStatusesAreStrictByDefault(t *testing.T) {
	comparison, err := shadowmw.DecisionParity().Compare(
		context.Background(),
		responseSnapshot(t, http.StatusForbidden, nil, nil, "same"),
		responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "same"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if comparison.Matched() || comparison.Differences[0].Dimension != "status" {
		t.Fatalf("Differences = %#v, want status mismatch", comparison.Differences)
	}
}

func TestDecisionParityRejectsDuplicateStatusWithinEquivalenceGroup(t *testing.T) {
	if _, err := shadowmw.NewRule(
		"decision",
		shadowmw.DecisionParity(shadowmw.EquivalentStatuses(400, 400)),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	); err == nil {
		t.Fatal("NewRule() error = nil, want duplicate equivalent status error")
	}
}

func TestDecisionParityValidatesStatusEquivalenceGroups(t *testing.T) {
	tests := []struct {
		name    string
		options []shadowmw.DecisionParityOption
	}{
		{name: "too small", options: []shadowmw.DecisionParityOption{shadowmw.EquivalentStatuses(400)}},
		{name: "below HTTP range", options: []shadowmw.DecisionParityOption{shadowmw.EquivalentStatuses(99, 400)}},
		{name: "above HTTP range", options: []shadowmw.DecisionParityOption{shadowmw.EquivalentStatuses(400, 600)}},
		{
			name: "overlapping groups",
			options: []shadowmw.DecisionParityOption{
				shadowmw.EquivalentStatuses(400, 422),
				shadowmw.EquivalentStatuses(403, 422),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := shadowmw.NewRule(
				"decision",
				shadowmw.DecisionParity(test.options...),
				shadowmw.MatchAll(),
				shadowmw.SampleAll(),
			); err == nil {
				t.Fatal("NewRule() error = nil, want invalid equivalence error")
			}
		})
	}
}

func TestDecisionParityRejectsIncompleteExtractorConfiguration(t *testing.T) {
	extractor := func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
		return "", false, nil
	}
	if _, err := shadowmw.NewRule(
		"decision",
		shadowmw.DecisionParity(shadowmw.WithRejectedErrorExtractors(extractor, nil)),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	); err == nil {
		t.Fatal("NewRule() error = nil, want incomplete extractor error")
	}
}

func TestDecisionParityEquivalenceSuppressesOnlyStatusDifference(t *testing.T) {
	comparison, err := shadowmw.DecisionParity(
		shadowmw.EquivalentStatuses(http.StatusBadRequest, http.StatusUnprocessableEntity),
		shadowmw.WithDecisionResponseOptions(shadowmw.CompareBodyAsBytes()),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusBadRequest, http.Header{"X-Test": {"a"}}, nil, "left"),
		responseSnapshot(t, http.StatusUnprocessableEntity, http.Header{"X-Test": {"b"}}, nil, "right"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if len(comparison.Differences) != 2 ||
		comparison.Differences[0].Dimension != "header" ||
		comparison.Differences[1].Dimension != "body" {
		t.Fatalf("Differences = %#v, want header and body differences only", comparison.Differences)
	}
}

func TestDecisionParityEquivalentStatusDoesNotConsumeDifferenceLimit(t *testing.T) {
	comparison, err := shadowmw.DecisionParity(
		shadowmw.EquivalentStatuses(http.StatusBadRequest, http.StatusUnprocessableEntity),
		shadowmw.WithDecisionResponseOptions(
			shadowmw.DisableBodyComparison(),
			shadowmw.WithMaxDifferences(1),
		),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusBadRequest, http.Header{"X-Test": {"a"}}, nil, ""),
		responseSnapshot(t, http.StatusUnprocessableEntity, http.Header{"X-Test": {"b"}}, nil, ""),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if len(comparison.Differences) != 1 ||
		comparison.Differences[0].Dimension != "header" {
		t.Fatalf("Differences = %#v, want bounded header difference", comparison.Differences)
	}
}

func TestDecisionParityClassifiesExtractorErrorAndPanic(t *testing.T) {
	tests := []struct {
		name      string
		extractor shadowmw.ErrorExtractor
		wantKind  string
	}{
		{
			name: "error",
			extractor: func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
				return "", false, errors.New("sensitive extractor error")
			},
			wantKind: "extractor_error",
		},
		{
			name: "panic",
			extractor: func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
				panic("sensitive extractor panic")
			},
			wantKind: "extractor_panic",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			okExtractor := func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
				return "invalid", true, nil
			}
			comparison, err := shadowmw.DecisionParity(
				shadowmw.WithRejectedErrorExtractors(test.extractor, okExtractor),
				shadowmw.CompareRejectedByExtractedCode(),
			).Compare(
				context.Background(),
				responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "left"),
				responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "right"),
			)
			if err != nil {
				t.Fatalf("Compare() error = %v", err)
			}
			last := comparison.Differences[len(comparison.Differences)-1]
			if last.Dimension != "rejection" || last.Kind != test.wantKind {
				t.Fatalf("Differences = %#v, want rejection %s", comparison.Differences, test.wantKind)
			}
		})
	}
}

func TestDecisionParityBoundsExtractedCodeDifferences(t *testing.T) {
	extract := func(code string) shadowmw.ErrorExtractor {
		return func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return code, true, nil
		}
	}
	comparison, err := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(extract("authoritative-long"), extract("shadow-long")),
		shadowmw.CompareRejectedByExtractedCode(),
		shadowmw.WithDecisionResponseOptions(
			shadowmw.WithMaxDifferences(1),
			shadowmw.WithMaxDifferenceValueBytes(4),
		),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "left"),
		responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "right"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if len(comparison.Differences) != 1 {
		t.Fatalf("len(Differences) = %d, want 1: %#v", len(comparison.Differences), comparison.Differences)
	}
	difference := comparison.Differences[0]
	if difference.Dimension != "rejection" ||
		len(difference.Expected) > 4 || len(difference.Actual) > 4 {
		t.Fatalf("Difference = %#v, want bounded rejection difference", difference)
	}
}

func TestDecisionParityComparesUnabridgedExtractedCodes(t *testing.T) {
	extract := func(code string) shadowmw.ErrorExtractor {
		return func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return code, true, nil
		}
	}
	comparison, err := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(
			extract("same-prefix-authoritative"),
			extract("same-prefix-shadow"),
		),
		shadowmw.CompareRejectedByExtractedCode(),
		shadowmw.WithDecisionResponseOptions(shadowmw.WithMaxDifferenceValueBytes(4)),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
		responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if comparison.Matched() {
		t.Fatal("Matched() = true, want extracted-code mismatch")
	}
	if got := comparison.RejectionCodes.Authoritative.Code; got != "same" {
		t.Fatalf("Authoritative evidence code = %q, want bounded %q", got, "same")
	}
	if got := comparison.RejectionCodes.Shadow.Code; got != "same" {
		t.Fatalf("Shadow evidence code = %q, want bounded %q", got, "same")
	}
}

func TestDecisionParityExtractsCodesWithoutChangingParityByDefault(t *testing.T) {
	authoritativeCalls := 0
	shadowCalls := 0
	comparison, err := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(
			func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
				authoritativeCalls++
				return "authoritative_invalid", true, nil
			},
			func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
				shadowCalls++
				return "shadow_invalid", true, nil
			},
		),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "same"),
		responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "same"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if !comparison.Matched() {
		t.Fatalf("Differences = %#v, want parity", comparison.Differences)
	}
	if authoritativeCalls != 1 || shadowCalls != 1 {
		t.Fatalf(
			"extractor calls = authoritative %d, shadow %d; want one each",
			authoritativeCalls,
			shadowCalls,
		)
	}
	if comparison.RejectionCodes == nil {
		t.Fatal("RejectionCodes = nil, want observability evidence")
	}
	if got := comparison.RejectionCodes.Authoritative; got.Result != shadowmw.RejectionCodeFound ||
		got.Code != "authoritative_invalid" {
		t.Fatalf("Authoritative evidence = %#v", got)
	}
	if got := comparison.RejectionCodes.Shadow; got.Result != shadowmw.RejectionCodeFound ||
		got.Code != "shadow_invalid" {
		t.Fatalf("Shadow evidence = %#v", got)
	}
}

func TestDecisionParityExtractorFailuresMismatchByDefault(t *testing.T) {
	comparison, err := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(
			func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
				return "", false, errors.New("sensitive authoritative failure")
			},
			func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
				panic("sensitive shadow failure")
			},
		),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "same"),
		responseSnapshot(t, http.StatusUnprocessableEntity, nil, nil, "same"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if comparison.Matched() || len(comparison.Differences) != 1 || comparison.Differences[0].Kind != "extractor_panic" {
		t.Fatalf("Differences = %#v, want extractor panic mismatch", comparison.Differences)
	}
	if comparison.RejectionCodes == nil {
		t.Fatal("RejectionCodes = nil, want operational evidence")
	}
	if got := comparison.RejectionCodes.Authoritative.Result; got != shadowmw.RejectionCodeExtractorError {
		t.Fatalf("Authoritative Result = %q, want extractor error", got)
	}
	if got := comparison.RejectionCodes.Shadow.Result; got != shadowmw.RejectionCodeExtractorPanic {
		t.Fatalf("Shadow Result = %q, want extractor panic", got)
	}
}

func TestDecisionParityObservationalExtractorFailureDoesNotHideResponseMismatch(t *testing.T) {
	extractor := func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
		return "", false, errors.New("sensitive extraction failure")
	}
	comparison, err := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(extractor, extractor),
		shadowmw.WithDecisionResponseOptions(shadowmw.CompareBodyAsBytes()),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusBadRequest, nil, nil, "authoritative"),
		responseSnapshot(t, http.StatusBadRequest, nil, nil, "shadow"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if comparison.Matched() || len(comparison.Differences) != 2 ||
		comparison.Differences[0].Dimension != "body" || comparison.Differences[1].Kind != "extractor_error" {
		t.Fatalf("Differences = %#v, want body and extractor mismatches", comparison.Differences)
	}
	if comparison.RejectionCodes.Authoritative.Result != shadowmw.RejectionCodeExtractorError ||
		comparison.RejectionCodes.Shadow.Result != shadowmw.RejectionCodeExtractorError {
		t.Fatalf("RejectionCodes = %#v, want extractor error evidence", comparison.RejectionCodes)
	}
}

func TestDecisionParityBoundsObservationalRejectionCodes(t *testing.T) {
	extract := func(code string) shadowmw.ErrorExtractor {
		return func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return code, true, nil
		}
	}
	comparison, err := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(
			extract("authoritative-long"),
			extract(string([]byte{0xff, 0xfe, 'x'})),
		),
		shadowmw.WithDecisionResponseOptions(shadowmw.WithMaxDifferenceValueBytes(4)),
	).Compare(
		context.Background(),
		responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
		responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
	)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if comparison.RejectionCodes == nil {
		t.Fatal("RejectionCodes = nil, want observability evidence")
	}
	authoritative := comparison.RejectionCodes.Authoritative.Code
	shadow := comparison.RejectionCodes.Shadow.Code
	if len(authoritative) > 4 || len(shadow) > 4 ||
		!utf8.ValidString(authoritative) || !utf8.ValidString(shadow) {
		t.Fatalf("unbounded rejection code evidence: authoritative %q, shadow %q", authoritative, shadow)
	}
}

func TestDecisionParityReportsNotFoundAndCancellation(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		comparison, err := shadowmw.DecisionParity(
			shadowmw.WithRejectedErrorExtractors(
				func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
					return "", false, nil
				},
				func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
					return "shadow_only", true, nil
				},
			),
		).Compare(
			context.Background(),
			responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
			responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
		)
		if err != nil {
			t.Fatalf("Compare() error = %v", err)
		}
		if !comparison.Matched() {
			t.Fatalf("Differences = %#v, want parity", comparison.Differences)
		}
		if got := comparison.RejectionCodes.Authoritative.Result; got != shadowmw.RejectionCodeNotFound {
			t.Fatalf("Authoritative Result = %q, want none", got)
		}
		if got := comparison.RejectionCodes.Shadow.Result; got != shadowmw.RejectionCodeFound {
			t.Fatalf("Shadow Result = %q, want found", got)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		extractor := func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return "ignored_cancel", true, nil
		}
		comparison, err := shadowmw.DecisionParity(
			shadowmw.WithRejectedErrorExtractors(extractor, extractor),
		).Compare(
			ctx,
			responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
			responseSnapshot(t, http.StatusBadRequest, nil, nil, "same"),
		)
		if err != nil {
			t.Fatalf("Compare() error = %v", err)
		}
		if comparison.Matched() || len(comparison.Differences) != 1 || comparison.Differences[0].Kind != "extractor_error" {
			t.Fatalf("Differences = %#v, want canceled extractor mismatch", comparison.Differences)
		}
		if got := comparison.RejectionCodes.Authoritative.Result; got != shadowmw.RejectionCodeExtractorCanceled {
			t.Fatalf("Authoritative Result = %q, want canceled", got)
		}
		if got := comparison.RejectionCodes.Shadow.Result; got != shadowmw.RejectionCodeExtractorCanceled {
			t.Fatalf("Shadow Result = %q, want canceled", got)
		}
	})
}

// --- merged from decision_regression_test.go ---

func decisionRegressionOutcome(t *testing.T, policy shadowmw.Policy, limit int64, authoritative, shadow http.Handler) shadowmw.Outcome {
	t.Helper()
	outcomes := make(chan shadowmw.Outcome, 1)
	engine := newAcceptanceEngine(t, 1, outcomes)
	rule, err := shadowmw.NewRule("decision.regression", policy, shadowmw.MatchAll(), shadowmw.SampleAll())
	if err != nil {
		t.Fatal(err)
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	cfg.MaxResponseBodyBytes = limit
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, cfg, rule)
	if err != nil {
		t.Fatal(err)
	}
	middleware.Wrap(shadow).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	return waitForOutcome(t, outcomes)
}

func TestDecisionParityObservationalExtractorFailuresMismatch(t *testing.T) {
	ok := func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
		return "invalid", true, nil
	}
	for _, test := range []struct {
		name      string
		extractor shadowmw.ErrorExtractor
		kind      string
	}{
		{name: "error", extractor: func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return "", false, errors.New("private error")
		}, kind: "extractor_error"},
		{name: "panic", extractor: func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) { panic("private panic") }, kind: "extractor_panic"},
	} {
		for _, branch := range []string{"authoritative", "shadow"} {
			t.Run(test.name+"/"+branch, func(t *testing.T) {
				authoritativeExtractor, shadowExtractor := ok, ok
				if branch == "authoritative" {
					authoritativeExtractor = test.extractor
				} else {
					shadowExtractor = test.extractor
				}
				handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, "same rejection")
				})
				outcome := decisionRegressionOutcome(t, shadowmw.DecisionParity(shadowmw.WithRejectedErrorExtractors(authoritativeExtractor, shadowExtractor)), 64, handler, handler)
				if outcome.Classification() != shadowmw.OutcomeMismatch {
					t.Fatalf("classification = %s, want mismatch", outcome.Classification())
				}
				differences := outcome.Differences()
				if len(differences) != 1 || differences[0].Dimension != "rejection" || differences[0].Kind != test.kind {
					t.Errorf("differences = %+v, want bounded %s", differences, test.kind)
				}
			})
		}
	}
}

func TestDecisionParitySuccessfulTruncationIsIncomplete(t *testing.T) {
	for _, test := range []struct {
		name                          string
		authoritativeBody, shadowBody string
		shadowStatus                  int
		options                       []shadowmw.DecisionParityOption
	}{
		{name: "both", authoritativeBody: "too large", shadowBody: "too large"},
		{name: "authoritative", authoritativeBody: "too large", shadowBody: "a"},
		{name: "shadow", authoritativeBody: "a", shadowBody: "too large"},
		{name: "different statuses", authoritativeBody: "too large", shadowBody: "too large", shadowStatus: http.StatusAccepted},
		{name: "equivalent statuses", authoritativeBody: "too large", shadowBody: "too large", shadowStatus: http.StatusAccepted, options: []shadowmw.DecisionParityOption{shadowmw.EquivalentStatuses(http.StatusCreated, http.StatusAccepted)}},
		{name: "success headers enabled", authoritativeBody: "too large", shadowBody: "a", options: []shadowmw.DecisionParityOption{shadowmw.CompareSuccessfulHeaders()}},
		{name: "success bodies enabled", authoritativeBody: "a", shadowBody: "too large", options: []shadowmw.DecisionParityOption{shadowmw.CompareSuccessfulBodies()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := func(status int, body string) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status); _, _ = io.WriteString(w, body) })
			}
			shadowStatus := test.shadowStatus
			if shadowStatus == 0 {
				shadowStatus = http.StatusCreated
			}
			outcome := decisionRegressionOutcome(t, shadowmw.DecisionParity(test.options...), 1, handler(http.StatusCreated, test.authoritativeBody), handler(shadowStatus, test.shadowBody))
			if outcome.AuthoritativeResponse().Truncated() != (len(test.authoritativeBody) > 1) || outcome.ShadowResponse().Truncated() != (len(test.shadowBody) > 1) {
				t.Fatal("unexpected branch truncation evidence")
			}
			if outcome.Classification() != shadowmw.OutcomeMismatch || outcome.Reason() != shadowmw.ReasonResponseTruncated {
				t.Fatalf("outcome = %s/%s, want incomplete mismatch", outcome.Classification(), outcome.Reason())
			}
		})
	}
}

func TestDecisionParityRejectedTruncationRunsExtractorsAndRetainsIncomplete(t *testing.T) {
	for _, test := range []struct {
		name                          string
		authoritativeBody, shadowBody string
		authoritativeCode, shadowCode string
		wantRejectionDifference       bool
	}{
		{
			name:              "authoritative truncated matching codes",
			authoritativeBody: "too large",
			shadowBody:        "a",
			authoritativeCode: "invalid_input",
			shadowCode:        "invalid_input",
		},
		{
			name:              "shadow truncated matching codes",
			authoritativeBody: "a",
			shadowBody:        "too large",
			authoritativeCode: "invalid_input",
			shadowCode:        "invalid_input",
		},
		{
			name:                    "both truncated differing codes",
			authoritativeBody:       "too large",
			shadowBody:              "too large",
			authoritativeCode:       "invalid_input",
			shadowCode:              "conflict",
			wantRejectionDifference: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var authoritativeCalls, shadowCalls int
			policy := shadowmw.DecisionParity(
				shadowmw.WithRejectedErrorExtractors(
					func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
						authoritativeCalls++
						return test.authoritativeCode, true, nil
					},
					func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
						shadowCalls++
						return test.shadowCode, true, nil
					},
				),
				shadowmw.CompareRejectedByExtractedCode(),
			)
			handler := func(body string) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, body)
				})
			}
			outcome := decisionRegressionOutcome(
				t,
				policy,
				1,
				handler(test.authoritativeBody),
				handler(test.shadowBody),
			)
			if authoritativeCalls != 1 || shadowCalls != 1 {
				t.Fatalf("extractor calls = %d/%d, want 1/1", authoritativeCalls, shadowCalls)
			}
			if outcome.Classification() != shadowmw.OutcomeMismatch ||
				outcome.Reason() != shadowmw.ReasonResponseTruncated {
				t.Fatalf("outcome = %s/%s, want mismatch/response_truncated", outcome.Classification(), outcome.Reason())
			}
			differences := outcome.Differences()
			wantLen := 1
			if test.wantRejectionDifference {
				wantLen = 2
			}
			if len(differences) != wantLen ||
				differences[0].Dimension != "body" ||
				differences[0].Kind != "comparison_incomplete" {
				t.Fatalf("differences = %+v, want leading body comparison_incomplete (len %d)", differences, wantLen)
			}
			if test.wantRejectionDifference &&
				(differences[1].Dimension != "rejection" ||
					differences[1].Path != "code" ||
					differences[1].Kind != "different") {
				t.Fatalf("differences[1] = %+v, want rejection/code/different", differences[1])
			}
		})
	}
}
