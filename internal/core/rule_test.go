package core_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	shadowmw "github.com/sergeybataev/shadowmw/internal/core"
)

type nilPolicy struct{}

func (*nilPolicy) Compare(
	context.Context,
	shadowmw.ResponseSnapshot,
	shadowmw.ResponseSnapshot,
) (shadowmw.Comparison, error) {
	return shadowmw.Comparison{}, nil
}

func TestRuleMatchesMethodsAsAlternativesAndDimensionsStrictly(t *testing.T) {
	rule, err := shadowmw.NewRule(
		"orders",
		shadowmw.ResponseParity(),
		shadowmw.MatchMethods(http.MethodGet, http.MethodHead),
		shadowmw.MatchPathPrefix("/orders/"),
		shadowmw.MatchWhen(func(request *http.Request) bool {
			return request.URL.Query().Get("compare") == "yes"
		}),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	for _, test := range []struct {
		method string
		target string
		want   bool
	}{
		{method: http.MethodGet, target: "/orders/1?compare=yes", want: true},
		{method: http.MethodHead, target: "/orders/1?compare=yes", want: true},
		{method: http.MethodPost, target: "/orders/1?compare=yes", want: false},
		{method: http.MethodGet, target: "/other/1?compare=yes", want: false},
		{method: http.MethodGet, target: "/orders/1?compare=no", want: false},
	} {
		t.Run(test.method+" "+test.target, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 1)
			engine := newAcceptanceEngine(t, 1, outcomes)
			authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Served-By", "authoritative")
				w.WriteHeader(http.StatusNoContent)
			})
			replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Served-By", "replacement")
				w.WriteHeader(http.StatusNoContent)
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			middleware, err := shadowmw.NewMiddleware(engine, authoritative, cfg, rule)
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			middleware.Wrap(replacement).ServeHTTP(response, httptest.NewRequest(test.method, test.target, nil))
			if got := response.Header().Get("X-Served-By") == "authoritative"; got != test.want {
				t.Errorf("authoritative served request = %v, want %v", got, test.want)
			}
			if test.want {
				waitForOutcome(t, outcomes)
			}
		})
	}
}

func TestRuleRejectsNonFiniteSamplingPercentage(t *testing.T) {
	for _, percentage := range []float64{math.NaN(), math.Inf(-1), math.Inf(1)} {
		if _, err := shadowmw.NewRule(
			"sample",
			shadowmw.ResponseParity(),
			shadowmw.MatchAll(),
			shadowmw.SamplePercent(percentage),
		); err == nil {
			t.Errorf("NewRule(SamplePercent(%v)) error = nil, want error", percentage)
		}
	}
}

func TestRuleSamplingBoundariesAndCustomSampler(t *testing.T) {
	for _, test := range []struct {
		name   string
		option shadowmw.RuleOption
		want   bool
	}{
		{name: "none", option: shadowmw.SampleNone()},
		{name: "zero percent", option: shadowmw.SamplePercent(0)},
		{name: "all", option: shadowmw.SampleAll(), want: true},
		{name: "one hundred percent", option: shadowmw.SamplePercent(100), want: true},
		{
			name: "custom",
			option: shadowmw.SampleWith(func(request *http.Request) bool {
				return request.Method == http.MethodGet
			}),
			want: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rule, err := shadowmw.NewRule(
				"sampling",
				shadowmw.ResponseParity(),
				shadowmw.MatchAll(),
				test.option,
			)
			if err != nil {
				t.Fatalf("NewRule() error = %v", err)
			}
			outcomes := make(chan shadowmw.Outcome, 1)
			engine := newAcceptanceEngine(t, 1, outcomes)
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			middleware, err := shadowmw.NewMiddleware(engine, handler, cfg, rule)
			if err != nil {
				t.Fatal(err)
			}
			middleware.Wrap(handler).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			outcome := waitForOutcome(t, outcomes)
			if test.want {
				if outcome.Classification() != shadowmw.OutcomeMatch {
					t.Fatalf("classification = %s, want sampled match", outcome.Classification())
				}
			} else if outcome.Classification() != shadowmw.OutcomeSkipped || outcome.Reason() != shadowmw.ReasonNotSampled {
				t.Fatalf("outcome = %s/%s, want not_sampled skip", outcome.Classification(), outcome.Reason())
			}
		})
	}
}

func TestRuleRejectsEmptyRegularExpressionMatcher(t *testing.T) {
	if _, err := shadowmw.NewRule(
		"empty-regexp",
		shadowmw.ResponseParity(),
		shadowmw.MatchPathRegexp(""),
		shadowmw.SampleAll(),
	); err == nil {
		t.Fatal("NewRule(MatchPathRegexp(\"\")) error = nil, want error")
	}
}

func TestRuleRejectsTypedNilPolicy(t *testing.T) {
	var policy *nilPolicy
	if _, err := shadowmw.NewRule(
		"nil-policy",
		policy,
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	); err == nil {
		t.Fatal("NewRule(typed nil policy) error = nil, want error")
	}
}
