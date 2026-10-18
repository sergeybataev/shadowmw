package core_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	shadowmw "github.com/sergeybataev/shadowmw/internal/core"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type observerFunc func(context.Context, shadowmw.Outcome) error

func (f observerFunc) Observe(ctx context.Context, outcome shadowmw.Outcome) error {
	return f(ctx, outcome)
}

func TestObserverReceivesImmutableMatchedOutcome(t *testing.T) {
	observed := make(chan shadowmw.Outcome, 1)
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.Observer = &shadowmw.ObserverDelivery{
		Observer: observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
			observed <- outcome
			return nil
		}),
		QueueCapacity:  4,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter:         shadowmw.ObserveMatches,
		Detail:         shadowmw.OutcomeMetadataOnly,
	}
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := engine.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	rule, err := shadowmw.NewRule(
		"users.get",
		shadowmw.ResponseParity(),
		shadowmw.MatchPath("/users/42"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	response := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":42}`))
	})
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(engine, response, middlewareConfig, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	middleware.Wrap(response).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/users/42", nil),
	)

	select {
	case outcome := <-observed:
		if outcome.Classification() != shadowmw.OutcomeMatch {
			t.Fatalf("Classification() = %v, want %v", outcome.Classification(), shadowmw.OutcomeMatch)
		}
		if outcome.SessionID().String() == "" {
			t.Fatal("SessionID() is empty")
		}
		if outcome.RuleName() != "users.get" {
			t.Fatalf("RuleName() = %q, want users.get", outcome.RuleName())
		}
		if len(outcome.Differences()) != 0 {
			t.Fatalf("Differences() = %v, want none", outcome.Differences())
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not receive a comparison outcome")
	}
}

func TestResponseComparisonUsesCommittedHeadersAndSupportsFlush(t *testing.T) {
	observed := make(chan shadowmw.Outcome, 1)
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.Observer = &shadowmw.ObserverDelivery{
		Observer: observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
			observed <- outcome
			return nil
		}),
		QueueCapacity:  4,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter:         shadowmw.ObserveMatches | shadowmw.ObserveMismatches,
		Detail:         shadowmw.OutcomeMetadataOnly,
	}
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := engine.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	rule, err := shadowmw.NewRule(
		"stream.get",
		shadowmw.ResponseParity(),
		shadowmw.MatchPath("/stream"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	handler := func(lateValue string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			flusher, ok := w.(http.Flusher)
			if !ok {
				t.Error("response writer does not implement http.Flusher")
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("first"))
			flusher.Flush()
			w.Header().Set("X-Late-Unsent", lateValue)
			_, _ = w.Write([]byte("-second"))
		})
	}

	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		handler("authoritative-only"),
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	middleware.Wrap(handler("shadow-only")).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/stream", nil),
	)
	if got := recorder.Body.String(); got != "first-second" {
		t.Fatalf("client body = %q, want first-second", got)
	}

	select {
	case outcome := <-observed:
		if outcome.Classification() != shadowmw.OutcomeMatch {
			t.Fatalf(
				"Classification() = %v, want match; differences = %v",
				outcome.Classification(),
				outcome.Differences(),
			)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not receive a comparison outcome")
	}
}

func TestResponseParityComparesJSONStructurally(t *testing.T) {
	outcome := runComparison(
		t,
		shadowmw.ResponseParity(shadowmw.IgnoreJSONPointers("/volatile")),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"n":1,"smth":{},"volatile":"python"}`))
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"volatile":"go","smth":{},"n":1.0}`))
		}),
	)
	if outcome.Classification() != shadowmw.OutcomeMatch {
		t.Fatalf(
			"Classification() = %v, want match; differences = %v",
			outcome.Classification(),
			outcome.Differences(),
		)
	}
}

func TestResponseParityTreatsInvalidJSONAsMismatch(t *testing.T) {
	outcome := runComparison(
		t,
		shadowmw.ResponseParity(),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"broken"`))
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"broken"`))
		}),
	)
	if outcome.Classification() != shadowmw.OutcomeMismatch {
		t.Fatalf("Classification() = %v, want mismatch", outcome.Classification())
	}
}

func TestResponseParityComparesHeadersByDefault(t *testing.T) {
	outcome := runComparison(
		t,
		shadowmw.ResponseParity(),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("ETag", `"python"`)
			w.WriteHeader(http.StatusNoContent)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("ETag", `"go"`)
			w.WriteHeader(http.StatusNoContent)
		}),
	)
	if outcome.Classification() != shadowmw.OutcomeMismatch {
		t.Fatalf("Classification() = %v, want mismatch", outcome.Classification())
	}
}

func TestDecisionParityIgnoresEqualSuccessRepresentationsButComparesRejections(t *testing.T) {
	success := runComparison(
		t,
		shadowmw.DecisionParity(),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Implementation", "python")
			_, _ = w.Write([]byte(`{"result":"created"}`))
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Implementation", "go")
			_, _ = w.Write([]byte(`{"validation":"passed"}`))
		}),
	)
	if success.Classification() != shadowmw.OutcomeMatch {
		t.Fatalf("equal success Classification() = %v, want match", success.Classification())
	}

	rejection := runComparison(
		t,
		shadowmw.DecisionParity(),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"missing_name"}`))
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"invalid_name"}`))
		}),
	)
	if rejection.Classification() != shadowmw.OutcomeMismatch {
		t.Fatalf("different rejection Classification() = %v, want mismatch", rejection.Classification())
	}
}

func TestDecisionParityRequiresExplicitStatusEquivalence(t *testing.T) {
	strict := runComparison(
		t,
		shadowmw.DecisionParity(),
		statusHandler(http.StatusBadRequest, `{"error":"invalid"}`),
		statusHandler(http.StatusUnprocessableEntity, `{"error":"invalid"}`),
	)
	if strict.Classification() != shadowmw.OutcomeMismatch {
		t.Fatalf("strict Classification() = %v, want mismatch", strict.Classification())
	}

	equivalent := runComparison(
		t,
		shadowmw.DecisionParity(shadowmw.EquivalentStatuses(
			http.StatusBadRequest,
			http.StatusUnprocessableEntity,
		)),
		statusHandler(http.StatusBadRequest, `{"error":"invalid"}`),
		statusHandler(http.StatusUnprocessableEntity, `{"error":"invalid"}`),
	)
	if equivalent.Classification() != shadowmw.OutcomeMatch {
		t.Fatalf(
			"equivalent Classification() = %v, want match; differences = %v",
			equivalent.Classification(),
			equivalent.Differences(),
		)
	}
}

func TestDecisionParityCanCompareBranchSpecificExtractedErrorCodes(t *testing.T) {
	extract := func(field string) shadowmw.ErrorExtractor {
		return func(
			_ context.Context,
			response shadowmw.ResponseSnapshot,
		) (string, bool, error) {
			var envelope map[string]any
			if err := json.Unmarshal(response.Body(), &envelope); err != nil {
				return "", false, err
			}
			value, found := envelope[field].(string)
			return value, found, nil
		}
	}
	policy := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(extract("python_code"), extract("go_code")),
		shadowmw.CompareRejectedByExtractedCode(),
	)

	outcome := runComparison(
		t,
		policy,
		statusHandler(http.StatusUnprocessableEntity, `{"python_code":"missing_name"}`),
		statusHandler(http.StatusUnprocessableEntity, `{"go_code":"missing_name"}`),
	)
	if outcome.Classification() != shadowmw.OutcomeMatch {
		t.Fatalf(
			"Classification() = %v, want match; differences = %v",
			outcome.Classification(),
			outcome.Differences(),
		)
	}
}

func statusHandler(status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func runComparison(
	t *testing.T,
	policy shadowmw.Policy,
	authoritative http.Handler,
	replacement http.Handler,
) shadowmw.Outcome {
	t.Helper()
	observed := make(chan shadowmw.Outcome, 1)
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.Observer = &shadowmw.ObserverDelivery{
		Observer: observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
			observed <- outcome
			return nil
		}),
		QueueCapacity:  4,
		MaxQueuedBytes: 4 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter:         shadowmw.ObserveMatches | shadowmw.ObserveMismatches,
		Detail:         shadowmw.OutcomeDiff,
	}
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := engine.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	rule, err := shadowmw.NewRule(
		"response.compare",
		policy,
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		authoritative,
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	middleware.Wrap(replacement).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/", nil),
	)

	select {
	case outcome := <-observed:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("observer did not receive outcome")
		return shadowmw.Outcome{}
	}
}

func TestObserverFiltersByReasonRuleAndDimension(t *testing.T) {
	tests := []struct {
		name       string
		reasons    []string
		rules      []string
		dimensions []string
		want       bool
	}{
		{name: "all match", reasons: []string{"difference"}, rules: []string{"response.compare"}, dimensions: []string{"status"}, want: true},
		{name: "reason rejected", reasons: []string{"comparison_timeout"}},
		{name: "rule rejected", rules: []string{"another.rule"}},
		{name: "dimension rejected", dimensions: []string{"body"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observed := make(chan shadowmw.Outcome, 1)
			cfg := observerEngineConfig(observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
				observed <- outcome
				return nil
			}))
			cfg.Observer.Reasons = test.reasons
			cfg.Observer.Rules = test.rules
			cfg.Observer.Dimensions = test.dimensions
			engine := startObserverEngine(t, cfg)

			runWithEngine(
				t,
				engine,
				statusHandler(http.StatusOK, `{"same":true}`),
				statusHandler(http.StatusCreated, `{"same":true}`),
			)

			select {
			case outcome := <-observed:
				if !test.want {
					t.Fatalf("observer received a filtered outcome: %+v", outcome.Differences())
				}
			case <-time.After(50 * time.Millisecond):
				if test.want {
					t.Fatal("observer did not receive matching outcome")
				}
			}
		})
	}
}

func TestObserverFilterConfigurationIsDetachedAtConstruction(t *testing.T) {
	observed := make(chan shadowmw.Outcome, 1)
	cfg := observerEngineConfig(observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
		observed <- outcome
		return nil
	}))
	cfg.Observer.Rules = []string{"response.compare"}
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	cfg.Observer.Rules[0] = "mutated.rule"
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := engine.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	runWithEngine(
		t,
		engine,
		statusHandler(http.StatusNoContent, ""),
		statusHandler(http.StatusNoContent, ""),
	)
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("post-construction filter mutation changed engine behavior")
	}
}

func TestMetadataObserverRedactsValuesAfterBoundedAdmission(t *testing.T) {
	observed := make(chan shadowmw.Outcome, 1)
	cfg := observerEngineConfig(observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
		observed <- outcome
		return nil
	}))
	cfg.Observer.MaxQueuedBytes = 64 << 10
	engine := startObserverEngine(t, cfg)

	value := strings.Repeat("private", 200)
	runWithEngine(
		t,
		engine,
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Private", value)
			_, _ = w.Write([]byte(value))
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Private", value+"-different")
			_, _ = w.Write([]byte(value + "-different"))
		}),
	)

	select {
	case outcome := <-observed:
		if got := outcome.AuthoritativeResponse().Header().Get("X-Private"); got != "" {
			t.Fatalf("metadata header value = %q, want redacted", got)
		}
		if got := len(outcome.AuthoritativeResponse().Body()); got != 0 {
			t.Fatalf("metadata body bytes = %d, want 0", got)
		}
		for _, difference := range outcome.Differences() {
			if difference.Expected != "" || difference.Actual != "" {
				t.Fatalf("metadata difference leaked values: %+v", difference)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("metadata outcome was not delivered")
	}
}

func TestObserverDeliveryResultsAreDistinctAndShutdownIsBounded(t *testing.T) {
	tests := []struct {
		name       string
		observer   func(<-chan struct{}) observerFunc
		wantResult string
	}{
		{
			name: "error",
			observer: func(<-chan struct{}) observerFunc {
				return func(context.Context, shadowmw.Outcome) error {
					return errors.New("private observer error")
				}
			},
			wantResult: "failed",
		},
		{
			name: "panic",
			observer: func(<-chan struct{}) observerFunc {
				return func(context.Context, shadowmw.Outcome) error {
					panic("private observer panic")
				}
			},
			wantResult: "panicked",
		},
		{
			name: "timeout",
			observer: func(release <-chan struct{}) observerFunc {
				return func(context.Context, shadowmw.Outcome) error {
					<-release
					return nil
				}
			},
			wantResult: "timed_out",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			recorder := &measurementRecorder{notify: make(chan struct{}, 16)}
			cfg := observerEngineConfig(test.observer(release))
			cfg.Observer.Timeout = 10 * time.Millisecond
			engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
			if err != nil {
				t.Fatalf("NewEngine() error = %v", err)
			}
			if err := engine.Start(context.Background()); err != nil {
				t.Fatalf("Start() error = %v", err)
			}

			runWithEngine(
				t,
				engine,
				statusHandler(http.StatusOK, "same"),
				statusHandler(http.StatusOK, "same"),
			)

			deadline := time.After(time.Second)
			for !recorder.hasDeliveryResult(test.wantResult) {
				select {
				case <-recorder.notify:
				case <-deadline:
					t.Fatalf("observer result %q was not recorded; measurements = %+v", test.wantResult, recorder.snapshot())
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if err := engine.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
			if got := engine.State(); got != shadowmw.EngineStopped {
				t.Fatalf("State() = %v, want stopped", got)
			}
		})
	}
}

func TestObserverByteBudgetIncludesActiveDelivery(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var delivered atomic.Int32
	recorder := &measurementRecorder{notify: make(chan struct{}, 32)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		delivered.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}))
	cfg.Observer.MaxQueuedBytes = 400
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	runWithEngine(
		t,
		engine,
		statusHandler(http.StatusNoContent, ""),
		statusHandler(http.StatusNoContent, ""),
	)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first observer delivery did not start")
	}
	runWithEngine(
		t,
		engine,
		statusHandler(http.StatusOK, strings.Repeat("x", 32)),
		statusHandler(http.StatusOK, strings.Repeat("x", 32)),
	)

	deadline := time.After(time.Second)
	for !recorder.hasDeliveryResult("dropped") {
		select {
		case <-recorder.notify:
		case <-deadline:
			t.Fatalf("byte-budget drop was not recorded: %+v", recorder.snapshot())
		}
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := delivered.Load(); got != 1 {
		t.Fatalf("observer calls = %d, want 1", got)
	}
}

func TestObserverByteBudgetAccountsForOriginalAndPreparedCopies(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseObserver := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseObserver()
	var delivered atomic.Int32
	observed := make(chan struct{}, 4)
	recorder := &measurementRecorder{notify: make(chan struct{}, 64)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		delivered.Add(1)
		observed <- struct{}{}
		return nil
	}))
	cfg.Observer.Detail = shadowmw.OutcomeCustom
	cfg.Observer.Renderer = func(context.Context, shadowmw.Outcome) ([]byte, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	}
	cfg.Observer.MaxQueuedBytes = 9_000
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	body := strings.Repeat("x", 1_024)
	runWithEngine(t, engine, statusHandler(http.StatusOK, body), statusHandler(http.StatusOK, body))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first observer delivery did not start")
	}
	runWithEngine(t, engine, statusHandler(http.StatusOK, body), statusHandler(http.StatusOK, body))

	deadline := time.After(time.Second)
	for !recorder.hasDeliveryResult("dropped") {
		select {
		case <-recorder.notify:
		case <-deadline:
			t.Fatalf(
				"coexisting original and prepared copies did not consume the byte budget: %+v",
				recorder.snapshot(),
			)
		}
	}

	releaseObserver()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("first observer delivery did not release its reservations")
	}
	runWithEngine(t, engine, statusHandler(http.StatusOK, body), statusHandler(http.StatusOK, body))
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatalf("observer deliveries = %d, want 2 after reservation release", delivered.Load())
	}
	if got := delivered.Load(); got != 2 {
		t.Fatalf("observer deliveries = %d, want 2 after reservation release", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	var lastQueueBytes float64 = -1
	for _, measurement := range recorder.snapshot() {
		if measurement.Name != shadowmw.MetricObserverQueueBytes {
			continue
		}
		if measurement.Value < 0 ||
			measurement.Value > float64(cfg.Observer.MaxQueuedBytes) {
			t.Fatalf(
				"observer queue bytes = %v, want within [0,%d]",
				measurement.Value,
				cfg.Observer.MaxQueuedBytes,
			)
		}
		lastQueueBytes = measurement.Value
	}
	if lastQueueBytes != 0 {
		t.Fatalf("final observer queue bytes = %v, want 0", lastQueueBytes)
	}
}

func TestObserverCountCapacityIncludesActiveDelivery(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var delivered atomic.Int32
	recorder := &measurementRecorder{notify: make(chan struct{}, 32)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		delivered.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}))
	cfg.Observer.QueueCapacity = 1
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	runWithEngine(t, engine, statusHandler(http.StatusNoContent, ""), statusHandler(http.StatusNoContent, ""))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first observer delivery did not start")
	}
	runWithEngine(t, engine, statusHandler(http.StatusNoContent, ""), statusHandler(http.StatusNoContent, ""))

	deadline := time.After(time.Second)
	for !recorder.hasDeliveryResult("dropped") {
		select {
		case <-recorder.notify:
		case <-deadline:
			t.Fatalf("count-capacity drop was not recorded: %+v", recorder.snapshot())
		}
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := delivered.Load(); got != 1 {
		t.Fatalf("observer calls = %d, want 1", got)
	}
}

func TestObserverRecordsQueueDuration(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	recorder := &measurementRecorder{notify: make(chan struct{}, 64)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		select {
		case started <- struct{}{}:
			<-release
		default:
		}
		return nil
	}))
	cfg.Observer.Workers = 1
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	runWithEngine(t, engine, statusHandler(http.StatusNoContent, ""), statusHandler(http.StatusNoContent, ""))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first observer delivery did not start")
	}
	runWithEngine(t, engine, statusHandler(http.StatusNoContent, ""), statusHandler(http.StatusNoContent, ""))
	close(release)

	deadline := time.After(time.Second)
	for !recorder.hasPositiveMeasurement(shadowmw.MetricObserverQueueDuration) {
		select {
		case <-recorder.notify:
		case <-deadline:
			t.Fatalf("observer queue duration was not recorded: %+v", recorder.snapshot())
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestCustomObserverRenderingIsOutputBounded(t *testing.T) {
	observed := make(chan shadowmw.Outcome, 1)
	cfg := observerEngineConfig(observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
		observed <- outcome
		return nil
	}))
	cfg.Observer.MaxQueuedBytes = 512
	cfg.Observer.Detail = shadowmw.OutcomeCustom
	cfg.Observer.Renderer = func(context.Context, shadowmw.Outcome) ([]byte, error) {
		return []byte(strings.Repeat("x", 4096)), nil
	}
	engine := startObserverEngine(t, cfg)

	runWithEngine(
		t,
		engine,
		statusHandler(http.StatusOK, `{"value":1}`),
		statusHandler(http.StatusCreated, `{"value":1}`),
	)
	select {
	case outcome := <-observed:
		if got := len(outcome.RenderedDetail()); got == 0 || got >= int(cfg.Observer.MaxQueuedBytes) {
			t.Fatalf("rendered bytes = %d, want non-zero and below total outcome budget", got)
		}
		if len(outcome.AuthoritativeResponse().Body()) != 0 {
			t.Fatal("custom detail exposed captured body outside renderer output")
		}
	case <-time.After(time.Second):
		t.Fatal("custom-rendered outcome was not delivered")
	}
}

func TestCustomRendererFailuresAreIsolated(t *testing.T) {
	tests := []struct {
		name       string
		renderer   func(<-chan struct{}) shadowmw.OutcomeRenderer
		wantResult string
	}{
		{
			name: "error",
			renderer: func(<-chan struct{}) shadowmw.OutcomeRenderer {
				return func(context.Context, shadowmw.Outcome) ([]byte, error) {
					return nil, errors.New("private renderer error")
				}
			},
			wantResult: "failed",
		},
		{
			name: "panic",
			renderer: func(<-chan struct{}) shadowmw.OutcomeRenderer {
				return func(context.Context, shadowmw.Outcome) ([]byte, error) {
					panic("private renderer panic")
				}
			},
			wantResult: "panicked",
		},
		{
			name: "timeout",
			renderer: func(release <-chan struct{}) shadowmw.OutcomeRenderer {
				return func(context.Context, shadowmw.Outcome) ([]byte, error) {
					<-release
					return nil, nil
				}
			},
			wantResult: "timed_out",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			var observerCalls atomic.Int32
			recorder := &measurementRecorder{notify: make(chan struct{}, 16)}
			cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
				observerCalls.Add(1)
				return nil
			}))
			cfg.Observer.Detail = shadowmw.OutcomeCustom
			cfg.Observer.Renderer = test.renderer(release)
			cfg.Observer.Timeout = 10 * time.Millisecond
			engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
			if err != nil {
				t.Fatalf("NewEngine() error = %v", err)
			}
			if err := engine.Start(context.Background()); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			runWithEngine(
				t,
				engine,
				statusHandler(http.StatusNoContent, ""),
				statusHandler(http.StatusNoContent, ""),
			)

			deadline := time.After(time.Second)
			for !recorder.hasDeliveryResult(test.wantResult) {
				select {
				case <-recorder.notify:
				case <-deadline:
					t.Fatalf("renderer result %q was not recorded: %+v", test.wantResult, recorder.snapshot())
				}
			}
			if !recorder.hasComparisonResult("mismatch") {
				t.Fatalf(
					"renderer failure changed or lost comparison result: %+v",
					recorder.snapshot(),
				)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := engine.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
			if got := observerCalls.Load(); got != 0 {
				t.Fatalf("observer calls = %d, want 0 after renderer failure", got)
			}
		})
	}
}

func TestShutdownDeadlineRecordsAbandonedObserverWork(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	recorder := &measurementRecorder{notify: make(chan struct{}, 32)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		started <- struct{}{}
		<-release
		return nil
	}))
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	runWithEngine(t, engine, statusHandler(http.StatusNoContent, ""), statusHandler(http.StatusNoContent, ""))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("observer delivery did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err = engine.Shutdown(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want deadline exceeded", err)
	}
	deadline := time.After(time.Second)
	for !recorder.hasDeliveryResult("abandoned") {
		select {
		case <-recorder.notify:
		case <-deadline:
			t.Fatalf("abandoned result was not recorded: %+v", recorder.snapshot())
		}
	}

	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func observerEngineConfig(observer shadowmw.Observer) shadowmw.EngineConfig {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.Observer = &shadowmw.ObserverDelivery{
		Observer:       observer,
		QueueCapacity:  4,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter:         shadowmw.ObserveMatches | shadowmw.ObserveMismatches | shadowmw.ObserveSkipped,
		Detail:         shadowmw.OutcomeMetadataOnly,
	}
	return cfg
}

func startObserverEngine(t *testing.T, cfg shadowmw.EngineConfig) *shadowmw.Engine {
	t.Helper()
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := engine.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	return engine
}

func runWithEngine(
	t *testing.T,
	engine *shadowmw.Engine,
	authoritative http.Handler,
	shadow http.Handler,
) {
	t.Helper()
	rule, err := shadowmw.NewRule(
		"response.compare",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, cfg, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	middleware.Wrap(shadow).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
}

type measurementRecorder struct {
	mu           sync.Mutex
	measurements []shadowmw.Measurement
	notify       chan struct{}
}

func (r *measurementRecorder) Record(_ context.Context, measurement shadowmw.Measurement) {
	r.mu.Lock()
	r.measurements = append(r.measurements, measurement)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *measurementRecorder) snapshot() []shadowmw.Measurement {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]shadowmw.Measurement(nil), r.measurements...)
}

func (r *measurementRecorder) hasDeliveryResult(want string) bool {
	for _, measurement := range r.snapshot() {
		if measurement.Name != shadowmw.MetricObserverDeliveries {
			continue
		}
		for _, attribute := range measurement.Attributes {
			if attribute.Key == "result" && attribute.Value == want {
				return true
			}
		}
	}
	return false
}

func (r *measurementRecorder) hasComparisonResult(want string) bool {
	for _, measurement := range r.snapshot() {
		if measurement.Name != shadowmw.MetricComparisons {
			continue
		}
		for _, attribute := range measurement.Attributes {
			if attribute.Key == "result" && attribute.Value == want {
				return true
			}
		}
	}
	return false
}

func (r *measurementRecorder) hasPositiveMeasurement(name shadowmw.MetricName) bool {
	for _, measurement := range r.snapshot() {
		if measurement.Name == name && measurement.Value > 0 {
			return true
		}
	}
	return false
}

func TestObserverDetailPolicyMustBeExplicit(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*shadowmw.ObserverDelivery)
	}{
		{
			name:      "missing detail",
			configure: func(*shadowmw.ObserverDelivery) {},
		},
		{
			name: "custom detail without renderer",
			configure: func(delivery *shadowmw.ObserverDelivery) {
				delivery.Detail = shadowmw.OutcomeCustom
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := observerEngineConfig(observerFunc(func(
				context.Context,
				shadowmw.Outcome,
			) error {
				return nil
			}))
			cfg.Observer.Detail = 0
			test.configure(cfg.Observer)
			if _, err := shadowmw.NewEngine(cfg); err == nil {
				t.Fatal("NewEngine() error = nil, want explicit detail error")
			}
		})
	}
}

func TestObserverMetadataOnlyOmitsComparedValues(t *testing.T) {
	const secret = "ticket-30-private-value"
	outcome := observeTicket30Outcome(
		t,
		shadowmw.OutcomeMetadataOnly,
		shadowmw.DefaultMiddlewareConfig(),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Private", secret)
			w.Header().Set("Trailer", "X-Private-Trailer")
			_, _ = w.Write([]byte(`{"secret":"` + secret + `"}`))
			w.Header().Set("X-Private-Trailer", secret)
		}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Private", secret+"-shadow")
			w.Header().Set("Trailer", "X-Private-Trailer")
			_, _ = w.Write([]byte(`{"secret":"different"}`))
			w.Header().Set("X-Private-Trailer", secret+"-shadow")
		}),
	)

	exposed := []struct {
		name  string
		value []byte
	}{
		{"authoritative body", outcome.AuthoritativeResponse().Body()},
		{"shadow body", outcome.ShadowResponse().Body()},
		{"authoritative compared body", outcome.AuthoritativeComparedBody()},
		{"shadow compared body", outcome.ShadowComparedBody()},
		{"rendered detail", outcome.RenderedDetail()},
	}
	for _, item := range exposed {
		if len(item.value) != 0 {
			t.Errorf("%s = %q, want omitted", item.name, item.value)
		}
	}
	for _, representation := range []shadowmw.BodyRepresentation{
		outcome.AuthoritativeBodyRepresentation(),
		outcome.ShadowBodyRepresentation(),
	} {
		if representation.Present() ||
			representation.Kind() != shadowmw.BodyRepresentationNone ||
			representation.DecodingState() != shadowmw.BodyDecodingUnavailable ||
			len(representation.Body()) != 0 {
			t.Errorf("metadata detail exposed body representation: %+v", representation)
		}
	}
	for _, response := range []shadowmw.ResponseSnapshot{
		outcome.AuthoritativeResponse(),
		outcome.ShadowResponse(),
	} {
		if _, ok := response.Header()["X-Private"]; !ok {
			t.Error("metadata detail omitted the header field name")
		}
		if _, ok := response.Trailer()["X-Private-Trailer"]; !ok {
			t.Error("metadata detail omitted the trailer field name")
		}
		for name, values := range response.Header() {
			for _, value := range values {
				if value != "" {
					t.Errorf("header %s exposed value %q", name, value)
				}
			}
		}
		for name, values := range response.Trailer() {
			for _, value := range values {
				if value != "" {
					t.Errorf("trailer %s exposed value %q", name, value)
				}
			}
		}
	}
	for _, difference := range outcome.Differences() {
		if difference.Expected != "" || difference.Actual != "" {
			t.Errorf("difference exposed field values: %+v", difference)
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", outcome.Differences()), secret) {
		t.Fatal("structured differences exposed private field value")
	}
}

func TestObserverDiffDetailLabelsTruncatedCapturesGolden(t *testing.T) {
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.MaxResponseBodyBytes = 5
	outcome := observeTicket30Outcome(
		t,
		shadowmw.OutcomeDiff,
		cfg,
		textHandler("authoritative-overflow"),
		textHandler("shadow-overflow"),
	)
	if !outcome.AuthoritativeResponse().Truncated() ||
		!outcome.ShadowResponse().Truncated() {
		t.Fatal("metadata snapshots must preserve capture truncation")
	}
	assertGolden(
		t,
		"observer-diff-truncated.golden",
		outcome.RenderedDetail(),
	)
}

func TestObserverFullDetailKeepsBranchCapturesSeparateGolden(t *testing.T) {
	cfg := shadowmw.DefaultMiddlewareConfig()
	outcome := observeTicket30Outcome(
		t,
		shadowmw.OutcomeFull,
		cfg,
		jsonHandler(`{"branch":"authoritative"}`),
		jsonHandler(`{"branch":"shadow"}`),
	)

	detail := fmt.Sprintf(
		"authoritative:\n  status: %d\n  capture-truncated: %t\n  captured: %q\n"+
			"  decoding-state: %s\n  representation-kind: %s\n  representation-present: %t\n  compared: %q\n"+
			"shadow:\n  status: %d\n  capture-truncated: %t\n  captured: %q\n"+
			"  decoding-state: %s\n  representation-kind: %s\n  representation-present: %t\n  compared: %q\n",
		outcome.AuthoritativeResponse().StatusCode(),
		outcome.AuthoritativeResponse().Truncated(),
		outcome.AuthoritativeResponse().Body(),
		outcome.AuthoritativeBodyRepresentation().DecodingState(),
		outcome.AuthoritativeBodyRepresentation().Kind(),
		outcome.AuthoritativeBodyRepresentation().Present(),
		outcome.AuthoritativeComparedBody(),
		outcome.ShadowResponse().StatusCode(),
		outcome.ShadowResponse().Truncated(),
		outcome.ShadowResponse().Body(),
		outcome.ShadowBodyRepresentation().DecodingState(),
		outcome.ShadowBodyRepresentation().Kind(),
		outcome.ShadowBodyRepresentation().Present(),
		outcome.ShadowComparedBody(),
	)
	assertGolden(t, "observer-full-detail.golden", []byte(detail))
}

func TestObserverFullDetailTruncatedCaptureDoesNotClaimDecoding(t *testing.T) {
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.MaxResponseBodyBytes = 4
	outcome := observeTicket30Outcome(
		t,
		shadowmw.OutcomeFull,
		cfg,
		textHandler("authoritative overflow"),
		textHandler("shadow overflow"),
	)
	for branch, result := range map[string]struct {
		response       shadowmw.ResponseSnapshot
		representation shadowmw.BodyRepresentation
	}{
		"authoritative": {
			response:       outcome.AuthoritativeResponse(),
			representation: outcome.AuthoritativeBodyRepresentation(),
		},
		"shadow": {
			response:       outcome.ShadowResponse(),
			representation: outcome.ShadowBodyRepresentation(),
		},
	} {
		if !result.response.Truncated() {
			t.Errorf("%s capture is not marked truncated", branch)
		}
		if got := result.representation.DecodingState(); got != shadowmw.BodyDecodingNotAttempted {
			t.Errorf("%s decoding state = %v, want not attempted", branch, got)
		}
		if result.representation.Present() ||
			result.representation.Kind() != shadowmw.BodyRepresentationNone {
			t.Errorf("%s representation = %+v, want absent", branch, result.representation)
		}
	}
}

func TestObserverTruncationEvidencePreservesDifferenceKind(t *testing.T) {
	outcome := observeTicket30PolicyOutcome(
		t,
		shadowmw.OutcomeFull,
		shadowmw.DefaultMiddlewareConfig(),
		shadowmw.ResponseParity(
			shadowmw.CompareBodyAsBytes(),
			shadowmw.WithMaxDifferenceValueBytes(3),
		),
		textHandler("ab"),
		textHandler("b"),
	)
	differences := outcome.Differences()
	if len(differences) != 1 {
		t.Fatalf("Differences() = %#v, want one", differences)
	}
	difference := differences[0]
	if difference.Kind != "value" {
		t.Fatalf("Kind = %q, want stable value", difference.Kind)
	}
	if !difference.ExpectedTruncated() || difference.ActualTruncated() {
		t.Fatalf("truncation evidence = %#v", difference)
	}
}

func TestObserverFullDetailReportsBranchRepresentationMetadata(t *testing.T) {
	tests := []struct {
		name                    string
		policy                  shadowmw.Policy
		authoritative           http.Handler
		shadow                  http.Handler
		wantAuthoritativeState  shadowmw.BodyDecodingState
		wantShadowState         shadowmw.BodyDecodingState
		wantAuthoritativeKind   shadowmw.BodyRepresentationKind
		wantShadowKind          shadowmw.BodyRepresentationKind
		wantAuthoritativeBody   string
		wantShadowBody          string
		wantAuthoritativeExists bool
		wantShadowExists        bool
	}{
		{
			name:                    "empty decoded representation is present",
			policy:                  shadowmw.ResponseParity(shadowmw.CompareBodyAsBytes()),
			authoritative:           textHandler(""),
			shadow:                  textHandler(""),
			wantAuthoritativeState:  shadowmw.BodyDecodingSucceeded,
			wantShadowState:         shadowmw.BodyDecodingSucceeded,
			wantAuthoritativeKind:   shadowmw.BodyRepresentationDecoded,
			wantShadowKind:          shadowmw.BodyRepresentationDecoded,
			wantAuthoritativeExists: true,
			wantShadowExists:        true,
		},
		{
			name:                   "decision success does not attempt decoding",
			policy:                 shadowmw.DecisionParity(),
			authoritative:          jsonHandler(`{"branch":"authoritative"}`),
			shadow:                 jsonHandler(`{"branch":"shadow"}`),
			wantAuthoritativeState: shadowmw.BodyDecodingNotAttempted,
			wantShadowState:        shadowmw.BodyDecodingNotAttempted,
			wantAuthoritativeKind:  shadowmw.BodyRepresentationNone,
			wantShadowKind:         shadowmw.BodyRepresentationNone,
		},
		{
			name:                    "JSON is decoded then normalized",
			policy:                  shadowmw.ResponseParity(),
			authoritative:           encodedHandler("application/json", "gzip", gzipBody(t, `{"n":1.0,"a":true}`)),
			shadow:                  encodedHandler("application/json", "gzip", gzipBody(t, `{"a":true,"n":2e0}`)),
			wantAuthoritativeState:  shadowmw.BodyDecodingSucceeded,
			wantShadowState:         shadowmw.BodyDecodingSucceeded,
			wantAuthoritativeKind:   shadowmw.BodyRepresentationNormalized,
			wantShadowKind:          shadowmw.BodyRepresentationNormalized,
			wantAuthoritativeBody:   `{"a":true,"n":1}`,
			wantShadowBody:          `{"a":true,"n":2}`,
			wantAuthoritativeExists: true,
			wantShadowExists:        true,
		},
		{
			name:                    "form is normalized",
			policy:                  shadowmw.ResponseParity(),
			authoritative:           encodedHandler("application/x-www-form-urlencoded", "", []byte("z=last&a=old+value")),
			shadow:                  encodedHandler("application/x-www-form-urlencoded", "", []byte("a=new%20value&z=last")),
			wantAuthoritativeState:  shadowmw.BodyDecodingSucceeded,
			wantShadowState:         shadowmw.BodyDecodingSucceeded,
			wantAuthoritativeKind:   shadowmw.BodyRepresentationNormalized,
			wantShadowKind:          shadowmw.BodyRepresentationNormalized,
			wantAuthoritativeBody:   "a=old+value&z=last",
			wantShadowBody:          "a=new+value&z=last",
			wantAuthoritativeExists: true,
			wantShadowExists:        true,
		},
		{
			name:                    "normalization cannot exceed decoded body bound",
			policy:                  shadowmw.ResponseParity(shadowmw.WithMaxDecodedBodyBytes(16)),
			authoritative:           jsonHandler(`{"n":1e10}`),
			shadow:                  jsonHandler(`{"n":2e10}`),
			wantAuthoritativeState:  shadowmw.BodyDecodingSucceeded,
			wantShadowState:         shadowmw.BodyDecodingSucceeded,
			wantAuthoritativeKind:   shadowmw.BodyRepresentationDecoded,
			wantShadowKind:          shadowmw.BodyRepresentationDecoded,
			wantAuthoritativeBody:   `{"n":1e10}`,
			wantShadowBody:          `{"n":2e10}`,
			wantAuthoritativeExists: true,
			wantShadowExists:        true,
		},
		{
			name:                   "branch decoding failure is explicit",
			policy:                 shadowmw.ResponseParity(shadowmw.CompareBodyAsBytes()),
			authoritative:          encodedHandler("application/octet-stream", "gzip", []byte("not gzip")),
			shadow:                 encodedHandler("application/octet-stream", "", []byte("decoded")),
			wantAuthoritativeState: shadowmw.BodyDecodingFailed,
			wantShadowState:        shadowmw.BodyDecodingSucceeded,
			wantAuthoritativeKind:  shadowmw.BodyRepresentationNone,
			wantShadowKind:         shadowmw.BodyRepresentationDecoded,
			wantShadowBody:         "decoded",
			wantShadowExists:       true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := observeTicket30PolicyOutcome(
				t,
				shadowmw.OutcomeFull,
				shadowmw.DefaultMiddlewareConfig(),
				test.policy,
				test.authoritative,
				test.shadow,
			)
			assertBodyRepresentation(
				t,
				"authoritative",
				outcome.AuthoritativeBodyRepresentation(),
				test.wantAuthoritativeState,
				test.wantAuthoritativeKind,
				test.wantAuthoritativeExists,
				test.wantAuthoritativeBody,
			)
			assertBodyRepresentation(
				t,
				"shadow",
				outcome.ShadowBodyRepresentation(),
				test.wantShadowState,
				test.wantShadowKind,
				test.wantShadowExists,
				test.wantShadowBody,
			)
		})
	}
}

func assertBodyRepresentation(
	t *testing.T,
	branch string,
	representation shadowmw.BodyRepresentation,
	wantState shadowmw.BodyDecodingState,
	wantKind shadowmw.BodyRepresentationKind,
	wantPresent bool,
	wantBody string,
) {
	t.Helper()
	if got := representation.DecodingState(); got != wantState {
		t.Errorf("%s decoding state = %v, want %v", branch, got, wantState)
	}
	if got := representation.Kind(); got != wantKind {
		t.Errorf("%s representation kind = %v, want %v", branch, got, wantKind)
	}
	if got := representation.Present(); got != wantPresent {
		t.Errorf("%s representation present = %t, want %t", branch, got, wantPresent)
	}
	if got := string(representation.Body()); got != wantBody {
		t.Errorf("%s representation body = %q, want %q", branch, got, wantBody)
	}
	mutated := representation.Body()
	if len(mutated) > 0 {
		mutated[0] ^= 0xff
		if got := string(representation.Body()); got != wantBody {
			t.Errorf("%s representation was mutable: got %q, want %q", branch, got, wantBody)
		}
	}
}

func gzipBody(t *testing.T, body string) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(body)); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	return compressed.Bytes()
}

func encodedHandler(contentType, contentEncoding string, body []byte) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		if contentEncoding != "" {
			w.Header().Set("Content-Encoding", contentEncoding)
		}
		_, _ = w.Write(body)
	})
}

func TestObserverQueueByteBudgetIncludesRetainedRenderedDetail(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var observerCalls atomic.Int32
	recorder := &measurementRecorder{notify: make(chan struct{}, 32)}
	cfg := observerEngineConfig(observerFunc(func(
		context.Context,
		shadowmw.Outcome,
	) error {
		observerCalls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}))
	cfg.Observer.Detail = shadowmw.OutcomeCustom
	cfg.Observer.Renderer = func(context.Context, shadowmw.Outcome) ([]byte, error) {
		return []byte(strings.Repeat("rendered", 512)), nil
	}
	cfg.Observer.MaxQueuedBytes = 5_000
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(recorder),
	)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	body := strings.Repeat("x", 128)
	runWithEngine(t, engine, textHandler(body), textHandler(body+"y"))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first rendered observer delivery did not start")
	}
	runWithEngine(
		t,
		engine,
		textHandler(body),
		textHandler(body+strings.Repeat("z", 128)),
	)

	deadline := time.After(time.Second)
	for !recorder.hasDeliveryResult("dropped") {
		select {
		case <-recorder.notify:
		case <-deadline:
			t.Fatalf(
				"retained rendered detail did not consume queue bytes: %+v",
				recorder.snapshot(),
			)
		}
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := observerCalls.Load(); got != 1 {
		t.Fatalf("observer calls = %d, want 1 after rendered-detail drop", got)
	}
}

func observeTicket30Outcome(
	t *testing.T,
	detail shadowmw.OutcomeDetail,
	middlewareConfig shadowmw.MiddlewareConfig,
	authoritative http.Handler,
	shadow http.Handler,
) shadowmw.Outcome {
	t.Helper()
	return observeTicket30PolicyOutcome(
		t,
		detail,
		middlewareConfig,
		shadowmw.ResponseParity(),
		authoritative,
		shadow,
	)
}

func observeTicket30PolicyOutcome(
	t *testing.T,
	detail shadowmw.OutcomeDetail,
	middlewareConfig shadowmw.MiddlewareConfig,
	policy shadowmw.Policy,
	authoritative http.Handler,
	shadow http.Handler,
) shadowmw.Outcome {
	t.Helper()
	observed := make(chan shadowmw.Outcome, 1)
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.Observer = &shadowmw.ObserverDelivery{
		Observer: observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
			observed <- outcome
			return nil
		}),
		QueueCapacity:  2,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter:         shadowmw.ObserveMatches | shadowmw.ObserveMismatches,
		Detail:         detail,
	}
	engine := startObserverEngine(t, cfg)
	rule, err := shadowmw.NewRule(
		"ticket30.detail",
		policy,
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		authoritative,
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	middleware.Wrap(shadow).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	select {
	case outcome := <-observed:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("observer did not receive ticket-30 outcome")
		return shadowmw.Outcome{}
	}
}

func textHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(body))
	})
}

func jsonHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
}

const taxonomyRuleName = "taxonomy.target"

type taxonomyPolicyFunc func(
	context.Context,
	shadowmw.ResponseSnapshot,
	shadowmw.ResponseSnapshot,
) (shadowmw.Comparison, error)

func (f taxonomyPolicyFunc) Compare(
	ctx context.Context,
	authoritative shadowmw.ResponseSnapshot,
	shadow shadowmw.ResponseSnapshot,
) (shadowmw.Comparison, error) {
	return f(ctx, authoritative, shadow)
}

type taxonomyScenario struct {
	engineConfig     func(*shadowmw.EngineConfig)
	policy           shadowmw.Policy
	sampling         shadowmw.RuleOption
	middlewareConfig func(*shadowmw.MiddlewareConfig)
	authoritative    http.Handler
	shadow           http.Handler
	request          *http.Request
	writer           http.ResponseWriter
	startBeforeServe bool
	invoke           func(*testing.T, *shadowmw.Engine, http.Handler)
	cleanup          func()
}

type taxonomyObservation struct {
	outcome      shadowmw.Outcome
	measurements []shadowmw.Measurement
	failures     []shadowmw.TelemetryFailureCount
}

func TestEveryExportedTerminalReasonUsesItsRealProductionPath(t *testing.T) {
	tests := []struct {
		name           string
		reason         string
		classification shadowmw.OutcomeClassification
		inDenominator  bool
		configure      func(*taxonomyScenario)
	}{
		{
			name:           "parity",
			reason:         "parity",
			classification: shadowmw.OutcomeMatch,
			inDenominator:  true,
		},
		{
			name:           "difference",
			reason:         "difference",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.authoritative = statusHandler(http.StatusOK, `{"value":1}`)
				scenario.shadow = statusHandler(http.StatusCreated, `{"value":1}`)
			},
		},
		{
			name:           "comparison error",
			reason:         "comparison_error",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.policy = taxonomyPolicyFunc(func(
					context.Context,
					shadowmw.ResponseSnapshot,
					shadowmw.ResponseSnapshot,
				) (shadowmw.Comparison, error) {
					return shadowmw.Comparison{}, errors.New("comparison failed")
				})
			},
		},
		{
			name:           "comparison panic",
			reason:         "comparison_panic",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.policy = taxonomyPolicyFunc(func(
					context.Context,
					shadowmw.ResponseSnapshot,
					shadowmw.ResponseSnapshot,
				) (shadowmw.Comparison, error) {
					panic("comparison defect")
				})
			},
		},
		{
			name:           "comparison timeout",
			reason:         "comparison_timeout",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.engineConfig = func(cfg *shadowmw.EngineConfig) {
					cfg.ComparisonTimeout = 20 * time.Millisecond
				}
				scenario.policy = taxonomyPolicyFunc(func(
					ctx context.Context,
					_ shadowmw.ResponseSnapshot,
					_ shadowmw.ResponseSnapshot,
				) (shadowmw.Comparison, error) {
					<-ctx.Done()
					return shadowmw.Comparison{}, nil
				})
			},
		},
		{
			name:           "response truncated",
			reason:         "response_truncated",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.middlewareConfig = func(cfg *shadowmw.MiddlewareConfig) {
					cfg.MaxResponseBodyBytes = 2
				}
				scenario.authoritative = statusHandler(http.StatusOK, "authoritative")
				scenario.shadow = statusHandler(http.StatusOK, "shadow")
			},
		},
		{
			name:           "decoding failure",
			reason:         "decoding_failure",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				encoded := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
					writer.Header().Set("Content-Encoding", "unsupported")
					_, _ = writer.Write([]byte("same"))
				})
				scenario.authoritative = encoded
				scenario.shadow = encoded
			},
		},
		{
			name:           "custom comparator error",
			reason:         "custom_comparator_error",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.policy = shadowmw.ResponseParity(shadowmw.CompareBodyWith(
					func(
						context.Context,
						shadowmw.BodyComparisonInput,
					) ([]shadowmw.Difference, error) {
						return nil, errors.New("custom comparison failed")
					},
				))
			},
		},
		{
			name:           "custom comparator panic",
			reason:         "custom_comparator_panic",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.policy = shadowmw.ResponseParity(shadowmw.CompareBodyWith(
					func(
						context.Context,
						shadowmw.BodyComparisonInput,
					) ([]shadowmw.Difference, error) {
						panic("custom comparator defect")
					},
				))
			},
		},
		{
			name:           "custom comparator canceled",
			reason:         "custom_comparator_canceled",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				delegate := shadowmw.ResponseParity(shadowmw.CompareBodyWith(
					func(
						context.Context,
						shadowmw.BodyComparisonInput,
					) ([]shadowmw.Difference, error) {
						return nil, nil
					},
				))
				scenario.policy = taxonomyPolicyFunc(func(
					ctx context.Context,
					authoritative shadowmw.ResponseSnapshot,
					shadow shadowmw.ResponseSnapshot,
				) (shadowmw.Comparison, error) {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					return delegate.Compare(canceled, authoritative, shadow)
				})
			},
		},
		{
			name:           "session timeout",
			reason:         "session_timeout",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				release := make(chan struct{})
				var once sync.Once
				scenario.cleanup = func() {
					once.Do(func() { close(release) })
				}
				scenario.engineConfig = func(cfg *shadowmw.EngineConfig) {
					cfg.SessionTimeout = 20 * time.Millisecond
				}
				scenario.shadow = http.HandlerFunc(func(
					http.ResponseWriter,
					*http.Request,
				) {
					<-release
				})
			},
		},
		{
			name:           "authoritative panic",
			reason:         "authoritative_panic",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				shadowDone := make(chan struct{})
				scenario.authoritative = http.HandlerFunc(func(
					http.ResponseWriter,
					*http.Request,
				) {
					<-shadowDone
					panic("authoritative defect")
				})
				scenario.shadow = http.HandlerFunc(func(
					writer http.ResponseWriter,
					_ *http.Request,
				) {
					writer.WriteHeader(http.StatusCreated)
					close(shadowDone)
				})
				scenario.invoke = func(
					t *testing.T,
					_ *shadowmw.Engine,
					handler http.Handler,
				) {
					t.Helper()
					defer func() {
						if recovered := recover(); recovered != "authoritative defect" {
							t.Fatalf("recovered panic = %v, want authoritative defect", recovered)
						}
					}()
					handler.ServeHTTP(scenario.writer, scenario.request)
				}
			},
		},
		{
			name:           "authoritative setup error",
			reason:         "authoritative_setup_error",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.middlewareConfig = func(cfg *shadowmw.MiddlewareConfig) {
					cfg.Customizations.Authoritative.BodyTransforms = []shadowmw.BodyTransform{
						func(context.Context, []byte) ([]byte, error) {
							return nil, errors.New("authoritative setup failed")
						},
					}
				}
			},
		},
		{
			name:           "authoritative transport error",
			reason:         "authoritative_transport_error",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.authoritative = http.HandlerFunc(func(
					writer http.ResponseWriter,
					_ *http.Request,
				) {
					_, _ = writer.Write([]byte("authoritative"))
				})
				scenario.shadow = statusHandler(http.StatusOK, "sha")
				scenario.writer = &partialResponseWriter{
					header: make(http.Header),
					n:      3,
					err:    errors.New("client disconnected"),
				}
			},
		},
		{
			name:           "shadow panic",
			reason:         "panic",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.shadow = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					panic("shadow defect")
				})
			},
		},
		{
			name:           "shadow setup error",
			reason:         "shadow_setup_error",
			classification: shadowmw.OutcomeMismatch,
			inDenominator:  true,
			configure: func(scenario *taxonomyScenario) {
				scenario.middlewareConfig = func(cfg *shadowmw.MiddlewareConfig) {
					cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
						func(*http.Request) (*http.Request, error) {
							return nil, errors.New("shadow setup failed")
						},
					}
				}
			},
		},
		{
			name:           "engine unavailable",
			reason:         "engine_unavailable",
			classification: shadowmw.OutcomeSkipped,
			configure: func(scenario *taxonomyScenario) {
				scenario.startBeforeServe = false
			},
		},
		{
			name:           "capacity",
			reason:         "capacity",
			classification: shadowmw.OutcomeSkipped,
			configure: func(scenario *taxonomyScenario) {
				scenario.engineConfig = func(cfg *shadowmw.EngineConfig) {
					cfg.MaxInFlightSessions = 1
				}
				release := make(chan struct{})
				var once sync.Once
				scenario.cleanup = func() {
					once.Do(func() { close(release) })
				}
				scenario.invoke = func(
					t *testing.T,
					engine *shadowmw.Engine,
					target http.Handler,
				) {
					t.Helper()
					started := make(chan struct{})
					holderRule, err := shadowmw.NewRule(
						"taxonomy.holder",
						shadowmw.ResponseParity(),
						shadowmw.MatchAll(),
						shadowmw.SampleAll(),
					)
					if err != nil {
						t.Fatalf("NewRule(holder) error = %v", err)
					}
					cfg := shadowmw.DefaultMiddlewareConfig()
					cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
					holder, err := shadowmw.NewMiddleware(
						engine,
						http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
							writer.WriteHeader(http.StatusNoContent)
						}),
						cfg,
						holderRule,
					)
					if err != nil {
						t.Fatalf("NewMiddleware(holder) error = %v", err)
					}
					holder.Wrap(http.HandlerFunc(func(
						writer http.ResponseWriter,
						_ *http.Request,
					) {
						close(started)
						<-release
						writer.WriteHeader(http.StatusNoContent)
					})).ServeHTTP(
						httptest.NewRecorder(),
						httptest.NewRequest(http.MethodGet, "/holder", nil),
					)
					select {
					case <-started:
					case <-time.After(time.Second):
						t.Fatal("capacity holder did not start")
					}
					target.ServeHTTP(scenario.writer, scenario.request)
				}
			},
		},
		{
			name:           "not sampled",
			reason:         "not_sampled",
			classification: shadowmw.OutcomeSkipped,
			configure: func(scenario *taxonomyScenario) {
				scenario.sampling = shadowmw.SampleNone()
			},
		},
		{
			name:           "unsupported exchange",
			reason:         "unsupported_exchange",
			classification: shadowmw.OutcomeSkipped,
			configure: func(scenario *taxonomyScenario) {
				scenario.request.Header.Set("Accept", "text/event-stream")
			},
		},
		{
			name:           "request too large",
			reason:         "request_too_large",
			classification: shadowmw.OutcomeSkipped,
			configure: func(scenario *taxonomyScenario) {
				scenario.middlewareConfig = func(cfg *shadowmw.MiddlewareConfig) {
					cfg.MaxRequestBodyBytes = 1
				}
				scenario.request = httptest.NewRequest(
					http.MethodPost,
					"/",
					strings.NewReader("too large"),
				)
			},
		},
		{
			name:           "body read error",
			reason:         "body_read_error",
			classification: shadowmw.OutcomeSkipped,
			configure: func(scenario *taxonomyScenario) {
				request := httptest.NewRequest(http.MethodPost, "/", nil)
				request.Body = &simultaneousErrorBody{err: errors.New("request read failed")}
				request.ContentLength = -1
				scenario.request = request
			},
		},
	}

	seen := make(map[string]struct{}, len(tests))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, duplicate := seen[test.reason]; duplicate {
				t.Fatalf("duplicate terminal reason %q", test.reason)
			}
			seen[test.reason] = struct{}{}

			observation := exerciseTaxonomyScenario(t, test.configure)
			if observation.outcome.Classification() != test.classification ||
				observation.outcome.Reason() != test.reason {
				t.Fatalf(
					"outcome = %s/%q, want %s/%q",
					observation.outcome.Classification(),
					observation.outcome.Reason(),
					test.classification,
					test.reason,
				)
			}
			assertTaxonomyTerminalCounters(
				t,
				observation.measurements,
				test.classification,
				test.reason,
				test.inDenominator,
				observation.failures,
			)
		})
	}

	if len(tests) != 22 || len(seen) != 22 {
		t.Fatalf("terminal reason coverage = %d rows/%d unique, want 22/22", len(tests), len(seen))
	}
}

func exerciseTaxonomyScenario(
	t *testing.T,
	configure func(*taxonomyScenario),
) taxonomyObservation {
	t.Helper()
	scenario := taxonomyScenario{
		policy:   shadowmw.ResponseParity(),
		sampling: shadowmw.SampleAll(),
		authoritative: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusNoContent)
		}),
		shadow: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusNoContent)
		}),
		request:          httptest.NewRequest(http.MethodGet, "/", nil),
		writer:           httptest.NewRecorder(),
		startBeforeServe: true,
	}
	if configure != nil {
		configure(&scenario)
	}
	if scenario.cleanup != nil {
		defer scenario.cleanup()
	}

	outcomes := make(chan shadowmw.Outcome, 4)
	recorder := &measurementRecorder{notify: make(chan struct{}, 128)}
	cfg := observerEngineConfig(acceptanceObserver(func(
		_ context.Context,
		outcome shadowmw.Outcome,
	) error {
		outcomes <- outcome
		return nil
	}))
	if scenario.engineConfig != nil {
		scenario.engineConfig(&cfg)
	}
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	start := func() {
		t.Helper()
		if err := engine.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	}
	if scenario.startBeforeServe {
		start()
	}

	rule, err := shadowmw.NewRule(
		taxonomyRuleName,
		scenario.policy,
		shadowmw.MatchAll(),
		scenario.sampling,
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	if scenario.middlewareConfig != nil {
		scenario.middlewareConfig(&middlewareConfig)
	}
	middleware, err := shadowmw.NewMiddleware(
		engine,
		scenario.authoritative,
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	handler := middleware.Wrap(scenario.shadow)
	if scenario.invoke != nil {
		scenario.invoke(t, engine, handler)
	} else {
		handler.ServeHTTP(scenario.writer, scenario.request)
	}
	if !scenario.startBeforeServe {
		start()
	}

	outcome := waitForTaxonomyOutcome(t, outcomes)
	if scenario.cleanup != nil {
		scenario.cleanup()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	return taxonomyObservation{
		outcome:      outcome,
		measurements: recorder.snapshot(),
		failures:     engine.TelemetryFailures(),
	}
}

func waitForTaxonomyOutcome(
	t *testing.T,
	outcomes <-chan shadowmw.Outcome,
) shadowmw.Outcome {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case outcome := <-outcomes:
			if outcome.RuleName() == taxonomyRuleName {
				return outcome
			}
		case <-deadline:
			t.Fatal("observer did not receive target terminal outcome")
			return shadowmw.Outcome{}
		}
	}
}

func assertTaxonomyTerminalCounters(
	t *testing.T,
	measurements []shadowmw.Measurement,
	classification shadowmw.OutcomeClassification,
	reason string,
	inDenominator bool,
	failures []shadowmw.TelemetryFailureCount,
) {
	t.Helper()
	terminalCount := 0
	acceptedCount := 0
	for _, measurement := range measurements {
		attributes := taxonomyAttributes(measurement)
		if attributes["rule"] != taxonomyRuleName {
			continue
		}
		switch measurement.Name {
		case shadowmw.MetricComparisons:
			terminalCount++
			if measurement.Value != 1 ||
				attributes["result"] != classification.String() ||
				attributes["reason"] != reason {
				t.Errorf("comparison terminal measurement = %+v", measurement)
			}
		case shadowmw.MetricSessions:
			if attributes["result"] == "accepted" {
				acceptedCount++
				continue
			}
			terminalCount++
			if measurement.Value != 1 || attributes["result"] != reason {
				t.Errorf("skip terminal measurement = %+v", measurement)
			}
		}
	}
	if terminalCount != 1 {
		t.Fatalf(
			"terminal counter count = %d, want exactly one; telemetry failures: %+v",
			terminalCount,
			failures,
		)
	}
	if inDenominator {
		if classification == shadowmw.OutcomeSkipped || acceptedCount != 1 {
			t.Fatalf(
				"denominator membership = classification:%s accepted:%d, want non-skip/1",
				classification,
				acceptedCount,
			)
		}
		return
	}
	if classification != shadowmw.OutcomeSkipped || acceptedCount != 0 {
		t.Fatalf(
			"denominator exclusion = classification:%s accepted:%d, want skipped/0",
			classification,
			acceptedCount,
		)
	}
}

func taxonomyAttributes(measurement shadowmw.Measurement) map[string]string {
	attributes := make(map[string]string, len(measurement.Attributes))
	for _, attribute := range measurement.Attributes {
		attributes[attribute.Key] = attribute.Value
	}
	return attributes
}

func TestObserverDiffDetailGolden(t *testing.T) {
	tests := []struct {
		name          string
		contentType   string
		authoritative []byte
		shadow        []byte
		golden        string
	}{
		{
			name:          "structured JSON",
			contentType:   "application/json",
			authoritative: []byte(`{"stable":true,"value":{"name":"old","count":1}}`),
			shadow:        []byte(`{"value":{"count":2,"name":"new"},"stable":true}`),
			golden:        "observer-diff-json.golden",
		},
		{
			name:          "form",
			contentType:   "application/x-www-form-urlencoded",
			authoritative: []byte("stable=yes&name=old+value"),
			shadow:        []byte("name=new%20value&stable=yes"),
			golden:        "observer-diff-form.golden",
		},
		{
			name:          "binary summary",
			contentType:   "application/octet-stream",
			authoritative: []byte{0x00, 0x01, 0x02, 0x03},
			shadow:        []byte{0x00, 0x01, 0x02, 0xff},
			golden:        "observer-diff-binary.golden",
		},
		{
			name:          "unified text",
			contentType:   "text/plain",
			authoritative: []byte("alpha\nold\nomega"),
			shadow:        []byte("alpha\nnew\nomega"),
			golden:        "observer-diff-unified.golden",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := func(body []byte) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", test.contentType)
					_, _ = w.Write(body)
				})
			}
			outcome := runComparison(
				t,
				shadowmw.ResponseParity(),
				handler(test.authoritative),
				handler(test.shadow),
			)
			assertGolden(t, test.golden, outcome.RenderedDetail())
		})
	}
}

func TestObserverJSONDiffRenderingUsesSemanticNumberAndKeyNormalization(t *testing.T) {
	handler := func(version, body string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Version", version)
			_, _ = w.Write([]byte(body))
		})
	}
	outcome := runComparison(
		t,
		shadowmw.ResponseParity(),
		handler("authoritative", `{"a":1,"nested":{"b":1e0}}`),
		handler("shadow", `{"nested":{"b":1.00},"a":1.0}`),
	)
	assertGolden(
		t,
		"observer-diff-json-normalized.golden",
		outcome.RenderedDetail(),
	)
}

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	assertNoTrailingWhitespace(t, "rendered detail", got)
	assertNoTrailingWhitespace(t, path, want)
	if !bytes.Equal(got, want) {
		t.Fatalf(
			"%s mismatch\n--- got ---\n%s--- want ---\n%s",
			path,
			got,
			want,
		)
	}
}

func assertNoTrailingWhitespace(t *testing.T, source string, value []byte) {
	t.Helper()
	for lineNumber, line := range bytes.Split(value, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		last := line[len(line)-1]
		if last == ' ' || last == '\t' {
			t.Fatalf(
				"%s line %d has trailing whitespace: %q",
				source,
				lineNumber+1,
				line,
			)
		}
	}
}
