package core_test

import (
	"context"
	"errors"
	shadowmw "github.com/sergeybataev/shadowmw/internal/core"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInstrumentationUsesFixedPrivacyPreservingMeasurements(t *testing.T) {
	recorder := &measurementRecorder{notify: make(chan struct{}, 32)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		return nil
	}))
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
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

	private := "/users/private?token=secret"
	runWithEngine(
		t,
		engine,
		statusHandler(http.StatusOK, `{"secret":"authoritative"}`),
		statusHandler(http.StatusCreated, `{"secret":"shadow"}`),
	)

	deadline := time.After(time.Second)
	for {
		measurements := recorder.snapshot()
		if containsMeasurement(measurements, shadowmw.MetricComparisons) {
			for _, measurement := range measurements {
				if !shadowmw.ValidMeasurement(measurement) {
					t.Fatalf("invalid measurement escaped core: %+v", measurement)
				}
				for _, attribute := range measurement.Attributes {
					if attribute.Value == private ||
						attribute.Key == "session_id" ||
						attribute.Key == "path" ||
						attribute.Key == "error" {
						t.Fatalf("private or unbounded attribute escaped core: %+v", attribute)
					}
				}
			}
			return
		}
		select {
		case <-recorder.notify:
		case <-deadline:
			t.Fatalf("comparison measurement not recorded: %+v", measurements)
		}
	}
}

func TestFastInstrumentationSurvivesSchedulerDelay(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(previousProcs)
	})

	stopPressure := startRunnableCPUPressure(t, 128)
	defer stopPressure()

	recorder := &measurementRecorder{notify: make(chan struct{}, 32)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		return nil
	}))
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	failures := engine.TelemetryFailures()
	if hasTelemetryFailure(failures, shadowmw.TelemetryInstrumentationTimeout) {
		t.Fatalf("fast instrumentation falsely timed out under scheduler pressure: %+v", failures)
	}
	if hasTelemetryFailure(failures, shadowmw.TelemetryInstrumentationDropped) {
		t.Fatalf("fast instrumentation dropped measurements under scheduler pressure: %+v", failures)
	}
	if !containsMeasurement(recorder.snapshot(), shadowmw.MetricComparisons) {
		t.Fatalf(
			"fast instrumentation lost the terminal comparison measurement; failures: %+v",
			failures,
		)
	}
}

func TestPanickingInstrumentationAndTracingAreIsolated(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(panickingInstrumentation{}),
		shadowmw.WithTracing(panickingTracing{}),
	)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func startRunnableCPUPressure(t *testing.T, goroutines int) func() {
	t.Helper()
	stop := make(chan struct{})
	var started sync.WaitGroup
	var stopped sync.WaitGroup
	started.Add(goroutines)
	stopped.Add(goroutines)
	for range goroutines {
		go func() {
			defer stopped.Done()
			started.Done()
			var work uint64
			for {
				for range 1_000 {
					work++
				}
				runtime.KeepAlive(work)
				select {
				case <-stop:
					return
				default:
					runtime.Gosched()
				}
			}
		}()
	}
	started.Wait()
	return func() {
		close(stop)
		stopped.Wait()
	}
}

func TestSanitizeMeasurementCollapsesUnboundedExtensionValues(t *testing.T) {
	measurement, ok := shadowmw.SanitizeMeasurement(shadowmw.Measurement{
		Name:  shadowmw.MetricDifferences,
		Value: 1,
		Attributes: []shadowmw.Attribute{
			{Key: "rule", Value: "users.get"},
			{Key: "dimension", Value: "private-field-name"},
			{Key: "kind", Value: "private-value"},
		},
	})
	if !ok {
		t.Fatal("SanitizeMeasurement() rejected a structurally valid measurement")
	}
	for _, attribute := range measurement.Attributes {
		switch attribute.Key {
		case "dimension", "kind":
			if attribute.Value != "other" {
				t.Fatalf("%s = %q, want other", attribute.Key, attribute.Value)
			}
		}
	}
}

func TestAuthoritativeTransportErrorRetainsBoundedMetricAndSpanReason(t *testing.T) {
	recorder := &measurementRecorder{notify: make(chan struct{}, 64)}
	tracing := &recordingTracing{}
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(recorder),
		shadowmw.WithTracing(tracing),
	)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	rule, err := shadowmw.NewRule(
		"transport.failure",
		shadowmw.ResponseParity(),
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
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("authoritative"))
		}),
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	writer := &partialResponseWriter{
		header: make(http.Header),
		n:      3,
		err:    errors.New("client disconnected"),
	}
	middleware.Wrap(statusHandler(http.StatusOK, "shadow")).ServeHTTP(
		writer,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if !measurementHasAttribute(
		recorder.snapshot(),
		shadowmw.MetricComparisons,
		"reason",
		shadowmw.ReasonAuthoritativeTransportError,
	) {
		t.Fatal("comparison metric did not retain authoritative_transport_error")
	}
	if !tracing.hasEventAttribute(
		"shadowmw.incomplete",
		"reason",
		shadowmw.ReasonAuthoritativeTransportError,
	) {
		t.Fatal("span event did not retain authoritative_transport_error")
	}
}

func TestRejectionCodeMetricsUseExplicitAllowlistAndFallbacks(t *testing.T) {
	recorder := &measurementRecorder{notify: make(chan struct{}, 64)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		return nil
	}))
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(recorder),
		shadowmw.WithRejectionCodeMetricAllowlist("known"),
	)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	policy := shadowmw.DecisionParity(shadowmw.WithRejectedErrorExtractors(
		func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return "known", true, nil
		},
		func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return "private-unlisted-code", true, nil
		},
	))
	runPolicyWithEngine(
		t,
		engine,
		policy,
		statusHandler(http.StatusUnprocessableEntity, `{"error":"same"}`),
		statusHandler(http.StatusUnprocessableEntity, `{"error":"same"}`),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	codes := metricAttributeValues(
		recorder.snapshot(),
		shadowmw.MetricRejectionCodes,
		"code",
	)
	if !containsString(codes, "known") || !containsString(codes, "other") {
		t.Fatalf("rejection metric codes = %v, want known and other", codes)
	}
	for _, code := range codes {
		if code == "private-unlisted-code" {
			t.Fatal("unlisted rejection code escaped metric allowlisting")
		}
	}
}

func TestRejectionCodeMetricUsesNoneForAbsentCode(t *testing.T) {
	recorder := &measurementRecorder{notify: make(chan struct{}, 64)}
	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		return nil
	}))
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithInstrumentation(recorder))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	policy := shadowmw.DecisionParity(shadowmw.WithRejectedErrorExtractors(
		func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return "", false, nil
		},
		func(context.Context, shadowmw.ResponseSnapshot) (string, bool, error) {
			return "private-failure-value", true, errors.New("private extractor error")
		},
	))
	runPolicyWithEngine(
		t,
		engine,
		policy,
		statusHandler(http.StatusBadRequest, `{"error":"same"}`),
		statusHandler(http.StatusBadRequest, `{"error":"same"}`),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	codes := metricAttributeValues(
		recorder.snapshot(),
		shadowmw.MetricRejectionCodes,
		"code",
	)
	if len(codes) != 2 || codes[0] != "none" || codes[1] != "other" {
		t.Fatalf("rejection metric codes = %v, want [none other]", codes)
	}
}

func TestRejectionCodeMetricAllowlistIsValidated(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	if _, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithRejectionCodeMetricAllowlist("duplicate", "duplicate"),
	); err == nil {
		t.Fatal("NewEngine() duplicate rejection allowlist error = nil")
	}
}

func TestTracingEmitsFixedPrivacyPreservingOperations(t *testing.T) {
	tracing := &recordingTracing{}
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithTracing(tracing))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	runWithEngine(
		t,
		engine,
		statusHandler(http.StatusOK, `{"value":1}`),
		statusHandler(http.StatusCreated, `{"value":1}`),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	tracing.mu.Lock()
	starts := append([]shadowmw.SpanStart(nil), tracing.starts...)
	tracing.mu.Unlock()
	names := make(map[string]bool)
	for _, start := range starts {
		names[start.Name] = true
		for _, attribute := range start.Attributes {
			switch attribute.Key {
			case "rule", "branch", "outcome", "reason", "dimension", "kind":
			default:
				t.Fatalf("unbounded span attribute: %+v", attribute)
			}
		}
	}
	for _, name := range []string{
		"shadowmw.session",
		"shadowmw.authoritative",
		"shadowmw.shadow",
		"shadowmw.compare",
	} {
		if !names[name] {
			t.Errorf("span %q was not started; starts = %+v", name, starts)
		}
	}
}

func TestSlowTelemetryCollaboratorsCannotDelayAuthoritativeTraffic(t *testing.T) {
	release := make(chan struct{})
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(blockingInstrumentation{release: release}),
		shadowmw.WithTracing(blockingTracing{release: release}),
		shadowmw.WithLogger(slog.New(blockingSlogHandler{release: release})),
	)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	started := make(chan error, 1)
	go func() {
		started <- engine.Start(context.Background())
	}()
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("slow logging delayed Engine.Start")
	}

	served := make(chan struct{})
	go func() {
		runWithEngine(
			t,
			engine,
			statusHandler(http.StatusOK, `{"value":1}`),
			statusHandler(http.StatusCreated, `{"value":1}`),
		)
		close(served)
	}()
	select {
	case <-served:
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("slow instrumentation or tracing delayed authoritative traffic")
	}

	deadline := time.After(time.Second)
	for {
		failures := engine.TelemetryFailures()
		if hasTelemetryFailure(failures, shadowmw.TelemetryInstrumentationTimeout) &&
			hasTelemetryFailure(failures, shadowmw.TelemetryTracingTimeout) &&
			hasTelemetryFailure(failures, shadowmw.TelemetryLoggingTimeout) {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			close(release)
			t.Fatalf("missing bounded telemetry failure evidence: %+v", failures)
		}
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestBlockingInstrumentationCannotExtendShutdownBeyondWatchdog(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCallback := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseCallback)

	cfg := observerEngineConfig(observerFunc(func(context.Context, shadowmw.Outcome) error {
		return nil
	}))
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(signalingBlockingInstrumentation{
			entered: entered,
			release: release,
		}),
	)
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
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking instrumentation callback did not start")
	}

	shutdownStarted := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if elapsed := time.Since(shutdownStarted); elapsed > 500*time.Millisecond {
		t.Fatalf("Shutdown() elapsed = %s, want bounded telemetry drain", elapsed)
	}
	if failures := engine.TelemetryFailures(); !hasTelemetryFailure(
		failures,
		shadowmw.TelemetryInstrumentationTimeout,
	) {
		t.Fatalf("TelemetryFailures() missing instrumentation timeout: %+v", failures)
	}
}

func TestPanickingTelemetryEmitsIndependentCategoricalEvidence(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(panickingInstrumentation{}),
		shadowmw.WithTracing(panickingTracing{}),
		shadowmw.WithLogger(slog.New(panickingSlogHandler{})),
	)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	failures := engine.TelemetryFailures()
	for _, kind := range []shadowmw.TelemetryFailureKind{
		shadowmw.TelemetryInstrumentationPanic,
		shadowmw.TelemetryTracingPanic,
		shadowmw.TelemetryLoggingPanic,
	} {
		if !hasTelemetryFailure(failures, kind) {
			t.Errorf("TelemetryFailures() missing %q: %+v", kind, failures)
		}
	}
}

func hasTelemetryFailure(
	failures []shadowmw.TelemetryFailureCount,
	kind shadowmw.TelemetryFailureKind,
) bool {
	for _, failure := range failures {
		if failure.Kind == kind && failure.Count > 0 {
			return true
		}
	}
	return false
}

func containsMeasurement(measurements []shadowmw.Measurement, name shadowmw.MetricName) bool {
	for _, measurement := range measurements {
		if measurement.Name == name {
			return true
		}
	}
	return false
}

func runPolicyWithEngine(
	t *testing.T,
	engine *shadowmw.Engine,
	policy shadowmw.Policy,
	authoritative http.Handler,
	replacement http.Handler,
) {
	t.Helper()
	rule, err := shadowmw.NewRule(
		"rejection.metrics",
		policy,
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
	middleware.Wrap(replacement).ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
}

func metricAttributeValues(
	measurements []shadowmw.Measurement,
	name shadowmw.MetricName,
	key string,
) []string {
	var values []string
	for _, measurement := range measurements {
		if measurement.Name != name {
			continue
		}
		for _, attribute := range measurement.Attributes {
			if attribute.Key == key {
				values = append(values, attribute.Value)
			}
		}
	}
	return values
}

func measurementHasAttribute(
	measurements []shadowmw.Measurement,
	name shadowmw.MetricName,
	key string,
	value string,
) bool {
	for _, candidate := range metricAttributeValues(measurements, name, key) {
		if candidate == value {
			return true
		}
	}
	return false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type panickingInstrumentation struct{}

func (panickingInstrumentation) Record(context.Context, shadowmw.Measurement) {
	panic("instrumentation panic")
}

type panickingTracing struct{}

func (panickingTracing) Start(context.Context, shadowmw.SpanStart) (context.Context, shadowmw.Span) {
	panic("tracing panic")
}

type recordingTracing struct {
	mu     sync.Mutex
	starts []shadowmw.SpanStart
	events []recordedSpanEvent
}

func (r *recordingTracing) Start(ctx context.Context, start shadowmw.SpanStart) (context.Context, shadowmw.Span) {
	r.mu.Lock()
	r.starts = append(r.starts, start)
	r.mu.Unlock()
	return ctx, recordingSpan{tracing: r}
}

type recordedSpanEvent struct {
	name       string
	attributes []shadowmw.Attribute
}

func (r *recordingTracing) hasEventAttribute(eventName, key, value string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, event := range r.events {
		if event.name != eventName {
			continue
		}
		for _, attribute := range event.attributes {
			if attribute.Key == key && attribute.Value == value {
				return true
			}
		}
	}
	return false
}

type recordingSpan struct {
	tracing *recordingTracing
}

func (s recordingSpan) AddEvent(name string, attributes ...shadowmw.Attribute) {
	if s.tracing == nil {
		return
	}
	s.tracing.mu.Lock()
	s.tracing.events = append(s.tracing.events, recordedSpanEvent{
		name:       name,
		attributes: append([]shadowmw.Attribute(nil), attributes...),
	})
	s.tracing.mu.Unlock()
}

func (recordingSpan) End() {}

type blockingInstrumentation struct {
	release <-chan struct{}
}

func (b blockingInstrumentation) Record(context.Context, shadowmw.Measurement) {
	<-b.release
}

type signalingBlockingInstrumentation struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (b signalingBlockingInstrumentation) Record(context.Context, shadowmw.Measurement) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
}

type blockingTracing struct {
	release <-chan struct{}
}

func (b blockingTracing) Start(ctx context.Context, _ shadowmw.SpanStart) (context.Context, shadowmw.Span) {
	<-b.release
	return ctx, recordingSpan{}
}

type blockingSlogHandler struct {
	release <-chan struct{}
}

func (h blockingSlogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h blockingSlogHandler) Handle(context.Context, slog.Record) error {
	<-h.release
	return nil
}
func (h blockingSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h blockingSlogHandler) WithGroup(string) slog.Handler      { return h }

type panickingSlogHandler struct{}

func (panickingSlogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (panickingSlogHandler) Handle(context.Context, slog.Record) error {
	panic("logging panic")
}
func (h panickingSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h panickingSlogHandler) WithGroup(string) slog.Handler      { return h }

func TestStructuredLoggingLevelsAndPrivacy(t *testing.T) {
	handler := &recordingSlogHandler{}
	logger := slog.New(handler)
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithLogger(logger))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	runWithEngine(
		t,
		engine,
		statusHandler(http.StatusOK, `{"secret":"authoritative"}`),
		statusHandler(http.StatusCreated, `{"secret":"shadow"}`),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	records := handler.snapshot()
	if !hasLogLevel(records, slog.LevelInfo) {
		t.Fatal("lifecycle info log was not emitted")
	}
	if !hasLogLevel(records, slog.LevelWarn) {
		t.Fatal("behavioral mismatch warning was not emitted")
	}
	for _, record := range records {
		text := record.Message
		record.Attrs(func(attribute slog.Attr) bool {
			text += " " + attribute.String()
			return true
		})
		for _, private := range []string{`{"secret":"authoritative"}`, `{"secret":"shadow"}`} {
			if strings.Contains(text, private) {
				t.Fatalf("log leaked compared data: %s", text)
			}
		}
	}
}

func TestOperationalMismatchLogsAtError(t *testing.T) {
	handler := &recordingSlogHandler{}
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithLogger(slog.New(handler)))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	encoded := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "unsupported")
		_, _ = w.Write([]byte("private"))
	})
	runWithEngine(t, engine, encoded, encoded)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if !hasLogLevel(handler.snapshot(), slog.LevelError) {
		t.Fatal("operational comparison failure did not emit an error log")
	}
}

func TestRejectionExtractorFailuresLogAsOperationalWithoutChangingParity(t *testing.T) {
	tests := []struct {
		name   string
		result shadowmw.RejectionCodeResult
	}{
		{name: "error", result: shadowmw.RejectionCodeExtractorError},
		{name: "panic", result: shadowmw.RejectionCodeExtractorPanic},
		{name: "cancellation", result: shadowmw.RejectionCodeExtractorCanceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := &recordingSlogHandler{}
			outcomes := make(chan shadowmw.Outcome, 1)
			cfg := observerEngineConfig(observerFunc(func(
				_ context.Context,
				outcome shadowmw.Outcome,
			) error {
				outcomes <- outcome
				return nil
			}))
			engine, err := shadowmw.NewEngine(
				cfg,
				shadowmw.WithLogger(slog.New(records)),
			)
			if err != nil {
				t.Fatalf("NewEngine() error = %v", err)
			}
			if err := engine.Start(context.Background()); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			runPolicyWithEngine(
				t,
				engine,
				rejectionEvidencePolicy{result: test.result},
				statusHandler(http.StatusBadRequest, "same"),
				statusHandler(http.StatusBadRequest, "same"),
			)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := engine.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown() error = %v", err)
			}
			select {
			case outcome := <-outcomes:
				if outcome.Classification() != shadowmw.OutcomeMatch {
					t.Fatalf("outcome = %s, want match", outcome.Classification())
				}
			default:
				t.Fatal("observer did not receive parity outcome")
			}
			if !hasLogLevel(records.snapshot(), slog.LevelError) {
				t.Fatal("rejection extractor operational failure did not emit an error log")
			}
			if hasLogLevel(records.snapshot(), slog.LevelWarn) {
				t.Fatal("rejection extractor operational failure emitted an ordinary mismatch warning")
			}
		})
	}
}

func TestRejectionExtractorFailureUpgradesMismatchLogWithoutChangingMismatch(t *testing.T) {
	records := &recordingSlogHandler{}
	outcomes := make(chan shadowmw.Outcome, 1)
	cfg := observerEngineConfig(observerFunc(func(
		_ context.Context,
		outcome shadowmw.Outcome,
	) error {
		outcomes <- outcome
		return nil
	}))
	engine, err := shadowmw.NewEngine(cfg, shadowmw.WithLogger(slog.New(records)))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	runPolicyWithEngine(
		t,
		engine,
		rejectionEvidencePolicy{
			result: shadowmw.RejectionCodeExtractorError,
			differences: []shadowmw.Difference{{
				Dimension: "body",
				Kind:      "different",
			}},
		},
		statusHandler(http.StatusBadRequest, "authoritative"),
		statusHandler(http.StatusBadRequest, "shadow"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	select {
	case outcome := <-outcomes:
		if outcome.Classification() != shadowmw.OutcomeMismatch {
			t.Fatalf("outcome = %s, want mismatch", outcome.Classification())
		}
	default:
		t.Fatal("observer did not receive mismatch outcome")
	}
	if !hasLogLevel(records.snapshot(), slog.LevelError) {
		t.Fatal("rejection extractor failure did not upgrade mismatch log to error")
	}
	if hasLogLevel(records.snapshot(), slog.LevelWarn) {
		t.Fatal("rejection extractor failure emitted an ordinary mismatch warning")
	}
}

type rejectionEvidencePolicy struct {
	result      shadowmw.RejectionCodeResult
	differences []shadowmw.Difference
}

func (p rejectionEvidencePolicy) Compare(
	context.Context,
	shadowmw.ResponseSnapshot,
	shadowmw.ResponseSnapshot,
) (shadowmw.Comparison, error) {
	return shadowmw.Comparison{
		Differences: append([]shadowmw.Difference(nil), p.differences...),
		RejectionCodes: &shadowmw.RejectionCodeEvidence{
			Authoritative: shadowmw.ExtractedRejectionCode{Result: p.result},
			Shadow:        shadowmw.ExtractedRejectionCode{Result: shadowmw.RejectionCodeFound},
		},
	}, nil
}

type recordingSlogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingSlogHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record.Clone())
	h.mu.Unlock()
	return nil
}

func (h *recordingSlogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *recordingSlogHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *recordingSlogHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

func hasLogLevel(records []slog.Record, level slog.Level) bool {
	for _, record := range records {
		if record.Level == level {
			return true
		}
	}
	return false
}
