package core_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shadowmw "github.com/sergeybataev/shadowmw/internal/core"
)

// --- merged from engine_test.go ---

func TestEngineLifecycle(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 2

	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if got := engine.State(); got != shadowmw.EngineNew {
		t.Fatalf("initial State() = %v, want %v", got, shadowmw.EngineNew)
	}

	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := engine.State(); got != shadowmw.EngineRunning {
		t.Fatalf("running State() = %v, want %v", got, shadowmw.EngineRunning)
	}
	if err := engine.Start(context.Background()); err == nil {
		t.Fatal("second Start() error = nil, want an error")
	}

	if err := engine.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := engine.State(); got != shadowmw.EngineStopped {
		t.Fatalf("stopped State() = %v, want %v", got, shadowmw.EngineStopped)
	}
	if err := engine.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func TestNewEngineRequiresExplicitPositiveBounds(t *testing.T) {
	valid := shadowmw.DefaultEngineConfig()
	valid.ComparisonWorkers = 1
	tests := []struct {
		name   string
		mutate func(*shadowmw.EngineConfig)
	}{
		{name: "in flight", mutate: func(cfg *shadowmw.EngineConfig) { cfg.MaxInFlightSessions = 0 }},
		{name: "workers", mutate: func(cfg *shadowmw.EngineConfig) { cfg.ComparisonWorkers = 0 }},
		{name: "session timeout", mutate: func(cfg *shadowmw.EngineConfig) { cfg.SessionTimeout = 0 }},
		{name: "comparison timeout", mutate: func(cfg *shadowmw.EngineConfig) { cfg.ComparisonTimeout = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if _, err := shadowmw.NewEngine(cfg); err == nil {
				t.Fatal("NewEngine() error = nil")
			}
		})
	}
}

func TestEngineStartHonorsContextAndStoppedEngineCannotRestart(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := engine.Start(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start(cancelled) error = %v, want context canceled", err)
	}
	if got := engine.State(); got != shadowmw.EngineNew {
		t.Fatalf("State() after cancelled start = %v, want new", got)
	}
	if err := engine.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown(new) error = %v", err)
	}
	if err := engine.Start(context.Background()); !errors.Is(err, shadowmw.ErrEngineAlreadyStarted) {
		t.Fatalf("Start(stopped) error = %v, want ErrEngineAlreadyStarted", err)
	}
}

func TestConcurrentEngineShutdownIsIdempotent(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	results := make(chan error, 8)
	for range cap(results) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results <- engine.Shutdown(ctx)
		}()
	}
	for range cap(results) {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Shutdown() error = %v", err)
		}
	}
	if got := engine.State(); got != shadowmw.EngineStopped {
		t.Fatalf("State() = %v, want stopped", got)
	}
}

func TestShutdownBetweenStateCheckAndAdmissionEmitsOneUnavailableSkip(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	cfg := observerEngineConfig(observerFunc(func(
		_ context.Context,
		outcome shadowmw.Outcome,
	) error {
		outcomes <- outcome
		return nil
	}))
	cfg.Observer.Filter = shadowmw.ObserveSkipped
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	samplerEntered := make(chan struct{})
	releaseSampler := make(chan struct{})
	var releaseSamplerOnce sync.Once
	releaseRequest := func() {
		releaseSamplerOnce.Do(func() { close(releaseSampler) })
	}
	defer releaseRequest()

	rule, err := shadowmw.NewRule(
		"shutdown.admission",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleWith(func(request *http.Request) bool {
			if request.URL.Path == "/race" {
				close(samplerEntered)
				<-releaseSampler
			}
			return true
		}),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	var authoritativeCalls atomic.Int32
	var shadowRaceCalls atomic.Int32
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		authoritativeCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/race" {
			shadowRaceCalls.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, middlewareConfig, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	handler := middleware.Wrap(replacement)

	raceDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, "/race", nil),
		)
		close(raceDone)
	}()
	select {
	case <-samplerEntered:
	case <-time.After(time.Second):
		t.Fatal("race request did not pass the middleware engine-state check")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- engine.Shutdown(context.Background())
	}()
	deadline := time.After(time.Second)
	for engine.State() != shadowmw.EngineDraining {
		select {
		case <-deadline:
			t.Fatal("engine did not begin draining")
		default:
		}
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown() completed before in-progress admission decision: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	duringShutdown := httptest.NewRequest(http.MethodGet, "/during-shutdown", nil)
	duringShutdown.Header.Set("Accept", "text/event-stream")
	handler.ServeHTTP(httptest.NewRecorder(), duringShutdown)
	if got := authoritativeCalls.Load(); got != 1 {
		t.Fatalf("authoritative calls during shutdown = %d, want 1", got)
	}
	unsupported := waitForOutcome(t, outcomes)
	if unsupported.Classification() != shadowmw.OutcomeSkipped ||
		unsupported.Reason() != shadowmw.ReasonUnsupportedExchange {
		t.Fatalf(
			"concurrent outcome = %s/%q, want skipped/unsupported",
			unsupported.Classification(),
			unsupported.Reason(),
		)
	}

	releaseRequest()
	select {
	case <-raceDone:
	case <-time.After(time.Second):
		t.Fatal("authoritative race request did not continue")
	}

	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeSkipped ||
		outcome.Reason() != shadowmw.ReasonEngineUnavailable {
		t.Fatalf(
			"outcome = %s/%q, want skipped/engine_unavailable",
			outcome.Classification(),
			outcome.Reason(),
		)
	}
	if got := authoritativeCalls.Load(); got != 2 {
		t.Errorf("authoritative calls = %d, want 2", got)
	}
	if got := shadowRaceCalls.Load(); got != 0 {
		t.Errorf("shadow race calls = %d, want 0", got)
	}

	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	select {
	case duplicate := <-outcomes:
		t.Fatalf(
			"duplicate skipped outcome = %s/%q",
			duplicate.Classification(),
			duplicate.Reason(),
		)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestShutdownAdmissionDecisionRaceStress(t *testing.T) {
	const requests = 64

	outcomes := make(chan shadowmw.Outcome, requests*2)
	cfg := observerEngineConfig(observerFunc(func(
		_ context.Context,
		outcome shadowmw.Outcome,
	) error {
		outcomes <- outcome
		return nil
	}))
	cfg.Observer.Filter = shadowmw.ObserveSkipped
	cfg.Observer.QueueCapacity = requests * 2
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	samplerEntered := make(chan struct{}, requests)
	releaseSamplers := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseSamplers) })
	}
	defer release()
	rule, err := shadowmw.NewRule(
		"shutdown.admission.stress",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleWith(func(*http.Request) bool {
			samplerEntered <- struct{}{}
			<-releaseSamplers
			return true
		}),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	var authoritativeCalls atomic.Int32
	var shadowCalls atomic.Int32
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			authoritativeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		}),
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	handler := middleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		shadowCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))

	var served sync.WaitGroup
	served.Add(requests)
	for index := range requests {
		go func() {
			defer served.Done()
			handler.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(
					http.MethodGet,
					fmt.Sprintf("/race/%d", index),
					nil,
				),
			)
		}()
	}
	for range requests {
		select {
		case <-samplerEntered:
		case <-time.After(time.Second):
			t.Fatal("sampled requests did not reach paused admission decisions")
		}
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- engine.Shutdown(context.Background())
	}()
	deadline := time.After(time.Second)
	for engine.State() != shadowmw.EngineDraining {
		select {
		case <-deadline:
			t.Fatal("engine did not begin draining")
		default:
		}
	}
	release()
	servedDone := make(chan struct{})
	go func() {
		served.Wait()
		close(servedDone)
	}()
	select {
	case <-servedDone:
	case <-time.After(time.Second):
		t.Fatal("authoritative requests did not continue after admission refusal")
	}
	for range requests {
		outcome := waitForOutcome(t, outcomes)
		if outcome.Classification() != shadowmw.OutcomeSkipped ||
			outcome.Reason() != shadowmw.ReasonEngineUnavailable {
			t.Fatalf(
				"outcome = %s/%q, want skipped/engine_unavailable",
				outcome.Classification(),
				outcome.Reason(),
			)
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := authoritativeCalls.Load(); got != requests {
		t.Errorf("authoritative calls = %d, want %d", got, requests)
	}
	if got := shadowCalls.Load(); got != 0 {
		t.Errorf("shadow calls = %d, want 0", got)
	}
	select {
	case duplicate := <-outcomes:
		t.Fatalf(
			"duplicate skipped outcome = %s/%q",
			duplicate.Classification(),
			duplicate.Reason(),
		)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAuthoritativePanicReturnsWhileShadowIsStillRunning(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	cfg := observerEngineConfig(observerFunc(func(
		_ context.Context,
		outcome shadowmw.Outcome,
	) error {
		outcomes <- outcome
		return nil
	}))
	cfg.SessionTimeout = 500 * time.Millisecond
	cfg.Observer.Detail = shadowmw.OutcomeFull
	engine := startObserverEngine(t, cfg)

	shadowStarted := make(chan struct{})
	releaseShadow := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseShadow) })
	}
	defer release()
	authoritative := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-shadowStarted
		panic("authoritative defect")
	})
	shadow := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte("shadow-complete"))
		close(shadowStarted)
		<-releaseShadow
	})
	rule, err := shadowmw.NewRule(
		"authoritative.panic",
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
		authoritative,
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}

	returned := make(chan any, 1)
	go func() {
		defer func() {
			returned <- recover()
		}()
		middleware.Wrap(shadow).ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, "/", nil),
		)
	}()
	select {
	case recovered := <-returned:
		if recovered != "authoritative defect" {
			t.Fatalf("recovered panic = %v, want authoritative defect", recovered)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("authoritative panic waited for the running shadow")
	}
	select {
	case outcome := <-outcomes:
		t.Fatalf(
			"outcome arrived while shadow was running: %s/%q",
			outcome.Classification(),
			outcome.Reason(),
		)
	default:
	}

	release()
	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeMismatch ||
		outcome.Reason() != shadowmw.ReasonAuthoritativePanic {
		t.Fatalf(
			"outcome = %s/%q, want mismatch/authoritative_panic",
			outcome.Classification(),
			outcome.Reason(),
		)
	}
	if outcome.ShadowStatusCode() != http.StatusCreated ||
		string(outcome.ShadowResponse().Body()) != "shadow-complete" {
		t.Fatalf(
			"shadow snapshot = %d/%q, want 201/shadow-complete",
			outcome.ShadowStatusCode(),
			outcome.ShadowResponse().Body(),
		)
	}
	select {
	case duplicate := <-outcomes:
		t.Fatalf(
			"duplicate outcome = %s/%q",
			duplicate.Classification(),
			duplicate.Reason(),
		)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAcceptedSessionsReceiveDistinctOpaqueIdentifiers(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	cfg := observerEngineConfig(observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
		outcomes <- outcome
		return nil
	}))
	engine := startObserverEngine(t, cfg)
	for range 2 {
		runWithEngine(
			t,
			engine,
			statusHandler(http.StatusNoContent, ""),
			statusHandler(http.StatusNoContent, ""),
		)
	}
	next := func() shadowmw.Outcome {
		t.Helper()
		select {
		case outcome := <-outcomes:
			return outcome
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for outcome")
			return shadowmw.Outcome{}
		}
	}
	first := next()
	second := next()
	if got := len(first.SessionID().String()); got != 32 {
		t.Fatalf("session ID length = %d, want 32 hex characters", got)
	}
	if first.SessionID().String() == second.SessionID().String() {
		t.Fatal("accepted sessions received the same identifier")
	}
}

func TestEngine_Concurrent(t *testing.T) {
	const (
		goroutines = 10
		iterations = 5
	)
	var observed atomic.Int64
	cfg := observerEngineConfig(observerFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
		if outcome.Classification() != shadowmw.OutcomeMatch {
			return fmt.Errorf("unexpected classification %s", outcome.Classification())
		}
		observed.Add(1)
		return nil
	}))
	cfg.MaxInFlightSessions = goroutines * iterations
	cfg.ComparisonWorkers = 4
	cfg.Observer.Workers = 2
	cfg.Observer.QueueCapacity = goroutines * iterations
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	rule, err := shadowmw.NewRule(
		"concurrent.engine",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	middlewareCfg := shadowmw.DefaultMiddlewareConfig()
	middlewareCfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		statusHandler(http.StatusOK, `{"ok":true}`),
		middlewareCfg,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	handler := middleware.Wrap(statusHandler(http.StatusOK, `{"ok":true}`))

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				if state := engine.State(); state != shadowmw.EngineRunning {
					t.Errorf("State() = %v, want %v", state, shadowmw.EngineRunning)
					return
				}
				request := httptest.NewRequest(http.MethodGet, "/concurrent", nil)
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusOK {
					t.Errorf("status = %d, want %d", recorder.Code, http.StatusOK)
					return
				}
				if failures := engine.TelemetryFailures(); len(failures) != 0 {
					t.Errorf("TelemetryFailures() = %+v, want empty", failures)
					return
				}
			}
		}()
	}
	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := observed.Load(); got != goroutines*iterations {
		t.Fatalf("observed outcomes = %d, want %d", got, goroutines*iterations)
	}
}

// --- merged from public_session_behavior_test.go ---

func TestAuthoritativePanicTimeoutIgnoresLateShadowCompletion(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	cfg := observerEngineConfig(observerFunc(func(
		_ context.Context,
		outcome shadowmw.Outcome,
	) error {
		outcomes <- outcome
		return nil
	}))
	cfg.SessionTimeout = 50 * time.Millisecond
	cfg.Observer.Detail = shadowmw.OutcomeFull
	engine := startObserverEngine(t, cfg)

	shadowStarted := make(chan struct{})
	releaseShadow := make(chan struct{})
	shadowReturned := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseShadow) })
	}
	defer release()
	authoritative := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-shadowStarted
		panic("authoritative defect")
	})
	shadow := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		defer close(shadowReturned)
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte("shadow-late"))
		close(shadowStarted)
		<-releaseShadow
	})
	rule, err := shadowmw.NewRule(
		"authoritative.panic.timeout",
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
		authoritative,
		middlewareConfig,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}

	returned := make(chan any, 1)
	go func() {
		defer func() {
			returned <- recover()
		}()
		middleware.Wrap(shadow).ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, "/", nil),
		)
	}()
	select {
	case recovered := <-returned:
		if recovered != "authoritative defect" {
			t.Fatalf("recovered panic = %v, want authoritative defect", recovered)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("authoritative panic waited for the running shadow")
	}

	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeMismatch ||
		outcome.Reason() != shadowmw.ReasonAuthoritativePanic {
		t.Fatalf(
			"outcome = %s/%q, want mismatch/authoritative_panic",
			outcome.Classification(),
			outcome.Reason(),
		)
	}
	if outcome.ShadowStatusCode() != 0 || len(outcome.ShadowResponse().Body()) != 0 {
		t.Fatalf(
			"shadow snapshot = %d/%q, want empty timed-out branch",
			outcome.ShadowStatusCode(),
			outcome.ShadowResponse().Body(),
		)
	}

	release()
	select {
	case <-shadowReturned:
	case <-time.After(time.Second):
		t.Fatal("late shadow did not return")
	}
	select {
	case duplicate := <-outcomes:
		t.Fatalf(
			"late shadow emitted duplicate outcome %s/%q",
			duplicate.Classification(),
			duplicate.Reason(),
		)
	case <-time.After(50 * time.Millisecond):
	}
}
