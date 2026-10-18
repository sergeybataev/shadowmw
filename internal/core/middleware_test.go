package core_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shadowmw "github.com/sergeybataev/shadowmw/internal/core"
)

func TestMiddlewareUsesAuthoritativeHandlerOnlyForSampledMatches(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
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
		"orders.get",
		shadowmw.ResponseParity(),
		shadowmw.MatchMethods(http.MethodGet),
		shadowmw.MatchPath("/orders"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	var authoritativeCalls atomic.Int32
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		authoritativeCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"source":"authoritative"}`))
	})

	var replacementCalls atomic.Int32
	replacementCalled := make(chan struct{}, 2)
	replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		replacementCalls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"source":"replacement"}`))
		replacementCalled <- struct{}{}
	})

	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, middlewareConfig, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	handler := middleware.Wrap(replacement)

	unmatched := httptest.NewRecorder()
	handler.ServeHTTP(unmatched, httptest.NewRequest(http.MethodGet, "/health", nil))
	if unmatched.Code != http.StatusOK {
		t.Fatalf("unmatched status = %d, want %d", unmatched.Code, http.StatusOK)
	}
	if authoritativeCalls.Load() != 0 {
		t.Fatalf("authoritative calls after unmatched request = %d, want 0", authoritativeCalls.Load())
	}
	<-replacementCalled

	matched := httptest.NewRecorder()
	handler.ServeHTTP(matched, httptest.NewRequest(http.MethodGet, "/orders", nil))
	if matched.Code != http.StatusAccepted {
		t.Fatalf("matched status = %d, want %d", matched.Code, http.StatusAccepted)
	}
	if got := matched.Body.String(); got != `{"source":"authoritative"}` {
		t.Fatalf("matched body = %q, want authoritative body", got)
	}

	select {
	case <-replacementCalled:
	case <-time.After(time.Second):
		t.Fatal("replacement shadow handler was not called")
	}

	if authoritativeCalls.Load() != 1 {
		t.Fatalf("authoritative calls = %d, want 1", authoritativeCalls.Load())
	}
	if replacementCalls.Load() != 2 {
		t.Fatalf("replacement calls = %d, want unmatched plus shadow", replacementCalls.Load())
	}
}

func TestMiddlewareClonesRequestBodyAndMarksBranches(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
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
		"mutation.validate",
		shadowmw.ResponseParity(),
		shadowmw.MatchMethods(http.MethodPost),
		shadowmw.MatchPath("/mutate"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	shadowBody := make(chan string, 1)
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !shadowmw.IsShadow(request.Context()) {
			t.Error("replacement sampled request is not marked as shadow")
		}
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Errorf("shadow body read error = %v", readErr)
		}
		shadowBody <- string(body)
		w.WriteHeader(http.StatusNoContent)
	})
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := shadowmw.BranchFromContext(request.Context()); got != shadowmw.BranchAuthoritative {
			t.Errorf("authoritative branch = %v, want %v", got, shadowmw.BranchAuthoritative)
		}
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Errorf("authoritative body read error = %v", readErr)
		}
		if got := string(body); got != "name=sergey" {
			t.Errorf("authoritative body = %q, want name=sergey", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, middlewareConfig, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/mutate",
		bytes.NewBufferString("name=sergey"),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	middleware.Wrap(replacement).ServeHTTP(recorder, request)

	select {
	case got := <-shadowBody:
		if got != "name=sergey" {
			t.Fatalf("shadow body = %q, want name=sergey", got)
		}
	case <-time.After(time.Second):
		t.Fatal("shadow handler did not read its request body")
	}
}

func TestMiddlewareAppliesScopedBodyTransformsAndRequestHooksInOrder(t *testing.T) {
	type contextKey string
	const key contextKey = "branch"

	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
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
		"customized.post",
		shadowmw.ResponseParity(),
		shadowmw.MatchPath("/customized"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	assertBranch := func(
		t *testing.T,
		request *http.Request,
		wantBody string,
		wantHeader string,
		wantContext string,
	) {
		t.Helper()
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatalf("ReadAll() error = %v", readErr)
		}
		if got := string(body); got != wantBody {
			t.Errorf("body = %q, want %q", got, wantBody)
		}
		if got := request.Header.Get("X-Branch"); got != wantHeader {
			t.Errorf("X-Branch = %q, want %q", got, wantHeader)
		}
		if got := request.Context().Value(key); got != wantContext {
			t.Errorf("context branch = %v, want %q", got, wantContext)
		}
		if got := request.Header.Get("X-Common"); got != "yes" {
			t.Errorf("X-Common = %q, want yes", got)
		}
	}

	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		assertBranch(t, request, "base-common-authoritative", "authoritative", "authoritative")
		w.WriteHeader(http.StatusNoContent)
	})
	shadowDone := make(chan struct{}, 1)
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		assertBranch(t, request, "base-common-shadow", "shadow", "shadow")
		shadowDone <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	})

	appendBody := func(suffix string) shadowmw.BodyTransform {
		return func(_ context.Context, body []byte) ([]byte, error) {
			return append(body, suffix...), nil
		}
	}
	setMetadata := func(branch string) shadowmw.RequestHook {
		return func(request *http.Request) (*http.Request, error) {
			request.Header.Set("X-Branch", branch)
			return request.WithContext(context.WithValue(request.Context(), key, branch)), nil
		}
	}

	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middlewareConfig.Customizations = shadowmw.RequestCustomizations{
		Both: shadowmw.BranchCustomization{
			BodyTransforms: []shadowmw.BodyTransform{appendBody("-common")},
			RequestHooks: []shadowmw.RequestHook{
				func(request *http.Request) (*http.Request, error) {
					request.Header.Set("X-Common", "yes")
					return request, nil
				},
			},
		},
		Authoritative: shadowmw.BranchCustomization{
			BodyTransforms: []shadowmw.BodyTransform{appendBody("-authoritative")},
			RequestHooks:   []shadowmw.RequestHook{setMetadata("authoritative")},
		},
		Shadow: shadowmw.BranchCustomization{
			BodyTransforms: []shadowmw.BodyTransform{appendBody("-shadow")},
			RequestHooks:   []shadowmw.RequestHook{setMetadata("shadow")},
		},
	}
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, middlewareConfig, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	middleware.Wrap(replacement).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodPost, "/customized", bytes.NewBufferString("base")),
	)

	select {
	case <-shadowDone:
	case <-time.After(time.Second):
		t.Fatal("shadow handler was not called")
	}
}

type failingBody struct {
	delivered bool
	err       error
}

func (b *failingBody) Read(destination []byte) (int, error) {
	if !b.delivered {
		b.delivered = true
		return copy(destination, "partial"), nil
	}
	return 0, b.err
}

func (*failingBody) Close() error { return nil }

func TestMiddlewareCanReplayCaptureErrorToAuthoritativeHandler(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
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
		_ = engine.Shutdown(ctx)
	})

	rule, err := shadowmw.NewRule(
		"body.error",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	wantErr := errors.New("broken request stream")
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, readErr := io.ReadAll(request.Body)
		if string(body) != "partial" {
			t.Errorf("replayed body = %q, want partial", body)
		}
		if !errors.Is(readErr, wantErr) {
			t.Errorf("replayed error = %v, want %v", readErr, wantErr)
		}
		w.WriteHeader(http.StatusBadRequest)
	})
	shadowCalled := false
	replacement := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		shadowCalled = true
	})
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.PassBodyReadErrorsToAuthoritative()
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, middlewareConfig, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = &failingBody{err: wantErr}
	request.ContentLength = -1
	recorder := httptest.NewRecorder()
	middleware.Wrap(replacement).ServeHTTP(recorder, request)

	if shadowCalled {
		t.Fatal("shadow handler was called after request capture failed")
	}
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestMiddleware_Concurrent(t *testing.T) {
	const (
		goroutines = 10
		iterations = 5
	)
	cfg := shadowmw.DefaultEngineConfig()
	cfg.MaxInFlightSessions = goroutines * iterations
	cfg.ComparisonWorkers = 4
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = engine.Shutdown(ctx)
	})

	rule, err := shadowmw.NewRule(
		"concurrent.middleware",
		shadowmw.ResponseParity(),
		shadowmw.MatchPathPrefix("/sampled"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middlewareConfig.Customizations.Both.BodyTransforms = []shadowmw.BodyTransform{
		func(_ context.Context, body []byte) ([]byte, error) {
			return append([]byte(nil), body...), nil
		},
	}

	var authoritativeCalls, shadowCalls atomic.Int64
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := shadowmw.BranchFromContext(r.Context()); got != shadowmw.BranchAuthoritative {
			t.Errorf("authoritative BranchFromContext = %v, want %v", got, shadowmw.BranchAuthoritative)
		}
		authoritativeCalls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("authoritative"))
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shadowmw.IsShadow(r.Context()) {
			shadowCalls.Add(1)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("authoritative"))
	})
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, middlewareConfig, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	handler := middleware.Wrap(replacement)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				req := httptest.NewRequest(http.MethodPost, "/sampled/items", bytes.NewBufferString("payload"))
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK || rec.Body.String() != "authoritative" {
					t.Errorf("sampled response = %d/%q, want 200/authoritative", rec.Code, rec.Body.String())
					return
				}
			}
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if got := authoritativeCalls.Load(); got != goroutines*iterations {
		t.Fatalf("authoritativeCalls = %d, want %d", got, goroutines*iterations)
	}
	if got := shadowCalls.Load(); got != goroutines*iterations {
		t.Fatalf("shadowCalls = %d, want %d", got, goroutines*iterations)
	}
}

type acceptanceObserver func(context.Context, shadowmw.Outcome) error

func (f acceptanceObserver) Observe(ctx context.Context, outcome shadowmw.Outcome) error {
	return f(ctx, outcome)
}

func newAcceptanceEngine(
	t *testing.T,
	maxInFlight int,
	outcomes chan<- shadowmw.Outcome,
) *shadowmw.Engine {
	t.Helper()
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.MaxInFlightSessions = maxInFlight
	if outcomes != nil {
		cfg.Observer = &shadowmw.ObserverDelivery{
			Observer: acceptanceObserver(func(_ context.Context, outcome shadowmw.Outcome) error {
				outcomes <- outcome
				return nil
			}),
			QueueCapacity:  maxInFlight + 8,
			MaxQueuedBytes: 8 << 20,
			Workers:        1,
			Timeout:        time.Second,
			Filter: shadowmw.ObserveMatches |
				shadowmw.ObserveMismatches |
				shadowmw.ObserveSkipped,
			Detail: shadowmw.OutcomeFull,
		}
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
	return engine
}

func newAcceptanceRule(t *testing.T) *shadowmw.Rule {
	t.Helper()
	rule, err := shadowmw.NewRule(
		"middleware.acceptance",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	return rule
}

func newAcceptanceMiddleware(
	t *testing.T,
	engine *shadowmw.Engine,
	authoritative http.Handler,
	cfg shadowmw.MiddlewareConfig,
) *shadowmw.Middleware {
	t.Helper()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		authoritative,
		cfg,
		newAcceptanceRule(t),
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	return middleware
}

func waitForOutcome(t *testing.T, outcomes <-chan shadowmw.Outcome) shadowmw.Outcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for comparison outcome")
		return shadowmw.Outcome{}
	}
}

func TestDefaultMiddlewareConfigUsesOneMiBCaptureBoundsAndAllowsPositiveOverrides(t *testing.T) {
	cfg := shadowmw.DefaultMiddlewareConfig()
	if cfg.MaxRequestBodyBytes != 1<<20 {
		t.Errorf("default request limit = %d, want 1 MiB", cfg.MaxRequestBodyBytes)
	}
	if cfg.MaxResponseBodyBytes != 1<<20 {
		t.Errorf("default response limit = %d, want 1 MiB", cfg.MaxResponseBodyBytes)
	}

	engine := newAcceptanceEngine(t, 1, nil)
	cfg.MaxRequestBodyBytes = 17
	cfg.MaxResponseBodyBytes = 23
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	if _, err := shadowmw.NewMiddleware(
		engine,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		cfg,
		newAcceptanceRule(t),
	); err != nil {
		t.Fatalf("NewMiddleware() with positive capture overrides error = %v", err)
	}
}

func TestMiddlewareLeavesUnmatchedRequestsWithoutBranchIdentity(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)
	rule, err := shadowmw.NewRule(
		"matched.only",
		shadowmw.ResponseParity(),
		shadowmw.MatchPath("/matched"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
		cfg,
		rule,
	)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	var gotBranch shadowmw.Branch
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		gotBranch = shadowmw.BranchFromContext(request.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodGet, "/unmatched", nil)
	request.Header.Set("X-Shadowmw-Branch", "shadow")

	middleware.Wrap(replacement).ServeHTTP(httptest.NewRecorder(), request)

	if gotBranch != shadowmw.BranchUnknown {
		t.Errorf("unmatched branch = %v, want unknown", gotBranch)
	}
}

type trackedBody struct {
	reader *strings.Reader
	read   atomic.Int64
	closed atomic.Bool
}

func newTrackedBody(body string) *trackedBody {
	return &trackedBody{reader: strings.NewReader(body)}
}

func (b *trackedBody) Read(destination []byte) (int, error) {
	n, err := b.reader.Read(destination)
	b.read.Add(int64(n))
	return n, err
}

func (b *trackedBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestMiddlewareReconstructsUnknownLengthOversizedBodyAndPreservesClose(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)

	body := newTrackedBody("abcdef")
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := body.read.Load(); got != 5 {
			t.Errorf("bytes read before authoritative handler = %d, want limit+1 (5)", got)
		}
		got, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Fatalf("ReadAll() error = %v", readErr)
		}
		if string(got) != "abcdef" {
			t.Errorf("authoritative body = %q, want %q", got, "abcdef")
		}
		if closeErr := request.Body.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	var shadowCalls atomic.Int32
	replacement := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		shadowCalls.Add(1)
	})

	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.MaxRequestBodyBytes = 4
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware := newAcceptanceMiddleware(t, engine, authoritative, middlewareConfig)

	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = body
	request.ContentLength = -1
	recorder := httptest.NewRecorder()
	middleware.Wrap(replacement).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if got := shadowCalls.Load(); got != 0 {
		t.Fatalf("shadow calls = %d, want 0", got)
	}
	if !body.closed.Load() {
		t.Fatal("closing reconstructed authoritative body did not close original body")
	}
}

func TestMiddlewareBypassesDeclaredOversizedBodyWithoutReadingAndReleasesCapacity(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 4)
	engine := newAcceptanceEngine(t, 1, outcomes)
	type contextKey string
	const key contextKey = "original"

	largeBody := newTrackedBody("abcdef")
	originalURL := &url.URL{Path: "/large", RawQuery: "preserved=yes"}
	authoritativeCalls := atomic.Int32{}
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		call := authoritativeCalls.Add(1)
		if call == 1 {
			if request.Body != largeBody {
				t.Error("declared oversized request did not preserve original body")
			}
			if got := largeBody.read.Load(); got != 0 {
				t.Errorf("declared oversized body was pre-read by %d bytes", got)
			}
			if request.URL != originalURL {
				t.Error("declared oversized request did not preserve original URL")
			}
			if got := request.Context().Value(key); got != "value" {
				t.Errorf("context value = %v, want value", got)
			}
		}
		_, _ = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	})
	shadowCalled := make(chan struct{}, 1)
	replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		shadowCalled <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	})
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.MaxRequestBodyBytes = 4
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, middlewareConfig).Wrap(replacement)

	largeRequest := httptest.NewRequest(http.MethodPost, "/large", nil)
	largeRequest.Body = largeBody
	largeRequest.ContentLength = 6
	largeRequest.URL = originalURL
	largeRequest = largeRequest.WithContext(context.WithValue(largeRequest.Context(), key, "value"))
	handler.ServeHTTP(httptest.NewRecorder(), largeRequest)

	skip := waitForOutcome(t, outcomes)
	if skip.Classification() != shadowmw.OutcomeSkipped || skip.Reason() != "request_too_large" {
		t.Fatalf("oversized outcome = %s/%q, want skipped/request_too_large", skip.Classification(), skip.Reason())
	}

	smallRequest := httptest.NewRequest(http.MethodPost, "/small", strings.NewReader("ok"))
	handler.ServeHTTP(httptest.NewRecorder(), smallRequest)
	select {
	case <-shadowCalled:
	case <-time.After(time.Second):
		t.Fatal("capacity was not released after declared oversized bypass")
	}
}

func TestMiddlewareClosesCapturedSourceAndGivesBranchesIndependentBodies(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)
	source := newTrackedBody("payload")
	type branchResult struct {
		branch        shadowmw.Branch
		body          string
		getBody       string
		contentLength int64
		closeErr      error
	}
	results := make(chan branchResult, 2)
	readBranch := func(request *http.Request) branchResult {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			return branchResult{branch: shadowmw.BranchFromContext(request.Context()), closeErr: readErr}
		}
		closeErr := request.Body.Close()
		second, getErr := request.GetBody()
		if getErr != nil {
			return branchResult{branch: shadowmw.BranchFromContext(request.Context()), closeErr: getErr}
		}
		secondBody, secondErr := io.ReadAll(second)
		if secondErr == nil {
			secondErr = second.Close()
		}
		if closeErr == nil {
			closeErr = secondErr
		}
		return branchResult{
			branch:        shadowmw.BranchFromContext(request.Context()),
			body:          string(body),
			getBody:       string(secondBody),
			contentLength: request.ContentLength,
			closeErr:      closeErr,
		}
	}
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		results <- readBranch(request)
		w.WriteHeader(http.StatusNoContent)
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		results <- readBranch(request)
		w.WriteHeader(http.StatusNoContent)
	})
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, middlewareConfig).Wrap(replacement)

	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = source
	request.ContentLength = -1
	handler.ServeHTTP(httptest.NewRecorder(), request)

	seen := make(map[shadowmw.Branch]bool)
	for range 2 {
		select {
		case result := <-results:
			if result.closeErr != nil {
				t.Errorf("%v branch body error = %v", result.branch, result.closeErr)
			}
			if result.body != "payload" || result.getBody != "payload" {
				t.Errorf("%v branch bodies = %q/%q, want payload/payload", result.branch, result.body, result.getBody)
			}
			if result.contentLength != 7 {
				t.Errorf("%v branch content length = %d, want 7", result.branch, result.contentLength)
			}
			seen[result.branch] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for branch body")
		}
	}
	if !seen[shadowmw.BranchAuthoritative] || !seen[shadowmw.BranchShadow] {
		t.Fatalf("branch identities = %v, want authoritative and shadow", seen)
	}
	if !source.closed.Load() {
		t.Fatal("successfully captured source body was not closed")
	}
}

type simultaneousErrorBody struct {
	closed atomic.Bool
	err    error
}

func (b *simultaneousErrorBody) Read(destination []byte) (int, error) {
	return copy(destination, "partial"), b.err
}

func (b *simultaneousErrorBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestMiddlewareBodyReadErrorPolicies(t *testing.T) {
	t.Run("construction requires explicit policy", func(t *testing.T) {
		engine := newAcceptanceEngine(t, 1, nil)
		cfg := shadowmw.DefaultMiddlewareConfig()
		if _, err := shadowmw.NewMiddleware(
			engine,
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
			cfg,
			newAcceptanceRule(t),
		); err == nil {
			t.Fatal("NewMiddleware() error = nil, want explicit body-read policy error")
		}
	})

	for _, test := range []struct {
		name       string
		policy     func(http.Handler) shadowmw.BodyReadErrorPolicy
		wantStatus int
		wantHandle bool
	}{
		{
			name:       "reject",
			policy:     func(http.Handler) shadowmw.BodyReadErrorPolicy { return shadowmw.RejectBodyReadErrors() },
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "custom handler",
			policy: func(handler http.Handler) shadowmw.BodyReadErrorPolicy {
				return shadowmw.HandleBodyReadErrors(handler)
			},
			wantStatus: http.StatusUnprocessableEntity,
			wantHandle: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 2)
			engine := newAcceptanceEngine(t, 1, outcomes)
			var authoritativeCalls atomic.Int32
			var shadowCalls atomic.Int32
			var handlerCalls atomic.Int32
			customHandler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				handlerCalls.Add(1)
				_ = request.Body.Close()
				w.WriteHeader(http.StatusUnprocessableEntity)
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = test.policy(customHandler)
			middleware := newAcceptanceMiddleware(t, engine, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				authoritativeCalls.Add(1)
			}), cfg)
			body := &simultaneousErrorBody{err: errors.New("read failed")}
			request := httptest.NewRequest(http.MethodPost, "/", nil)
			request.Body = body
			request.ContentLength = -1
			recorder := httptest.NewRecorder()
			middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				shadowCalls.Add(1)
			})).ServeHTTP(recorder, request)

			if recorder.Code != test.wantStatus {
				t.Errorf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			if authoritativeCalls.Load() != 0 || shadowCalls.Load() != 0 {
				t.Errorf("branch calls = authoritative %d, shadow %d; want zero", authoritativeCalls.Load(), shadowCalls.Load())
			}
			if got := handlerCalls.Load() > 0; got != test.wantHandle {
				t.Errorf("custom handler called = %v, want %v", got, test.wantHandle)
			}
			skip := waitForOutcome(t, outcomes)
			if skip.Classification() != shadowmw.OutcomeSkipped || skip.Reason() != "body_read_error" {
				t.Errorf("outcome = %s/%q, want skipped/body_read_error", skip.Classification(), skip.Reason())
			}
		})
	}
}

func TestMiddlewarePassThroughReplaysConsumedBytesAndOriginalErrorThenClosesSource(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 4)
	engine := newAcceptanceEngine(t, 1, outcomes)
	wantErr := errors.New("request stream failed")
	source := &simultaneousErrorBody{err: wantErr}
	var authoritativeCalls atomic.Int32
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		call := authoritativeCalls.Add(1)
		body, err := io.ReadAll(request.Body)
		if call == 1 {
			if string(body) != "partial" {
				t.Errorf("replayed body = %q, want partial", body)
			}
			if !errors.Is(err, wantErr) {
				t.Errorf("replayed error = %v, want %v", err, wantErr)
			}
		}
		if err := request.Body.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
		w.WriteHeader(http.StatusBadRequest)
	})
	shadowCalled := make(chan struct{}, 1)
	replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		shadowCalled <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.PassBodyReadErrorsToAuthoritative()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = source
	request.ContentLength = -1

	handler.ServeHTTP(httptest.NewRecorder(), request)

	if authoritativeCalls.Load() != 1 {
		t.Errorf("authoritative calls = %d, want 1", authoritativeCalls.Load())
	}
	if !source.closed.Load() {
		t.Fatal("closing replay body did not close original source")
	}
	skip := waitForOutcome(t, outcomes)
	if skip.Classification() != shadowmw.OutcomeSkipped || skip.Reason() != "body_read_error" {
		t.Errorf("outcome = %s/%q, want skipped/body_read_error", skip.Classification(), skip.Reason())
	}

	handler.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/", strings.NewReader("ok")),
	)
	select {
	case <-shadowCalled:
	case <-time.After(time.Second):
		t.Fatal("capacity was not released after body read error")
	}
}

type partialResponseWriter struct {
	header http.Header
	status int
	body   strings.Builder
	n      int
	err    error
}

func (w *partialResponseWriter) Header() http.Header {
	return w.header
}

func (w *partialResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *partialResponseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n := min(w.n, len(body))
	_, _ = w.body.Write(body[:n])
	return n, w.err
}

func TestMiddlewarePreservesAuthoritativePartialWriteResultAndReportsTransportMismatch(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	engine := newAcceptanceEngine(t, 1, outcomes)
	writeErr := errors.New("client disconnected")
	type writeResult struct {
		n   int
		err error
	}
	result := make(chan writeResult, 1)
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n, err := w.Write([]byte("abcdef"))
		result <- writeResult{n: n, err: err}
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("abc"))
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
	client := &partialResponseWriter{
		header: make(http.Header),
		n:      3,
		err:    writeErr,
	}

	handler.ServeHTTP(client, httptest.NewRequest(http.MethodGet, "/", nil))

	gotResult := <-result
	if gotResult.n != 3 || !errors.Is(gotResult.err, writeErr) {
		t.Errorf("authoritative Write() = (%d, %v), want (3, %v)", gotResult.n, gotResult.err, writeErr)
	}
	if got := client.body.String(); got != "abc" {
		t.Errorf("client body = %q, want abc", got)
	}
	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeMismatch ||
		outcome.Reason() != shadowmw.ReasonAuthoritativeTransportError {
		t.Fatalf("outcome = %s/%q, want mismatch/authoritative_transport_error; authoritative capture %q",
			outcome.Classification(),
			outcome.Reason(),
			outcome.AuthoritativeResponse().Body(),
		)
	}
	if got := string(outcome.AuthoritativeResponse().Body()); got != "abc" {
		t.Errorf("authoritative captured body = %q, want abc", got)
	}
}

func TestMiddlewareCapturesFirstCommitHeadersAndTrailers(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	engine := newAcceptanceEngine(t, 1, outcomes)
	response := func(prefixed bool, mutateLate bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Committed", "first")
			w.Header().Add("Trailer", "X-Declared")
			if prefixed {
				w.Header().Set(http.TrailerPrefix+"X-Prefixed", "prefixed")
			} else {
				w.Header().Add("Trailer", "X-Prefixed")
			}
			w.WriteHeader(http.StatusCreated)
			if mutateLate {
				w.Header().Set("X-Committed", "late")
				w.Header().Set("X-Unsent", "late")
			}
			w.Header().Set("X-Declared", "declared")
			w.Header().Set(http.TrailerPrefix+"X-Prefixed", "prefixed")
			_, _ = w.Write([]byte("response"))
		})
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, response(true, true), cfg).
		Wrap(response(false, false))

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	outcome := waitForOutcome(t, outcomes)

	if outcome.Classification() != shadowmw.OutcomeMatch {
		t.Fatalf("outcome = %s/%q, want match; differences = %+v",
			outcome.Classification(),
			outcome.Reason(),
			outcome.Differences(),
		)
	}
	snapshot := outcome.AuthoritativeResponse()
	if got := snapshot.Header().Get("X-Committed"); got != "first" {
		t.Errorf("committed X-Committed = %q, want first", got)
	}
	if got := snapshot.Header().Get("X-Unsent"); got != "" {
		t.Errorf("late X-Unsent = %q, want absent", got)
	}
	if got := snapshot.Trailer().Get("X-Declared"); got != "declared" {
		t.Errorf("declared trailer = %q, want declared", got)
	}
	if got := snapshot.Trailer().Get("X-Prefixed"); got != "prefixed" {
		t.Errorf("prefixed trailer = %q, want prefixed", got)
	}
	header := snapshot.Header()
	header.Set("X-Committed", "mutated")
	body := snapshot.Body()
	body[0] = 'X'
	if got := outcome.AuthoritativeResponse().Header().Get("X-Committed"); got != "first" {
		t.Errorf("snapshot header was not detached: %q", got)
	}
	if got := string(outcome.AuthoritativeResponse().Body()); got != "response" {
		t.Errorf("snapshot body was not detached: %q", got)
	}
}

func TestMiddlewareFlushCommitsAndReachesClient(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	engine := newAcceptanceEngine(t, 1, outcomes)
	flushed := make(chan bool, 1)
	handlerFunc := func(record bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Committed", "first")
			flusher, ok := w.(http.Flusher)
			if !ok {
				t.Error("response writer does not expose http.Flusher")
				return
			}
			flusher.Flush()
			w.Header().Set("X-Committed", "late")
			if record {
				flushed <- true
			}
		})
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, handlerFunc(true), cfg).
		Wrap(handlerFunc(false))
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if !recorder.Flushed {
		t.Fatal("authoritative Flush() did not reach client writer")
	}
	if recorder.Code != http.StatusOK {
		t.Errorf("flush status = %d, want 200", recorder.Code)
	}
	<-flushed
	outcome := waitForOutcome(t, outcomes)
	if got := outcome.AuthoritativeResponse().Header().Get("X-Committed"); got != "first" {
		t.Errorf("header committed by flush = %q, want first", got)
	}
}

func TestMiddlewareTruncationStreamsFullClientBodyAndProducesIncompleteMismatch(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 2)
	engine := newAcceptanceEngine(t, 1, outcomes)
	response := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("abcdefgh"))
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.MaxResponseBodyBytes = 4
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, response, cfg).Wrap(response)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := recorder.Body.String(); got != "abcdefgh" {
		t.Errorf("client body = %q, want full authoritative body", got)
	}
	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeMismatch {
		t.Fatalf("outcome = %s, want mismatch", outcome.Classification())
	}
	if !outcome.AuthoritativeResponse().Truncated() || !outcome.ShadowResponse().Truncated() {
		t.Errorf("truncation flags = authoritative %v, shadow %v; want both true",
			outcome.AuthoritativeResponse().Truncated(),
			outcome.ShadowResponse().Truncated(),
		)
	}
	if got := string(outcome.AuthoritativeResponse().Body()); got != "abcd" {
		t.Errorf("captured authoritative body = %q, want abcd", got)
	}
	differences := outcome.Differences()
	if len(differences) == 0 || differences[len(differences)-1].Kind != "comparison_incomplete" {
		t.Errorf("differences = %+v, want comparison_incomplete", differences)
	}
}

func TestMiddlewareClassifiesUnsupportedExchangesAndReleasesCapacity(t *testing.T) {
	for _, test := range []struct {
		name    string
		request func() *http.Request
	}{
		{
			name: "websocket upgrade",
			request: func() *http.Request {
				request := httptest.NewRequest(http.MethodGet, "/", nil)
				request.Header.Set("Connection", "keep-alive, Upgrade")
				request.Header.Set("Upgrade", "websocket")
				return request
			},
		},
		{
			name: "event stream request",
			request: func() *http.Request {
				request := httptest.NewRequest(http.MethodGet, "/", nil)
				request.Header.Set("Accept", "text/event-stream; charset=utf-8")
				return request
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 4)
			engine := newAcceptanceEngine(t, 1, outcomes)
			var shadowCalls atomic.Int32
			authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			replacement := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				shadowCalls.Add(1)
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)

			handler.ServeHTTP(httptest.NewRecorder(), test.request())

			if shadowCalls.Load() != 0 {
				t.Fatalf("shadow calls = %d, want 0", shadowCalls.Load())
			}
			outcome := waitForOutcome(t, outcomes)
			if outcome.Classification() != shadowmw.OutcomeSkipped || outcome.Reason() != "unsupported_exchange" {
				t.Errorf("outcome = %s/%q, want skipped/unsupported_exchange", outcome.Classification(), outcome.Reason())
			}
		})
	}

	t.Run("event stream response", func(t *testing.T) {
		outcomes := make(chan shadowmw.Outcome, 4)
		engine := newAcceptanceEngine(t, 1, outcomes)
		var requestNumber atomic.Int32
		shadowCalled := make(chan struct{}, 1)
		authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if requestNumber.Add(1) == 1 {
				w.Header().Set("Content-Type", "text/event-stream")
			}
			w.WriteHeader(http.StatusNoContent)
		})
		replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			shadowCalled <- struct{}{}
			w.WriteHeader(http.StatusNoContent)
		})
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)

		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		outcome := waitForOutcome(t, outcomes)
		if outcome.Classification() != shadowmw.OutcomeSkipped || outcome.Reason() != "unsupported_exchange" {
			t.Errorf("outcome = %s/%q, want skipped/unsupported_exchange", outcome.Classification(), outcome.Reason())
		}

		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		select {
		case <-shadowCalled:
		case <-time.After(time.Second):
			t.Fatal("capacity was not released after unsupported response")
		}
	})
}

func TestMiddlewareSetupFailuresAndPanicsHaveStableOutcomes(t *testing.T) {
	t.Run("authoritative setup failure returns bad gateway", func(t *testing.T) {
		outcomes := make(chan shadowmw.Outcome, 2)
		engine := newAcceptanceEngine(t, 1, outcomes)
		var authoritativeCalls atomic.Int32
		var shadowCalls atomic.Int32
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		cfg.Customizations.Authoritative.BodyTransforms = []shadowmw.BodyTransform{
			func(context.Context, []byte) ([]byte, error) {
				return nil, errors.New("cannot prepare authoritative request")
			},
		}
		middleware := newAcceptanceMiddleware(t, engine, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			authoritativeCalls.Add(1)
		}), cfg)
		recorder := httptest.NewRecorder()

		middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			shadowCalls.Add(1)
		})).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body")))

		if recorder.Code != http.StatusBadGateway {
			t.Errorf("status = %d, want 502", recorder.Code)
		}
		if authoritativeCalls.Load() != 0 || shadowCalls.Load() != 0 {
			t.Errorf("branch calls = authoritative %d, shadow %d; want zero", authoritativeCalls.Load(), shadowCalls.Load())
		}
		outcome := waitForOutcome(t, outcomes)
		if outcome.Classification() != shadowmw.OutcomeMismatch || outcome.Reason() != "authoritative_setup_error" {
			t.Errorf("outcome = %s/%q, want mismatch/authoritative_setup_error", outcome.Classification(), outcome.Reason())
		}
	})

	t.Run("shadow setup failure preserves authoritative response", func(t *testing.T) {
		outcomes := make(chan shadowmw.Outcome, 2)
		engine := newAcceptanceEngine(t, 1, outcomes)
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
			func(*http.Request) (*http.Request, error) {
				panic("shadow hook defect")
			},
		}
		authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("authoritative"))
		})
		var shadowCalls atomic.Int32
		recorder := httptest.NewRecorder()

		newAcceptanceMiddleware(t, engine, authoritative, cfg).
			Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				shadowCalls.Add(1)
			})).
			ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

		if recorder.Code != http.StatusAccepted || recorder.Body.String() != "authoritative" {
			t.Errorf("client response = %d/%q, want 202/authoritative", recorder.Code, recorder.Body.String())
		}
		if shadowCalls.Load() != 0 {
			t.Errorf("shadow calls = %d, want zero", shadowCalls.Load())
		}
		outcome := waitForOutcome(t, outcomes)
		if outcome.Classification() != shadowmw.OutcomeMismatch || outcome.Reason() != "shadow_setup_error" {
			t.Fatalf("outcome = %s/%q, want mismatch/shadow_setup_error", outcome.Classification(), outcome.Reason())
		}
		if outcome.AuthoritativeStatusCode() != http.StatusAccepted ||
			string(outcome.AuthoritativeResponse().Body()) != "authoritative" {
			t.Errorf("retained authoritative snapshot = %d/%q, want 202/authoritative",
				outcome.AuthoritativeStatusCode(),
				outcome.AuthoritativeResponse().Body(),
			)
		}
	})

	t.Run("shadow panic is recovered", func(t *testing.T) {
		outcomes := make(chan shadowmw.Outcome, 2)
		engine := newAcceptanceEngine(t, 1, outcomes)
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})

		newAcceptanceMiddleware(t, engine, authoritative, cfg).
			Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				panic("shadow defect")
			})).
			ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

		outcome := waitForOutcome(t, outcomes)
		if outcome.Classification() != shadowmw.OutcomeMismatch || outcome.Reason() != "panic" {
			t.Errorf("outcome = %s/%q, want mismatch/panic", outcome.Classification(), outcome.Reason())
		}
		if outcome.AuthoritativeStatusCode() != http.StatusNoContent {
			t.Errorf("retained authoritative status = %d, want 204", outcome.AuthoritativeStatusCode())
		}
	})

	t.Run("authoritative panic is recorded and rethrown", func(t *testing.T) {
		outcomes := make(chan shadowmw.Outcome, 2)
		engine := newAcceptanceEngine(t, 1, outcomes)
		shadowDone := make(chan struct{})
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		handler := newAcceptanceMiddleware(t, engine, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			<-shadowDone
			panic("authoritative defect")
		}), cfg).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("shadow-complete"))
			close(shadowDone)
		}))

		var recovered any
		func() {
			defer func() {
				recovered = recover()
			}()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		}()

		if recovered != "authoritative defect" {
			t.Fatalf("recovered panic = %v, want authoritative defect", recovered)
		}
		outcome := waitForOutcome(t, outcomes)
		if outcome.Classification() != shadowmw.OutcomeMismatch || outcome.Reason() != "authoritative_panic" {
			t.Fatalf("outcome = %s/%q, want mismatch/authoritative_panic", outcome.Classification(), outcome.Reason())
		}
		if outcome.ShadowStatusCode() != http.StatusCreated ||
			string(outcome.ShadowResponse().Body()) != "shadow-complete" {
			t.Errorf("retained shadow snapshot = %d/%q, want 201/shadow-complete",
				outcome.ShadowStatusCode(),
				outcome.ShadowResponse().Body(),
			)
		}
	})
}

func TestMiddlewareValidationRejectsNilCustomizationCallbacks(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)
	for _, test := range []struct {
		name string
		cfg  func() shadowmw.MiddlewareConfig
	}{
		{
			name: "nil transform",
			cfg: func() shadowmw.MiddlewareConfig {
				cfg := shadowmw.DefaultMiddlewareConfig()
				cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
				cfg.Customizations.Both.BodyTransforms = []shadowmw.BodyTransform{nil}
				return cfg
			},
		},
		{
			name: "nil hook",
			cfg: func() shadowmw.MiddlewareConfig {
				cfg := shadowmw.DefaultMiddlewareConfig()
				cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
				cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{nil}
				return cfg
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := shadowmw.NewMiddleware(
				engine,
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
				test.cfg(),
				newAcceptanceRule(t),
			)
			if err == nil {
				t.Fatal("NewMiddleware() error = nil, want callback validation error")
			}
		})
	}
}

func TestMiddlewareHooksCannotReplacePreparedBodyOrBranchIdentity(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)
	results := make(chan string, 2)
	handler := func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			results <- "read error"
			return
		}
		results <- string(body) + ":" + string(rune('0'+shadowmw.BranchFromContext(request.Context())))
		w.WriteHeader(http.StatusNoContent)
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	cfg.Customizations.Both.BodyTransforms = []shadowmw.BodyTransform{
		func(_ context.Context, body []byte) ([]byte, error) {
			return append(body, "-prepared"...), nil
		},
	}
	cfg.Customizations.Both.RequestHooks = []shadowmw.RequestHook{
		func(request *http.Request) (*http.Request, error) {
			replacement := request.Clone(context.Background())
			replacement.Body = io.NopCloser(strings.NewReader("forged"))
			replacement.GetBody = nil
			replacement.ContentLength = 6
			return replacement, nil
		},
	}
	middleware := newAcceptanceMiddleware(t, engine, http.HandlerFunc(handler), cfg)

	middleware.Wrap(http.HandlerFunc(handler)).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body")))

	seen := make(map[string]bool)
	for range 2 {
		select {
		case result := <-results:
			seen[result] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for customized branch")
		}
	}
	if !seen["body-prepared:1"] || !seen["body-prepared:2"] {
		t.Errorf("branch results = %v, want protected prepared bodies and identities", seen)
	}
}

func TestMiddlewareAuthoritativeCompletionDoesNotWaitForShadow(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)
	shadowStarted := make(chan struct{})
	releaseShadow := make(chan struct{})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), cfg).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(shadowStarted)
		<-releaseShadow
		w.WriteHeader(http.StatusNoContent)
	}))
	returned := make(chan struct{})

	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		close(returned)
	}()

	select {
	case <-shadowStarted:
	case <-time.After(time.Second):
		t.Fatal("shadow handler did not start")
	}
	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("authoritative ServeHTTP waited for shadow completion")
	}
	close(releaseShadow)
}

func TestMiddlewareKeepsBranchRequestsAndOriginalRequestIsolated(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)
	type result struct {
		branch      shadowmw.Branch
		initialHost string
		finalHost   string
		header      string
	}
	results := make(chan result, 2)
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	run := func(host, header string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			initialHost := request.URL.Host
			request.URL.Host = host
			request.Header.Set("X-Branch-Mutation", header)
			if shadowmw.BranchFromContext(request.Context()) == shadowmw.BranchAuthoritative {
				w.WriteHeader(http.StatusNoContent)
			}
			arrived <- struct{}{}
			<-release
			results <- result{
				branch:      shadowmw.BranchFromContext(request.Context()),
				initialHost: initialHost,
				finalHost:   request.URL.Host,
				header:      request.Header.Get("X-Branch-Mutation"),
			}
			w.WriteHeader(http.StatusNoContent)
		})
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, run("authoritative.example", "authoritative"), cfg).
		Wrap(run("shadow.example", "shadow"))
	request := httptest.NewRequest(http.MethodPost, "http://original.example/", strings.NewReader("body"))
	request.Header.Set("X-Original", "preserved")
	returned := make(chan struct{})

	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), request)
		close(returned)
	}()
	<-arrived
	<-arrived
	close(release)
	<-returned

	seen := make(map[shadowmw.Branch]result)
	for range 2 {
		got := <-results
		seen[got.branch] = got
	}
	if got := seen[shadowmw.BranchAuthoritative]; got.initialHost != "original.example" ||
		got.finalHost != "authoritative.example" || got.header != "authoritative" {
		t.Errorf("authoritative request = %+v", got)
	}
	if got := seen[shadowmw.BranchShadow]; got.initialHost != "original.example" ||
		got.finalHost != "shadow.example" || got.header != "shadow" {
		t.Errorf("shadow request = %+v", got)
	}
	if request.URL.Host != "original.example" || request.Header.Get("X-Branch-Mutation") != "" {
		t.Errorf("original request was mutated: host %q, branch header %q",
			request.URL.Host,
			request.Header.Get("X-Branch-Mutation"),
		)
	}
}

type ambiguousLogHandler struct {
	started chan struct{}
	release <-chan struct{}
	panic   bool
	once    sync.Once
}

func (*ambiguousLogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *ambiguousLogHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message != "multiple shadow comparison rules matched; first rule selected" {
		return nil
	}
	h.once.Do(func() {
		if h.started != nil {
			close(h.started)
		}
	})
	if h.panic {
		panic("ambiguous log panic")
	}
	if h.release != nil {
		<-h.release
	}
	return nil
}

func (h *ambiguousLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *ambiguousLogHandler) WithGroup(string) slog.Handler      { return h }

func TestMiddlewareAmbiguousMatchLoggingCannotDisruptAuthoritativeTraffic(t *testing.T) {
	tests := []struct {
		name  string
		panic bool
	}{
		{name: "slow logger"},
		{name: "panicking logger", panic: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseLogger := func() {
				releaseOnce.Do(func() {
					close(release)
				})
			}
			defer releaseLogger()
			loggerHandler := &ambiguousLogHandler{
				started: started,
				release: release,
				panic:   test.panic,
			}
			engineConfig := shadowmw.DefaultEngineConfig()
			engineConfig.ComparisonWorkers = 1
			engine, err := shadowmw.NewEngine(
				engineConfig,
				shadowmw.WithLogger(slog.New(loggerHandler)),
			)
			if err != nil {
				t.Fatalf("NewEngine() error = %v", err)
			}
			if err := engine.Start(context.Background()); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				releaseLogger()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := engine.Shutdown(ctx); err != nil {
					t.Errorf("Shutdown() error = %v", err)
				}
			})

			newRule := func(name string) *shadowmw.Rule {
				t.Helper()
				rule, ruleErr := shadowmw.NewRule(
					name,
					shadowmw.ResponseParity(),
					shadowmw.MatchAll(),
					shadowmw.SampleNone(),
				)
				if ruleErr != nil {
					t.Fatalf("NewRule(%q) error = %v", name, ruleErr)
				}
				return rule
			}
			var authoritativeCalls atomic.Int32
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			middleware, err := shadowmw.NewMiddleware(
				engine,
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					authoritativeCalls.Add(1)
					w.WriteHeader(http.StatusNoContent)
				}),
				cfg,
				newRule("ambiguous.first"),
				newRule("ambiguous.second"),
			)
			if err != nil {
				t.Fatalf("NewMiddleware() error = %v", err)
			}

			returned := make(chan any, 1)
			go func() {
				var recovered any
				func() {
					defer func() {
						recovered = recover()
					}()
					middleware.Wrap(http.NotFoundHandler()).ServeHTTP(
						httptest.NewRecorder(),
						httptest.NewRequest(http.MethodGet, "/", nil),
					)
				}()
				returned <- recovered
			}()

			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("ambiguous-match log was not attempted")
			}
			select {
			case recovered := <-returned:
				if recovered != nil {
					t.Fatalf("middleware propagated logger panic: %v", recovered)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("ambiguous-match logging delayed authoritative traffic")
			}
			if authoritativeCalls.Load() != 1 {
				t.Errorf("authoritative calls = %d, want 1", authoritativeCalls.Load())
			}
		})
	}
}

func TestMiddlewareEngineUnavailableEmitsSkippedOutcomeAndServesAuthoritative(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 1)
	engineConfig := shadowmw.DefaultEngineConfig()
	engineConfig.ComparisonWorkers = 1
	engineConfig.Observer = &shadowmw.ObserverDelivery{
		Observer: acceptanceObserver(func(_ context.Context, outcome shadowmw.Outcome) error {
			outcomes <- outcome
			return nil
		}),
		QueueCapacity:  1,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter:         shadowmw.ObserveSkipped,
		Detail:         shadowmw.OutcomeMetadataOnly,
	}
	engine, err := shadowmw.NewEngine(engineConfig)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	var authoritativeCalls atomic.Int32
	var shadowCalls atomic.Int32
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware := newAcceptanceMiddleware(
		t,
		engine,
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			authoritativeCalls.Add(1)
			w.WriteHeader(http.StatusAccepted)
		}),
		cfg,
	)
	recorder := httptest.NewRecorder()

	middleware.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		shadowCalls.Add(1)
	})).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", recorder.Code)
	}
	if authoritativeCalls.Load() != 1 || shadowCalls.Load() != 0 {
		t.Errorf("branch calls = authoritative %d, shadow %d; want 1/0",
			authoritativeCalls.Load(),
			shadowCalls.Load(),
		)
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
	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeSkipped ||
		outcome.Reason() != shadowmw.ReasonEngineUnavailable {
		t.Errorf("outcome = %s/%q, want skipped/engine_unavailable",
			outcome.Classification(),
			outcome.Reason(),
		)
	}
}

func TestMiddlewareDetectsEventStreamInAcceptMediaRangeLists(t *testing.T) {
	tests := []struct {
		name        string
		accept      []string
		wantSkipped bool
	}{
		{
			name:        "after another media range",
			accept:      []string{"application/json, text/event-stream"},
			wantSkipped: true,
		},
		{
			name:        "with parameters and quality",
			accept:      []string{"text/plain; q=0.5, text/event-stream; q=0.9, application/json"},
			wantSkipped: true,
		},
		{
			name:   "quality zero is not accepted",
			accept: []string{"application/json, text/event-stream; q=0"},
		},
		{
			name:   "quality zero with parameters across header lines is not accepted",
			accept: []string{"text/event-stream; charset=utf-8; q=0", "application/json"},
		},
		{
			name:   "quality zero with empty fractional part is not accepted",
			accept: []string{"text/event-stream; q=0."},
		},
		{
			name:        "accepted range follows quality zero range",
			accept:      []string{"text/event-stream; q=0, application/json", "text/event-stream; q=0.2"},
			wantSkipped: true,
		},
		{
			name:        "minimum positive quality is accepted",
			accept:      []string{"text/event-stream; q=0.001"},
			wantSkipped: true,
		},
		{
			name:        "quality parameter name is case insensitive",
			accept:      []string{"text/event-stream; Q=0.5"},
			wantSkipped: true,
		},
		{
			name:        "maximum quality with three zeroes is accepted",
			accept:      []string{"text/event-stream; q=1.000"},
			wantSkipped: true,
		},
		{
			name:        "maximum quality with empty fractional part is accepted",
			accept:      []string{"text/event-stream; q=1."},
			wantSkipped: true,
		},
		{
			name:   "scientific notation is malformed",
			accept: []string{"text/event-stream; q=1e0"},
		},
		{
			name:   "leading zero integer is malformed",
			accept: []string{"text/event-stream; q=01"},
		},
		{
			name:   "more than three decimal digits is malformed",
			accept: []string{"text/event-stream; q=0.1234"},
		},
		{
			name:   "four zeroes after one is malformed",
			accept: []string{"text/event-stream; q=1.0000"},
		},
		{
			name:   "one with nonzero fraction is out of range",
			accept: []string{"text/event-stream; q=1.001"},
		},
		{
			name:   "quality above one is out of range",
			accept: []string{"text/event-stream; q=2"},
		},
		{
			name:   "positive sign is malformed",
			accept: []string{"text/event-stream; q=+0.5"},
		},
		{
			name:   "negative sign is malformed",
			accept: []string{"text/event-stream; q=-0.5"},
		},
		{
			name:   "missing leading zero is malformed",
			accept: []string{"text/event-stream; q=.5"},
		},
		{
			name:   "quoted quality is malformed",
			accept: []string{`text/event-stream; q="0.5"`},
		},
		{
			name:        "valid later range follows malformed quality",
			accept:      []string{"text/event-stream; q=01, application/json", "text/event-stream; q=0.5"},
			wantSkipped: true,
		},
		{
			name:        "valid range after malformed sibling",
			accept:      []string{"not a media range, text/event-stream"},
			wantSkipped: true,
		},
		{
			name:        "across header lines",
			accept:      []string{"application/json", "text/event-stream"},
			wantSkipped: true,
		},
		{
			name:   "quoted mention is not a media range",
			accept: []string{`application/example; note="text/event-stream,still", application/json`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 2)
			engine := newAcceptanceEngine(t, 1, outcomes)
			var shadowCalls atomic.Int32
			response := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			handler := newAcceptanceMiddleware(t, engine, response, cfg).Wrap(
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					shadowCalls.Add(1)
					w.WriteHeader(http.StatusNoContent)
				}),
			)
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header["Accept"] = append([]string(nil), test.accept...)

			handler.ServeHTTP(httptest.NewRecorder(), request)

			outcome := waitForOutcome(t, outcomes)
			if test.wantSkipped {
				if outcome.Classification() != shadowmw.OutcomeSkipped ||
					outcome.Reason() != shadowmw.ReasonUnsupportedExchange {
					t.Errorf("outcome = %s/%q, want skipped/unsupported_exchange",
						outcome.Classification(),
						outcome.Reason(),
					)
				}
				if shadowCalls.Load() != 0 {
					t.Errorf("shadow calls = %d, want 0", shadowCalls.Load())
				}
				return
			}
			if outcome.Classification() != shadowmw.OutcomeMatch {
				t.Errorf("outcome = %s/%q, want match", outcome.Classification(), outcome.Reason())
			}
			if shadowCalls.Load() != 1 {
				t.Errorf("shadow calls = %d, want 1", shadowCalls.Load())
			}
		})
	}
}

type blockingBody struct {
	started chan struct{}
	release <-chan struct{}
	data    []byte
	err     error
	once    sync.Once
	read    atomic.Bool
	closed  atomic.Bool
}

func (b *blockingBody) Read(destination []byte) (int, error) {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	if b.read.Swap(true) {
		return 0, io.EOF
	}
	return copy(destination, b.data), b.err
}

func (b *blockingBody) Close() error {
	b.closed.Store(true)
	return nil
}

func newCycle6Engine(
	t *testing.T,
	outcomes chan<- shadowmw.Outcome,
) *shadowmw.Engine {
	t.Helper()
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.MaxInFlightSessions = 1
	cfg.SessionTimeout = 250 * time.Millisecond
	cfg.Observer = &shadowmw.ObserverDelivery{
		Observer: acceptanceObserver(func(_ context.Context, outcome shadowmw.Outcome) error {
			outcomes <- outcome
			return nil
		}),
		QueueCapacity:  4,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter: shadowmw.ObserveMatches |
			shadowmw.ObserveMismatches |
			shadowmw.ObserveSkipped,
		Detail: shadowmw.OutcomeFull,
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
	return engine
}

func TestMiddlewareOversizedCaptureAfterTimeoutPreservesAuthoritativeRequest(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 4)
	engine := newCycle6Engine(t, outcomes)
	captureStarted := make(chan struct{})
	releaseCapture := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCapture)
		})
	}
	defer release()
	source := &blockingBody{
		started: captureStarted,
		release: releaseCapture,
		data:    []byte("abcde"),
		err:     io.EOF,
	}
	var authoritativeCalls atomic.Int32
	var shadowCalls atomic.Int32
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		authoritativeCalls.Add(1)
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("authoritative body read error = %v", err)
		}
		if got := string(body); got != "abcde" {
			t.Errorf("authoritative body = %q, want abcde", got)
		}
		if err := request.Body.Close(); err != nil {
			t.Errorf("authoritative body close error = %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.MaxRequestBodyBytes = 4
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			shadowCalls.Add(1)
		}),
	)
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = source
	request.ContentLength = -1
	recorder := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(returned)
	}()

	select {
	case <-captureStarted:
	case <-time.After(time.Second):
		t.Fatal("request capture did not start")
	}
	timeout := waitForOutcome(t, outcomes)
	if timeout.Classification() != shadowmw.OutcomeMismatch ||
		timeout.Reason() != shadowmw.ReasonSessionTimeout {
		t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
			timeout.Classification(),
			timeout.Reason(),
		)
	}
	release()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("authoritative request did not complete")
	}

	if recorder.Code != http.StatusAccepted {
		t.Errorf("client status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	if authoritativeCalls.Load() != 1 || shadowCalls.Load() != 0 {
		t.Errorf("branch calls = authoritative %d, shadow %d; want 1/0",
			authoritativeCalls.Load(),
			shadowCalls.Load(),
		)
	}
	if !source.closed.Load() {
		t.Error("authoritative request did not close reconstructed source body")
	}
	assertNoOutcome(t, outcomes)
}

func TestMiddlewareReadErrorAfterTimeoutPreservesConfiguredAuthoritativeBehavior(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 4)
	engine := newCycle6Engine(t, outcomes)
	captureStarted := make(chan struct{})
	releaseCapture := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCapture)
		})
	}
	defer release()
	wantErr := errors.New("request stream failed")
	source := &blockingBody{
		started: captureStarted,
		release: releaseCapture,
		data:    []byte("partial"),
		err:     wantErr,
	}
	var authoritativeCalls atomic.Int32
	var shadowCalls atomic.Int32
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		authoritativeCalls.Add(1)
		body, err := io.ReadAll(request.Body)
		if got := string(body); got != "partial" {
			t.Errorf("authoritative body = %q, want partial", got)
		}
		if !errors.Is(err, wantErr) {
			t.Errorf("authoritative body error = %v, want %v", err, wantErr)
		}
		if err := request.Body.Close(); err != nil {
			t.Errorf("authoritative body close error = %v", err)
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.PassBodyReadErrorsToAuthoritative()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			shadowCalls.Add(1)
		}),
	)
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Body = source
	request.ContentLength = -1
	recorder := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(returned)
	}()

	select {
	case <-captureStarted:
	case <-time.After(time.Second):
		t.Fatal("request capture did not start")
	}
	timeout := waitForOutcome(t, outcomes)
	if timeout.Classification() != shadowmw.OutcomeMismatch ||
		timeout.Reason() != shadowmw.ReasonSessionTimeout {
		t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
			timeout.Classification(),
			timeout.Reason(),
		)
	}
	release()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("configured authoritative body-read behavior did not complete")
	}

	if recorder.Code != http.StatusUnprocessableEntity {
		t.Errorf("client status = %d, want %d", recorder.Code, http.StatusUnprocessableEntity)
	}
	if authoritativeCalls.Load() != 1 || shadowCalls.Load() != 0 {
		t.Errorf("branch calls = authoritative %d, shadow %d; want 1/0",
			authoritativeCalls.Load(),
			shadowCalls.Load(),
		)
	}
	if !source.closed.Load() {
		t.Error("authoritative request did not close replayed source body")
	}
	assertNoOutcome(t, outcomes)
}

func TestMiddlewareSuccessfulCaptureAfterTimeoutContinuesAuthoritativeOnly(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 6)
	engine := newCycle6Engine(t, outcomes)
	captureStarted := make(chan struct{})
	releaseCapture := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCapture)
		})
	}
	defer release()
	source := &blockingBody{
		started: captureStarted,
		release: releaseCapture,
		data:    []byte("captured"),
		err:     io.EOF,
	}
	var delayedAuthoritativeCalls atomic.Int32
	var delayedShadowCalls atomic.Int32
	delayedShadow := make(chan struct{}, 1)
	secondShadow := make(chan struct{}, 1)
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/delayed" {
			delayedAuthoritativeCalls.Add(1)
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("authoritative body read error = %v", err)
			}
			if got := string(body); got != "captured" {
				t.Errorf("authoritative body = %q, want captured", got)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("authoritative"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/delayed" {
			delayedShadowCalls.Add(1)
			delayedShadow <- struct{}{}
		} else {
			secondShadow <- struct{}{}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
	request := httptest.NewRequest(http.MethodPost, "/delayed", nil)
	request.Body = source
	request.ContentLength = -1
	recorder := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(returned)
	}()

	select {
	case <-captureStarted:
	case <-time.After(time.Second):
		t.Fatal("request capture did not start")
	}
	timeout := waitForOutcome(t, outcomes)
	if timeout.Classification() != shadowmw.OutcomeMismatch ||
		timeout.Reason() != shadowmw.ReasonSessionTimeout {
		t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
			timeout.Classification(),
			timeout.Reason(),
		)
	}

	handler.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/second", nil),
	)
	select {
	case <-secondShadow:
	case <-time.After(time.Second):
		t.Fatal("timed-out capture did not release comparison capacity")
	}
	second := waitForOutcome(t, outcomes)
	if second.Classification() != shadowmw.OutcomeMatch {
		t.Fatalf("second outcome = %s/%q, want match", second.Classification(), second.Reason())
	}

	release()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("authoritative request did not complete after capture")
	}
	if recorder.Code != http.StatusAccepted ||
		recorder.Body.String() != "authoritative" {
		t.Errorf("client response = %d/%q, want 202/authoritative",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	select {
	case <-delayedShadow:
		t.Fatal("shadow launched after successful capture completed past timeout")
	case <-time.After(100 * time.Millisecond):
	}
	if delayedAuthoritativeCalls.Load() != 1 || delayedShadowCalls.Load() != 0 {
		t.Errorf("delayed branch calls = authoritative %d, shadow %d; want 1/0",
			delayedAuthoritativeCalls.Load(),
			delayedShadowCalls.Load(),
		)
	}
	assertNoOutcome(t, outcomes)
}

func TestMiddlewareShadowTransformReturningAfterTimeoutDoesNotLaunchShadow(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 6)
	engine := newCycle6Engine(t, outcomes)
	setupStarted := make(chan struct{})
	releaseSetup := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseSetup)
		})
	}
	defer release()
	var delayedAuthoritativeCalls atomic.Int32
	delayedShadow := make(chan struct{}, 1)
	continuedShadowSetup := make(chan struct{}, 1)
	secondShadow := make(chan struct{}, 1)
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/delayed" {
			delayedAuthoritativeCalls.Add(1)
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("authoritative body read error = %v", err)
			}
			if got := string(body); got != "delayed" {
				t.Errorf("authoritative body = %q, want delayed", got)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("authoritative"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/delayed" {
			delayedShadow <- struct{}{}
		} else {
			secondShadow <- struct{}{}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	cfg.Customizations.Shadow.BodyTransforms = []shadowmw.BodyTransform{
		func(ctx context.Context, body []byte) ([]byte, error) {
			if string(body) != "delayed" {
				return body, nil
			}
			close(setupStarted)
			<-releaseSetup
			if ctx.Err() == nil {
				t.Error("delayed shadow transform context was not canceled at timeout")
			}
			return body, nil
		},
		func(_ context.Context, body []byte) ([]byte, error) {
			if string(body) == "delayed" {
				continuedShadowSetup <- struct{}{}
			}
			return body, nil
		},
	}
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
	recorder := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		handler.ServeHTTP(
			recorder,
			httptest.NewRequest(http.MethodPost, "/delayed", strings.NewReader("delayed")),
		)
		close(returned)
	}()

	select {
	case <-setupStarted:
	case <-time.After(time.Second):
		t.Fatal("shadow transform did not start")
	}
	select {
	case outcome := <-outcomes:
		t.Fatalf("admitted shadow transform released session early with %s/%q",
			outcome.Classification(),
			outcome.Reason(),
		)
	case <-time.After(350 * time.Millisecond):
	}

	handler.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/second", nil),
	)
	second := waitForOutcome(t, outcomes)
	if second.Classification() != shadowmw.OutcomeSkipped ||
		second.Reason() != shadowmw.ReasonCapacity {
		t.Fatalf("second outcome = %s/%q, want skipped/capacity",
			second.Classification(),
			second.Reason(),
		)
	}
	select {
	case <-secondShadow:
		t.Fatal("second shadow ran while transform retained capacity")
	default:
	}

	release()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("authoritative request did not complete after delayed setup")
	}
	timeout := waitForOutcome(t, outcomes)
	if timeout.Classification() != shadowmw.OutcomeMismatch ||
		timeout.Reason() != shadowmw.ReasonSessionTimeout {
		t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
			timeout.Classification(),
			timeout.Reason(),
		)
	}
	if recorder.Code != http.StatusAccepted ||
		recorder.Body.String() != "authoritative" {
		t.Errorf("client response = %d/%q, want 202/authoritative",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	select {
	case <-continuedShadowSetup:
		t.Fatal("shadow setup continued after delayed transform returned past timeout")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-delayedShadow:
		t.Fatal("shadow launched after delayed transform returned past timeout")
	case <-time.After(100 * time.Millisecond):
	}
	if delayedAuthoritativeCalls.Load() != 1 {
		t.Errorf("delayed authoritative calls = %d, want 1", delayedAuthoritativeCalls.Load())
	}
	assertNoOutcome(t, outcomes)
}

func TestMiddlewareCommonTransformReturningAfterTimeoutContinuesAuthoritativeOnly(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 4)
	engine := newCycle6Engine(t, outcomes)
	setupStarted := make(chan struct{})
	releaseSetup := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseSetup)
		})
	}
	defer release()
	delayedShadow := make(chan struct{}, 1)
	shadowSetup := make(chan struct{}, 1)
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("authoritative body read error = %v", err)
		}
		if got := string(body); got != "delayed-common" {
			t.Errorf("authoritative body = %q, want delayed-common", got)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("authoritative"))
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	cfg.Customizations.Both.BodyTransforms = []shadowmw.BodyTransform{
		func(_ context.Context, body []byte) ([]byte, error) {
			close(setupStarted)
			<-releaseSetup
			return append(body, "-common"...), nil
		},
	}
	cfg.Customizations.Shadow.BodyTransforms = []shadowmw.BodyTransform{
		func(_ context.Context, body []byte) ([]byte, error) {
			shadowSetup <- struct{}{}
			return body, nil
		},
	}
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			delayedShadow <- struct{}{}
		}),
	)
	recorder := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		handler.ServeHTTP(
			recorder,
			httptest.NewRequest(http.MethodPost, "/delayed", strings.NewReader("delayed")),
		)
		close(returned)
	}()

	select {
	case <-setupStarted:
	case <-time.After(time.Second):
		t.Fatal("common transform did not start")
	}
	timeout := waitForOutcome(t, outcomes)
	if timeout.Classification() != shadowmw.OutcomeMismatch ||
		timeout.Reason() != shadowmw.ReasonSessionTimeout {
		t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
			timeout.Classification(),
			timeout.Reason(),
		)
	}
	release()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("authoritative request did not complete after common transform")
	}
	if recorder.Code != http.StatusAccepted ||
		recorder.Body.String() != "authoritative" {
		t.Errorf("client response = %d/%q, want 202/authoritative",
			recorder.Code,
			recorder.Body.String(),
		)
	}
	select {
	case <-shadowSetup:
		t.Fatal("shadow setup continued after common transform returned past timeout")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-delayedShadow:
		t.Fatal("shadow launched after common transform returned past timeout")
	case <-time.After(100 * time.Millisecond):
	}
	assertNoOutcome(t, outcomes)
}

func TestMiddlewareRequestHookReturningAfterTimeoutContinuesAuthoritativeOnly(t *testing.T) {
	tests := []struct {
		name      string
		configure func(
			t *testing.T,
			cfg *shadowmw.MiddlewareConfig,
			started chan struct{},
			release <-chan struct{},
			calls *atomic.Int32,
		)
		wantHeader   string
		holdsSession bool
	}{
		{
			name: "common hook",
			configure: func(
				t *testing.T,
				cfg *shadowmw.MiddlewareConfig,
				started chan struct{},
				release <-chan struct{},
				calls *atomic.Int32,
			) {
				cfg.Customizations.Both.RequestHooks = []shadowmw.RequestHook{
					func(request *http.Request) (*http.Request, error) {
						if calls.Add(1) != 1 {
							t.Error("common hook continued into shadow setup after timeout")
							return request, nil
						}
						close(started)
						<-release
						request.Header.Set("X-Prepared", "common")
						return request, nil
					},
				}
			},
			wantHeader: "common",
		},
		{
			name: "shadow hook ignoring cancellation",
			configure: func(
				t *testing.T,
				cfg *shadowmw.MiddlewareConfig,
				started chan struct{},
				release <-chan struct{},
				calls *atomic.Int32,
			) {
				cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
					func(request *http.Request) (*http.Request, error) {
						calls.Add(1)
						close(started)
						<-release
						if request.Context().Err() == nil {
							t.Error("delayed shadow hook context was not canceled at timeout")
						}
						return request, nil
					},
					func(request *http.Request) (*http.Request, error) {
						calls.Add(1)
						return request, nil
					},
				}
			},
			holdsSession: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 4)
			engine := newCycle6Engine(t, outcomes)
			setupStarted := make(chan struct{})
			releaseSetup := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseSetup)
				})
			}
			defer release()
			var hookCalls atomic.Int32
			delayedShadow := make(chan struct{}, 1)
			authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if got := request.Header.Get("X-Prepared"); got != test.wantHeader {
					t.Errorf("authoritative X-Prepared = %q, want %q", got, test.wantHeader)
				}
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("authoritative"))
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(t, &cfg, setupStarted, releaseSetup, &hookCalls)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					delayedShadow <- struct{}{}
				}),
			)
			recorder := httptest.NewRecorder()
			returned := make(chan struct{})
			go func() {
				handler.ServeHTTP(
					recorder,
					httptest.NewRequest(http.MethodGet, "/delayed", nil),
				)
				close(returned)
			}()

			select {
			case <-setupStarted:
			case <-time.After(time.Second):
				t.Fatal("request hook did not start")
			}
			var timeout shadowmw.Outcome
			if test.holdsSession {
				select {
				case outcome := <-outcomes:
					t.Fatalf("admitted shadow hook released session early with %s/%q",
						outcome.Classification(),
						outcome.Reason(),
					)
				case <-time.After(350 * time.Millisecond):
				}
				release()
				select {
				case <-returned:
				case <-time.After(time.Second):
					t.Fatal("authoritative request did not complete after delayed hook")
				}
				timeout = waitForOutcome(t, outcomes)
			} else {
				timeout = waitForOutcome(t, outcomes)
				release()
				select {
				case <-returned:
				case <-time.After(time.Second):
					t.Fatal("authoritative request did not complete after delayed hook")
				}
			}
			if timeout.Classification() != shadowmw.OutcomeMismatch ||
				timeout.Reason() != shadowmw.ReasonSessionTimeout {
				t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
					timeout.Classification(),
					timeout.Reason(),
				)
			}
			if recorder.Code != http.StatusAccepted ||
				recorder.Body.String() != "authoritative" {
				t.Errorf("client response = %d/%q, want 202/authoritative",
					recorder.Code,
					recorder.Body.String(),
				)
			}
			select {
			case <-delayedShadow:
				t.Fatal("shadow launched after request hook returned past timeout")
			case <-time.After(100 * time.Millisecond):
			}
			if hookCalls.Load() != 1 {
				t.Errorf("hook calls = %d, want 1", hookCalls.Load())
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

type shadowSetupCase struct {
	name      string
	configure func(
		cfg *shadowmw.MiddlewareConfig,
		started chan<- struct{},
		release <-chan struct{},
	)
}

func shadowSetupCases() []shadowSetupCase {
	return []shadowSetupCase{
		{
			name: "body transform",
			configure: func(
				cfg *shadowmw.MiddlewareConfig,
				started chan<- struct{},
				release <-chan struct{},
			) {
				cfg.Customizations.Shadow.BodyTransforms = []shadowmw.BodyTransform{
					func(_ context.Context, body []byte) ([]byte, error) {
						if string(body) == "blocked" {
							started <- struct{}{}
							<-release
						}
						return body, nil
					},
				}
			},
		},
		{
			name: "request hook",
			configure: func(
				cfg *shadowmw.MiddlewareConfig,
				started chan<- struct{},
				release <-chan struct{},
			) {
				cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
					func(request *http.Request) (*http.Request, error) {
						if request.URL.Path == "/blocked" {
							started <- struct{}{}
							<-release
						}
						return request, nil
					},
				}
			},
		},
	}
}

func TestMiddlewareTimeoutBeforeShadowCallbackAdmissionPreventsStart(t *testing.T) {
	for _, test := range shadowSetupCases() {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 4)
			engine := newCycle6Engine(t, outcomes)
			commonStarted := make(chan struct{})
			releaseCommon := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseCommon)
				})
			}
			defer release()
			callbackStarted := make(chan struct{}, 1)
			callbackRelease := make(chan struct{})
			var callbackReleaseOnce sync.Once
			releaseCallback := func() {
				callbackReleaseOnce.Do(func() {
					close(callbackRelease)
				})
			}
			defer releaseCallback()
			var authoritativeCalls atomic.Int32
			var shadowCalls atomic.Int32
			authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				authoritativeCalls.Add(1)
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("authoritative"))
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			cfg.Customizations.Both.BodyTransforms = []shadowmw.BodyTransform{
				func(_ context.Context, body []byte) ([]byte, error) {
					close(commonStarted)
					<-releaseCommon
					return body, nil
				},
			}
			test.configure(&cfg, callbackStarted, callbackRelease)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					shadowCalls.Add(1)
				}),
			)
			recorder := httptest.NewRecorder()
			returned := make(chan struct{})
			go func() {
				handler.ServeHTTP(
					recorder,
					httptest.NewRequest(
						http.MethodPost,
						"/blocked",
						strings.NewReader("blocked"),
					),
				)
				close(returned)
			}()

			select {
			case <-commonStarted:
			case <-time.After(time.Second):
				t.Fatal("common transform did not start")
			}
			timeout := waitForOutcome(t, outcomes)
			if timeout.Classification() != shadowmw.OutcomeMismatch ||
				timeout.Reason() != shadowmw.ReasonSessionTimeout {
				t.Fatalf("outcome = %s/%q, want mismatch/session_timeout",
					timeout.Classification(),
					timeout.Reason(),
				)
			}
			release()
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("authoritative request did not complete")
			}
			select {
			case <-callbackStarted:
				t.Fatal("shadow callback started after session timeout")
			case <-time.After(100 * time.Millisecond):
			}
			if recorder.Code != http.StatusAccepted ||
				recorder.Body.String() != "authoritative" {
				t.Errorf("client response = %d/%q, want 202/authoritative",
					recorder.Code,
					recorder.Body.String(),
				)
			}
			if authoritativeCalls.Load() != 1 || shadowCalls.Load() != 0 {
				t.Errorf("branch calls = authoritative %d, shadow %d; want 1/0",
					authoritativeCalls.Load(),
					shadowCalls.Load(),
				)
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

func TestMiddlewareAdmittedShadowCallbackRetainsCapacityUntilReturn(t *testing.T) {
	for _, test := range shadowSetupCases() {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 6)
			engine := newCycle6Engine(t, outcomes)
			callbackStarted := make(chan struct{}, 1)
			releaseCallback := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseCallback)
				})
			}
			defer release()
			firstShadow := make(chan struct{}, 1)
			secondShadow := make(chan struct{}, 1)
			authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/blocked" {
					w.WriteHeader(http.StatusAccepted)
					_, _ = w.Write([]byte("authoritative"))
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(&cfg, callbackStarted, releaseCallback)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
				http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					if request.URL.Path == "/blocked" {
						firstShadow <- struct{}{}
					} else {
						secondShadow <- struct{}{}
					}
					w.WriteHeader(http.StatusNoContent)
				}),
			)
			firstRecorder := httptest.NewRecorder()
			firstReturned := make(chan struct{})
			go func() {
				handler.ServeHTTP(
					firstRecorder,
					httptest.NewRequest(
						http.MethodPost,
						"/blocked",
						strings.NewReader("blocked"),
					),
				)
				close(firstReturned)
			}()

			select {
			case <-callbackStarted:
			case <-time.After(time.Second):
				t.Fatal("shadow callback did not start")
			}
			select {
			case outcome := <-outcomes:
				t.Fatalf("callback admission released session early with %s/%q",
					outcome.Classification(),
					outcome.Reason(),
				)
			case <-time.After(350 * time.Millisecond):
			}

			secondRecorder := httptest.NewRecorder()
			handler.ServeHTTP(
				secondRecorder,
				httptest.NewRequest(http.MethodGet, "/second", nil),
			)
			capacity := waitForOutcome(t, outcomes)
			if capacity.Classification() != shadowmw.OutcomeSkipped ||
				capacity.Reason() != shadowmw.ReasonCapacity {
				t.Fatalf("second outcome = %s/%q, want skipped/capacity",
					capacity.Classification(),
					capacity.Reason(),
				)
			}
			if secondRecorder.Code != http.StatusNoContent {
				t.Errorf("second client status = %d, want 204", secondRecorder.Code)
			}
			select {
			case <-secondShadow:
				t.Fatal("second shadow ran while callback retained session capacity")
			default:
			}

			release()
			select {
			case <-firstReturned:
			case <-time.After(time.Second):
				t.Fatal("authoritative request did not complete after callback returned")
			}
			timeout := waitForOutcome(t, outcomes)
			if timeout.Classification() != shadowmw.OutcomeMismatch ||
				timeout.Reason() != shadowmw.ReasonSessionTimeout {
				t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
					timeout.Classification(),
					timeout.Reason(),
				)
			}
			if firstRecorder.Code != http.StatusAccepted ||
				firstRecorder.Body.String() != "authoritative" {
				t.Errorf("first client response = %d/%q, want 202/authoritative",
					firstRecorder.Code,
					firstRecorder.Body.String(),
				)
			}
			select {
			case <-firstShadow:
				t.Fatal("first shadow handler ran after callback returned past deadline")
			case <-time.After(100 * time.Millisecond):
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

func TestMiddlewareShadowOnlySetupDoesNotDelayAuthoritativeHandling(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*shadowmw.MiddlewareConfig, chan<- struct{}, <-chan struct{})
	}{
		{
			name: "body transform",
			configure: func(
				cfg *shadowmw.MiddlewareConfig,
				started chan<- struct{},
				release <-chan struct{},
			) {
				cfg.Customizations.Shadow.BodyTransforms = []shadowmw.BodyTransform{
					func(_ context.Context, body []byte) ([]byte, error) {
						close(started)
						<-release
						return body, nil
					},
				}
			},
		},
		{
			name: "request hook",
			configure: func(
				cfg *shadowmw.MiddlewareConfig,
				started chan<- struct{},
				release <-chan struct{},
			) {
				cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
					func(request *http.Request) (*http.Request, error) {
						close(started)
						<-release
						return request, nil
					},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 2)
			engine := newAcceptanceEngine(t, 1, outcomes)
			setupStarted := make(chan struct{})
			releaseSetup := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseSetup)
				})
			}
			t.Cleanup(release)

			authoritativeStarted := make(chan struct{}, 1)
			authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				authoritativeStarted <- struct{}{}
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("same"))
			})
			shadowStarted := make(chan struct{}, 1)
			replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				shadowStarted <- struct{}{}
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("same"))
			})

			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(&cfg, setupStarted, releaseSetup)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
			recorder := httptest.NewRecorder()
			returned := make(chan struct{})
			go func() {
				handler.ServeHTTP(
					recorder,
					httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body")),
				)
				close(returned)
			}()

			select {
			case <-setupStarted:
			case <-time.After(time.Second):
				t.Fatal("shadow-only setup did not start")
			}
			select {
			case <-authoritativeStarted:
			case <-time.After(time.Second):
				t.Fatal("authoritative handler did not start while shadow-only setup was blocked")
			}
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("client response waited for blocked shadow-only setup")
			}
			if recorder.Code != http.StatusAccepted || recorder.Body.String() != "same" {
				t.Fatalf("client response = %d/%q, want 202/same", recorder.Code, recorder.Body.String())
			}
			select {
			case <-shadowStarted:
				t.Fatal("shadow handler started before shadow-only setup completed")
			default:
			}

			release()
			select {
			case <-shadowStarted:
			case <-time.After(time.Second):
				t.Fatal("shadow handler did not start after shadow-only setup completed")
			}
			outcome := waitForOutcome(t, outcomes)
			if outcome.Classification() != shadowmw.OutcomeMatch {
				t.Fatalf("outcome = %s/%q, want match", outcome.Classification(), outcome.Reason())
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

func TestMiddlewareAsyncShadowSetupFailurePreservesAuthoritativeResponse(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*shadowmw.MiddlewareConfig)
	}{
		{
			name: "transform error",
			configure: func(cfg *shadowmw.MiddlewareConfig) {
				cfg.Customizations.Shadow.BodyTransforms = []shadowmw.BodyTransform{
					func(context.Context, []byte) ([]byte, error) {
						return nil, errors.New("shadow transform defect")
					},
				}
			},
		},
		{
			name: "hook panic",
			configure: func(cfg *shadowmw.MiddlewareConfig) {
				cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
					func(*http.Request) (*http.Request, error) {
						panic("shadow hook defect")
					},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 2)
			engine := newAcceptanceEngine(t, 1, outcomes)
			var shadowCalls atomic.Int32
			authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("authoritative"))
			})
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(&cfg)

			recorder := httptest.NewRecorder()
			newAcceptanceMiddleware(t, engine, authoritative, cfg).
				Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					shadowCalls.Add(1)
				})).
				ServeHTTP(
					recorder,
					httptest.NewRequest(http.MethodPost, "/", strings.NewReader("body")),
				)

			if recorder.Code != http.StatusAccepted ||
				recorder.Body.String() != "authoritative" {
				t.Fatalf(
					"client response = %d/%q, want 202/authoritative",
					recorder.Code,
					recorder.Body.String(),
				)
			}
			if shadowCalls.Load() != 0 {
				t.Fatalf("shadow handler calls = %d, want zero", shadowCalls.Load())
			}
			outcome := waitForOutcome(t, outcomes)
			if outcome.Classification() != shadowmw.OutcomeMismatch ||
				outcome.Reason() != "shadow_setup_error" {
				t.Fatalf(
					"outcome = %s/%q, want mismatch/shadow_setup_error",
					outcome.Classification(),
					outcome.Reason(),
				)
			}
			if outcome.AuthoritativeStatusCode() != http.StatusAccepted ||
				string(outcome.AuthoritativeResponse().Body()) != "authoritative" {
				t.Fatalf(
					"authoritative outcome = %d/%q, want 202/authoritative",
					outcome.AuthoritativeStatusCode(),
					outcome.AuthoritativeResponse().Body(),
				)
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

type cycle9CanceledSetupCase struct {
	name      string
	configure func(
		cfg *shadowmw.MiddlewareConfig,
		started chan<- struct{},
		canceled chan<- struct{},
		release <-chan struct{},
	)
}

func cycle9CanceledSetupCases() []cycle9CanceledSetupCase {
	return []cycle9CanceledSetupCase{
		{
			name: "body transform",
			configure: func(
				cfg *shadowmw.MiddlewareConfig,
				started chan<- struct{},
				canceled chan<- struct{},
				release <-chan struct{},
			) {
				cfg.Customizations.Shadow.BodyTransforms = []shadowmw.BodyTransform{
					func(ctx context.Context, body []byte) ([]byte, error) {
						close(started)
						<-ctx.Done()
						close(canceled)
						<-release
						return body, nil
					},
				}
			},
		},
		{
			name: "request hook",
			configure: func(
				cfg *shadowmw.MiddlewareConfig,
				started chan<- struct{},
				canceled chan<- struct{},
				release <-chan struct{},
			) {
				cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
					func(request *http.Request) (*http.Request, error) {
						close(started)
						<-request.Context().Done()
						close(canceled)
						<-release
						return request, nil
					},
				}
			},
		},
	}
}

func newCycle9Engine(
	t *testing.T,
	outcomes chan<- shadowmw.Outcome,
) *shadowmw.Engine {
	t.Helper()
	return newCycle9EngineWithTimeout(t, outcomes, 100*time.Millisecond)
}

func newCycle9EngineWithTimeout(
	t *testing.T,
	outcomes chan<- shadowmw.Outcome,
	sessionTimeout time.Duration,
) *shadowmw.Engine {
	t.Helper()
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1
	cfg.MaxInFlightSessions = 1
	cfg.SessionTimeout = sessionTimeout
	cfg.Observer = &shadowmw.ObserverDelivery{
		Observer: acceptanceObserver(func(_ context.Context, outcome shadowmw.Outcome) error {
			outcomes <- outcome
			return nil
		}),
		QueueCapacity:  4,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter: shadowmw.ObserveMatches |
			shadowmw.ObserveMismatches |
			shadowmw.ObserveSkipped,
		Detail: shadowmw.OutcomeFull,
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
	return engine
}

func TestMiddlewareTimeoutRecordsAuthoritativeSnapshotWhileShadowSetupIsBlocked(
	t *testing.T,
) {
	for _, test := range cycle9CanceledSetupCases() {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 4)
			engine := newCycle9Engine(t, outcomes)
			setupStarted := make(chan struct{})
			setupCanceled := make(chan struct{})
			releaseSetup := make(chan struct{})
			allowAuthoritative := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseSetup)
				})
			}
			t.Cleanup(release)
			var allowOnce sync.Once
			allow := func() {
				allowOnce.Do(func() {
					close(allowAuthoritative)
				})
			}
			t.Cleanup(allow)

			authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/blocked" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				<-setupStarted
				<-allowAuthoritative
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("authoritative"))
			})
			shadowStarted := make(chan struct{}, 1)
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(&cfg, setupStarted, setupCanceled, releaseSetup)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					shadowStarted <- struct{}{}
				}),
			)

			recorder := httptest.NewRecorder()
			returned := make(chan struct{})
			go func() {
				handler.ServeHTTP(
					recorder,
					httptest.NewRequest(
						http.MethodPost,
						"/blocked",
						strings.NewReader("blocked"),
					),
				)
				close(returned)
			}()

			select {
			case <-setupCanceled:
			case <-time.After(time.Second):
				t.Fatal("session timeout did not promptly cancel admitted shadow setup")
			}
			allow()
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("client response waited for blocked shadow setup")
			}

			handler.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/capacity", nil),
			)
			capacity := waitForOutcome(t, outcomes)
			if capacity.Classification() != shadowmw.OutcomeSkipped ||
				capacity.Reason() != shadowmw.ReasonCapacity {
				t.Fatalf(
					"capacity outcome = %s/%q, want skipped/capacity",
					capacity.Classification(),
					capacity.Reason(),
				)
			}

			release()
			timeout := waitForOutcome(t, outcomes)
			if timeout.Classification() != shadowmw.OutcomeMismatch ||
				timeout.Reason() != shadowmw.ReasonSessionTimeout {
				t.Fatalf(
					"terminal outcome = %s/%q, want mismatch/session_timeout",
					timeout.Classification(),
					timeout.Reason(),
				)
			}
			if timeout.AuthoritativeStatusCode() != http.StatusAccepted ||
				string(timeout.AuthoritativeResponse().Body()) != "authoritative" {
				t.Fatalf(
					"authoritative snapshot = %d/%q, want 202/authoritative",
					timeout.AuthoritativeStatusCode(),
					timeout.AuthoritativeResponse().Body(),
				)
			}
			select {
			case <-shadowStarted:
				t.Fatal("shadow handler started after timeout terminal intent")
			default:
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

func TestMiddlewareResponseDeclaredSSECancelsBlockedShadowSetup(t *testing.T) {
	for _, test := range cycle9CanceledSetupCases() {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 4)
			engine := newCycle9EngineWithTimeout(t, outcomes, 2*time.Second)
			setupStarted := make(chan struct{})
			setupCanceled := make(chan struct{})
			releaseSetup := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseSetup)
				})
			}
			t.Cleanup(release)

			authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/blocked" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				<-setupStarted
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("data: authoritative\n\n"))
			})
			shadowStarted := make(chan struct{}, 1)
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(&cfg, setupStarted, setupCanceled, releaseSetup)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					shadowStarted <- struct{}{}
				}),
			)

			returned := make(chan struct{})
			go func() {
				handler.ServeHTTP(
					httptest.NewRecorder(),
					httptest.NewRequest(
						http.MethodPost,
						"/blocked",
						strings.NewReader("blocked"),
					),
				)
				close(returned)
			}()

			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("SSE response waited for blocked shadow setup")
			}
			select {
			case <-setupCanceled:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("response-declared SSE did not promptly cancel shadow setup")
			}

			handler.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/capacity", nil),
			)
			capacity := waitForOutcome(t, outcomes)
			if capacity.Classification() != shadowmw.OutcomeSkipped ||
				capacity.Reason() != shadowmw.ReasonCapacity {
				t.Fatalf(
					"capacity outcome = %s/%q, want skipped/capacity",
					capacity.Classification(),
					capacity.Reason(),
				)
			}

			release()
			unsupported := waitForOutcome(t, outcomes)
			if unsupported.Classification() != shadowmw.OutcomeSkipped ||
				unsupported.Reason() != shadowmw.ReasonUnsupportedExchange {
				t.Fatalf(
					"terminal outcome = %s/%q, want skipped/unsupported_exchange",
					unsupported.Classification(),
					unsupported.Reason(),
				)
			}
			select {
			case <-shadowStarted:
				t.Fatal("shadow handler started after unsupported terminal intent")
			default:
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

func TestMiddlewareAuthoritativePanicWinsWhileShadowSetupIsBlocked(t *testing.T) {
	for _, test := range cycle9CanceledSetupCases() {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 4)
			engine := newCycle9EngineWithTimeout(t, outcomes, 2*time.Second)
			setupStarted := make(chan struct{})
			setupCanceled := make(chan struct{})
			releaseSetup := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseSetup)
				})
			}
			t.Cleanup(release)

			authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/blocked" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				<-setupStarted
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("before panic"))
				panic("authoritative defect")
			})
			shadowStarted := make(chan struct{}, 1)
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(&cfg, setupStarted, setupCanceled, releaseSetup)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					shadowStarted <- struct{}{}
				}),
			)

			recovered := make(chan any, 1)
			go func() {
				defer func() {
					recovered <- recover()
				}()
				handler.ServeHTTP(
					httptest.NewRecorder(),
					httptest.NewRequest(
						http.MethodPost,
						"/blocked",
						strings.NewReader("blocked"),
					),
				)
			}()

			select {
			case got := <-recovered:
				if got != "authoritative defect" {
					t.Fatalf("recovered panic = %v, want authoritative defect", got)
				}
			case <-time.After(time.Second):
				t.Fatal("authoritative panic propagation waited for blocked shadow setup")
			}
			select {
			case <-setupCanceled:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("authoritative panic did not promptly cancel shadow setup")
			}

			handler.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/capacity", nil),
			)
			capacity := waitForOutcome(t, outcomes)
			if capacity.Classification() != shadowmw.OutcomeSkipped ||
				capacity.Reason() != shadowmw.ReasonCapacity {
				t.Fatalf(
					"capacity outcome = %s/%q, want skipped/capacity",
					capacity.Classification(),
					capacity.Reason(),
				)
			}

			release()
			panicOutcome := waitForOutcome(t, outcomes)
			if panicOutcome.Classification() != shadowmw.OutcomeMismatch ||
				panicOutcome.Reason() != shadowmw.ReasonAuthoritativePanic {
				t.Fatalf(
					"terminal outcome = %s/%q, want mismatch/authoritative_panic",
					panicOutcome.Classification(),
					panicOutcome.Reason(),
				)
			}
			if panicOutcome.AuthoritativeStatusCode() != http.StatusAccepted ||
				string(panicOutcome.AuthoritativeResponse().Body()) != "before panic" {
				t.Fatalf(
					"authoritative snapshot = %d/%q, want 202/before panic",
					panicOutcome.AuthoritativeStatusCode(),
					panicOutcome.AuthoritativeResponse().Body(),
				)
			}
			select {
			case <-shadowStarted:
				t.Fatal("shadow handler started after authoritative panic terminal intent")
			default:
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

func TestMiddlewareAuthoritativeTransportErrorWinsWhileShadowSetupIsBlocked(
	t *testing.T,
) {
	for _, test := range cycle9CanceledSetupCases() {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 4)
			engine := newCycle9EngineWithTimeout(t, outcomes, 2*time.Second)
			setupStarted := make(chan struct{})
			setupCanceled := make(chan struct{})
			releaseSetup := make(chan struct{})
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(releaseSetup)
				})
			}
			t.Cleanup(release)

			authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/blocked" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				<-setupStarted
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte("authoritative"))
			})
			shadowStarted := make(chan struct{}, 1)
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			test.configure(&cfg, setupStarted, setupCanceled, releaseSetup)
			handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(
				http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					shadowStarted <- struct{}{}
				}),
			)
			client := &partialResponseWriter{
				header: make(http.Header),
				n:      3,
				err:    errors.New("client disconnected"),
			}

			returned := make(chan struct{})
			go func() {
				handler.ServeHTTP(
					client,
					httptest.NewRequest(
						http.MethodPost,
						"/blocked",
						strings.NewReader("blocked"),
					),
				)
				close(returned)
			}()

			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("transport failure handling waited for blocked shadow setup")
			}
			select {
			case <-setupCanceled:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("transport failure did not promptly cancel shadow setup")
			}

			handler.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/capacity", nil),
			)
			capacity := waitForOutcome(t, outcomes)
			if capacity.Classification() != shadowmw.OutcomeSkipped ||
				capacity.Reason() != shadowmw.ReasonCapacity {
				t.Fatalf(
					"capacity outcome = %s/%q, want skipped/capacity",
					capacity.Classification(),
					capacity.Reason(),
				)
			}

			release()
			transport := waitForOutcome(t, outcomes)
			if transport.Classification() != shadowmw.OutcomeMismatch ||
				transport.Reason() != shadowmw.ReasonAuthoritativeTransportError {
				t.Fatalf(
					"terminal outcome = %s/%q, want mismatch/authoritative_transport_error",
					transport.Classification(),
					transport.Reason(),
				)
			}
			if transport.AuthoritativeStatusCode() != http.StatusAccepted ||
				string(transport.AuthoritativeResponse().Body()) != "aut" {
				t.Fatalf(
					"authoritative snapshot = %d/%q, want 202/aut",
					transport.AuthoritativeStatusCode(),
					transport.AuthoritativeResponse().Body(),
				)
			}
			select {
			case <-shadowStarted:
				t.Fatal("shadow handler started after transport terminal intent")
			default:
			}
			assertNoOutcome(t, outcomes)
		})
	}
}

func TestMiddlewareComparesImplicitContentTypesWithBodyComparisonDisabled(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 1)
	engine := newAcceptanceEngine(t, 1, outcomes)
	rule, err := shadowmw.NewRule("implicit.types", shadowmw.ResponseParity(shadowmw.DisableBodyComparison()), shadowmw.MatchAll(), shadowmw.SampleAll())
	if err != nil {
		t.Fatal(err)
	}
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "<html>page</html>")
	})
	shadow := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "plain text")
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, cfg, rule)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(middleware.Wrap(shadow))
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeMismatch {
		t.Errorf("classification = %s, want mismatch for different inferred Content-Type", outcome.Classification())
	}
	if got := outcome.AuthoritativeResponse().Header().Get("Content-Type"); got != response.Header.Get("Content-Type") || got != "text/html; charset=utf-8" {
		t.Errorf("captured authoritative Content-Type = %q, wire = %q", got, response.Header.Get("Content-Type"))
	}
	if got := outcome.ShadowResponse().Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("captured shadow Content-Type = %q, want text/plain", got)
	}
}

func TestMiddlewareCapturesImplicitContentTypeLikeHTTPServer(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(http.ResponseWriter)
		limit int64
	}{
		{name: "explicit final status", write: func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, "<html>page</html>")
		}},
		{name: "split first bytes", write: func(w http.ResponseWriter) {
			_, _ = io.WriteString(w, "<")
			_, _ = io.WriteString(w, "html>page</html>")
		}},
		{name: "empty write before body", write: func(w http.ResponseWriter) { _, _ = w.Write(nil); _, _ = io.WriteString(w, "<html>page</html>") }},
		{name: "flush before body", write: func(w http.ResponseWriter) { w.(http.Flusher).Flush(); _, _ = io.WriteString(w, "<html>page</html>") }},
		{name: "flush partial body", write: func(w http.ResponseWriter) {
			_, _ = io.WriteString(w, "<")
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, "html>page</html>")
		}},
		{name: "explicit type", write: func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/custom")
			_, _ = io.WriteString(w, "<html>page</html>")
		}},
		{name: "explicit suppression", write: func(w http.ResponseWriter) {
			w.Header()["Content-Type"] = nil
			_, _ = io.WriteString(w, "<html>page</html>")
		}},
		{name: "encoded body", write: func(w http.ResponseWriter) {
			w.Header().Set("Content-Encoding", "custom")
			_, _ = io.WriteString(w, "<html>page</html>")
		}},
		{name: "transfer encoding", write: func(w http.ResponseWriter) {
			w.Header().Set("Transfer-Encoding", "identity")
			_, _ = io.WriteString(w, "<html>page</html>")
		}},
		{name: "late type ignored", write: func(w http.ResponseWriter) {
			w.Header().Set("X-Commit", "yes")
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/custom")
			_, _ = io.WriteString(w, "<html>page</html>")
		}},
		{name: "informational type not final", write: func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusEarlyHints)
			w.Header().Del("Content-Type")
			_, _ = io.WriteString(w, "<html>page</html>")
		}},
		{name: "capture smaller than sniff prefix", limit: 1, write: func(w http.ResponseWriter) { _, _ = io.WriteString(w, "<html>page</html>") }},
		{name: "empty response", write: func(w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }},
		{name: "no content", write: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 1)
			engine := newAcceptanceEngine(t, 1, outcomes)
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { test.write(w) })
			rule, err := shadowmw.NewRule("content.type", shadowmw.ResponseParity(shadowmw.DisableBodyComparison()), shadowmw.MatchAll(), shadowmw.SampleAll())
			if err != nil {
				t.Fatal(err)
			}
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			if test.limit > 0 {
				cfg.MaxResponseBodyBytes = test.limit
			}
			middleware, err := shadowmw.NewMiddleware(engine, handler, cfg, rule)
			if err != nil {
				t.Fatal(err)
			}
			direct := httptest.NewServer(handler)
			defer direct.Close()
			wrapped := httptest.NewServer(middleware.Wrap(handler))
			defer wrapped.Close()
			getType := func(server *httptest.Server) string {
				t.Helper()
				response, err := server.Client().Get(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if _, err := io.Copy(io.Discard, response.Body); err != nil {
					t.Fatal(err)
				}
				return response.Header.Get("Content-Type")
			}
			want := getType(direct)
			if got := getType(wrapped); got != want {
				t.Errorf("wrapped wire Content-Type = %q, direct = %q", got, want)
			}
			outcome := waitForOutcome(t, outcomes)
			for _, snapshot := range []shadowmw.ResponseSnapshot{outcome.AuthoritativeResponse(), outcome.ShadowResponse()} {
				if got := snapshot.Header().Get("Content-Type"); got != want {
					t.Errorf("captured Content-Type = %q, wire = %q", got, want)
				}
			}
			if test.limit == 0 && outcome.Classification() != shadowmw.OutcomeMatch {
				t.Errorf("classification = %s, want match", outcome.Classification())
			}
		})
	}
}

func TestMiddlewarePreservesFinalResponseAfterInformationalHeaders(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 1)
	engine := newAcceptanceEngine(t, 1, outcomes)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Set("X-Final", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "created")
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware := newAcceptanceMiddleware(t, engine, handler, cfg)
	server := httptest.NewServer(middleware.Wrap(handler))
	defer server.Close()

	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated || string(body) != "created" {
		t.Errorf("client response = %d %q, want 201 created", response.StatusCode, body)
	}
	outcome := waitForOutcome(t, outcomes)
	if outcome.Classification() != shadowmw.OutcomeMatch || outcome.AuthoritativeStatusCode() != http.StatusCreated || outcome.ShadowStatusCode() != http.StatusCreated {
		t.Errorf("outcome = %s, statuses %d/%d, want match at 201/201", outcome.Classification(), outcome.AuthoritativeStatusCode(), outcome.ShadowStatusCode())
	}
	for _, snapshot := range []shadowmw.ResponseSnapshot{outcome.AuthoritativeResponse(), outcome.ShadowResponse()} {
		if snapshot.Header().Get("X-Final") != "yes" {
			t.Errorf("final header missing from snapshot: %v", snapshot.Header())
		}
	}
}

func TestMiddlewareComparesOnlyHTTP1Requests(t *testing.T) {
	tests := []struct {
		name        string
		proto       string
		protoMajor  int
		protoMinor  int
		wantShadow  bool
		wantOutcome shadowmw.OutcomeClassification
		wantReason  string
	}{
		{
			name:        "HTTP 1.0 is eligible",
			proto:       "HTTP/1.0",
			protoMajor:  1,
			protoMinor:  0,
			wantShadow:  true,
			wantOutcome: shadowmw.OutcomeMatch,
			wantReason:  shadowmw.ReasonParity,
		},
		{
			name:        "HTTP 1.1 is eligible",
			proto:       "HTTP/1.1",
			protoMajor:  1,
			protoMinor:  1,
			wantShadow:  true,
			wantOutcome: shadowmw.OutcomeMatch,
			wantReason:  shadowmw.ReasonParity,
		},
		{
			name:        "HTTP 2 is unsupported",
			proto:       "HTTP/2.0",
			protoMajor:  2,
			protoMinor:  0,
			wantOutcome: shadowmw.OutcomeSkipped,
			wantReason:  shadowmw.ReasonUnsupportedExchange,
		},
		{
			name:        "HTTP 3 is unsupported",
			proto:       "HTTP/3.0",
			protoMajor:  3,
			protoMinor:  0,
			wantOutcome: shadowmw.OutcomeSkipped,
			wantReason:  shadowmw.ReasonUnsupportedExchange,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 2)
			engine := newAcceptanceEngine(t, 1, outcomes)
			var authoritativeCalls atomic.Int32
			var shadowCalls atomic.Int32
			response := func(calls *atomic.Int32) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					w.WriteHeader(http.StatusNoContent)
				})
			}
			cfg := shadowmw.DefaultMiddlewareConfig()
			cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			handler := newAcceptanceMiddleware(
				t,
				engine,
				response(&authoritativeCalls),
				cfg,
			).Wrap(response(&shadowCalls))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Proto = test.proto
			request.ProtoMajor = test.protoMajor
			request.ProtoMinor = test.protoMinor

			handler.ServeHTTP(httptest.NewRecorder(), request)

			if authoritativeCalls.Load() != 1 {
				t.Errorf("authoritative calls = %d, want 1", authoritativeCalls.Load())
			}
			outcome := waitForOutcome(t, outcomes)
			if outcome.Classification() != test.wantOutcome || outcome.Reason() != test.wantReason {
				t.Errorf("outcome = %s/%q, want %s/%q",
					outcome.Classification(),
					outcome.Reason(),
					test.wantOutcome,
					test.wantReason,
				)
			}
			if got := shadowCalls.Load() > 0; got != test.wantShadow {
				t.Errorf("shadow called = %v, want %v", got, test.wantShadow)
			}
		})
	}
}

func assertNoOutcome(t *testing.T, outcomes <-chan shadowmw.Outcome) {
	t.Helper()
	select {
	case outcome := <-outcomes:
		t.Fatalf("unexpected extra outcome = %s/%q", outcome.Classification(), outcome.Reason())
	case <-time.After(100 * time.Millisecond):
	}
}

func TestMiddlewareReleasesComparisonAtSSEResponseCommitWhileAuthoritativeStreams(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 4)
	engine := newAcceptanceEngine(t, 1, outcomes)
	sseAuthoritativeEntered := make(chan struct{})
	allowSSECommit := make(chan struct{})
	var commitOnce sync.Once
	commitSSE := func() {
		commitOnce.Do(func() {
			close(allowSSECommit)
		})
	}
	defer commitSSE()
	sseCommitted := make(chan struct{})
	releaseSSE := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseSSE)
		})
	}
	defer release()
	sseReturned := make(chan struct{})
	var sseShadowCalls atomic.Int32
	sseShadowStarted := make(chan struct{})
	sseShadowCanceled := make(chan struct{})
	finiteShadow := make(chan struct{})

	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/events" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		close(sseAuthoritativeEntered)
		<-allowSSECommit
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("authoritative response writer does not support flush")
			return
		}
		flusher.Flush()
		close(sseCommitted)
		<-releaseSSE
		_, _ = w.Write([]byte("data: still streaming\n\n"))
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/events" {
			sseShadowCalls.Add(1)
			close(sseShadowStarted)
			<-request.Context().Done()
			if request.Context().Err() != context.Canceled {
				t.Errorf("SSE shadow context error = %v, want context canceled", request.Context().Err())
			}
			close(sseShadowCanceled)
			return
		}
		close(finiteShadow)
		w.WriteHeader(http.StatusNoContent)
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
	sseRecorder := httptest.NewRecorder()
	go func() {
		handler.ServeHTTP(
			sseRecorder,
			httptest.NewRequest(http.MethodGet, "/events", nil),
		)
		close(sseReturned)
	}()

	select {
	case <-sseAuthoritativeEntered:
	case <-time.After(time.Second):
		t.Fatal("authoritative SSE handler did not start")
	}
	select {
	case <-sseShadowStarted:
	case <-time.After(time.Second):
		t.Error("shadow did not begin before response-declared SSE was knowable")
	}
	commitSSE()
	select {
	case <-sseCommitted:
	case <-time.After(time.Second):
		t.Fatal("authoritative SSE response did not commit")
	}
	skip := waitForOutcome(t, outcomes)
	if skip.Classification() != shadowmw.OutcomeSkipped ||
		skip.Reason() != shadowmw.ReasonUnsupportedExchange {
		t.Fatalf("SSE outcome = %s/%q, want skipped/unsupported_exchange",
			skip.Classification(),
			skip.Reason(),
		)
	}
	if got := sseShadowCalls.Load(); got != 1 {
		t.Fatalf("SSE shadow calls = %d, want 1 begun before response commitment", got)
	}
	select {
	case <-sseShadowCanceled:
	case <-time.After(time.Second):
		t.Fatal("response-declared SSE did not cancel the running shadow")
	}

	handler.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "/finite", nil),
	)
	select {
	case <-finiteShadow:
	case <-time.After(time.Second):
		t.Fatal("comparison capacity was not released while SSE handler remained active")
	}
	finite := waitForOutcome(t, outcomes)
	if finite.Classification() != shadowmw.OutcomeMatch {
		t.Fatalf("finite outcome = %s/%q, want match", finite.Classification(), finite.Reason())
	}

	release()
	select {
	case <-sseReturned:
	case <-time.After(time.Second):
		t.Fatal("authoritative SSE handler did not continue and return")
	}
	if got := sseRecorder.Body.String(); got != "data: still streaming\n\n" {
		t.Errorf("authoritative SSE body = %q, want continued stream data", got)
	}
	assertNoOutcome(t, outcomes)
}

func TestMiddlewareLateSSEResponseDoesNotReplaceSessionTimeout(t *testing.T) {
	outcomes := make(chan shadowmw.Outcome, 4)
	engineConfig := shadowmw.DefaultEngineConfig()
	engineConfig.ComparisonWorkers = 1
	engineConfig.MaxInFlightSessions = 1
	engineConfig.SessionTimeout = 250 * time.Millisecond
	engineConfig.Observer = &shadowmw.ObserverDelivery{
		Observer: acceptanceObserver(func(_ context.Context, outcome shadowmw.Outcome) error {
			outcomes <- outcome
			return nil
		}),
		QueueCapacity:  4,
		MaxQueuedBytes: 1 << 20,
		Workers:        1,
		Timeout:        time.Second,
		Filter: shadowmw.ObserveMatches |
			shadowmw.ObserveMismatches |
			shadowmw.ObserveSkipped,
		Detail: shadowmw.OutcomeFull,
	}
	engine, err := shadowmw.NewEngine(engineConfig)
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

	authoritativeStarted := make(chan struct{})
	allowSSECommit := make(chan struct{})
	var commitOnce sync.Once
	commitSSE := func() {
		commitOnce.Do(func() {
			close(allowSSECommit)
		})
	}
	defer commitSSE()
	shadowStarted := make(chan struct{})
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(authoritativeStarted)
		<-allowSSECommit
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
	})
	replacement := http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(shadowStarted)
		<-request.Context().Done()
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
	returned := make(chan struct{})
	go func() {
		handler.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, "/events", nil),
		)
		close(returned)
	}()

	select {
	case <-authoritativeStarted:
	case <-time.After(time.Second):
		t.Fatal("authoritative handler did not start")
	}
	select {
	case <-shadowStarted:
	case <-time.After(time.Second):
		t.Fatal("shadow handler did not start")
	}
	timeout := waitForOutcome(t, outcomes)
	if timeout.Classification() != shadowmw.OutcomeMismatch ||
		timeout.Reason() != shadowmw.ReasonSessionTimeout {
		t.Fatalf("first outcome = %s/%q, want mismatch/session_timeout",
			timeout.Classification(),
			timeout.Reason(),
		)
	}

	commitSSE()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("authoritative SSE handler did not return")
	}
	assertNoOutcome(t, outcomes)
}

func TestMiddlewareStartsBranchesConcurrentlyForLatencyObservation(t *testing.T) {
	engine := newAcceptanceEngine(t, 1, nil)
	authoritativeEntered := make(chan struct{})
	allowCommit := make(chan struct{})
	var allowOnce sync.Once
	commit := func() {
		allowOnce.Do(func() {
			close(allowCommit)
		})
	}
	defer commit()
	shadowStarted := make(chan struct{})
	releaseShadow := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseShadow)
		})
	}
	defer release()

	authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(authoritativeEntered)
		<-allowCommit
		w.WriteHeader(http.StatusNoContent)
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(shadowStarted)
		<-releaseShadow
		w.WriteHeader(http.StatusNoContent)
	})
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	handler := newAcceptanceMiddleware(t, engine, authoritative, cfg).Wrap(replacement)
	returned := make(chan struct{})
	go func() {
		handler.ServeHTTP(
			httptest.NewRecorder(),
			httptest.NewRequest(http.MethodGet, "/finite", nil),
		)
		close(returned)
	}()

	select {
	case <-authoritativeEntered:
	case <-time.After(time.Second):
		t.Fatal("authoritative handler did not start")
	}
	select {
	case <-shadowStarted:
	case <-time.After(time.Second):
		t.Error("shadow handler did not start while authoritative response was uncommitted")
	}

	commit()
	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("finite authoritative response waited for shadow completion")
	}
	release()
}

type tracingSpanKey struct{}

type parentRecordingTracing struct {
	mu      sync.Mutex
	parents map[string]string
}

func (t *parentRecordingTracing) Start(
	ctx context.Context,
	start shadowmw.SpanStart,
) (context.Context, shadowmw.Span) {
	parent, _ := ctx.Value(tracingSpanKey{}).(string)
	t.mu.Lock()
	t.parents[start.Name] = parent
	t.mu.Unlock()
	return context.WithValue(ctx, tracingSpanKey{}, start.Name), tracingTestSpan{}
}

func (t *parentRecordingTracing) parent(name string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.parents[name]
}

type tracingTestSpan struct{}

func (tracingTestSpan) AddEvent(string, ...shadowmw.Attribute) {}
func (tracingTestSpan) End()                                   {}

type authoritativeValueKey struct{}

func TestMiddlewareAuthoritativeSpanIsSessionChildWithoutChangingRequestContextSemantics(
	t *testing.T,
) {
	tracing := &parentRecordingTracing{parents: make(map[string]string)}
	engineConfig := shadowmw.DefaultEngineConfig()
	engineConfig.ComparisonWorkers = 1
	engineConfig.SessionTimeout = 250 * time.Millisecond
	engine, err := shadowmw.NewEngine(
		engineConfig,
		shadowmw.WithTracing(tracing),
	)
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
		"middleware.tracing",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}

	authoritativeEntered := make(chan struct{})
	type cancellationResult struct {
		err   error
		cause error
	}
	authoritativeResult := make(chan cancellationResult, 1)
	originalDeadline := time.Now().Add(5 * time.Second)
	authoritative := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := request.Context().Value(authoritativeValueKey{}); got != "preserved" {
			t.Errorf("authoritative context value = %v, want preserved", got)
		}
		if got := request.Context().Value(tracingSpanKey{}); got != "shadowmw.authoritative" {
			t.Errorf("active authoritative span = %v, want shadowmw.authoritative", got)
		}
		if got := shadowmw.BranchFromContext(request.Context()); got != shadowmw.BranchAuthoritative {
			t.Errorf("authoritative branch = %v, want authoritative", got)
		}
		deadline, ok := request.Context().Deadline()
		if !ok || !deadline.Equal(originalDeadline) {
			t.Errorf("authoritative deadline = %v/%v, want %v/true", deadline, ok, originalDeadline)
		}
		close(authoritativeEntered)
		<-request.Context().Done()
		authoritativeResult <- cancellationResult{
			err:   request.Context().Err(),
			cause: context.Cause(request.Context()),
		}
		w.WriteHeader(http.StatusNoContent)
	})
	replacement := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
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

	base := context.WithValue(context.Background(), tracingSpanKey{}, "incoming")
	base = context.WithValue(base, authoritativeValueKey{}, "preserved")
	requestLifetime, cancelRequest := context.WithCancelCause(base)
	requestContext, cancelDeadline := context.WithDeadline(requestLifetime, originalDeadline)
	defer cancelDeadline()
	request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(requestContext)
	returned := make(chan struct{})
	go func() {
		middleware.Wrap(replacement).ServeHTTP(httptest.NewRecorder(), request)
		close(returned)
	}()

	select {
	case <-authoritativeEntered:
	case <-time.After(time.Second):
		t.Fatal("authoritative handler did not start")
	}
	wantCause := errors.New("client disconnected")
	cancelRequest(wantCause)

	select {
	case got := <-authoritativeResult:
		if got.err != context.Canceled {
			t.Errorf("authoritative cancellation error = %v, want context canceled", got.err)
		}
		if !errors.Is(got.cause, wantCause) {
			t.Errorf("authoritative cancellation cause = %v, want %v", got.cause, wantCause)
		}
	case <-time.After(time.Second):
		t.Fatal("authoritative request did not observe inbound cancellation")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("middleware did not return after authoritative cancellation")
	}

	if got := tracing.parent("shadowmw.session"); got != "incoming" {
		t.Errorf("session span parent = %q, want incoming", got)
	}
	if got := tracing.parent("shadowmw.authoritative"); got != "shadowmw.session" {
		t.Errorf("authoritative span parent = %q, want shadowmw.session", got)
	}
}

type mutationValidationObserver func(context.Context, shadowmw.Outcome) error

func (f mutationValidationObserver) Observe(
	ctx context.Context,
	outcome shadowmw.Outcome,
) error {
	return f(ctx, outcome)
}

type validationModeKey struct{}

func TestMutationValidationComparesResponsesWithoutShadowSideEffects(t *testing.T) {
	tests := []struct {
		name                 string
		requestBody          string
		wantClientBody       string
		wantShadowBody       string
		wantShadowStatus     int
		wantOutcome          shadowmw.OutcomeClassification
		wantReason           string
		wantStatusDifference bool
	}{
		{
			name:             "valid mutation matches",
			requestBody:      `{"name":"accepted"}`,
			wantClientBody:   `{"id":"authoritative-1"}`,
			wantShadowBody:   `{"validation":"accepted"}`,
			wantShadowStatus: http.StatusCreated,
			wantOutcome:      shadowmw.OutcomeMatch,
			wantReason:       shadowmw.ReasonParity,
		},
		{
			name:                 "shadow validation rejection mismatches",
			requestBody:          `{"name":"shadow-reject"}`,
			wantClientBody:       `{"id":"authoritative-1"}`,
			wantShadowBody:       `{"code":"shadow_validation_rejected"}`,
			wantShadowStatus:     http.StatusUnprocessableEntity,
			wantOutcome:          shadowmw.OutcomeMismatch,
			wantReason:           shadowmw.ReasonDifference,
			wantStatusDifference: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(chan shadowmw.Outcome, 1)
			engineConfig := shadowmw.DefaultEngineConfig()
			engineConfig.ComparisonWorkers = 1
			engineConfig.Observer = &shadowmw.ObserverDelivery{
				Observer: mutationValidationObserver(func(
					_ context.Context,
					outcome shadowmw.Outcome,
				) error {
					outcomes <- outcome
					return nil
				}),
				QueueCapacity:  1,
				MaxQueuedBytes: 1 << 20,
				Workers:        1,
				Timeout:        time.Second,
				Filter: shadowmw.ObserveMatches |
					shadowmw.ObserveMismatches,
				Detail: shadowmw.OutcomeFull,
			}
			engine, err := shadowmw.NewEngine(engineConfig)
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
				"orders.create.validation",
				shadowmw.DecisionParity(),
				shadowmw.MatchMethods(http.MethodPost),
				shadowmw.MatchPath("/orders"),
				shadowmw.SampleAll(),
			)
			if err != nil {
				t.Fatalf("NewRule() error = %v", err)
			}

			type order struct {
				Name string `json:"name"`
			}
			type shadowObservation struct {
				branch         shadowmw.Branch
				validationMode string
			}

			var authoritativeSideEffects atomic.Int32
			authoritative := http.HandlerFunc(func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				var submitted order
				if err := json.NewDecoder(request.Body).Decode(&submitted); err != nil {
					http.Error(writer, "invalid request", http.StatusBadRequest)
					return
				}
				if submitted.Name == "" {
					http.Error(writer, "name is required", http.StatusUnprocessableEntity)
					return
				}

				id := authoritativeSideEffects.Add(1)
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusCreated)
				_, _ = fmt.Fprintf(writer, `{"id":"authoritative-%d"}`, id)
			})

			shadowObserved := make(chan shadowObservation, 1)
			var shadowSideEffects atomic.Int32
			replacement := http.HandlerFunc(func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				branch := shadowmw.BranchFromContext(request.Context())
				validationMode, _ := request.Context().Value(validationModeKey{}).(string)
				shadowObserved <- shadowObservation{
					branch:         branch,
					validationMode: validationMode,
				}

				var submitted order
				if err := json.NewDecoder(request.Body).Decode(&submitted); err != nil {
					http.Error(writer, "invalid request", http.StatusBadRequest)
					return
				}
				if submitted.Name == "" || submitted.Name == "shadow-reject" {
					writer.Header().Set("Content-Type", "application/json")
					writer.WriteHeader(http.StatusUnprocessableEntity)
					_, _ = writer.Write([]byte(`{"code":"shadow_validation_rejected"}`))
					return
				}
				if branch == shadowmw.BranchShadow {
					writer.Header().Set("Content-Type", "application/json")
					writer.WriteHeader(http.StatusCreated)
					_, _ = writer.Write([]byte(`{"validation":"accepted"}`))
					return
				}

				shadowSideEffects.Add(1)
				writer.WriteHeader(http.StatusCreated)
			})

			middlewareConfig := shadowmw.DefaultMiddlewareConfig()
			middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
			middlewareConfig.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
				func(request *http.Request) (*http.Request, error) {
					ctx := context.WithValue(
						request.Context(),
						validationModeKey{},
						"load-test",
					)
					return request.WithContext(ctx), nil
				},
			}
			middleware, err := shadowmw.NewMiddleware(
				engine,
				authoritative,
				middlewareConfig,
				rule,
			)
			if err != nil {
				t.Fatalf("NewMiddleware() error = %v", err)
			}

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(
				http.MethodPost,
				"/orders",
				strings.NewReader(test.requestBody),
			)
			request.Header.Set("Content-Type", "application/json")
			middleware.Wrap(replacement).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusCreated {
				t.Fatalf("client status = %d, want %d", recorder.Code, http.StatusCreated)
			}
			if got := recorder.Body.String(); got != test.wantClientBody {
				t.Fatalf("client body = %q, want %q", got, test.wantClientBody)
			}

			select {
			case observed := <-shadowObserved:
				if observed.branch != shadowmw.BranchShadow {
					t.Errorf("replacement branch = %v, want %v", observed.branch, shadowmw.BranchShadow)
				}
				if observed.validationMode != "load-test" {
					t.Errorf("validation mode = %q, want %q", observed.validationMode, "load-test")
				}
			case <-time.After(time.Second):
				t.Fatal("shadow replacement was not executed")
			}

			var outcome shadowmw.Outcome
			select {
			case outcome = <-outcomes:
			case <-time.After(time.Second):
				t.Fatal("observer did not receive a terminal comparison outcome")
			}

			if got := authoritativeSideEffects.Load(); got != 1 {
				t.Errorf("authoritative side effects = %d, want 1", got)
			}
			if got := shadowSideEffects.Load(); got != 0 {
				t.Errorf("shadow side effects = %d, want 0", got)
			}
			if outcome.Classification() != test.wantOutcome {
				t.Errorf("outcome classification = %v, want %v", outcome.Classification(), test.wantOutcome)
			}
			if outcome.Reason() != test.wantReason {
				t.Errorf("outcome reason = %q, want %q", outcome.Reason(), test.wantReason)
			}
			if outcome.AuthoritativeStatusCode() != http.StatusCreated {
				t.Errorf(
					"authoritative outcome status = %d, want %d",
					outcome.AuthoritativeStatusCode(),
					http.StatusCreated,
				)
			}
			if outcome.ShadowStatusCode() != test.wantShadowStatus {
				t.Errorf(
					"shadow outcome status = %d, want %d",
					outcome.ShadowStatusCode(),
					test.wantShadowStatus,
				)
			}
			if got := string(outcome.AuthoritativeResponse().Body()); got != test.wantClientBody {
				t.Errorf("observed authoritative body = %q, want %q", got, test.wantClientBody)
			}
			if got := string(outcome.ShadowResponse().Body()); got != test.wantShadowBody {
				t.Errorf("observed shadow body = %q, want %q", got, test.wantShadowBody)
			}

			hasStatusDifference := false
			for _, difference := range outcome.Differences() {
				if difference.Dimension == "status" {
					hasStatusDifference = true
					break
				}
			}
			if hasStatusDifference != test.wantStatusDifference {
				t.Errorf(
					"status difference present = %v, want %v; differences = %+v",
					hasStatusDifference,
					test.wantStatusDifference,
					outcome.Differences(),
				)
			}
		})
	}
}

func TestResponseDeclaredSSESkipsOutsideAcceptedDenominator(t *testing.T) {
	const ruleName = "response.sse.denominator"

	releaseShadow := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseShadow) })
	}

	outcomes := make(chan shadowmw.Outcome, 2)
	measurements := &measurementRecorder{notify: make(chan struct{}, 128)}
	cfg := observerEngineConfig(acceptanceObserver(func(
		_ context.Context,
		outcome shadowmw.Outcome,
	) error {
		outcomes <- outcome
		return nil
	}))
	engine, err := shadowmw.NewEngine(
		cfg,
		shadowmw.WithInstrumentation(measurements),
	)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	var shutdownOnce sync.Once
	var shutdownErr error
	shutdown := func() error {
		shutdownOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			shutdownErr = engine.Shutdown(ctx)
		})
		return shutdownErr
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		release()
		if err := shutdown(); err != nil {
			t.Errorf("cleanup Shutdown() error = %v", err)
		}
	})

	shadowStarted := make(chan struct{})
	shadowReturned := make(chan struct{})

	authoritative := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		<-shadowStarted
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("data: authoritative\n\n"))
	})
	shadow := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(shadowStarted)
		<-releaseShadow
		close(shadowReturned)
	})
	rule, err := shadowmw.NewRule(
		ruleName,
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

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	if request.Header.Get("Accept") != "" {
		t.Fatalf("request unexpectedly declares SSE: Accept = %q", request.Header.Get("Accept"))
	}
	recorder := httptest.NewRecorder()
	returned := make(chan struct{})
	go func() {
		middleware.Wrap(shadow).ServeHTTP(recorder, request)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("client response waited for blocked shadow")
	}
	if recorder.Code != http.StatusOK ||
		recorder.Header().Get("Content-Type") != "text/event-stream" ||
		recorder.Body.String() != "data: authoritative\n\n" {
		t.Fatalf(
			"client response = %d %q %q, want prompt authoritative SSE response",
			recorder.Code,
			recorder.Header().Get("Content-Type"),
			recorder.Body.String(),
		)
	}

	outcome := waitForOutcome(t, outcomes)
	if outcome.RuleName() != ruleName ||
		outcome.Classification() != shadowmw.OutcomeSkipped ||
		outcome.Reason() != shadowmw.ReasonUnsupportedExchange {
		t.Fatalf(
			"outcome = %q %s/%q, want %q skipped/unsupported_exchange",
			outcome.RuleName(),
			outcome.Classification(),
			outcome.Reason(),
			ruleName,
		)
	}

	release()
	select {
	case <-shadowReturned:
	case <-time.After(time.Second):
		t.Fatal("shadow did not return after release")
	}
	if err := shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	accepted, skipped, comparisons, otherTerminals := 0, 0, 0, 0
	for _, measurement := range measurements.snapshot() {
		attributes := taxonomyAttributes(measurement)
		if attributes["rule"] != ruleName {
			continue
		}
		switch measurement.Name {
		case shadowmw.MetricSessions:
			switch attributes["result"] {
			case "accepted":
				accepted++
			case shadowmw.ReasonUnsupportedExchange:
				if measurement.Value != 1 {
					t.Errorf("skipped session counter value = %v, want 1", measurement.Value)
				}
				skipped++
			default:
				otherTerminals++
			}
		case shadowmw.MetricComparisons:
			comparisons++
		}
	}
	if accepted != 0 || skipped != 1 || comparisons != 0 || otherTerminals != 0 {
		t.Fatalf(
			"terminal metrics = accepted:%d skipped:%d comparisons:%d other:%d, want 0/1/0/0",
			accepted,
			skipped,
			comparisons,
			otherTerminals,
		)
	}
	select {
	case duplicate := <-outcomes:
		t.Fatalf(
			"duplicate outcome = %q %s/%q",
			duplicate.RuleName(),
			duplicate.Classification(),
			duplicate.Reason(),
		)
	default:
	}
}
