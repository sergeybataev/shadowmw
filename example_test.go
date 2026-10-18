package shadowmw_test

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/sergeybataev/shadowmw"
)

type exampleCorrelationKey struct{}

type exampleTraceMetadataKey struct{}

type exampleActiveTraceKey struct{}

type exampleTraceMetadata struct {
	branch string
	route  string
}

// exampleTracing represents a service tracing bridge. An OpenTelemetry service
// would normally use contrib/otel instead.
type exampleTracing struct{}

func (exampleTracing) Start(
	ctx context.Context,
	start shadowmw.SpanStart,
) (context.Context, shadowmw.Span) {
	metadata, _ := ctx.Value(exampleTraceMetadataKey{}).(exampleTraceMetadata)
	parentTrace, _ := ctx.Value(exampleActiveTraceKey{}).(string)
	activeTrace := start.Name
	if parentTrace != "" {
		activeTrace = parentTrace + "/" + activeTrace
	}
	if metadata.route != "" {
		activeTrace += "@" + metadata.route + ":" + metadata.branch
	}
	return context.WithValue(ctx, exampleActiveTraceKey{}, activeTrace), exampleSpan{}
}

type exampleSpan struct{}

func (exampleSpan) AddEvent(string, ...shadowmw.Attribute) {}
func (exampleSpan) End()                                   {}

func Example_responseParity() {
	engineConfig := shadowmw.DefaultEngineConfig()
	engineConfig.ComparisonWorkers = 2
	engine, err := shadowmw.NewEngine(engineConfig)
	if err != nil {
		panic(err)
	}
	if err := engine.Start(context.Background()); err != nil {
		panic(err)
	}
	defer engine.Shutdown(context.Background())

	rule, err := shadowmw.NewRule(
		"users.get",
		shadowmw.ResponseParity(
			shadowmw.IgnoreHeaders("Date"),
			shadowmw.IgnoreJSONPointers("/generated_at"),
		),
		shadowmw.MatchMethods(http.MethodGet),
		shadowmw.MatchPathPrefix("/users/"),
		shadowmw.SamplePercent(10),
	)
	if err != nil {
		panic(err)
	}

	authoritative := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":42}`))
	})
	replacement := authoritative
	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middleware, err := shadowmw.NewMiddleware(
		engine,
		authoritative,
		middlewareConfig,
		rule,
	)
	if err != nil {
		panic(err)
	}
	_ = middleware.Wrap(replacement)
}

func ExampleBranchFromContext() {
	var validateAndCreate = func(w http.ResponseWriter, request *http.Request) {
		if shadowmw.BranchFromContext(request.Context()) == shadowmw.BranchShadow {
			w.WriteHeader(http.StatusCreated)
			return
		}
		// Persist only on the authoritative branch.
		w.WriteHeader(http.StatusCreated)
	}
	_ = http.HandlerFunc(validateAndCreate)
}

func ExampleRequestCustomizations() {
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	cfg.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
		func(request *http.Request) (*http.Request, error) {
			ctx := context.WithValue(request.Context(), exampleCorrelationKey{}, "migration")
			copy := request.Clone(ctx)
			copy.Header.Set("X-Validation-Mode", "shadow")
			return copy, nil
		},
	}
}

func ExampleRequestCustomizations_tracingPropagation() {
	traceHook := func(branch string) shadowmw.RequestHook {
		return func(request *http.Request) (*http.Request, error) {
			metadata := exampleTraceMetadata{
				branch: branch,
				route:  "orders-migration",
			}
			ctx := context.WithValue(
				request.Context(),
				exampleTraceMetadataKey{},
				metadata,
			)
			copy := request.Clone(ctx)
			copy.Header.Set("X-Trace-Branch", branch)
			return copy, nil
		}
	}

	middlewareConfig := shadowmw.DefaultMiddlewareConfig()
	middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	middlewareConfig.Customizations.Authoritative.RequestHooks = []shadowmw.RequestHook{
		traceHook("authoritative"),
	}
	middlewareConfig.Customizations.Shadow.RequestHooks = []shadowmw.RequestHook{
		traceHook("shadow"),
	}

	engineConfig := shadowmw.DefaultEngineConfig()
	engineConfig.ComparisonWorkers = 2
	engine, err := shadowmw.NewEngine(
		engineConfig,
		shadowmw.WithTracing(exampleTracing{}),
	)
	if err != nil {
		panic(err)
	}

	// Request hooks run before branch-span creation. The context returned by
	// Tracing.Start is then installed on the request passed to each handler, so
	// downstream propagation sees the active authoritative or shadow span.
	handler := http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		downstream, err := http.NewRequestWithContext(
			request.Context(),
			http.MethodGet,
			"https://service.example/orders/42",
			nil,
		)
		if err != nil {
			panic(err)
		}
		activeTrace, _ := request.Context().Value(exampleActiveTraceKey{}).(string)
		downstream.Header.Set("X-Service-Trace", activeTrace)
		_ = downstream
	})

	rule, err := shadowmw.NewRule(
		"orders.get",
		shadowmw.ResponseParity(),
		shadowmw.MatchMethods(http.MethodGet),
		shadowmw.MatchPathPrefix("/orders/"),
		shadowmw.SampleAll(),
	)
	if err != nil {
		panic(err)
	}
	_, err = shadowmw.NewMiddleware(engine, handler, middlewareConfig, rule)
	if err != nil {
		panic(err)
	}
}

func ExampleDecisionParity() {
	extract := func(field string) shadowmw.ErrorExtractor {
		return func(
			_ context.Context,
			response shadowmw.ResponseSnapshot,
		) (string, bool, error) {
			var envelope map[string]string
			if err := json.Unmarshal(response.Body(), &envelope); err != nil {
				return "", false, err
			}
			code, found := envelope[field]
			return code, found, nil
		}
	}
	policy := shadowmw.DecisionParity(
		shadowmw.WithRejectedErrorExtractors(
			extract("legacy_code"),
			extract("replacement_code"),
		),
		shadowmw.CompareRejectedByExtractedCode(),
	)
	_, _ = shadowmw.NewRule(
		"orders.create",
		policy,
		shadowmw.MatchMethods(http.MethodPost),
		shadowmw.MatchPath("/orders"),
		shadowmw.SampleAll(),
	)
}
