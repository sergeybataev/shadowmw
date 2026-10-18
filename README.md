# shadowmw

`shadowmw` is a Go 1.26 HTTP library for measuring behavioral parity while an
existing implementation remains authoritative. The authoritative response is
streamed to the client; the replacement runs as a shadow and must not perform
production mutations.

## Install

```sh
go get github.com/sergeybataev/shadowmw
```

The root module has no OpenTelemetry or Fx dependencies. Optional adapters are
independent modules:

```sh
go get github.com/sergeybataev/shadowmw/contrib/otel
go get github.com/sergeybataev/shadowmw/contrib/fx
```

## Lifecycle and response parity

An engine is service-scoped and must be started before middleware use.
`ComparisonWorkers` must be chosen explicitly. `DefaultEngineConfig` supplies
128 maximum in-flight sessions, a five-second session timeout, and a one-second
comparison timeout. `DefaultMiddlewareConfig` supplies 1 MiB request and
response capture limits, while body-read failure handling remains explicit.
Observer delivery is optional; when enabled, its queue capacity, byte budget,
worker count, timeout, filter, and detail policy are all explicit. Core
telemetry uses an internal queue derived from the in-flight-session bound.
Collaborator execution has a fixed 100 ms watchdog that starts only after the
callback goroutine begins running; time waiting in the telemetry queue or for
the goroutine to be scheduled is not charged to that execution budget.

```go
cfg := shadowmw.DefaultEngineConfig()
cfg.ComparisonWorkers = 4
cfg.MaxInFlightSessions = 128

engine, err := shadowmw.NewEngine(cfg)
if err != nil {
	log.Fatal(err)
}
if err := engine.Start(context.Background()); err != nil {
	log.Fatal(err)
}
defer engine.Shutdown(context.Background())

rule, err := shadowmw.NewRule(
	"users.get",
	shadowmw.ResponseParity(),
	shadowmw.MatchMethods(http.MethodGet),
	shadowmw.MatchPathPrefix("/users/"),
	shadowmw.SamplePercent(10),
)
if err != nil {
	log.Fatal(err)
}

middlewareConfig := shadowmw.DefaultMiddlewareConfig()
middlewareConfig.BodyReadErrors = shadowmw.RejectBodyReadErrors()
middleware, err := shadowmw.NewMiddleware(
	engine,
	authoritativeHandler,
	middlewareConfig,
	rule,
)
if err != nil {
	log.Fatal(err)
}
handler := middleware.Wrap(replacementHandler)
```

`MaxInFlightSessions` bounds accepted sessions, including pending, queued, and
active comparisons. `ComparisonWorkers` bounds comparison concurrency and has
no implicit default. `MaxRequestBodyBytes` and `MaxResponseBodyBytes` default
to 1 MiB; oversized requests run authoritative-only, while truncated admitted
responses produce a mismatch. A body-read failure policy is mandatory.
`Shutdown` stops new admission, drains accepted work, is idempotent, and returns
the caller's context error if its shutdown deadline expires.

Rules use declaration-order first match. Matcher dimensions are ANDed, method
alternatives are ORed, and exactly one explicit sampling policy is required.
Matched but unsampled, unsupported, oversized, capacity-shed, or unavailable
requests run authoritative-only and are classified as skips outside the
accepted-comparison denominator.

For admitted HTTP/1.x samples, authoritative and shadow branches execute in
parallel. The client receives authoritative streaming without waiting for the
shadow. `shadowmw.branch.duration` records each branch's latency independently;
latency is observational and does not affect parity without a future explicit
latency policy.

HTTP/2 and later requests, WebSocket upgrades, and request-declared
Server-Sent Events run authoritative-only with the bounded
`unsupported_exchange` skip reason. Response-declared SSE is only knowable when
the authoritative response commits, so the concurrent shadow may already have
started; it is then cancelled and the outcome is skipped. Generic undeclared
indefinite HTTP/1.x exchanges are not preemptively skipped and remain bounded
by the configured session timeout.

Response parity is strict by default: status, committed headers, trailers, and
the codec-selected body are compared. Exceptions are opt-in:

- `DisableHeaderComparison`, `IgnoreHeaders`, `CompareOnlyHeaders`, and
  `SortHeaderValues`
- `DisableBodyComparison`, `CompareBodyAsJSON`, `CompareBodyAsForm`,
  `CompareBodyAsBytes`, and `CompareBodyWith`
- `IgnoreJSONPointers`, `IgnoreJSONFieldNames`, `EquateMissingWithEmptyObject`,
  and `SortFormValues`
- `WithContentDecoder`, `WithMaxDecodedBodyBytes`,
  `WithMaxDifferences`, and `WithMaxDifferenceValueBytes`

Decision parity supports explicit `EquivalentStatuses`, optional success
header/body comparison, branch-specific rejection extractors, and
`CompareRejectedByExtractedCode`. Its complete exception surface is
`EquivalentStatuses`, `CompareSuccessfulHeaders`,
`CompareSuccessfulBodies`, `WithDecisionResponseOptions`,
`WithRejectedErrorExtractors`, and `CompareRejectedByExtractedCode`.

## Mutation validation

Reuse the production handler and exit after its real validation path when the
branch is shadow:

```go
func createOrder(w http.ResponseWriter, r *http.Request) {
	order, err := validateOrder(r)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	if shadowmw.BranchFromContext(r.Context()) == shadowmw.BranchShadow {
		w.WriteHeader(http.StatusCreated) // validation succeeded; no mutation
		return
	}
	if err := persistOrder(r.Context(), order); err != nil {
		http.Error(w, "create failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}
```

Inbound headers or context values cannot forge the branch marker. Request hooks
may add branch-specific headers and context metadata, but the library restores
the prepared body and branch identity after each hook:

```go
type traceMetadataKey struct{}

traceHook := func(branch string) shadowmw.RequestHook {
	return func(r *http.Request) (*http.Request, error) {
		metadata := struct {
			Route  string
			Branch string
		}{"orders-migration", branch}
		copy := r.Clone(context.WithValue(r.Context(), traceMetadataKey{}, metadata))
		copy.Header.Set("X-Trace-Branch", branch)
		return copy, nil
	}
}
middlewareConfig.Customizations.Authoritative.RequestHooks =
	[]shadowmw.RequestHook{traceHook("authoritative")}
middlewareConfig.Customizations.Shadow.RequestHooks =
	[]shadowmw.RequestHook{traceHook("shadow")}
```

Hooks run before the branch span is created. A `Tracing.Start` implementation
can read this metadata; shadowmw installs the context returned by
`Tracing.Start` on the request passed to the handler. Downstream clients should
therefore inject trace propagation headers from the handler's active request
context, not from the earlier hook context. The build-checked
[`ExampleRequestCustomizations_tracingPropagation`](example_test.go) shows both
branch-specific trace headers/context metadata and downstream propagation.

## Observers and privacy

Observer delivery is asynchronous, best-effort, count-bounded, byte-bounded,
deadline-bounded, panic-safe, and never retried. Filters may select
classifications, reasons, rule names, and compared dimensions.

```go
cfg.Observer = &shadowmw.ObserverDelivery{
	Observer:       auditObserver,
	QueueCapacity:  256,
	MaxQueuedBytes: 8 << 20,
	Workers:        2,
	Timeout:        250 * time.Millisecond,
	Filter:         shadowmw.ObserveMismatches,
	Reasons:        []string{shadowmw.ReasonDifference},
	Rules:          []string{"users.get"},
	Dimensions:     []string{"status", "body"},
	Detail:         shadowmw.OutcomeDiff,
	DiffContextLines: 3,
}
```

Every observer must explicitly select one detail policy:

- `OutcomeMetadataOnly` removes bodies, header values, compared values, and
  difference values.
- `OutcomeDiff` retains bounded structured differences and a deterministic,
  codec-aware unified rendering; binary data uses a bounded hexadecimal
  summary.
- `OutcomeFull` retains separate bounded captured responses and the separate
  decoded/normalized bodies actually compared.
- `OutcomeCustom` invokes a service renderer under the callback deadline and
  output byte limit.

Full outcomes expose branch representations through
`AuthoritativeBodyRepresentation` and `ShadowBodyRepresentation`.
`BodyDecodingState` distinguishes unavailable, not-attempted, successful, and
failed decoding; `BodyRepresentationKind` distinguishes absent, decoded, and
normalized representations; `Present` distinguishes an empty representation
from no representation. The older compared-body accessors remain available.

`Difference.Kind` remains the stable difference category. Independently bounded
values and omitted extra differences are reported through
`ExpectedTruncated`, `ActualTruncated`, and `DifferencesTruncated` rather than by
modifying `Kind`.

Observer admission counts active and queued deliveries against both item and
byte limits. Cloning, redaction, rendering, and custom preparation run only on
observer dispatch workers, never comparison workers.

Custom policies, transforms, hooks, extractors, observers, instrumentation, and
tracing implementations must be concurrency-safe. Cancellation is cooperative:
callbacks that receive a context must check it, and content decoders must return
and consume input boundedly.

Once a shadow body transform or request hook is admitted, it runs synchronously
under the session continuation gate. It retains session capacity and serializes
terminal timeout publication until it returns; the session context is cancelled
at its deadline, but Go cannot forcibly stop the callback. Observer callbacks
run behind bounded dispatch and shadowmw stops waiting at the configured
observer timeout. Instrumentation, tracing, and logging collaborators use
bounded telemetry dispatch. Their 100 ms execution watchdog starts after the
callback goroutine begins running, separately from queue and scheduler wait.
During shutdown, the engine drains the bounded telemetry queue and stops waiting
for an executing uncooperative collaborator when that watchdog expires. Late
callback completion cannot change the already selected bounded failure
classification.

## Telemetry and lifecycle adapters

The core module exposes the framework-neutral `Instrumentation` and `Tracing`
interfaces. Install service implementations with `WithInstrumentation` and
`WithTracing`; `NoopInstrumentation` and `NoopTracing` preserve the dependency-
free default. The core emits only fixed metric names and bounded,
privacy-preserving attributes.

The optional OpenTelemetry adapter accepts explicit caller-owned
`metric.MeterProvider` and `trace.TracerProvider` values and returns both engine
options through `EngineOptions`. It does not consult globals or configure
exporters, readers, processors, resources, or provider shutdown. The optional
Fx module constructs one engine, forwards grouped `EngineOption` values, and
binds `Engine.Start`/`Engine.Shutdown` to `fx.Lifecycle`; rules, middleware, and
HTTP routing remain application-owned.

Metrics use fixed names and allowlisted attributes. Session IDs, paths, header
values, body values, JSON pointers, request IDs, trace IDs, and raw errors are
never metric attributes. Default logs are quiet for matches, warnings for
behavioral mismatches, and errors for operational failures. Sentry integration
belongs in the application's `slog.Handler`.

Extracted rejection-code metrics are opt-in:

```go
engine, err := shadowmw.NewEngine(
	cfg,
	shadowmw.WithRejectionCodeMetricAllowlist("invalid_email", "duplicate"),
)
```

Only explicitly approved found codes become metric attributes. Missing codes
are `none`; unapproved codes and extractor errors, panics, or cancellation are
`other`. `Engine.TelemetryFailures` exposes bounded operational counts if a
telemetry collaborator panics, times out, or overloads its internal queue.
Observer queue latency is recorded as
`shadowmw.observer.queue.duration`.

See the [architecture walkthrough](docs/ARCHITECTURE.md) and the
[OpenTelemetry](contrib/otel/README.md) and [Fx](contrib/fx/README.md) adapter
documentation.
