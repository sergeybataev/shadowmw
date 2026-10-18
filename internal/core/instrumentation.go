package core

import (
	"context"
	"math"
	"time"

	"github.com/sergeybataev/shadowmw/internal/telemetry"
)

// MetricName identifies one fixed shadowmw measurement.
type MetricName string

// MetricName constants identify the fixed measurements emitted by Engine and
// Middleware.
const (
	MetricSessions              MetricName = "shadowmw.sessions"
	MetricComparisons           MetricName = "shadowmw.comparisons"
	MetricDifferences           MetricName = "shadowmw.differences"
	MetricBranchDuration        MetricName = "shadowmw.branch.duration"
	MetricBodySize              MetricName = "shadowmw.body.size"
	MetricSessionsActive        MetricName = "shadowmw.sessions.active"
	MetricComparisonQueueTime   MetricName = "shadowmw.comparison.queue.duration"
	MetricObserverDeliveries    MetricName = "shadowmw.observer.deliveries"
	MetricObserverQueueItems    MetricName = "shadowmw.observer.queue.items"
	MetricObserverQueueBytes    MetricName = "shadowmw.observer.queue.bytes"
	MetricObserverQueueDuration MetricName = "shadowmw.observer.queue.duration"
	MetricRejectionCodes        MetricName = "shadowmw.rejection.codes"
)

// MetricKind describes how a fixed measurement is aggregated.
type MetricKind uint8

// MetricKind values classify the aggregation instrument for a MetricName.
const (
	MetricCounter MetricKind = iota + 1
	MetricHistogram
	MetricGauge
)

// MetricDescriptor is the stable contract for one core measurement.
type MetricDescriptor struct {
	Name MetricName
	Kind MetricKind
	Unit string
}

// Attribute is one bounded telemetry dimension. Core never emits request IDs,
// paths, header values, body values, or raw errors.
type Attribute struct {
	Key   string
	Value string
}

// Measurement is one framework-neutral metric recording.
type Measurement struct {
	Name       MetricName
	Value      float64
	Attributes []Attribute
}

// Instrumentation receives fixed, bounded measurements.
type Instrumentation interface {
	Record(context.Context, Measurement)
}

// SpanStart describes one bounded core span.
type SpanStart struct {
	Name       string
	Attributes []Attribute
}

// Span receives bounded events and a terminal end signal.
type Span interface {
	AddEvent(string, ...Attribute)
	End()
}

// Tracing creates spans without coupling core to a tracing framework.
type Tracing interface {
	Start(context.Context, SpanStart) (context.Context, Span)
}

// TelemetryFailureKind is one bounded operational telemetry failure category.
type TelemetryFailureKind string

// TelemetryFailureKind values classify collaborator panics, timeouts, and drops.
const (
	TelemetryInstrumentationPanic   TelemetryFailureKind = "instrumentation_panic"
	TelemetryInstrumentationTimeout TelemetryFailureKind = "instrumentation_timeout"
	TelemetryInstrumentationDropped TelemetryFailureKind = "instrumentation_dropped"
	TelemetryTracingPanic           TelemetryFailureKind = "tracing_panic"
	TelemetryTracingTimeout         TelemetryFailureKind = "tracing_timeout"
	TelemetryTracingDropped         TelemetryFailureKind = "tracing_dropped"
	TelemetryLoggingPanic           TelemetryFailureKind = "logging_panic"
	TelemetryLoggingTimeout         TelemetryFailureKind = "logging_timeout"
	TelemetryLoggingDropped         TelemetryFailureKind = "logging_dropped"
)

// TelemetryFailureCount is independent, bounded operational evidence for a
// failed, timed-out, or overloaded telemetry collaborator.
type TelemetryFailureCount struct {
	Kind  TelemetryFailureKind
	Count uint64
}

// MeasurementDescriptor returns the fixed aggregation contract for name.
func MeasurementDescriptor(name MetricName) (MetricDescriptor, bool) {
	descriptor, ok := metricDescriptors[name]
	return descriptor, ok
}

// NoopInstrumentation returns an instrumentation sink that discards metrics.
func NoopInstrumentation() Instrumentation { return noopInstrumentation{} }

// ValidMeasurement reports whether a measurement follows the fixed core
// privacy and cardinality contract.
func ValidMeasurement(measurement Measurement) bool {
	if _, ok := metricDescriptors[measurement.Name]; !ok ||
		measurement.Value < 0 ||
		math.IsNaN(measurement.Value) ||
		math.IsInf(measurement.Value, 0) {
		return false
	}
	allowed := allowedMetricAttributes[measurement.Name]
	seen := make(map[string]struct{}, len(measurement.Attributes))
	for _, attribute := range measurement.Attributes {
		if _, ok := allowed[attribute.Key]; !ok ||
			attribute.Value == "" ||
			len(attribute.Value) > 64 {
			return false
		}
		if _, duplicate := seen[attribute.Key]; duplicate {
			return false
		}
		seen[attribute.Key] = struct{}{}
	}
	return true
}

// SanitizeMeasurement copies a valid fixed measurement and collapses
// non-allowlisted attribute values to "other".
func SanitizeMeasurement(measurement Measurement) (Measurement, bool) {
	if !ValidMeasurement(measurement) {
		return Measurement{}, false
	}
	measurement = cloneMeasurement(measurement)
	for i := range measurement.Attributes {
		attribute := &measurement.Attributes[i]
		if attribute.Key == "rule" {
			continue
		}
		allowed, bounded := boundedAttributeValues[attribute.Key]
		if bounded {
			if _, ok := allowed[attribute.Value]; !ok {
				attribute.Value = "other"
			}
		}
	}
	return measurement, true
}

// NoopTracing returns a tracing implementation that preserves contexts.
func NoopTracing() Tracing { return noopTracing{} }

var metricDescriptors = map[MetricName]MetricDescriptor{
	MetricSessions:            {Name: MetricSessions, Kind: MetricCounter, Unit: "{session}"},
	MetricComparisons:         {Name: MetricComparisons, Kind: MetricCounter, Unit: "{comparison}"},
	MetricDifferences:         {Name: MetricDifferences, Kind: MetricCounter, Unit: "{difference}"},
	MetricBranchDuration:      {Name: MetricBranchDuration, Kind: MetricHistogram, Unit: "s"},
	MetricBodySize:            {Name: MetricBodySize, Kind: MetricHistogram, Unit: "By"},
	MetricSessionsActive:      {Name: MetricSessionsActive, Kind: MetricGauge, Unit: "{session}"},
	MetricComparisonQueueTime: {Name: MetricComparisonQueueTime, Kind: MetricHistogram, Unit: "s"},
	MetricObserverDeliveries:  {Name: MetricObserverDeliveries, Kind: MetricCounter, Unit: "{delivery}"},
	MetricObserverQueueItems:  {Name: MetricObserverQueueItems, Kind: MetricGauge, Unit: "{item}"},
	MetricObserverQueueBytes:  {Name: MetricObserverQueueBytes, Kind: MetricGauge, Unit: "By"},
	MetricObserverQueueDuration: {
		Name: MetricObserverQueueDuration, Kind: MetricHistogram, Unit: "s",
	},
	MetricRejectionCodes: {
		Name: MetricRejectionCodes, Kind: MetricCounter, Unit: "{code}",
	},
}

var allowedMetricAttributes = map[MetricName]map[string]struct{}{
	MetricSessions:              attributeSet("rule", "result"),
	MetricComparisons:           attributeSet("rule", "result", "reason"),
	MetricDifferences:           attributeSet("rule", "dimension", "kind"),
	MetricBranchDuration:        attributeSet("rule", "branch"),
	MetricBodySize:              attributeSet("rule", "kind", "branch"),
	MetricSessionsActive:        attributeSet(),
	MetricComparisonQueueTime:   attributeSet("rule"),
	MetricObserverDeliveries:    attributeSet("result"),
	MetricObserverQueueItems:    attributeSet(),
	MetricObserverQueueBytes:    attributeSet(),
	MetricObserverQueueDuration: attributeSet("rule"),
	MetricRejectionCodes:        attributeSet("rule", "branch", "result", "code"),
}

var boundedAttributeValues = map[string]map[string]struct{}{
	"branch":    attributeSet("authoritative", "shadow"),
	"dimension": attributeSet("status", "header", "trailer", "body", "rejection"),
	"kind": attributeSet(
		"request", "response", "different", "extractor_error", "extractor_panic",
		"decode_error", "codec_different", "comparison_incomplete",
		"differences_truncated", "missing", "unexpected", "invalid", "value",
		"type", "custom_comparator_error", "custom_comparator_panic",
		"custom_comparator_canceled",
	),
	"outcome": attributeSet("match", "mismatch", "skipped"),
	"reason": attributeSet(
		ReasonParity, ReasonDifference, ReasonComparisonError, ReasonComparisonPanic,
		ReasonComparisonTimeout, ReasonResponseTruncated, ReasonDecodingFailure,
		ReasonCustomComparatorError, ReasonCustomComparatorPanic,
		ReasonCustomComparatorCanceled, ReasonSessionTimeout, ReasonAuthoritativePanic,
		ReasonAuthoritativeSetupError, ReasonAuthoritativeTransportError,
		ReasonShadowPanic, ReasonShadowSetupError,
		ReasonEngineUnavailable, ReasonCapacity, ReasonNotSampled,
		ReasonUnsupportedExchange, ReasonRequestTooLarge, ReasonBodyReadError,
	),
	"result": attributeSet(
		"accepted", "match", "mismatch", "skipped", "delivered", "failed",
		"panicked", "timed_out", "abandoned", "dropped",
		string(RejectionCodeFound), string(RejectionCodeNotFound),
		string(RejectionCodeExtractorError), string(RejectionCodeExtractorPanic),
		string(RejectionCodeExtractorCanceled),
		ReasonEngineUnavailable, ReasonCapacity, ReasonNotSampled,
		ReasonUnsupportedExchange, ReasonRequestTooLarge, ReasonBodyReadError,
	),
}

var telemetryFailureKinds = [telemetry.FailureCount]TelemetryFailureKind{
	TelemetryInstrumentationPanic,
	TelemetryInstrumentationTimeout,
	TelemetryInstrumentationDropped,
	TelemetryTracingPanic,
	TelemetryTracingTimeout,
	TelemetryTracingDropped,
	TelemetryLoggingPanic,
	TelemetryLoggingTimeout,
	TelemetryLoggingDropped,
}

type noopInstrumentation struct{}

func (noopInstrumentation) Record(context.Context, Measurement) {}

func attributeSet(names ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

func cloneMeasurement(measurement Measurement) Measurement {
	measurement.Attributes = append([]Attribute(nil), measurement.Attributes...)
	return measurement
}

type noopTracing struct{}
type noopSpan struct{}

func (noopTracing) Start(ctx context.Context, _ SpanStart) (context.Context, Span) {
	return ctx, noopSpan{}
}

func (noopSpan) AddEvent(string, ...Attribute) {}
func (noopSpan) End()                          {}

func sanitizeTraceAttributes(attributes []Attribute) []Attribute {
	allowedKeys := attributeSet("rule", "branch", "outcome", "reason", "dimension", "kind")
	result := make([]Attribute, 0, len(attributes))
	for _, attribute := range attributes {
		if _, ok := allowedKeys[attribute.Key]; !ok ||
			attribute.Value == "" ||
			len(attribute.Value) > 64 {
			continue
		}
		if attribute.Key != "rule" {
			if allowed, bounded := boundedAttributeValues[attribute.Key]; bounded {
				if _, ok := allowed[attribute.Value]; !ok {
					attribute.Value = "other"
				}
			}
		}
		result = append(result, attribute)
	}
	return result
}

const telemetryCallbackTimeout = 100 * time.Millisecond

type telemetryRuntime struct {
	instrumentation Instrumentation
	tracing         Tracing
	runtime         *telemetry.Runtime
}

func newTelemetryRuntime(
	instrumentation Instrumentation,
	tracing Tracing,
	capacity int,
) *telemetryRuntime {
	return &telemetryRuntime{
		instrumentation: instrumentation,
		tracing:         tracing,
		runtime:         telemetry.NewRuntime(capacity),
	}
}

func (r *telemetryRuntime) start() {
	r.runtime.Start()
}

func (r *telemetryRuntime) shutdown() {
	if r == nil {
		return
	}
	r.runtime.Shutdown()
}

func (r *telemetryRuntime) record(
	ctx context.Context,
	measurement Measurement,
) {
	measurement, ok := SanitizeMeasurement(measurement)
	if !ok {
		return
	}
	r.runtime.Enqueue(telemetry.Task{
		Kind: telemetry.KindInstrumentation,
		Run: func() {
			r.instrumentation.Record(ctx, measurement)
		},
	})
}

func (r *telemetryRuntime) startSpan(
	ctx context.Context,
	start SpanStart,
) (context.Context, Span) {
	if r == nil || r.runtime.Disabled(telemetry.KindTracing) {
		return ctx, noopSpan{}
	}
	type result struct {
		ctx  context.Context
		span Span
	}
	returned := make(chan result, 1)
	panicked := make(chan struct{}, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		defer func() {
			if recover() != nil {
				panicked <- struct{}{}
			}
		}()
		spanContext, span := r.tracing.Start(ctx, SpanStart{
			Name:       start.Name,
			Attributes: sanitizeTraceAttributes(start.Attributes),
		})
		returned <- result{ctx: spanContext, span: span}
	}()
	<-started
	timer := time.NewTimer(telemetryCallbackTimeout)
	defer timer.Stop()
	select {
	case outcome := <-returned:
		if outcome.ctx == nil || outcome.span == nil {
			r.runtime.Disable(telemetry.KindTracing, telemetry.TracingPanic)
			return ctx, noopSpan{}
		}
		return outcome.ctx, safeSpan{runtime: r, delegate: outcome.span}
	case <-panicked:
		r.runtime.Disable(telemetry.KindTracing, telemetry.TracingPanic)
		return ctx, noopSpan{}
	case <-timer.C:
		r.runtime.Disable(telemetry.KindTracing, telemetry.TracingTimeout)
		return ctx, noopSpan{}
	}
}

func (r *telemetryRuntime) log(run func()) {
	r.runtime.Enqueue(telemetry.Task{Kind: telemetry.KindLogging, Run: run})
}

func (r *telemetryRuntime) failuresSnapshot() []TelemetryFailureCount {
	counts := r.runtime.Failures()
	result := make([]TelemetryFailureCount, 0, len(telemetryFailureKinds))
	for index, kind := range telemetryFailureKinds {
		if count := counts[index]; count != 0 {
			result = append(result, TelemetryFailureCount{Kind: kind, Count: count})
		}
	}
	return result
}

type asyncInstrumentation struct {
	runtime *telemetryRuntime
}

func (a asyncInstrumentation) Record(ctx context.Context, measurement Measurement) {
	a.runtime.record(ctx, measurement)
}

type boundedTracing struct {
	runtime *telemetryRuntime
}

func (t boundedTracing) Start(
	ctx context.Context,
	start SpanStart,
) (context.Context, Span) {
	return t.runtime.startSpan(ctx, start)
}

type safeSpan struct {
	runtime  *telemetryRuntime
	delegate Span
}

func (s safeSpan) AddEvent(name string, attributes ...Attribute) {
	attributes = sanitizeTraceAttributes(attributes)
	s.runtime.runtime.Enqueue(telemetry.Task{
		Kind: telemetry.KindTracing,
		Run: func() {
			s.delegate.AddEvent(name, attributes...)
		},
	})
}

func (s safeSpan) End() {
	s.runtime.runtime.Enqueue(telemetry.Task{
		Kind: telemetry.KindTracing,
		Run:  s.delegate.End,
	})
}
