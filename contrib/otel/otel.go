// Package shadowmwotel adapts shadowmw's framework-neutral telemetry seams
// to caller-owned OpenTelemetry providers.
package shadowmwotel

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/sergeybataev/shadowmw"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/sergeybataev/shadowmw"

// Adapter implements shadowmw.Instrumentation and shadowmw.Tracing without
// consulting or replacing OpenTelemetry global providers.
type Adapter struct {
	meter  metric.Meter
	tracer trace.Tracer

	mu          sync.Mutex
	instruments map[shadowmw.MetricName]instrument
}

type instrument struct {
	counter   metric.Float64Counter
	histogram metric.Float64Histogram
	gauge     *atomic.Uint64
	kind      shadowmw.MetricKind
}

// New constructs an adapter using only the explicit providers.
func New(
	meterProvider metric.MeterProvider,
	tracerProvider trace.TracerProvider,
) (*Adapter, error) {
	if meterProvider == nil {
		return nil, fmt.Errorf("shadowmwotel: meter provider is nil")
	}
	if tracerProvider == nil {
		return nil, fmt.Errorf("shadowmwotel: tracer provider is nil")
	}
	adapter := &Adapter{
		meter:       meterProvider.Meter(instrumentationName),
		tracer:      tracerProvider.Tracer(instrumentationName),
		instruments: make(map[shadowmw.MetricName]instrument),
	}
	for _, name := range []shadowmw.MetricName{
		shadowmw.MetricSessions,
		shadowmw.MetricComparisons,
		shadowmw.MetricDifferences,
		shadowmw.MetricBranchDuration,
		shadowmw.MetricBodySize,
		shadowmw.MetricSessionsActive,
		shadowmw.MetricComparisonQueueTime,
		shadowmw.MetricObserverDeliveries,
		shadowmw.MetricObserverQueueItems,
		shadowmw.MetricObserverQueueBytes,
		shadowmw.MetricObserverQueueDuration,
		shadowmw.MetricRejectionCodes,
	} {
		descriptor, _ := shadowmw.MeasurementDescriptor(name)
		created, err := newInstrument(adapter.meter, descriptor)
		if err != nil {
			return nil, fmt.Errorf("shadowmwotel: create %q: %w", name, err)
		}
		adapter.instruments[name] = created
	}
	return adapter, nil
}

// EngineOptions returns options that install both adapter seams on an Engine.
func (a *Adapter) EngineOptions() []shadowmw.EngineOption {
	return []shadowmw.EngineOption{
		shadowmw.WithInstrumentation(a),
		shadowmw.WithTracing(a),
	}
}

// Record translates one validated fixed measurement to its OTel instrument.
func (a *Adapter) Record(ctx context.Context, measurement shadowmw.Measurement) {
	if a == nil {
		return
	}
	measurement, ok := shadowmw.SanitizeMeasurement(measurement)
	if !ok {
		return
	}
	a.mu.Lock()
	target, ok := a.instruments[measurement.Name]
	a.mu.Unlock()
	if !ok {
		return
	}
	options := metric.WithAttributes(toOTelAttributes(measurement.Attributes)...)
	switch target.kind {
	case shadowmw.MetricCounter:
		target.counter.Add(ctx, measurement.Value, options)
	case shadowmw.MetricHistogram:
		target.histogram.Record(ctx, measurement.Value, options)
	case shadowmw.MetricGauge:
		target.gauge.Store(math.Float64bits(measurement.Value))
	}
}

// Start translates a bounded core span start to an OTel span.
func (a *Adapter) Start(
	ctx context.Context,
	start shadowmw.SpanStart,
) (context.Context, shadowmw.Span) {
	if a == nil {
		return ctx, noopSpan{}
	}
	spanContext, span := a.tracer.Start(
		ctx,
		start.Name,
		trace.WithAttributes(toOTelAttributes(start.Attributes)...),
	)
	return spanContext, otelSpan{span: span}
}

type otelSpan struct {
	span trace.Span
}

func (s otelSpan) AddEvent(name string, attributes ...shadowmw.Attribute) {
	s.span.AddEvent(name, trace.WithAttributes(toOTelAttributes(attributes)...))
}

func (s otelSpan) End() {
	s.span.End()
}

type noopSpan struct{}

func (noopSpan) AddEvent(string, ...shadowmw.Attribute) {}
func (noopSpan) End()                                   {}

func newInstrument(
	meter metric.Meter,
	descriptor shadowmw.MetricDescriptor,
) (instrument, error) {
	options := []metric.Float64CounterOption{metric.WithUnit(descriptor.Unit)}
	switch descriptor.Kind {
	case shadowmw.MetricCounter:
		counter, err := meter.Float64Counter(string(descriptor.Name), options...)
		return instrument{counter: counter, kind: descriptor.Kind}, err
	case shadowmw.MetricHistogram:
		histogram, err := meter.Float64Histogram(
			string(descriptor.Name),
			metric.WithUnit(descriptor.Unit),
		)
		return instrument{histogram: histogram, kind: descriptor.Kind}, err
	case shadowmw.MetricGauge:
		value := &atomic.Uint64{}
		_, err := meter.Float64ObservableGauge(
			string(descriptor.Name),
			metric.WithUnit(descriptor.Unit),
			metric.WithFloat64Callback(func(
				_ context.Context,
				observer metric.Float64Observer,
			) error {
				observer.Observe(math.Float64frombits(value.Load()))
				return nil
			}),
		)
		return instrument{gauge: value, kind: descriptor.Kind}, err
	default:
		return instrument{}, fmt.Errorf("unsupported metric kind %d", descriptor.Kind)
	}
}

func toOTelAttributes(attributes []shadowmw.Attribute) []attribute.KeyValue {
	result := make([]attribute.KeyValue, 0, len(attributes))
	for _, item := range attributes {
		if item.Key == "" || item.Value == "" || len(item.Key) > 64 || len(item.Value) > 64 {
			continue
		}
		result = append(result, attribute.String(item.Key, item.Value))
	}
	return result
}
