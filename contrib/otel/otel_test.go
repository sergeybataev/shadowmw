package shadowmwotel_test

import (
	"context"
	"sync"
	"testing"

	"github.com/sergeybataev/shadowmw"
	shadowmwotel "github.com/sergeybataev/shadowmw/contrib/otel"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestNewRequiresExplicitProviders(t *testing.T) {
	if _, err := shadowmwotel.New(nil, nooptrace.NewTracerProvider()); err == nil {
		t.Fatal("New(nil meter provider) error = nil")
	}
	if _, err := shadowmwotel.New(noop.NewMeterProvider(), nil); err == nil {
		t.Fatal("New(nil tracer provider) error = nil")
	}
}

func TestAdapterRecordsFixedMetricsWithExplicitProvider(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	adapter, err := shadowmwotel.New(meterProvider, nooptrace.NewTracerProvider())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	adapter.Record(context.Background(), shadowmw.Measurement{
		Name:  shadowmw.MetricComparisons,
		Value: 1,
		Attributes: []shadowmw.Attribute{
			{Key: "rule", Value: "users.get"},
			{Key: "result", Value: "mismatch"},
			{Key: "reason", Value: "difference"},
		},
	})
	adapter.Record(context.Background(), shadowmw.Measurement{
		Name:  shadowmw.MetricSessionsActive,
		Value: 3,
	})
	adapter.Record(context.Background(), shadowmw.Measurement{
		Name:  shadowmw.MetricObserverQueueDuration,
		Value: 0.025,
		Attributes: []shadowmw.Attribute{
			{Key: "rule", Value: "users.get"},
		},
	})
	adapter.Record(context.Background(), shadowmw.Measurement{
		Name:  shadowmw.MetricRejectionCodes,
		Value: 1,
		Attributes: []shadowmw.Attribute{
			{Key: "rule", Value: "users.get"},
			{Key: "branch", Value: "shadow"},
			{Key: "result", Value: "found"},
			{Key: "code", Value: "approved"},
		},
	})

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if !hasMetric(metrics, string(shadowmw.MetricComparisons)) {
		t.Fatalf("metric %q was not collected: %+v", shadowmw.MetricComparisons, metrics)
	}
	if !hasMetric(metrics, string(shadowmw.MetricSessionsActive)) {
		t.Fatalf("observable gauge %q was not collected: %+v", shadowmw.MetricSessionsActive, metrics)
	}
	if !hasMetric(metrics, string(shadowmw.MetricObserverQueueDuration)) {
		t.Fatalf("histogram %q was not collected: %+v", shadowmw.MetricObserverQueueDuration, metrics)
	}
	if !hasMetric(metrics, string(shadowmw.MetricRejectionCodes)) {
		t.Fatalf("counter %q was not collected: %+v", shadowmw.MetricRejectionCodes, metrics)
	}
	if unit := metricUnit(metrics, string(shadowmw.MetricComparisons)); unit != "{comparison}" {
		t.Fatalf("comparison metric unit = %q, want {comparison}", unit)
	}
	if unit := metricUnit(metrics, string(shadowmw.MetricObserverQueueDuration)); unit != "s" {
		t.Fatalf("observer queue duration unit = %q, want s", unit)
	}
}

func TestAdapterCreatesNestedSpansWithExplicitProvider(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	adapter, err := shadowmwotel.New(noop.NewMeterProvider(), tracerProvider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	sessionContext, session := adapter.Start(context.Background(), shadowmw.SpanStart{
		Name: "shadowmw.session",
		Attributes: []shadowmw.Attribute{
			{Key: "rule", Value: "users.get"},
		},
	})
	_, comparison := adapter.Start(sessionContext, shadowmw.SpanStart{Name: "shadowmw.compare"})
	comparison.AddEvent("shadowmw.outcome", shadowmw.Attribute{Key: "outcome", Value: "match"})
	comparison.End()
	session.End()

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	var sessionSpan, comparisonSpan sdktrace.ReadOnlySpan
	for _, span := range spans {
		switch span.Name() {
		case "shadowmw.session":
			sessionSpan = span
		case "shadowmw.compare":
			comparisonSpan = span
		}
	}
	if sessionSpan == nil || comparisonSpan == nil {
		t.Fatalf("missing expected spans: %+v", spans)
	}
	if comparisonSpan.Parent().SpanID() != sessionSpan.SpanContext().SpanID() {
		t.Fatal("comparison span is not a child of session span")
	}
}

func TestAdapterSupportsExplicitNoopProviders(t *testing.T) {
	adapter, err := shadowmwotel.New(noop.NewMeterProvider(), nooptrace.NewTracerProvider())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	adapter.Record(context.Background(), shadowmw.Measurement{
		Name:  shadowmw.MetricSessions,
		Value: 1,
		Attributes: []shadowmw.Attribute{
			{Key: "rule", Value: "users.get"},
			{Key: "result", Value: "accepted"},
		},
	})
	_, span := adapter.Start(context.Background(), shadowmw.SpanStart{Name: "shadowmw.session"})
	span.End()
}

func TestAdapter_Concurrent(t *testing.T) {
	const (
		goroutines = 10
		iterations = 5
	)
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	adapter, err := shadowmwotel.New(meterProvider, tracerProvider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				if options := adapter.EngineOptions(); len(options) != 2 {
					t.Errorf("EngineOptions() len = %d, want 2", len(options))
					return
				}
				adapter.Record(context.Background(), shadowmw.Measurement{
					Name:  shadowmw.MetricComparisons,
					Value: 1,
					Attributes: []shadowmw.Attribute{
						{Key: "rule", Value: "users.get"},
						{Key: "result", Value: "match"},
						{Key: "reason", Value: "parity"},
					},
				})
				adapter.Record(context.Background(), shadowmw.Measurement{
					Name:  shadowmw.MetricSessionsActive,
					Value: float64(i + 1),
				})
				spanCtx, span := adapter.Start(context.Background(), shadowmw.SpanStart{
					Name: "shadowmw.session",
					Attributes: []shadowmw.Attribute{
						{Key: "rule", Value: "users.get"},
					},
				})
				if spanCtx == nil || span == nil {
					t.Error("Start() returned nil context or span")
					return
				}
				span.AddEvent("shadowmw.outcome", shadowmw.Attribute{Key: "outcome", Value: "match"})
				span.End()
			}
		}()
	}
	wg.Wait()

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if !hasMetric(metrics, string(shadowmw.MetricComparisons)) {
		t.Fatalf("metric %q was not collected", shadowmw.MetricComparisons)
	}
	if got := len(recorder.Ended()); got != goroutines*iterations {
		t.Fatalf("ended spans = %d, want %d", got, goroutines*iterations)
	}
}

func hasMetric(metrics metricdata.ResourceMetrics, name string) bool {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return true
			}
		}
	}
	return false
}

func metricUnit(metrics metricdata.ResourceMetrics, name string) string {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return metric.Unit
			}
		}
	}
	return ""
}
