package core

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sergeybataev/shadowmw/internal/render"
)

// SessionID is an opaque library-generated comparison-session identifier.
type SessionID struct {
	value [16]byte
}

// String returns the lower-case hexadecimal encoding of id.
func (id SessionID) String() string {
	return hex.EncodeToString(id.value[:])
}

// OutcomeClassification describes a terminal comparison-session finding.
type OutcomeClassification uint8

// OutcomeClassification values classify terminal comparison sessions.
const (
	OutcomeMatch OutcomeClassification = iota + 1
	OutcomeMismatch
	OutcomeSkipped
)

// Reason constants identify bounded terminal session outcomes and skips.
const (
	ReasonParity                      = "parity"
	ReasonDifference                  = "difference"
	ReasonComparisonError             = "comparison_error"
	ReasonComparisonPanic             = "comparison_panic"
	ReasonComparisonTimeout           = "comparison_timeout"
	ReasonResponseTruncated           = "response_truncated"
	ReasonDecodingFailure             = "decoding_failure"
	ReasonCustomComparatorError       = "custom_comparator_error"
	ReasonCustomComparatorPanic       = "custom_comparator_panic"
	ReasonCustomComparatorCanceled    = "custom_comparator_canceled"
	ReasonSessionTimeout              = "session_timeout"
	ReasonAuthoritativePanic          = "authoritative_panic"
	ReasonAuthoritativeSetupError     = "authoritative_setup_error"
	ReasonAuthoritativeTransportError = "authoritative_transport_error"
	ReasonShadowPanic                 = "panic"
	ReasonShadowSetupError            = "shadow_setup_error"
	ReasonEngineUnavailable           = "engine_unavailable"
	ReasonCapacity                    = "capacity"
	ReasonNotSampled                  = "not_sampled"
	ReasonUnsupportedExchange         = "unsupported_exchange"
	ReasonRequestTooLarge             = "request_too_large"
	ReasonBodyReadError               = "body_read_error"
)

// String returns the canonical token for c.
func (c OutcomeClassification) String() string {
	switch c {
	case OutcomeMatch:
		return "match"
	case OutcomeMismatch:
		return "mismatch"
	case OutcomeSkipped:
		return "skipped"
	default:
		return fmt.Sprintf("OutcomeClassification(%d)", c)
	}
}

// Outcome is an immutable terminal comparison-session finding.
type Outcome struct {
	sessionID          SessionID
	ruleName           string
	classification     OutcomeClassification
	reason             string
	operationalFailure string
	differences        []Difference
	authoritative      ResponseSnapshot
	shadow             ResponseSnapshot
	authoritativeBody  BodyRepresentation
	shadowBody         BodyRepresentation
	rendered           []byte
	startedAt          time.Time
	completedAt        time.Time
}

// SessionID returns the opaque comparison-session identifier.
func (o Outcome) SessionID() SessionID { return o.sessionID }

// RuleName returns the matched comparison rule name.
func (o Outcome) RuleName() string { return o.ruleName }

// Classification returns the terminal outcome classification.
func (o Outcome) Classification() OutcomeClassification { return o.classification }

// Reason returns the bounded terminal outcome reason.
func (o Outcome) Reason() string { return o.reason }

// StartedAt returns the timestamp when the session started.
func (o Outcome) StartedAt() time.Time { return o.startedAt }

// CompletedAt returns the timestamp when the session reached a terminal state.
func (o Outcome) CompletedAt() time.Time { return o.completedAt }

// AuthoritativeStatusCode returns the committed authoritative HTTP status code.
func (o Outcome) AuthoritativeStatusCode() int { return o.authoritative.StatusCode() }

// ShadowStatusCode returns the committed shadow HTTP status code.
func (o Outcome) ShadowStatusCode() int { return o.shadow.StatusCode() }

// Differences returns a defensive copy of retained differences.
func (o Outcome) Differences() []Difference {
	return append([]Difference(nil), o.differences...)
}

// AuthoritativeResponse returns a defensive copy of the authoritative snapshot.
func (o Outcome) AuthoritativeResponse() ResponseSnapshot {
	return o.authoritative.clone()
}

// ShadowResponse returns a defensive copy of the shadow snapshot.
func (o Outcome) ShadowResponse() ResponseSnapshot { return o.shadow.clone() }

// AuthoritativeComparedBody returns a defensive copy of the compared
// authoritative body bytes.
func (o Outcome) AuthoritativeComparedBody() []byte {
	return o.authoritativeBody.Body()
}

// ShadowComparedBody returns a defensive copy of the compared shadow body bytes.
func (o Outcome) ShadowComparedBody() []byte {
	return o.shadowBody.Body()
}

// AuthoritativeBodyRepresentation returns a defensive copy of the authoritative
// compared body representation.
func (o Outcome) AuthoritativeBodyRepresentation() BodyRepresentation {
	return o.authoritativeBody.clone()
}

// ShadowBodyRepresentation returns a defensive copy of the shadow compared body
// representation.
func (o Outcome) ShadowBodyRepresentation() BodyRepresentation {
	return o.shadowBody.clone()
}

// RenderedDetail returns a defensive copy of the rendered diagnostic detail.
func (o Outcome) RenderedDetail() []byte {
	return append([]byte(nil), o.rendered...)
}

// Observer consumes terminal outcomes outside comparison workers.
type Observer interface {
	Observe(context.Context, Outcome) error
}

// OutcomeRenderer produces bounded service-specific observer detail.
type OutcomeRenderer func(context.Context, Outcome) ([]byte, error)

// OutcomeFilter selects terminal classifications for observer delivery.
type OutcomeFilter uint8

// OutcomeFilter bitmask values select which terminal classifications are observed.
const (
	ObserveMatches OutcomeFilter = 1 << iota
	ObserveMismatches
	ObserveSkipped
)

// OutcomeDetail selects retained diagnostic detail for observer delivery.
type OutcomeDetail uint8

// OutcomeDetail values configure observer diagnostic retention.
const (
	OutcomeMetadataOnly OutcomeDetail = iota + 1
	OutcomeDiff
	OutcomeFull
	OutcomeCustom
)

// ObserverDelivery configures isolated, bounded, best-effort delivery.
type ObserverDelivery struct {
	Observer         Observer
	QueueCapacity    int
	MaxQueuedBytes   int64
	Workers          int
	Timeout          time.Duration
	Filter           OutcomeFilter
	Reasons          []string
	Rules            []string
	Dimensions       []string
	Detail           OutcomeDetail
	DiffContextLines int
	Renderer         OutcomeRenderer
}

const allOutcomeFilters = ObserveMatches | ObserveMismatches | ObserveSkipped

func (o Outcome) retainedBytes() int64 {
	size := int64(len(o.ruleName) + len(o.reason) + len(o.operationalFailure) + 64)
	size += o.authoritative.retainedBytes()
	size += o.shadow.retainedBytes()
	size += o.authoritativeBody.retainedBytes()
	size += o.shadowBody.retainedBytes()
	size += int64(len(o.rendered))
	for _, difference := range o.differences {
		size += int64(
			len(difference.Dimension) +
				len(difference.Path) +
				len(difference.Kind) +
				len(difference.Expected) +
				len(difference.Actual) +
				1,
		)
	}
	return size
}

func (f OutcomeFilter) accepts(classification OutcomeClassification) bool {
	switch classification {
	case OutcomeMatch:
		return f&ObserveMatches != 0
	case OutcomeMismatch:
		return f&ObserveMismatches != 0
	case OutcomeSkipped:
		return f&ObserveSkipped != 0
	default:
		return false
	}
}

func renderOutcomeDiff(outcome Outcome, contextLines int, maxBytes int64) []byte {
	return render.OutcomeDiff(
		render.ResponseInput{
			StatusCode: outcome.authoritative.statusCode,
			Header:     outcome.authoritative.header,
			Trailer:    outcome.authoritative.trailer,
			Body:       outcome.authoritative.body,
			Compared:   outcome.authoritativeBody.Body(),
			Truncated:  outcome.authoritative.truncated,
		},
		render.ResponseInput{
			StatusCode: outcome.shadow.statusCode,
			Header:     outcome.shadow.header,
			Trailer:    outcome.shadow.trailer,
			Body:       outcome.shadow.body,
			Compared:   outcome.shadowBody.Body(),
			Truncated:  outcome.shadow.truncated,
		},
		contextLines,
		maxBytes,
	)
}

func validateObserverDelivery(cfg *ObserverDelivery) error {
	if cfg == nil {
		return nil
	}
	if err := validateObserverBounds(cfg); err != nil {
		return err
	}
	for name, values := range map[string][]string{
		"Reasons":    cfg.Reasons,
		"Rules":      cfg.Rules,
		"Dimensions": cfg.Dimensions,
	} {
		if err := validateObserverFilterValues(name, values); err != nil {
			return err
		}
	}
	return validateObserverDetail(cfg)
}

func validateObserverBounds(cfg *ObserverDelivery) error {
	if cfg.Observer == nil {
		return fmt.Errorf("shadowmw: observer is nil")
	}
	if cfg.QueueCapacity <= 0 {
		return fmt.Errorf("shadowmw: observer QueueCapacity must be positive")
	}
	if cfg.MaxQueuedBytes <= 0 {
		return fmt.Errorf("shadowmw: observer MaxQueuedBytes must be positive")
	}
	if cfg.Workers <= 0 {
		return fmt.Errorf("shadowmw: observer Workers must be positive")
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("shadowmw: observer Timeout must be positive")
	}
	if cfg.Filter == 0 || cfg.Filter&^allOutcomeFilters != 0 {
		return fmt.Errorf("shadowmw: observer Filter must explicitly select valid outcomes")
	}
	return nil
}

func validateObserverDetail(cfg *ObserverDelivery) error {
	switch cfg.Detail {
	case OutcomeMetadataOnly, OutcomeFull:
		return nil
	case OutcomeDiff:
		if cfg.DiffContextLines < 0 {
			return fmt.Errorf("shadowmw: observer DiffContextLines cannot be negative")
		}
		return nil
	case OutcomeCustom:
		if cfg.Renderer == nil {
			return fmt.Errorf("shadowmw: custom outcome detail requires a renderer")
		}
		return nil
	default:
		return fmt.Errorf("shadowmw: observer Detail must be explicitly configured")
	}
}

func validateObserverFilterValues(name string, values []string) error {
	if len(values) > 64 {
		return fmt.Errorf("shadowmw: observer %s has more than 64 values", name)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || len(value) > 64 {
			return fmt.Errorf("shadowmw: observer %s contains an invalid value", name)
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("shadowmw: observer %s contains duplicate %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

type observerRuntime struct {
	cfg             ObserverDelivery
	instrumentation Instrumentation
	log             func(context.Context, slog.Level, string, ...any)

	queue     chan observerItem
	slots     chan struct{}
	mu        sync.Mutex
	bytes     int64
	closed    bool
	abandoned bool
	wg        sync.WaitGroup
}

type observerItem struct {
	outcome  Outcome
	bytes    int64
	queuedAt time.Time
}

func newObserverRuntime(
	cfg *ObserverDelivery,
	instrumentation Instrumentation,
	log func(context.Context, slog.Level, string, ...any),
) *observerRuntime {
	if cfg == nil {
		return nil
	}
	delivery := *cfg
	delivery.Reasons = append([]string(nil), cfg.Reasons...)
	delivery.Rules = append([]string(nil), cfg.Rules...)
	delivery.Dimensions = append([]string(nil), cfg.Dimensions...)
	return &observerRuntime{
		cfg:             delivery,
		queue:           make(chan observerItem, cfg.QueueCapacity),
		slots:           make(chan struct{}, cfg.QueueCapacity),
		instrumentation: instrumentation,
		log:             log,
	}
}

func (r *observerRuntime) start() {
	for range r.cfg.Workers {
		r.wg.Add(1)
		go r.run()
	}
}

func (r *observerRuntime) enqueue(outcome Outcome) bool {
	if r == nil || !r.accepts(outcome) {
		return true
	}
	select {
	case r.slots <- struct{}{}:
	default:
		return false
	}
	size := outcome.retainedBytes()
	if size > r.cfg.MaxQueuedBytes {
		<-r.slots
		return false
	}

	r.mu.Lock()
	if r.closed || r.bytes+size > r.cfg.MaxQueuedBytes {
		r.mu.Unlock()
		<-r.slots
		return false
	}
	r.bytes += size
	queuedBytes := r.bytes

	select {
	case r.queue <- observerItem{
		outcome:  outcome,
		bytes:    size,
		queuedAt: time.Now(),
	}:
		r.mu.Unlock()
		r.instrumentation.Record(context.Background(), Measurement{
			Name:  MetricObserverQueueBytes,
			Value: float64(queuedBytes),
		})
		r.instrumentation.Record(context.Background(), Measurement{
			Name:  MetricObserverQueueItems,
			Value: float64(len(r.queue)),
		})
		return true
	default:
		r.bytes -= size
		r.mu.Unlock()
		<-r.slots
		return false
	}
}

func (r *observerRuntime) run() {
	defer r.wg.Done()
	for item := range r.queue {
		r.instrumentation.Record(context.Background(), Measurement{
			Name:  MetricObserverQueueDuration,
			Value: time.Since(item.queuedAt).Seconds(),
			Attributes: []Attribute{
				{Key: "rule", Value: item.outcome.ruleName},
			},
		})
		r.deliver(item)
		r.instrumentation.Record(context.Background(), Measurement{
			Name:  MetricObserverQueueItems,
			Value: float64(len(r.queue)),
		})
	}
}

func (r *observerRuntime) deliver(item observerItem) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Timeout)
	defer cancel()
	preparationBytes, reserved := r.reservePreparationBytes(item.bytes)
	if !reserved {
		r.recordDelivery(ctx, "dropped")
		r.releaseBytes(item.bytes)
		<-r.slots
		return
	}
	preparation := startCallback(func() (Outcome, error) {
		return r.prepare(ctx, cloneOutcome(item.outcome), preparationBytes)
	})
	var result callbackResult[Outcome]
	select {
	case result = <-preparation:
	case <-ctx.Done():
		go func() {
			<-preparation
			r.releaseBytes(item.bytes + preparationBytes)
			<-r.slots
		}()
		r.recordDelivery(ctx, "timed_out")
		return
	}
	item.outcome = Outcome{}
	r.releaseBytes(item.bytes)
	if result.status != "delivered" {
		r.releaseBytes(preparationBytes)
		r.recordDelivery(ctx, result.status)
		<-r.slots
		return
	}
	prepared := result.value
	preparedBytes := prepared.retainedBytes()
	if preparedBytes > preparationBytes {
		r.releaseBytes(preparationBytes)
		r.recordDelivery(ctx, "dropped")
		<-r.slots
		return
	}
	r.releaseBytes(preparationBytes - preparedBytes)
	observed := callBounded(ctx, func() (struct{}, error) {
		defer r.releaseBytes(preparedBytes)
		defer func() { <-r.slots }()
		return struct{}{}, r.cfg.Observer.Observe(ctx, prepared)
	})
	r.recordDelivery(ctx, observed.status)
}

type callbackResult[T any] struct {
	value  T
	status string
}

func callBounded[T any](
	ctx context.Context,
	callback func() (T, error),
) callbackResult[T] {
	result := startCallback(callback)
	select {
	case returned := <-result:
		return returned
	case <-ctx.Done():
		return callbackResult[T]{status: "timed_out"}
	}
}

func startCallback[T any](
	callback func() (T, error),
) <-chan callbackResult[T] {
	result := make(chan callbackResult[T], 1)
	go func() {
		var returned callbackResult[T]
		defer func() {
			if recover() != nil {
				returned.status = "panicked"
			}
			result <- returned
		}()
		value, err := callback()
		returned.value = value
		if err != nil {
			returned.status = "failed"
		} else {
			returned.status = "delivered"
		}
	}()
	return result
}

func (r *observerRuntime) recordDelivery(ctx context.Context, result string) {
	r.instrumentation.Record(ctx, Measurement{
		Name:  MetricObserverDeliveries,
		Value: 1,
		Attributes: []Attribute{
			{Key: "result", Value: result},
		},
	})
	if result != "delivered" && r.log != nil {
		r.log(
			ctx,
			slog.LevelError,
			"shadowmw observer delivery failed",
			"result",
			result,
		)
	}
}

func (r *observerRuntime) prepare(
	ctx context.Context,
	outcome Outcome,
	maxRetainedBytes int64,
) (Outcome, error) {
	switch r.cfg.Detail {
	case OutcomeMetadataOnly:
		outcome.authoritative = outcome.authoritative.metadataOnly()
		outcome.shadow = outcome.shadow.metadataOnly()
		outcome.authoritativeBody = BodyRepresentation{}
		outcome.shadowBody = BodyRepresentation{}
		for i := range outcome.differences {
			outcome.differences[i].Expected = ""
			outcome.differences[i].Actual = ""
			outcome.differences[i].truncation &^=
				differenceExpectedTruncated | differenceActualTruncated
		}
	case OutcomeDiff:
		contextLines := r.cfg.DiffContextLines
		if contextLines == 0 {
			contextLines = 3
		}
		detail := outcome
		detail.authoritative = outcome.authoritative.metadataOnly()
		detail.shadow = outcome.shadow.metadataOnly()
		detail.authoritativeBody = BodyRepresentation{}
		detail.shadowBody = BodyRepresentation{}
		detail.rendered = nil
		available := max(int64(0), maxRetainedBytes-detail.retainedBytes())
		detail.rendered = renderOutcomeDiff(
			outcome,
			contextLines,
			available,
		)
		outcome = detail
	case OutcomeFull:
	case OutcomeCustom:
		rendered, err := r.cfg.Renderer(ctx, outcome)
		if err != nil {
			return Outcome{}, err
		}
		outcome.authoritative = outcome.authoritative.metadataOnly()
		outcome.shadow = outcome.shadow.metadataOnly()
		outcome.authoritativeBody = BodyRepresentation{}
		outcome.shadowBody = BodyRepresentation{}
		outcome.rendered = nil
		available := max(int64(0), maxRetainedBytes-outcome.retainedBytes())
		if int64(len(rendered)) > available {
			rendered = rendered[:available]
		}
		outcome.rendered = append([]byte(nil), rendered...)
	}
	return outcome, nil
}

func (r *observerRuntime) reservePreparationBytes(
	minimum int64,
) (int64, bool) {
	r.mu.Lock()
	available := r.cfg.MaxQueuedBytes - r.bytes
	if available < minimum {
		r.mu.Unlock()
		return 0, false
	}
	reservation := minimum
	if r.cfg.Detail == OutcomeDiff || r.cfg.Detail == OutcomeCustom {
		reservation = available
	}
	r.bytes += reservation
	retained := r.bytes
	r.mu.Unlock()
	r.recordQueueBytes(retained)
	return reservation, true
}

func (r *observerRuntime) releaseBytes(size int64) {
	if size == 0 {
		return
	}
	r.mu.Lock()
	r.bytes -= size
	retained := r.bytes
	r.mu.Unlock()
	r.recordQueueBytes(retained)
}

func (r *observerRuntime) recordQueueBytes(retained int64) {
	r.instrumentation.Record(context.Background(), Measurement{
		Name:  MetricObserverQueueBytes,
		Value: float64(retained),
	})
}

func (r *observerRuntime) accepts(outcome Outcome) bool {
	if !r.cfg.Filter.accepts(outcome.classification) ||
		!containsOrEmpty(r.cfg.Reasons, outcome.reason) ||
		!containsOrEmpty(r.cfg.Rules, outcome.ruleName) {
		return false
	}
	if len(r.cfg.Dimensions) == 0 {
		return true
	}
	for _, difference := range outcome.differences {
		if containsOrEmpty(r.cfg.Dimensions, difference.Dimension) {
			return true
		}
	}
	return false
}

func containsOrEmpty(values []string, target string) bool {
	if len(values) == 0 {
		return true
	}
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cloneOutcome(outcome Outcome) Outcome {
	outcome.differences = append([]Difference(nil), outcome.differences...)
	outcome.authoritative = outcome.authoritative.clone()
	outcome.shadow = outcome.shadow.clone()
	outcome.authoritativeBody = outcome.authoritativeBody.clone()
	outcome.shadowBody = outcome.shadowBody.clone()
	outcome.rendered = append([]byte(nil), outcome.rendered...)
	return outcome
}

func (r *observerRuntime) shutdown() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	close(r.queue)
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *observerRuntime) recordAbandoned(ctx context.Context) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.abandoned || r.bytes == 0 {
		r.mu.Unlock()
		return
	}
	r.abandoned = true
	r.mu.Unlock()
	r.recordDelivery(ctx, "abandoned")
}
