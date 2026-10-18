package shadowmw

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/sergeybataev/shadowmw/internal/core"
)

// EngineState describes the lifecycle of an Engine.
type EngineState = core.EngineState

// EngineConfig controls bounded comparison execution.
type EngineConfig = core.EngineConfig

// EngineOption configures service collaborators without changing bounds.
type EngineOption = core.EngineOption

// Engine coordinates comparison sessions independently from client response
// delivery.
type Engine = core.Engine

// BodyReadErrorPolicy selects behavior when bounded request capture fails.
type BodyReadErrorPolicy = core.BodyReadErrorPolicy

// MiddlewareConfig controls bounded HTTP capture.
type MiddlewareConfig = core.MiddlewareConfig

// BodyTransform returns a replacement request body. The input is detached and
// may be retained by the transform.
type BodyTransform = core.BodyTransform

// RequestHook may replace request metadata and context. Body, GetBody, content
// length, and the internal branch marker are restored after every hook.
type RequestHook = core.RequestHook

// BranchCustomization applies body transforms followed by request hooks.
type BranchCustomization = core.BranchCustomization

// RequestCustomizations scopes transformations to both or one branch.
type RequestCustomizations = core.RequestCustomizations

// Middleware routes matched sampled traffic through the authoritative handler
// while executing the wrapped replacement handler as a shadow.
type Middleware = core.Middleware

// Branch identifies one execution of a sampled request.
type Branch = core.Branch

// Policy evaluates two complete response snapshots.
type Policy = core.Policy

// Comparison is the bounded result returned by a Policy.
type Comparison = core.Comparison

// RejectionCodeResult is one bounded branch extraction outcome.
type RejectionCodeResult = core.RejectionCodeResult

// ExtractedRejectionCode is bounded observability evidence from one branch.
// Code is populated only when Result is RejectionCodeFound and is bounded by
// the policy's maximum difference-value size. Code remains service data and
// must be allowlisted before use as a metric attribute.
type ExtractedRejectionCode = core.ExtractedRejectionCode

// RejectionCodeEvidence contains fixed-size authoritative and shadow
// extraction evidence. It does not affect Comparison.Matched.
type RejectionCodeEvidence = core.RejectionCodeEvidence

// Difference identifies one observable deviation between authoritative and
// shadow outcomes.
type Difference = core.Difference

// SamplingFunc selects a matched request for shadow execution.
type SamplingFunc = core.SamplingFunc

// Rule is an immutable request-selection and comparison policy.
type Rule = core.Rule

// RuleOption configures an immutable Rule.
type RuleOption = core.RuleOption

// ContentDecoder constructs a bounded-decoding stream for a custom
// Content-Encoding token.
type ContentDecoder = core.ContentDecoder

// BodyComparisonInput contains detached decoded bodies for a custom comparator.
type BodyComparisonInput = core.BodyComparisonInput

// BodyComparisonFunc compares decoded bodies. It must honor ctx cancellation
// and is treated as trusted concurrent service code.
type BodyComparisonFunc = core.BodyComparisonFunc

// ResponseParityOption configures strict response comparison.
type ResponseParityOption = core.ResponseParityOption

// ErrorExtractor extracts a stable rejection code from one branch response.
type ErrorExtractor = core.ErrorExtractor

// DecisionParityOption configures validation-decision comparison.
type DecisionParityOption = core.DecisionParityOption

// SessionID is an opaque library-generated comparison-session identifier.
type SessionID = core.SessionID

// OutcomeClassification describes a terminal comparison-session finding.
type OutcomeClassification = core.OutcomeClassification

// Outcome is an immutable terminal comparison-session finding.
type Outcome = core.Outcome

// Observer consumes terminal outcomes outside comparison workers.
type Observer = core.Observer

// OutcomeRenderer produces bounded service-specific observer detail.
type OutcomeRenderer = core.OutcomeRenderer

// OutcomeFilter selects terminal classifications for observer delivery.
type OutcomeFilter = core.OutcomeFilter

// OutcomeDetail selects retained diagnostic detail for observer delivery.
type OutcomeDetail = core.OutcomeDetail

// ObserverDelivery configures isolated, bounded, best-effort delivery.
type ObserverDelivery = core.ObserverDelivery

// MetricName identifies one fixed shadowmw measurement.
type MetricName = core.MetricName

// MetricKind describes how a fixed measurement is aggregated.
type MetricKind = core.MetricKind

// MetricDescriptor is the stable contract for one core measurement.
type MetricDescriptor = core.MetricDescriptor

// Attribute is one bounded telemetry dimension. Core never emits request IDs,
// paths, header values, body values, or raw errors.
type Attribute = core.Attribute

// Measurement is one framework-neutral metric recording.
type Measurement = core.Measurement

// Instrumentation receives fixed, bounded measurements.
type Instrumentation = core.Instrumentation

// SpanStart describes one bounded core span.
type SpanStart = core.SpanStart

// Span receives bounded events and a terminal end signal.
type Span = core.Span

// Tracing creates spans without coupling core to a tracing framework.
type Tracing = core.Tracing

// TelemetryFailureKind is one bounded operational telemetry failure category.
type TelemetryFailureKind = core.TelemetryFailureKind

// TelemetryFailureCount is independent, bounded operational evidence for a
// failed, timed-out, or overloaded telemetry collaborator.
type TelemetryFailureCount = core.TelemetryFailureCount

// BodyDecodingState reports whether comparison body decoding evidence is
// available, whether decoding ran, and whether it succeeded.
type BodyDecodingState = core.BodyDecodingState

// BodyRepresentationKind identifies the bounded body representation retained
// after decoding and optional structured normalization.
type BodyRepresentationKind = core.BodyRepresentationKind

// BodyRepresentation is immutable, bounded evidence of the body representation
// used for one branch comparison. Present distinguishes an empty
// representation from no representation.
type BodyRepresentation = core.BodyRepresentation

// ResponseSnapshot is a detached response representation safe for asynchronous
// comparison.
type ResponseSnapshot = core.ResponseSnapshot

// EngineState values classify an Engine's lifecycle stage.
const (
	EngineNew      = core.EngineNew
	EngineRunning  = core.EngineRunning
	EngineDraining = core.EngineDraining
	EngineStopped  = core.EngineStopped
)

// Branch values classify the execution branch assigned to a sampled request.
const (
	BranchUnknown       = core.BranchUnknown
	BranchAuthoritative = core.BranchAuthoritative
	BranchShadow        = core.BranchShadow
)

// RejectionCodeResult values classify branch rejection-code extraction outcomes.
const (
	RejectionCodeFound             = core.RejectionCodeFound
	RejectionCodeNotFound          = core.RejectionCodeNotFound
	RejectionCodeExtractorError    = core.RejectionCodeExtractorError
	RejectionCodeExtractorPanic    = core.RejectionCodeExtractorPanic
	RejectionCodeExtractorCanceled = core.RejectionCodeExtractorCanceled
)

// OutcomeClassification values classify terminal comparison sessions.
const (
	OutcomeMatch    = core.OutcomeMatch
	OutcomeMismatch = core.OutcomeMismatch
	OutcomeSkipped  = core.OutcomeSkipped
)

// Reason constants identify bounded terminal session outcomes and skips.
const (
	ReasonParity                      = core.ReasonParity
	ReasonDifference                  = core.ReasonDifference
	ReasonComparisonError             = core.ReasonComparisonError
	ReasonComparisonPanic             = core.ReasonComparisonPanic
	ReasonComparisonTimeout           = core.ReasonComparisonTimeout
	ReasonResponseTruncated           = core.ReasonResponseTruncated
	ReasonDecodingFailure             = core.ReasonDecodingFailure
	ReasonCustomComparatorError       = core.ReasonCustomComparatorError
	ReasonCustomComparatorPanic       = core.ReasonCustomComparatorPanic
	ReasonCustomComparatorCanceled    = core.ReasonCustomComparatorCanceled
	ReasonSessionTimeout              = core.ReasonSessionTimeout
	ReasonAuthoritativePanic          = core.ReasonAuthoritativePanic
	ReasonAuthoritativeSetupError     = core.ReasonAuthoritativeSetupError
	ReasonAuthoritativeTransportError = core.ReasonAuthoritativeTransportError
	ReasonShadowPanic                 = core.ReasonShadowPanic
	ReasonShadowSetupError            = core.ReasonShadowSetupError
	ReasonEngineUnavailable           = core.ReasonEngineUnavailable
	ReasonCapacity                    = core.ReasonCapacity
	ReasonNotSampled                  = core.ReasonNotSampled
	ReasonUnsupportedExchange         = core.ReasonUnsupportedExchange
	ReasonRequestTooLarge             = core.ReasonRequestTooLarge
	ReasonBodyReadError               = core.ReasonBodyReadError
)

// OutcomeFilter bitmask values select which terminal classifications are observed.
const (
	ObserveMatches    = core.ObserveMatches
	ObserveMismatches = core.ObserveMismatches
	ObserveSkipped    = core.ObserveSkipped
)

// OutcomeDetail values configure observer diagnostic retention.
const (
	OutcomeMetadataOnly = core.OutcomeMetadataOnly
	OutcomeDiff         = core.OutcomeDiff
	OutcomeFull         = core.OutcomeFull
	OutcomeCustom       = core.OutcomeCustom
)

// MetricName constants identify the fixed measurements emitted by Engine and
// Middleware.
const (
	MetricSessions              = core.MetricSessions
	MetricComparisons           = core.MetricComparisons
	MetricDifferences           = core.MetricDifferences
	MetricBranchDuration        = core.MetricBranchDuration
	MetricBodySize              = core.MetricBodySize
	MetricSessionsActive        = core.MetricSessionsActive
	MetricComparisonQueueTime   = core.MetricComparisonQueueTime
	MetricObserverDeliveries    = core.MetricObserverDeliveries
	MetricObserverQueueItems    = core.MetricObserverQueueItems
	MetricObserverQueueBytes    = core.MetricObserverQueueBytes
	MetricObserverQueueDuration = core.MetricObserverQueueDuration
	MetricRejectionCodes        = core.MetricRejectionCodes
)

// MetricKind values classify the aggregation instrument for a MetricName.
const (
	MetricCounter   = core.MetricCounter
	MetricHistogram = core.MetricHistogram
	MetricGauge     = core.MetricGauge
)

// TelemetryFailureKind values classify collaborator panics, timeouts, and drops.
const (
	TelemetryInstrumentationPanic   = core.TelemetryInstrumentationPanic
	TelemetryInstrumentationTimeout = core.TelemetryInstrumentationTimeout
	TelemetryInstrumentationDropped = core.TelemetryInstrumentationDropped
	TelemetryTracingPanic           = core.TelemetryTracingPanic
	TelemetryTracingTimeout         = core.TelemetryTracingTimeout
	TelemetryTracingDropped         = core.TelemetryTracingDropped
	TelemetryLoggingPanic           = core.TelemetryLoggingPanic
	TelemetryLoggingTimeout         = core.TelemetryLoggingTimeout
	TelemetryLoggingDropped         = core.TelemetryLoggingDropped
)

// BodyDecodingState values classify branch body content-decoding outcomes.
const (
	BodyDecodingUnavailable  = core.BodyDecodingUnavailable
	BodyDecodingNotAttempted = core.BodyDecodingNotAttempted
	BodyDecodingSucceeded    = core.BodyDecodingSucceeded
	BodyDecodingFailed       = core.BodyDecodingFailed
)

// BodyRepresentationKind values classify retained compared body bytes.
const (
	BodyRepresentationNone       = core.BodyRepresentationNone
	BodyRepresentationDecoded    = core.BodyRepresentationDecoded
	BodyRepresentationNormalized = core.BodyRepresentationNormalized
)

// ErrEngineAlreadyStarted reports a duplicate Engine.Start call.
var ErrEngineAlreadyStarted = core.ErrEngineAlreadyStarted

// WithInstrumentation installs a framework-neutral measurement sink.
func WithInstrumentation(instrumentation Instrumentation) EngineOption {
	return core.WithInstrumentation(instrumentation)
}

// WithTracing installs framework-neutral span creation.
func WithTracing(tracing Tracing) EngineOption {
	return core.WithTracing(tracing)
}

// WithLogger installs structured operational logging. Sentry integration, if
// desired, belongs to the application's slog.Handler.
func WithLogger(logger *slog.Logger) EngineOption {
	return core.WithLogger(logger)
}

// WithRejectionCodeMetricAllowlist permits selected extracted rejection codes
// to appear as bounded metric attributes. Found but unapproved codes and
// extraction failures are recorded as "other"; absent codes are "none".
func WithRejectionCodeMetricAllowlist(codes ...string) EngineOption {
	return core.WithRejectionCodeMetricAllowlist(codes...)
}

// DefaultEngineConfig returns safe bounds while intentionally leaving
// ComparisonWorkers unset. Callers must choose comparison concurrency.
func DefaultEngineConfig() EngineConfig {
	return core.DefaultEngineConfig()
}

// NewEngine validates config and constructs an unstarted Engine.
func NewEngine(cfg EngineConfig, options ...EngineOption) (*Engine, error) {
	return core.NewEngine(cfg, options...)
}

// RejectBodyReadErrors responds with 400 and does not call either branch.
func RejectBodyReadErrors() BodyReadErrorPolicy {
	return core.RejectBodyReadErrors()
}

// PassBodyReadErrorsToAuthoritative calls only the authoritative branch with a
// body that replays consumed bytes before reproducing the original read error.
func PassBodyReadErrorsToAuthoritative() BodyReadErrorPolicy {
	return core.PassBodyReadErrorsToAuthoritative()
}

// HandleBodyReadErrors delegates capture failures to handler.
func HandleBodyReadErrors(handler http.Handler) BodyReadErrorPolicy {
	return core.HandleBodyReadErrors(handler)
}

// DefaultMiddlewareConfig returns the default 1 MiB request and response
// capture limits while leaving body-read failure behavior explicit.
func DefaultMiddlewareConfig() MiddlewareConfig {
	return core.DefaultMiddlewareConfig()
}

// NewMiddleware validates immutable HTTP comparison configuration.
func NewMiddleware(engine *Engine, authoritative http.Handler, cfg MiddlewareConfig, rules ...*Rule) (*Middleware, error) {
	return core.NewMiddleware(engine, authoritative, cfg, rules...)
}

// BranchFromContext reports the internally assigned branch.
func BranchFromContext(ctx context.Context) Branch {
	return core.BranchFromContext(ctx)
}

// IsShadow reports whether ctx belongs to a shadow execution.
func IsShadow(ctx context.Context) bool {
	return core.IsShadow(ctx)
}

// NewRule validates and constructs an immutable comparison rule.
func NewRule(name string, policy Policy, options ...RuleOption) (*Rule, error) {
	return core.NewRule(name, policy, options...)
}

// MatchMethods matches any listed HTTP method.
func MatchMethods(methods ...string) RuleOption {
	return core.MatchMethods(methods...)
}

// MatchPath matches an exact URL path.
func MatchPath(path string) RuleOption {
	return core.MatchPath(path)
}

// MatchPathPrefix matches a URL path prefix.
func MatchPathPrefix(prefix string) RuleOption {
	return core.MatchPathPrefix(prefix)
}

// MatchPathRegexp matches a URL path with a compiled regular expression.
func MatchPathRegexp(expression string) RuleOption {
	return core.MatchPathRegexp(expression)
}

// MatchWhen adds a custom predicate. Multiple predicates are ANDed.
func MatchWhen(predicate func(*http.Request) bool) RuleOption {
	return core.MatchWhen(predicate)
}

// MatchAll explicitly matches every request.
func MatchAll() RuleOption {
	return core.MatchAll()
}

// SampleAll selects every matched request for shadow execution.
func SampleAll() RuleOption {
	return core.SampleAll()
}

// SampleNone selects no matched requests for shadow execution.
func SampleNone() RuleOption {
	return core.SampleNone()
}

// SamplePercent randomly selects the given percentage per matched request.
func SamplePercent(percent float64) RuleOption {
	return core.SamplePercent(percent)
}

// SampleWith installs a caller-provided deterministic or stateful sampler.
// The function must be safe for concurrent use.
func SampleWith(sample SamplingFunc) RuleOption {
	return core.SampleWith(sample)
}

// ResponseParity creates strict status, header, trailer, and body comparison.
func ResponseParity(options ...ResponseParityOption) Policy {
	return core.ResponseParity(options...)
}

// DisableHeaderComparison omits headers and trailers from parity.
func DisableHeaderComparison() ResponseParityOption {
	return core.DisableHeaderComparison()
}

// IgnoreHeaders compares every header except names.
func IgnoreHeaders(names ...string) ResponseParityOption {
	return core.IgnoreHeaders(names...)
}

// CompareOnlyHeaders compares only names.
func CompareOnlyHeaders(names ...string) ResponseParityOption {
	return core.CompareOnlyHeaders(names...)
}

// SortHeaderValues makes selected repeated-header values order-insensitive.
func SortHeaderValues(names ...string) ResponseParityOption {
	return core.SortHeaderValues(names...)
}

// CompareBodyAsJSON forces structural JSON comparison regardless of Content-Type.
func CompareBodyAsJSON() ResponseParityOption {
	return core.CompareBodyAsJSON()
}

// CompareBodyAsForm forces URL-encoded form comparison regardless of Content-Type.
func CompareBodyAsForm() ResponseParityOption {
	return core.CompareBodyAsForm()
}

// CompareBodyAsBytes forces exact byte-by-byte body comparison.
func CompareBodyAsBytes() ResponseParityOption {
	return core.CompareBodyAsBytes()
}

// DisableBodyComparison omits response bodies from parity evaluation.
func DisableBodyComparison() ResponseParityOption {
	return core.DisableBodyComparison()
}

// IgnoreJSONPointers excludes matching RFC 6901 JSON pointers from structural comparison.
func IgnoreJSONPointers(pointers ...string) ResponseParityOption {
	return core.IgnoreJSONPointers(pointers...)
}

// IgnoreJSONFieldNames excludes matching JSON object field names at every depth.
func IgnoreJSONFieldNames(names ...string) ResponseParityOption {
	return core.IgnoreJSONFieldNames(names...)
}

// EquateMissingWithEmptyObject treats a missing JSON field and an empty JSON
// object as equivalent at the specified RFC 6901 pointers.
func EquateMissingWithEmptyObject(pointers ...string) ResponseParityOption {
	return core.EquateMissingWithEmptyObject(pointers...)
}

// SortFormValues makes repeated form values order-insensitive for the listed keys.
func SortFormValues(keys ...string) ResponseParityOption {
	return core.SortFormValues(keys...)
}

// WithMaxDifferences sets the maximum number of differences retained per comparison.
func WithMaxDifferences(maximum int) ResponseParityOption {
	return core.WithMaxDifferences(maximum)
}

// WithMaxDecodedBodyBytes sets the maximum decoded response body size in bytes.
func WithMaxDecodedBodyBytes(maximum int64) ResponseParityOption {
	return core.WithMaxDecodedBodyBytes(maximum)
}

// WithMaxDifferenceValueBytes sets the maximum byte length of each Expected and
// Actual value retained in a Difference.
func WithMaxDifferenceValueBytes(maximum int) ResponseParityOption {
	return core.WithMaxDifferenceValueBytes(maximum)
}

// WithContentDecoder adds one custom Content-Encoding decoder.
func WithContentDecoder(name string, decoder ContentDecoder) ResponseParityOption {
	return core.WithContentDecoder(name, decoder)
}

// CompareBodyWith delegates decoded-body comparison to trusted service code.
func CompareBodyWith(comparator BodyComparisonFunc) ResponseParityOption {
	return core.CompareBodyWith(comparator)
}

// DecisionParity compares acceptance and rejection decisions. Equal 2xx
// responses compare status only by default; 4xx and all other classes use
// strict response parity.
func DecisionParity(options ...DecisionParityOption) Policy {
	return core.DecisionParity(options...)
}

// EquivalentStatuses permits one explicit group of interchangeable statuses.
func EquivalentStatuses(statuses ...int) DecisionParityOption {
	return core.EquivalentStatuses(statuses...)
}

// CompareSuccessfulHeaders enables strict headers for equal success decisions.
func CompareSuccessfulHeaders() DecisionParityOption {
	return core.CompareSuccessfulHeaders()
}

// CompareSuccessfulBodies enables response-codec comparison for equal success
// decisions.
func CompareSuccessfulBodies() DecisionParityOption {
	return core.CompareSuccessfulBodies()
}

// WithDecisionResponseOptions applies response-parity options to rejection and
// non-success comparison.
func WithDecisionResponseOptions(options ...ResponseParityOption) DecisionParityOption {
	return core.WithDecisionResponseOptions(options...)
}

// WithRejectedErrorExtractors configures branch-specific error-envelope
// extraction. Successfully extracted codes remain observability-only unless
// CompareRejectedByExtractedCode is also supplied; extractor failures always
// produce a mismatch.
func WithRejectedErrorExtractors(authoritative ErrorExtractor, shadow ErrorExtractor) DecisionParityOption {
	return core.WithRejectedErrorExtractors(authoritative, shadow)
}

// CompareRejectedByExtractedCode replaces rejection-body parity with
// extracted-code equivalence. Status and enabled headers remain compared.
func CompareRejectedByExtractedCode() DecisionParityOption {
	return core.CompareRejectedByExtractedCode()
}

// MeasurementDescriptor returns the fixed aggregation contract for name.
func MeasurementDescriptor(name MetricName) (MetricDescriptor, bool) {
	return core.MeasurementDescriptor(name)
}

// NoopInstrumentation returns an instrumentation sink that discards metrics.
func NoopInstrumentation() Instrumentation {
	return core.NoopInstrumentation()
}

// ValidMeasurement reports whether a measurement follows the fixed core
// privacy and cardinality contract.
func ValidMeasurement(measurement Measurement) bool {
	return core.ValidMeasurement(measurement)
}

// SanitizeMeasurement copies a valid fixed measurement and collapses
// non-allowlisted attribute values to "other".
func SanitizeMeasurement(measurement Measurement) (Measurement, bool) {
	return core.SanitizeMeasurement(measurement)
}

// NoopTracing returns a tracing implementation that preserves contexts.
func NoopTracing() Tracing {
	return core.NoopTracing()
}
