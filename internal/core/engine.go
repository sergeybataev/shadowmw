package core

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// EngineState describes the lifecycle of an Engine.
type EngineState uint8

// EngineState values classify an Engine's lifecycle stage.
const (
	EngineNew EngineState = iota
	EngineRunning
	EngineDraining
	EngineStopped
)

// String returns the canonical lifecycle state name.
func (s EngineState) String() string {
	switch s {
	case EngineNew:
		return "new"
	case EngineRunning:
		return "running"
	case EngineDraining:
		return "draining"
	case EngineStopped:
		return "stopped"
	default:
		return fmt.Sprintf("EngineState(%d)", s)
	}
}

// ErrEngineAlreadyStarted reports a duplicate Engine.Start call.
var ErrEngineAlreadyStarted = errors.New("shadowmw: engine has already been started")

// EngineConfig controls bounded comparison execution.
type EngineConfig struct {
	MaxInFlightSessions int
	ComparisonWorkers   int
	SessionTimeout      time.Duration
	ComparisonTimeout   time.Duration
	Observer            *ObserverDelivery
}

// EngineOption configures service collaborators without changing bounds.
type EngineOption func(*engineOptions) error

type engineOptions struct {
	instrumentation Instrumentation
	tracing         Tracing
	logger          *slog.Logger
	rejectionCodes  map[string]struct{}
}

// WithInstrumentation installs a framework-neutral measurement sink.
func WithInstrumentation(instrumentation Instrumentation) EngineOption {
	return func(options *engineOptions) error {
		if instrumentation == nil {
			return fmt.Errorf("shadowmw: instrumentation is nil")
		}
		options.instrumentation = instrumentation
		return nil
	}
}

// WithTracing installs framework-neutral span creation.
func WithTracing(tracing Tracing) EngineOption {
	return func(options *engineOptions) error {
		if tracing == nil {
			return fmt.Errorf("shadowmw: tracing is nil")
		}
		options.tracing = tracing
		return nil
	}
}

// WithLogger installs structured operational logging. Sentry integration, if
// desired, belongs to the application's slog.Handler.
func WithLogger(logger *slog.Logger) EngineOption {
	return func(options *engineOptions) error {
		if logger == nil {
			return fmt.Errorf("shadowmw: logger is nil")
		}
		options.logger = logger
		return nil
	}
}

// WithRejectionCodeMetricAllowlist permits selected extracted rejection codes
// to appear as bounded metric attributes. Found but unapproved codes and
// extraction failures are recorded as "other"; absent codes are "none".
func WithRejectionCodeMetricAllowlist(codes ...string) EngineOption {
	return func(options *engineOptions) error {
		if len(codes) > 64 {
			return fmt.Errorf("shadowmw: rejection-code metric allowlist exceeds 64 values")
		}
		allowlist := make(map[string]struct{}, len(codes))
		for _, code := range codes {
			if code == "" || code == "none" || code == "other" ||
				len(code) > 64 || !utf8.ValidString(code) {
				return fmt.Errorf("shadowmw: invalid rejection-code metric allowlist value")
			}
			if _, duplicate := allowlist[code]; duplicate {
				return fmt.Errorf(
					"shadowmw: duplicate rejection-code metric allowlist value %q",
					code,
				)
			}
			allowlist[code] = struct{}{}
		}
		options.rejectionCodes = allowlist
		return nil
	}
}

// DefaultEngineConfig returns safe bounds while intentionally leaving
// ComparisonWorkers unset. Callers must choose comparison concurrency.
func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		MaxInFlightSessions: 128,
		SessionTimeout:      5 * time.Second,
		ComparisonTimeout:   time.Second,
	}
}

// Engine coordinates comparison sessions independently from client response
// delivery.
type Engine struct {
	cfg EngineConfig

	mu              sync.RWMutex
	state           EngineState
	slots           chan struct{}
	ready           chan comparisonTask
	workers         sync.WaitGroup
	sessions        sync.WaitGroup
	admissions      sync.WaitGroup
	stopped         chan struct{}
	observer        *observerRuntime
	telemetry       *telemetryRuntime
	instrumentation Instrumentation
	tracing         Tracing
	logger          *slog.Logger
	rejectionCodes  map[string]struct{}
	activeSessions  atomic.Int64
}

// NewEngine validates config and constructs an unstarted Engine.
func NewEngine(cfg EngineConfig, options ...EngineOption) (*Engine, error) {
	if cfg.MaxInFlightSessions <= 0 {
		return nil, fmt.Errorf("shadowmw: MaxInFlightSessions must be positive")
	}
	if cfg.ComparisonWorkers <= 0 {
		return nil, fmt.Errorf("shadowmw: ComparisonWorkers must be explicitly set to a positive value")
	}
	if cfg.SessionTimeout <= 0 {
		return nil, fmt.Errorf("shadowmw: SessionTimeout must be positive")
	}
	if cfg.ComparisonTimeout <= 0 {
		return nil, fmt.Errorf("shadowmw: ComparisonTimeout must be positive")
	}
	if err := validateObserverDelivery(cfg.Observer); err != nil {
		return nil, err
	}
	collaborators := engineOptions{
		instrumentation: NoopInstrumentation(),
		tracing:         NoopTracing(),
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("shadowmw: nil engine option")
		}
		if err := option(&collaborators); err != nil {
			return nil, err
		}
	}

	telemetry := newTelemetryRuntime(
		collaborators.instrumentation,
		collaborators.tracing,
		cfg.MaxInFlightSessions*16,
	)
	engine := &Engine{
		cfg:             cfg,
		state:           EngineNew,
		slots:           make(chan struct{}, cfg.MaxInFlightSessions),
		ready:           make(chan comparisonTask, cfg.MaxInFlightSessions),
		stopped:         make(chan struct{}),
		telemetry:       telemetry,
		instrumentation: asyncInstrumentation{runtime: telemetry},
		tracing:         boundedTracing{runtime: telemetry},
		logger:          collaborators.logger,
		rejectionCodes:  cloneStringSet(collaborators.rejectionCodes),
	}
	engine.observer = newObserverRuntime(
		cfg.Observer,
		engine.instrumentation,
		engine.log,
	)
	return engine, nil
}

// State returns the current lifecycle state.
func (e *Engine) State() EngineState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}

// Start makes the Engine available for comparison admission.
func (e *Engine) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != EngineNew {
		return ErrEngineAlreadyStarted
	}
	e.state = EngineRunning
	e.telemetry.start()
	if e.observer != nil {
		e.observer.start()
	}
	for range e.cfg.ComparisonWorkers {
		e.workers.Add(1)
		go e.runWorker()
	}
	e.log(
		ctx,
		slog.LevelInfo,
		"shadowmw engine started",
		"comparison_workers", e.cfg.ComparisonWorkers,
		"max_in_flight_sessions", e.cfg.MaxInFlightSessions,
	)
	return nil
}

// Shutdown stops admission and drains accepted work. It is idempotent.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	switch e.state {
	case EngineStopped:
		e.mu.Unlock()
		return nil
	case EngineDraining:
		stopped := e.stopped
		e.mu.Unlock()
		select {
		case <-stopped:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case EngineNew:
		e.state = EngineStopped
		close(e.ready)
		close(e.stopped)
		e.mu.Unlock()
		return nil
	case EngineRunning:
		e.state = EngineDraining
	}
	e.mu.Unlock()

	go e.drain()
	select {
	case <-e.stopped:
		return nil
	case <-ctx.Done():
		e.observer.recordAbandoned(ctx)
		return ctx.Err()
	}
}

// TelemetryFailures returns bounded operational evidence collected
// independently from configured telemetry collaborators.
func (e *Engine) TelemetryFailures() []TelemetryFailureCount {
	return e.telemetry.failuresSnapshot()
}

type comparisonTask struct {
	session       *comparisonSession
	authoritative ResponseSnapshot
	shadow        ResponseSnapshot
	queuedAt      time.Time
}

func (e *Engine) admitSample(
	rule *Rule,
	request *http.Request,
) (*comparisonSession, bool) {
	if !e.beginAdmissionDecision() {
		e.emitSkip(request.Context(), rule.name, ReasonEngineUnavailable)
		return nil, false
	}
	var session *comparisonSession
	var sampled, admitted bool
	func() {
		defer e.finishAdmissionDecision()
		if !rule.sampler(request) {
			return
		}
		sampled = true
		session, admitted = e.admit(rule, request.Context())
	}()
	if !sampled {
		e.emitSkip(request.Context(), rule.name, ReasonNotSampled)
		return nil, false
	}
	return session, admitted
}

func (e *Engine) beginAdmissionDecision() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != EngineRunning {
		return false
	}
	e.admissions.Add(1)
	return true
}

func (e *Engine) finishAdmissionDecision() {
	e.admissions.Done()
}

func (e *Engine) drain() {
	e.admissions.Wait()
	e.sessions.Wait()
	close(e.ready)
	e.workers.Wait()
	e.observer.shutdown()
	e.log(context.Background(), slog.LevelInfo, "shadowmw engine stopped")
	e.telemetry.shutdown()

	e.mu.Lock()
	e.state = EngineStopped
	close(e.stopped)
	e.mu.Unlock()
}

func (e *Engine) runWorker() {
	defer e.workers.Done()
	for task := range e.ready {
		e.process(task)
		task.session.finish()
	}
}

func (e *Engine) process(task comparisonTask) {
	comparisonContext, comparisonSpan := e.tracing.Start(
		task.session.ctx,
		SpanStart{
			Name: "shadowmw.compare",
			Attributes: []Attribute{
				{Key: "rule", Value: task.session.rule.name},
			},
		},
	)
	defer comparisonSpan.End()
	e.instrumentation.Record(task.session.ctx, Measurement{
		Name:  MetricComparisonQueueTime,
		Value: time.Since(task.queuedAt).Seconds(),
		Attributes: []Attribute{
			{Key: "rule", Value: task.session.rule.name},
		},
	})
	outcome := Outcome{
		sessionID:      task.session.id,
		ruleName:       task.session.rule.name,
		classification: OutcomeMismatch,
		authoritative:  task.authoritative,
		shadow:         task.shadow,
		startedAt:      task.session.startedAt,
		completedAt:    time.Now(),
	}
	defer func() {
		if recover() != nil {
			outcome.reason = ReasonComparisonPanic
			e.emitOutcome(task.session.ctx, outcome)
		}
	}()

	ctx, cancel := context.WithTimeout(comparisonContext, e.cfg.ComparisonTimeout)
	comparison, err := task.session.rule.policy.Compare(ctx, task.authoritative, task.shadow)
	contextErr := ctx.Err()
	cancel()
	outcome.completedAt = time.Now()
	outcome.differences = append([]Difference(nil), comparison.Differences...)
	outcome.authoritativeBody = comparison.authoritativeBodyRepresentation()
	outcome.shadowBody = comparison.shadowBodyRepresentation()
	e.recordRejectionCodeEvidence(
		task.session.ctx,
		task.session.rule.name,
		comparison.RejectionCodes,
	)
	outcome.operationalFailure = rejectionExtractorOperationalFailure(
		comparison.RejectionCodes,
	)
	switch {
	case contextErr != nil:
		outcome.reason = ReasonComparisonTimeout
	case err != nil:
		outcome.reason = ReasonComparisonError
	case comparison.Matched():
		outcome.classification = OutcomeMatch
		outcome.reason = ReasonParity
	default:
		outcome.reason = comparisonReason(outcome.differences)
	}
	recordComparisonSpanEvents(task.session.span, comparisonSpan, outcome)
	e.emitOutcome(task.session.ctx, outcome)
}

func recordComparisonSpanEvents(
	sessionSpan Span,
	comparisonSpan Span,
	outcome Outcome,
) {
	for _, difference := range outcome.differences {
		sessionSpan.AddEvent(
			"shadowmw.difference",
			Attribute{Key: "dimension", Value: difference.Dimension},
			Attribute{Key: "kind", Value: difference.Kind},
		)
	}
	sessionSpan.AddEvent(
		"shadowmw.outcome",
		Attribute{Key: "outcome", Value: outcome.classification.String()},
		Attribute{Key: "reason", Value: outcome.reason},
	)
	comparisonSpan.AddEvent(
		"shadowmw.outcome",
		Attribute{Key: "outcome", Value: outcome.classification.String()},
		Attribute{Key: "reason", Value: outcome.reason},
	)
}

func (e *Engine) admit(rule *Rule, requestContext context.Context) (*comparisonSession, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != EngineRunning {
		e.emitSkip(requestContext, rule.name, ReasonEngineUnavailable)
		return nil, false
	}
	select {
	case e.slots <- struct{}{}:
	default:
		e.emitSkip(requestContext, rule.name, ReasonCapacity)
		return nil, false
	}
	var id SessionID
	rand.Read(id.value[:])

	base := context.WithoutCancel(requestContext)
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline, ok := requestContext.Deadline(); ok {
		deadlineContext, deadlineCancel := context.WithDeadline(base, deadline)
		ctx, cancel = context.WithTimeout(deadlineContext, e.cfg.SessionTimeout)
		cancel = combineCancel(cancel, deadlineCancel)
	} else {
		ctx, cancel = context.WithTimeout(base, e.cfg.SessionTimeout)
	}
	ctx, sessionSpan := e.tracing.Start(ctx, SpanStart{
		Name: "shadowmw.session",
		Attributes: []Attribute{
			{Key: "rule", Value: rule.name},
		},
	})
	session := &comparisonSession{
		engine:    e,
		rule:      rule,
		id:        id,
		ctx:       ctx,
		cancel:    cancel,
		span:      sessionSpan,
		startedAt: time.Now(),
	}
	e.sessions.Add(1)
	active := e.activeSessions.Add(1)
	e.instrumentation.Record(ctx, Measurement{
		Name:  MetricSessionsActive,
		Value: float64(active),
	})
	timeout := e.cfg.SessionTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	session.mu.Lock()
	session.timer = time.AfterFunc(timeout, session.timeout)
	session.mu.Unlock()
	return session, true
}

func (e *Engine) recordSession(ctx context.Context, ruleName, result string) {
	e.instrumentation.Record(ctx, Measurement{
		Name:  MetricSessions,
		Value: 1,
		Attributes: []Attribute{
			{Key: "rule", Value: ruleName},
			{Key: "result", Value: result},
		},
	})
}

func (e *Engine) emitSkip(ctx context.Context, ruleName, reason string) {
	e.recordSession(ctx, ruleName, reason)
	if !e.observer.enqueue(Outcome{
		ruleName:       ruleName,
		classification: OutcomeSkipped,
		reason:         reason,
		startedAt:      time.Now(),
		completedAt:    time.Now(),
	}) {
		e.instrumentation.Record(ctx, Measurement{
			Name:  MetricObserverDeliveries,
			Value: 1,
			Attributes: []Attribute{
				{Key: "result", Value: "dropped"},
			},
		})
	}
}

func (e *Engine) recordBranchDuration(
	ctx context.Context,
	ruleName string,
	branch string,
	duration time.Duration,
) {
	e.instrumentation.Record(ctx, Measurement{
		Name:  MetricBranchDuration,
		Value: duration.Seconds(),
		Attributes: []Attribute{
			{Key: "rule", Value: ruleName},
			{Key: "branch", Value: branch},
		},
	})
}

func (e *Engine) recordResponseBodySize(
	ctx context.Context,
	ruleName string,
	branch string,
	snapshot ResponseSnapshot,
) {
	e.instrumentation.Record(ctx, Measurement{
		Name:  MetricBodySize,
		Value: float64(len(snapshot.body)),
		Attributes: []Attribute{
			{Key: "rule", Value: ruleName},
			{Key: "kind", Value: "response"},
			{Key: "branch", Value: branch},
		},
	})
}

func (e *Engine) emitOutcome(ctx context.Context, outcome Outcome) {
	e.instrumentation.Record(ctx, Measurement{
		Name:  MetricComparisons,
		Value: 1,
		Attributes: []Attribute{
			{Key: "rule", Value: outcome.ruleName},
			{Key: "result", Value: outcome.classification.String()},
			{Key: "reason", Value: outcome.reason},
		},
	})
	for _, difference := range outcome.differences {
		e.instrumentation.Record(ctx, Measurement{
			Name:  MetricDifferences,
			Value: 1,
			Attributes: []Attribute{
				{Key: "rule", Value: outcome.ruleName},
				{Key: "dimension", Value: difference.Dimension},
				{Key: "kind", Value: difference.Kind},
			},
		})
	}
	if outcome.operationalFailure != "" {
		e.log(
			ctx,
			slog.LevelError,
			"shadow comparison operational failure",
			"session_id",
			outcome.sessionID.String(),
			"rule",
			outcome.ruleName,
			"reason",
			outcome.operationalFailure,
			"comparison_reason",
			outcome.reason,
			"difference_count",
			len(outcome.differences),
		)
	} else if outcome.classification == OutcomeMismatch {
		arguments := []any{
			"session_id", outcome.sessionID.String(),
			"rule", outcome.ruleName,
			"reason", outcome.reason,
			"difference_count", len(outcome.differences),
		}
		if operationalReason(outcome.reason) {
			e.log(ctx, slog.LevelError, "shadow comparison failed", arguments...)
		} else {
			e.log(ctx, slog.LevelWarn, "shadow comparison mismatch", arguments...)
		}
	}
	if !e.observer.enqueue(outcome) {
		e.instrumentation.Record(ctx, Measurement{
			Name:  MetricObserverDeliveries,
			Value: 1,
			Attributes: []Attribute{
				{Key: "result", Value: "dropped"},
			},
		})
	}
}

func combineCancel(first, second context.CancelFunc) context.CancelFunc {
	return func() {
		first()
		second()
	}
}

func (e *Engine) log(
	ctx context.Context,
	level slog.Level,
	message string,
	arguments ...any,
) {
	if e.logger == nil {
		return
	}
	arguments = append([]any(nil), arguments...)
	e.telemetry.log(func() {
		e.logger.Log(ctx, level, message, arguments...)
	})
}

func operationalReason(reason string) bool {
	switch reason {
	case ReasonDifference, ReasonParity:
		return false
	default:
		return true
	}
}

func comparisonReason(differences []Difference) string {
	for _, difference := range differences {
		switch difference.Kind {
		case "comparison_incomplete":
			return ReasonResponseTruncated
		case "decode_error":
			return ReasonDecodingFailure
		case "custom_comparator_error":
			return ReasonCustomComparatorError
		case "custom_comparator_panic":
			return ReasonCustomComparatorPanic
		case "custom_comparator_canceled":
			return ReasonCustomComparatorCanceled
		}
	}
	return ReasonDifference
}

func rejectionExtractorOperationalFailure(
	evidence *RejectionCodeEvidence,
) string {
	if evidence == nil {
		return ""
	}
	results := [...]RejectionCodeResult{
		evidence.Authoritative.Result,
		evidence.Shadow.Result,
	}
	for _, result := range results {
		if result == RejectionCodeExtractorPanic {
			return "rejection_extractor_panic"
		}
	}
	for _, result := range results {
		if result == RejectionCodeExtractorError {
			return "rejection_extractor_error"
		}
	}
	for _, result := range results {
		if result == RejectionCodeExtractorCanceled {
			return "rejection_extractor_canceled"
		}
	}
	return ""
}

func (e *Engine) recordRejectionCodeEvidence(
	ctx context.Context,
	ruleName string,
	evidence *RejectionCodeEvidence,
) {
	if evidence == nil {
		return
	}
	for _, branch := range []struct {
		name string
		code ExtractedRejectionCode
	}{
		{name: "authoritative", code: evidence.Authoritative},
		{name: "shadow", code: evidence.Shadow},
	} {
		e.instrumentation.Record(ctx, Measurement{
			Name:  MetricRejectionCodes,
			Value: 1,
			Attributes: []Attribute{
				{Key: "rule", Value: ruleName},
				{Key: "branch", Value: branch.name},
				{Key: "result", Value: string(branch.code.Result)},
				{Key: "code", Value: e.metricRejectionCode(branch.code)},
			},
		})
	}
}

func (e *Engine) metricRejectionCode(code ExtractedRejectionCode) string {
	if code.Result == RejectionCodeNotFound {
		return "none"
	}
	if code.Result == RejectionCodeFound {
		if _, allowed := e.rejectionCodes[code.Code]; allowed {
			return code.Code
		}
	}
	return "other"
}

func cloneStringSet(source map[string]struct{}) map[string]struct{} {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]struct{}, len(source))
	for value := range source {
		cloned[value] = struct{}{}
	}
	return cloned
}

type comparisonSession struct {
	engine    *Engine
	rule      *Rule
	id        SessionID
	ctx       context.Context
	cancel    context.CancelFunc
	timer     *time.Timer
	span      Span
	startedAt time.Time

	mu            sync.Mutex
	authoritative *ResponseSnapshot
	shadow        *ResponseSnapshot
	// pendingFailureReason keeps a branch failure non-terminal until the other
	// branch publishes its snapshot or the session deadline expires.
	pendingFailureReason string
	// activeContinuations retains capacity for admitted setup callbacks without
	// making logical terminal transitions wait on callback execution.
	activeContinuations int
	shadowExecution     bool
	waitForShadow       bool
	terminalKind        sessionTerminalKind
	terminalReason      string
	done                bool
	finalized           bool
}

type sessionTerminalKind uint8

const (
	sessionTerminalNone sessionTerminalKind = iota
	sessionTerminalComparison
	sessionTerminalFailure
	sessionTerminalSkip
)

type sessionFinalization struct {
	session       *comparisonSession
	kind          sessionTerminalKind
	reason        string
	authoritative ResponseSnapshot
	shadow        ResponseSnapshot
}

func (s *comparisonSession) recordRequestBodySize(ctx context.Context, body []byte) {
	s.engine.instrumentation.Record(ctx, Measurement{
		Name:  MetricBodySize,
		Value: float64(len(body)),
		Attributes: []Attribute{
			{Key: "rule", Value: s.rule.name},
			{Key: "kind", Value: "request"},
		},
	})
}

func (s *comparisonSession) startBranchSpan(
	ctx context.Context,
	spanName string,
	branch string,
) (context.Context, Span) {
	return s.engine.tracing.Start(
		ctx,
		SpanStart{
			Name: spanName,
			Attributes: []Attribute{
				{Key: "rule", Value: s.rule.name},
				{Key: "branch", Value: branch},
			},
		},
	)
}

func (s *comparisonSession) recordBranchDuration(
	ctx context.Context,
	branch string,
	duration time.Duration,
) {
	s.engine.recordBranchDuration(ctx, s.rule.name, branch, duration)
}

func (s *comparisonSession) recordResponseBodySize(
	ctx context.Context,
	branch string,
	snapshot ResponseSnapshot,
) {
	s.engine.recordResponseBodySize(ctx, s.rule.name, branch, snapshot)
}

func (s *comparisonSession) continueIfActive(continuation func()) bool {
	s.mu.Lock()
	if s.done || s.ctx.Err() != nil {
		s.mu.Unlock()
		return false
	}
	if continuation == nil {
		s.mu.Unlock()
		return true
	}
	s.activeContinuations++
	s.mu.Unlock()

	defer s.completeContinuation()
	continuation()
	return true
}

func (s *comparisonSession) completeContinuation() {
	s.mu.Lock()
	s.activeContinuations--
	finalization := s.takeFinalizationLocked()
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) submitAuthoritative(snapshot ResponseSnapshot) {
	s.mu.Lock()
	if s.finalized {
		s.mu.Unlock()
		return
	}
	s.authoritative = &snapshot
	if s.done {
		finalization := s.takeFinalizationLocked()
		s.mu.Unlock()
		finalization.run()
		return
	}
	finalization := s.completeIfPairedLocked()
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) submitShadow(snapshot ResponseSnapshot) {
	// Publishing the snapshot is the shadow branch's completion point.
	s.mu.Lock()
	s.shadowExecution = false
	if s.done {
		if !s.finalized && s.waitForShadow {
			s.shadow = &snapshot
			s.waitForShadow = false
			if s.timer != nil {
				s.timer.Stop()
			}
			finalization := s.takeFinalizationLocked()
			s.mu.Unlock()
			finalization.run()
			return
		}
		s.mu.Unlock()
		return
	}
	s.shadow = &snapshot
	finalization := s.completeIfPairedLocked()
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) beginShadowExecution() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.ctx.Err() != nil {
		return false
	}
	s.shadowExecution = true
	return true
}

func (s *comparisonSession) failShadowExecution(
	reason string,
	snapshot ResponseSnapshot,
) {
	s.mu.Lock()
	s.shadowExecution = false
	if s.done {
		if !s.finalized && s.waitForShadow {
			s.shadow = &snapshot
			s.waitForShadow = false
			if s.timer != nil {
				s.timer.Stop()
			}
			finalization := s.takeFinalizationLocked()
			s.mu.Unlock()
			finalization.run()
			return
		}
		s.mu.Unlock()
		return
	}
	if s.pendingFailureReason == "" {
		s.pendingFailureReason = reason
	}
	s.shadow = &snapshot
	if s.authoritative == nil {
		s.mu.Unlock()
		return
	}
	finalization := s.beginTerminalLocked(
		sessionTerminalFailure,
		s.pendingFailureReason,
	)
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) completeIfPairedLocked() sessionFinalization {
	if s.authoritative == nil || s.shadow == nil {
		return sessionFinalization{}
	}
	if s.pendingFailureReason != "" {
		return s.beginTerminalLocked(
			sessionTerminalFailure,
			s.pendingFailureReason,
		)
	}
	return s.beginTerminalLocked(sessionTerminalComparison, "")
}

func (s *comparisonSession) failShadow(reason string) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	if s.pendingFailureReason == "" {
		s.pendingFailureReason = reason
	}
	if s.shadow == nil {
		s.shadow = &ResponseSnapshot{}
	}
	if s.authoritative == nil {
		s.mu.Unlock()
		return
	}
	finalization := s.beginTerminalLocked(
		sessionTerminalFailure,
		s.pendingFailureReason,
	)
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) failAuthoritative(
	reason string,
	snapshot ResponseSnapshot,
) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	s.authoritative = &snapshot
	s.waitForShadow = reason == ReasonAuthoritativePanic &&
		s.shadowExecution
	if s.shadow == nil && !s.waitForShadow {
		s.shadow = &ResponseSnapshot{}
	}
	finalization := s.beginTerminalLocked(sessionTerminalFailure, reason)
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) timeout() {
	s.mu.Lock()
	if s.done {
		if s.finalized || !s.waitForShadow {
			s.mu.Unlock()
			return
		}
		s.waitForShadow = false
		if s.shadow == nil {
			s.shadow = &ResponseSnapshot{}
		}
		finalization := s.takeFinalizationLocked()
		s.mu.Unlock()
		finalization.run()
		return
	}
	reason := ReasonSessionTimeout
	if s.pendingFailureReason != "" {
		reason = s.pendingFailureReason
	}
	finalization := s.beginTerminalLocked(sessionTerminalFailure, reason)
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) fail(reason string) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	if s.pendingFailureReason != "" {
		reason = s.pendingFailureReason
	}
	finalization := s.beginTerminalLocked(sessionTerminalFailure, reason)
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) beginTerminalLocked(
	kind sessionTerminalKind,
	reason string,
) sessionFinalization {
	s.done = true
	s.terminalKind = kind
	s.terminalReason = reason
	if s.timer != nil && !s.waitForShadow {
		s.timer.Stop()
	}
	if kind != sessionTerminalComparison {
		s.cancel()
	}
	return s.takeFinalizationLocked()
}

func (s *comparisonSession) takeFinalizationLocked() sessionFinalization {
	if !s.done || s.finalized || s.activeContinuations != 0 ||
		s.waitForShadow {
		return sessionFinalization{}
	}
	s.finalized = true
	finalization := sessionFinalization{
		session: s,
		kind:    s.terminalKind,
		reason:  s.terminalReason,
	}
	if s.authoritative != nil {
		finalization.authoritative = *s.authoritative
	}
	if s.shadow != nil {
		finalization.shadow = *s.shadow
	}
	return finalization
}

func (f sessionFinalization) run() {
	if f.session == nil {
		return
	}
	if f.kind == sessionTerminalComparison ||
		f.kind == sessionTerminalFailure {
		f.session.engine.recordSession(
			f.session.ctx,
			f.session.rule.name,
			"accepted",
		)
	}
	switch f.kind {
	case sessionTerminalComparison:
		f.session.engine.ready <- comparisonTask{
			session:       f.session,
			authoritative: f.authoritative,
			shadow:        f.shadow,
			queuedAt:      time.Now(),
		}
	case sessionTerminalFailure:
		f.session.publishFailure(f.reason, f.authoritative, f.shadow)
		f.session.finish()
	case sessionTerminalSkip:
		f.session.engine.emitSkip(
			f.session.ctx,
			f.session.rule.name,
			f.reason,
		)
		f.session.finish()
	}
}

func (s *comparisonSession) publishFailure(
	reason string,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
) {
	s.span.AddEvent(
		"shadowmw.incomplete",
		Attribute{Key: "reason", Value: reason},
	)
	s.engine.emitOutcome(s.ctx, Outcome{
		sessionID:      s.id,
		ruleName:       s.rule.name,
		classification: OutcomeMismatch,
		reason:         reason,
		authoritative:  authoritative,
		shadow:         shadow,
		startedAt:      s.startedAt,
		completedAt:    time.Now(),
	})
}

func (s *comparisonSession) skip(reason string) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	finalization := s.beginTerminalLocked(sessionTerminalSkip, reason)
	s.mu.Unlock()
	finalization.run()
}

func (s *comparisonSession) finish() {
	s.span.End()
	s.cancel()
	<-s.engine.slots
	active := s.engine.activeSessions.Add(-1)
	s.engine.instrumentation.Record(context.Background(), Measurement{
		Name:  MetricSessionsActive,
		Value: float64(active),
	})
	s.engine.sessions.Done()
}
