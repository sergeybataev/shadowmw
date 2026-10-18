package core

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/sergeybataev/shadowmw/internal/httpcap"
)

const defaultCaptureLimit = int64(1 << 20)

// BodyReadErrorPolicy selects behavior when bounded request capture fails.
type BodyReadErrorPolicy struct {
	mode    bodyReadErrorMode
	handler http.Handler
}

type bodyReadErrorMode uint8

const (
	bodyReadErrorUnset bodyReadErrorMode = iota
	bodyReadErrorReject
	bodyReadErrorPass
	bodyReadErrorHandle
)

// RejectBodyReadErrors responds with 400 and does not call either branch.
func RejectBodyReadErrors() BodyReadErrorPolicy {
	return BodyReadErrorPolicy{mode: bodyReadErrorReject}
}

// PassBodyReadErrorsToAuthoritative calls only the authoritative branch with a
// body that replays consumed bytes before reproducing the original read error.
func PassBodyReadErrorsToAuthoritative() BodyReadErrorPolicy {
	return BodyReadErrorPolicy{mode: bodyReadErrorPass}
}

// HandleBodyReadErrors delegates capture failures to handler.
func HandleBodyReadErrors(handler http.Handler) BodyReadErrorPolicy {
	return BodyReadErrorPolicy{mode: bodyReadErrorHandle, handler: handler}
}

// MiddlewareConfig controls bounded HTTP capture.
type MiddlewareConfig struct {
	MaxRequestBodyBytes  int64
	MaxResponseBodyBytes int64
	BodyReadErrors       BodyReadErrorPolicy
	Customizations       RequestCustomizations
}

// BodyTransform returns a replacement request body. The input is detached and
// may be retained by the transform.
type BodyTransform func(context.Context, []byte) ([]byte, error)

// RequestHook may replace request metadata and context. Body, GetBody, content
// length, and the internal branch marker are restored after every hook.
type RequestHook func(*http.Request) (*http.Request, error)

// BranchCustomization applies body transforms followed by request hooks.
type BranchCustomization struct {
	BodyTransforms []BodyTransform
	RequestHooks   []RequestHook
}

// RequestCustomizations scopes transformations to both or one branch.
type RequestCustomizations struct {
	Both          BranchCustomization
	Authoritative BranchCustomization
	Shadow        BranchCustomization
}

// DefaultMiddlewareConfig returns the default 1 MiB request and response
// capture limits while leaving body-read failure behavior explicit.
func DefaultMiddlewareConfig() MiddlewareConfig {
	return MiddlewareConfig{
		MaxRequestBodyBytes:  defaultCaptureLimit,
		MaxResponseBodyBytes: defaultCaptureLimit,
	}
}

// Middleware routes matched sampled traffic through the authoritative handler
// while executing the wrapped replacement handler as a shadow.
type Middleware struct {
	engine        *Engine
	authoritative http.Handler
	cfg           MiddlewareConfig
	rules         []*Rule
}

// NewMiddleware validates immutable HTTP comparison configuration.
func NewMiddleware(
	engine *Engine,
	authoritative http.Handler,
	cfg MiddlewareConfig,
	rules ...*Rule,
) (*Middleware, error) {
	if engine == nil {
		return nil, fmt.Errorf("shadowmw: middleware requires an engine")
	}
	if authoritative == nil {
		return nil, fmt.Errorf("shadowmw: middleware requires an authoritative handler")
	}
	if cfg.MaxRequestBodyBytes <= 0 {
		return nil, fmt.Errorf("shadowmw: MaxRequestBodyBytes must be positive")
	}
	if cfg.MaxResponseBodyBytes <= 0 {
		return nil, fmt.Errorf("shadowmw: MaxResponseBodyBytes must be positive")
	}
	if err := validateBodyReadErrorPolicy(cfg.BodyReadErrors); err != nil {
		return nil, err
	}
	if err := validateMiddlewareRules(rules); err != nil {
		return nil, err
	}
	customizations, err := cloneCustomizations(cfg.Customizations)
	if err != nil {
		return nil, err
	}
	cfg.Customizations = customizations
	return &Middleware{
		engine:        engine,
		authoritative: authoritative,
		cfg:           cfg,
		rules:         append([]*Rule(nil), rules...),
	}, nil
}

// Wrap returns a handler that uses replacement normally for unmatched requests
// and as the shadow branch for admitted comparison samples.
func (m *Middleware) Wrap(replacement http.Handler) http.Handler {
	if replacement == nil {
		panic("shadowmw: nil replacement handler")
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		m.serveHTTP(writer, request, replacement)
	})
}

// Branch identifies one execution of a sampled request.
type Branch uint8

// Branch values classify the execution branch assigned to a sampled request.
const (
	BranchUnknown Branch = iota
	BranchAuthoritative
	BranchShadow
)

// BranchFromContext reports the internally assigned branch.
func BranchFromContext(ctx context.Context) Branch {
	branch, _ := ctx.Value(branchContextKey{}).(Branch)
	return branch
}

// IsShadow reports whether ctx belongs to a shadow execution.
func IsShadow(ctx context.Context) bool {
	return BranchFromContext(ctx) == BranchShadow
}

type branchContextKey struct{}

func validateBodyReadErrorPolicy(policy BodyReadErrorPolicy) error {
	switch policy.mode {
	case bodyReadErrorReject, bodyReadErrorPass:
		return nil
	case bodyReadErrorHandle:
		if policy.handler == nil {
			return fmt.Errorf("shadowmw: body-read error handler is nil")
		}
		return nil
	default:
		return fmt.Errorf("shadowmw: BodyReadErrors must be explicitly configured")
	}
}

func validateMiddlewareRules(rules []*Rule) error {
	if len(rules) == 0 {
		return fmt.Errorf("shadowmw: middleware requires at least one rule")
	}
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		if rule == nil {
			return fmt.Errorf("shadowmw: middleware contains a nil rule")
		}
		if _, ok := seen[rule.name]; ok {
			return fmt.Errorf("shadowmw: duplicate rule name %q", rule.name)
		}
		seen[rule.name] = struct{}{}
	}
	return nil
}

func (m *Middleware) serveHTTP(
	writer http.ResponseWriter,
	request *http.Request,
	replacement http.Handler,
) {
	rule := m.firstMatch(request)
	if rule == nil {
		replacement.ServeHTTP(writer, request)
		return
	}
	if httpcap.UnsupportedRequest(request) {
		m.engine.emitSkip(
			request.Context(),
			rule.name,
			ReasonUnsupportedExchange,
		)
		m.authoritative.ServeHTTP(writer, request)
		return
	}
	session, admitted := m.engine.admitSample(rule, request)
	if !admitted {
		m.authoritative.ServeHTTP(writer, request)
		return
	}
	m.serveSampledRequest(writer, request, replacement, session)
}

func (m *Middleware) serveSampledRequest(
	writer http.ResponseWriter,
	request *http.Request,
	replacement http.Handler,
	session *comparisonSession,
) {
	body, tooLarge, readErr := httpcap.CaptureRequestBody(request, m.cfg.MaxRequestBodyBytes)
	if tooLarge {
		session.skip(ReasonRequestTooLarge)
		m.authoritative.ServeHTTP(writer, request)
		return
	}
	if readErr != nil {
		session.skip(ReasonBodyReadError)
		m.handleBodyReadError(writer, request)
		return
	}
	if !session.continueIfActive(nil) {
		m.serveCapturedAuthoritative(writer, request, body)
		return
	}
	session.recordRequestBodySize(request.Context(), body)

	authoritativeContext, cancelAuthoritativeContext := httpcap.ContextWithLifetime(
		session.ctx,
		request.Context(),
	)
	defer cancelAuthoritativeContext()
	commonBody, authoritativeRequest, ok := m.prepareAuthoritativeExecution(
		writer,
		request,
		authoritativeContext,
		session,
		body,
	)
	if !ok {
		return
	}
	if !session.continueIfActive(func() {
		m.startShadowBranch(request, replacement, session, commonBody)
	}) {
		m.authoritative.ServeHTTP(writer, authoritativeRequest)
		return
	}
	m.executeAuthoritativeBranch(
		writer,
		request,
		authoritativeRequest,
		session,
	)
}

func (m *Middleware) prepareAuthoritativeExecution(
	writer http.ResponseWriter,
	request *http.Request,
	authoritativeContext context.Context,
	session *comparisonSession,
	body []byte,
) ([]byte, *http.Request, bool) {
	commonBody, ok := m.prepareCommonBody(writer, request, session, body)
	if !ok {
		return nil, nil, false
	}
	if !session.continueIfActive(nil) {
		m.serveCommonPreparedAuthoritative(writer, request, commonBody)
		return nil, nil, false
	}
	authoritativeBody, ok := m.prepareAuthoritativeBody(
		writer,
		request,
		session,
		commonBody,
	)
	if !ok {
		return nil, nil, false
	}
	if !session.continueIfActive(nil) {
		m.servePreparedAuthoritative(writer, request, authoritativeBody)
		return nil, nil, false
	}
	authoritativeRequest, ok := m.prepareAuthoritativeRequest(
		writer,
		request,
		authoritativeContext,
		session,
		authoritativeBody,
	)
	if !ok {
		return nil, nil, false
	}
	if !session.continueIfActive(nil) {
		m.authoritative.ServeHTTP(writer, authoritativeRequest)
		return nil, nil, false
	}
	return commonBody, authoritativeRequest, true
}

func (m *Middleware) startShadowBranch(
	request *http.Request,
	replacement http.Handler,
	session *comparisonSession,
	commonBody []byte,
) {
	go m.runShadowBranch(request, replacement, session, commonBody)
}

func (m *Middleware) runShadowBranch(
	request *http.Request,
	replacement http.Handler,
	session *comparisonSession,
	commonBody []byte,
) {
	shadowBody, continued, err := applyBodyTransformsUntil(
		session.ctx,
		commonBody,
		m.cfg.Customizations.Shadow.BodyTransforms,
		session.continueIfActive,
	)
	if !continued {
		return
	}
	var shadowRequest *http.Request
	if err == nil {
		shadowRequest = cloneBranchRequest(
			request,
			session.ctx,
			shadowBody,
			BranchShadow,
		)
		shadowRequest, continued, err = applyRequestHooksUntil(
			shadowRequest,
			BranchShadow,
			session.continueIfActive,
			m.cfg.Customizations.Both.RequestHooks,
			m.cfg.Customizations.Shadow.RequestHooks,
		)
		if !continued {
			return
		}
	}
	if err != nil {
		session.failShadow(ReasonShadowSetupError)
		return
	}
	if !session.beginShadowExecution() {
		return
	}
	shadowContext, shadowSpan := session.startBranchSpan(
		shadowRequest.Context(),
		"shadowmw.shadow",
		"shadow",
	)
	shadowRequest = shadowRequest.WithContext(
		withBranch(shadowContext, BranchShadow),
	)
	started := time.Now()
	recorder := httpcap.NewSnapshotWriter(m.cfg.MaxResponseBodyBytes)
	defer func() {
		shadowSpan.End()
		session.recordBranchDuration(session.ctx, "shadow", time.Since(started))
		if recovered := recover(); recovered != nil {
			session.failShadowExecution(
				ReasonShadowPanic,
				snapshotFromCaptured(recorder.Snapshot()),
			)
			return
		}
		snapshot := snapshotFromCaptured(recorder.Snapshot())
		session.recordResponseBodySize(session.ctx, "shadow", snapshot)
		session.submitShadow(snapshot)
	}()
	replacement.ServeHTTP(recorder, shadowRequest)
}

func (m *Middleware) executeAuthoritativeBranch(
	writer http.ResponseWriter,
	request *http.Request,
	authoritativeRequest *http.Request,
	session *comparisonSession,
) {
	authoritativeContext, authoritativeSpan := session.startBranchSpan(
		authoritativeRequest.Context(),
		"shadowmw.authoritative",
		"authoritative",
	)
	authoritativeRequest = authoritativeRequest.WithContext(
		withBranch(authoritativeContext, BranchAuthoritative),
	)

	unsupportedResponse := false
	skipUnsupportedResponse := func(header http.Header) {
		if unsupportedResponse {
			return
		}
		if !httpcap.IsEventStream(header.Get("Content-Type")) {
			return
		}
		unsupportedResponse = true
		session.skip(ReasonUnsupportedExchange)
	}
	observing := httpcap.NewObservingWriter(
		writer,
		m.cfg.MaxResponseBodyBytes,
		skipUnsupportedResponse,
	)
	authoritativeStarted := time.Now()
	func() {
		defer func() {
			authoritativeSpan.End()
			session.recordBranchDuration(
				request.Context(),
				"authoritative",
				time.Since(authoritativeStarted),
			)
			if recovered := recover(); recovered != nil {
				session.failAuthoritative(
					ReasonAuthoritativePanic,
					snapshotFromCaptured(observing.Snapshot()),
				)
				panic(recovered)
			}
		}()
		m.authoritative.ServeHTTP(observing, authoritativeRequest)
	}()
	authoritativeSnapshot := snapshotFromCaptured(observing.Snapshot())
	session.recordResponseBodySize(
		request.Context(),
		"authoritative",
		authoritativeSnapshot,
	)
	if unsupportedResponse {
		return
	}
	if observing.TransportFailed() {
		session.failAuthoritative(
			ReasonAuthoritativeTransportError,
			authoritativeSnapshot,
		)
		return
	}
	if httpcap.IsEventStream(authoritativeSnapshot.header.Get("Content-Type")) {
		skipUnsupportedResponse(authoritativeSnapshot.header)
		return
	}
	session.submitAuthoritative(authoritativeSnapshot)
}

func (m *Middleware) serveCapturedAuthoritative(
	writer http.ResponseWriter,
	request *http.Request,
	body []byte,
) {
	commonBody, ok := m.prepareCommonBody(writer, request, nil, body)
	if !ok {
		return
	}
	m.serveCommonPreparedAuthoritative(writer, request, commonBody)
}

func (m *Middleware) serveCommonPreparedAuthoritative(
	writer http.ResponseWriter,
	request *http.Request,
	commonBody []byte,
) {
	authoritativeBody, ok := m.prepareAuthoritativeBody(
		writer,
		request,
		nil,
		commonBody,
	)
	if !ok {
		return
	}
	m.servePreparedAuthoritative(writer, request, authoritativeBody)
}

func (m *Middleware) servePreparedAuthoritative(
	writer http.ResponseWriter,
	request *http.Request,
	authoritativeBody []byte,
) {
	authoritativeRequest, ok := m.prepareAuthoritativeRequest(
		writer,
		request,
		request.Context(),
		nil,
		authoritativeBody,
	)
	if !ok {
		return
	}
	m.authoritative.ServeHTTP(writer, authoritativeRequest)
}

func (m *Middleware) prepareCommonBody(
	writer http.ResponseWriter,
	request *http.Request,
	session *comparisonSession,
	body []byte,
) ([]byte, bool) {
	commonBody, err := applyBodyTransforms(
		request.Context(),
		body,
		m.cfg.Customizations.Both.BodyTransforms,
	)
	if err != nil {
		failAuthoritativeSetup(writer, session)
		return nil, false
	}
	return commonBody, true
}

func (m *Middleware) prepareAuthoritativeBody(
	writer http.ResponseWriter,
	request *http.Request,
	session *comparisonSession,
	commonBody []byte,
) ([]byte, bool) {
	authoritativeBody, err := applyBodyTransforms(
		request.Context(),
		commonBody,
		m.cfg.Customizations.Authoritative.BodyTransforms,
	)
	if err != nil {
		failAuthoritativeSetup(writer, session)
		return nil, false
	}
	return authoritativeBody, true
}

func (m *Middleware) prepareAuthoritativeRequest(
	writer http.ResponseWriter,
	request *http.Request,
	ctx context.Context,
	session *comparisonSession,
	authoritativeBody []byte,
) (*http.Request, bool) {
	authoritativeRequest := cloneBranchRequest(
		request,
		ctx,
		authoritativeBody,
		BranchAuthoritative,
	)
	authoritativeRequest, err := applyRequestHooks(
		authoritativeRequest,
		BranchAuthoritative,
		m.cfg.Customizations.Both.RequestHooks,
		m.cfg.Customizations.Authoritative.RequestHooks,
	)
	if err != nil {
		failAuthoritativeSetup(writer, session)
		return nil, false
	}
	return authoritativeRequest, true
}

func failAuthoritativeSetup(writer http.ResponseWriter, session *comparisonSession) {
	if session != nil {
		session.fail(ReasonAuthoritativeSetupError)
	}
	http.Error(writer, "authoritative request setup failed", http.StatusBadGateway)
}

func cloneCustomizations(customizations RequestCustomizations) (RequestCustomizations, error) {
	cloneBranch := func(name string, branch BranchCustomization) (BranchCustomization, error) {
		for _, transform := range branch.BodyTransforms {
			if transform == nil {
				return BranchCustomization{}, fmt.Errorf(
					"shadowmw: %s body transforms contain nil",
					name,
				)
			}
		}
		for _, hook := range branch.RequestHooks {
			if hook == nil {
				return BranchCustomization{}, fmt.Errorf(
					"shadowmw: %s request hooks contain nil",
					name,
				)
			}
		}
		return BranchCustomization{
			BodyTransforms: append([]BodyTransform(nil), branch.BodyTransforms...),
			RequestHooks:   append([]RequestHook(nil), branch.RequestHooks...),
		}, nil
	}
	both, err := cloneBranch("common", customizations.Both)
	if err != nil {
		return RequestCustomizations{}, err
	}
	authoritative, err := cloneBranch("authoritative", customizations.Authoritative)
	if err != nil {
		return RequestCustomizations{}, err
	}
	shadow, err := cloneBranch("shadow", customizations.Shadow)
	if err != nil {
		return RequestCustomizations{}, err
	}
	return RequestCustomizations{
		Both:          both,
		Authoritative: authoritative,
		Shadow:        shadow,
	}, nil
}

func applyBodyTransforms(
	ctx context.Context,
	body []byte,
	transforms []BodyTransform,
) ([]byte, error) {
	transformed, _, err := applyBodyTransformsUntil(ctx, body, transforms, nil)
	return transformed, err
}

func applyBodyTransformsUntil(
	ctx context.Context,
	body []byte,
	transforms []BodyTransform,
	continueComparison func(func()) bool,
) (transformed []byte, continued bool, err error) {
	transformed = append([]byte(nil), body...)
	continued = true
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("body transform panicked")
		}
	}()
	for _, transform := range transforms {
		if continueComparison == nil {
			transformed, err = transform(ctx, transformed)
		} else if !continueComparison(func() {
			transformed, err = transform(ctx, transformed)
		}) {
			return transformed, false, nil
		}
		if err != nil {
			return nil, true, err
		}
		if transformed == nil {
			transformed = []byte{}
		}
		transformed = append([]byte(nil), transformed...)
		if continueComparison != nil && !continueComparison(nil) {
			return transformed, false, nil
		}
	}
	return transformed, true, nil
}

func applyRequestHooks(
	request *http.Request,
	branch Branch,
	groups ...[]RequestHook,
) (*http.Request, error) {
	result, _, err := applyRequestHooksUntil(request, branch, nil, groups...)
	return result, err
}

func applyRequestHooksUntil(
	request *http.Request,
	branch Branch,
	continueComparison func(func()) bool,
	groups ...[]RequestHook,
) (result *http.Request, continued bool, err error) {
	result = request
	continued = true
	defer func() {
		if recover() != nil {
			result = nil
			err = fmt.Errorf("request hook panicked")
		}
	}()
	for _, hooks := range groups {
		for _, hook := range hooks {
			var keepGoing bool
			result, keepGoing, err = applySingleRequestHook(
				result,
				branch,
				hook,
				continueComparison,
			)
			if err != nil || !keepGoing {
				return result, keepGoing, err
			}
		}
	}
	return result, true, nil
}

func applySingleRequestHook(
	request *http.Request,
	branch Branch,
	hook RequestHook,
	continueComparison func(func()) bool,
) (*http.Request, bool, error) {
	body := request.Body
	getBody := request.GetBody
	contentLength := request.ContentLength
	var result *http.Request
	var err error
	if continueComparison == nil {
		result, err = hook(request)
	} else if !continueComparison(func() {
		result, err = hook(request)
	}) {
		return request, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if result == nil {
		return nil, true, fmt.Errorf("request hook returned nil request")
	}
	result.Body = body
	result.GetBody = getBody
	result.ContentLength = contentLength
	result = result.WithContext(withBranch(result.Context(), branch))
	if continueComparison != nil && !continueComparison(nil) {
		return result, false, nil
	}
	return result, true, nil
}

func (m *Middleware) firstMatch(request *http.Request) *Rule {
	var first *Rule
	matches := 0
	for _, rule := range m.rules {
		if rule.matches(request) {
			matches++
			if first == nil {
				first = rule
			}
		}
	}
	if matches > 1 {
		m.engine.log(
			request.Context(),
			slog.LevelWarn,
			"multiple shadow comparison rules matched; first rule selected",
			"rule",
			first.name,
			"matching_rules",
			matches,
		)
	}
	return first
}

func (m *Middleware) handleBodyReadError(writer http.ResponseWriter, request *http.Request) {
	switch m.cfg.BodyReadErrors.mode {
	case bodyReadErrorReject:
		http.Error(writer, "invalid request body", http.StatusBadRequest)
	case bodyReadErrorPass:
		m.authoritative.ServeHTTP(writer, request)
	case bodyReadErrorHandle:
		m.cfg.BodyReadErrors.handler.ServeHTTP(writer, request)
	}
}

func cloneBranchRequest(
	request *http.Request,
	ctx context.Context,
	body []byte,
	branch Branch,
) *http.Request {
	return httpcap.CloneRequest(request, withBranch(ctx, branch), body)
}

func withBranch(ctx context.Context, branch Branch) context.Context {
	return context.WithValue(ctx, branchContextKey{}, branch)
}
