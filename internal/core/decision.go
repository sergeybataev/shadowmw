package core

import (
	"context"
	"fmt"
)

// ErrorExtractor extracts a stable rejection code from one branch response.
type ErrorExtractor func(
	context.Context,
	ResponseSnapshot,
) (code string, found bool, err error)

// DecisionParityOption configures validation-decision comparison.
type DecisionParityOption func(*decisionParityConfig)

// DecisionParity compares acceptance and rejection decisions. Equal 2xx
// responses compare status only by default; 4xx and all other classes use
// strict response parity.
func DecisionParity(options ...DecisionParityOption) Policy {
	cfg := decisionParityConfig{}
	for _, option := range options {
		if option == nil {
			cfg.err = fmt.Errorf("nil decision parity option")
			continue
		}
		option(&cfg)
	}
	response := ResponseParity(cfg.responseOptions...).(*responseParityPolicy)
	return &decisionParityPolicy{cfg: cfg, response: response}
}

// EquivalentStatuses permits one explicit group of interchangeable statuses.
func EquivalentStatuses(statuses ...int) DecisionParityOption {
	return func(cfg *decisionParityConfig) {
		cfg.equivalentGroups = append(
			cfg.equivalentGroups,
			append([]int(nil), statuses...),
		)
	}
}

// CompareSuccessfulHeaders enables strict headers for equal success decisions.
func CompareSuccessfulHeaders() DecisionParityOption {
	return func(cfg *decisionParityConfig) {
		cfg.compareSuccessHeaders = true
	}
}

// CompareSuccessfulBodies enables response-codec comparison for equal success
// decisions.
func CompareSuccessfulBodies() DecisionParityOption {
	return func(cfg *decisionParityConfig) {
		cfg.compareSuccessBody = true
	}
}

// WithDecisionResponseOptions applies response-parity options to rejection and
// non-success comparison.
func WithDecisionResponseOptions(options ...ResponseParityOption) DecisionParityOption {
	return func(cfg *decisionParityConfig) {
		cfg.responseOptions = append([]ResponseParityOption(nil), options...)
	}
}

// WithRejectedErrorExtractors configures branch-specific error-envelope
// extraction. Successfully extracted codes remain observability-only unless
// CompareRejectedByExtractedCode is also supplied; extractor failures always
// produce a mismatch.
func WithRejectedErrorExtractors(
	authoritative ErrorExtractor,
	shadow ErrorExtractor,
) DecisionParityOption {
	return func(cfg *decisionParityConfig) {
		cfg.authoritativeExtractor = authoritative
		cfg.shadowExtractor = shadow
	}
}

// CompareRejectedByExtractedCode replaces rejection-body parity with
// extracted-code equivalence. Status and enabled headers remain compared.
func CompareRejectedByExtractedCode() DecisionParityOption {
	return func(cfg *decisionParityConfig) {
		cfg.compareRejectedCode = true
	}
}

type decisionParityConfig struct {
	equivalentGroups       [][]int
	compareSuccessHeaders  bool
	compareSuccessBody     bool
	compareRejectedCode    bool
	authoritativeExtractor ErrorExtractor
	shadowExtractor        ErrorExtractor
	responseOptions        []ResponseParityOption
	err                    error
}

type decisionParityPolicy struct {
	cfg      decisionParityConfig
	response *responseParityPolicy
}

func (p *decisionParityPolicy) Compare(
	ctx context.Context,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
) (Comparison, error) {
	authoritativeStatus := authoritative.statusCode
	shadowStatus := shadow.statusCode
	statusEquivalent := p.statusesEquivalent(authoritativeStatus, shadowStatus)

	if is2xx(authoritativeStatus) && is2xx(shadowStatus) {
		return p.compareSuccessDecision(
			ctx,
			authoritative,
			shadow,
			statusEquivalent,
		)
	}
	if is4xx(authoritativeStatus) && is4xx(shadowStatus) &&
		p.hasRejectedCodeExtractors() {
		return p.compareRejectedDecision(
			ctx,
			authoritative,
			shadow,
			statusEquivalent,
		)
	}
	return p.response.compare(ctx, authoritative, shadow, !statusEquivalent)
}

func (p *decisionParityPolicy) validationError() error {
	if p.cfg.err != nil {
		return p.cfg.err
	}
	if err := p.response.validationError(); err != nil {
		return err
	}
	if p.compareRejectedCodeEnabled() &&
		(p.cfg.authoritativeExtractor == nil || p.cfg.shadowExtractor == nil) {
		return fmt.Errorf(
			"rejected-code comparison requires authoritative and shadow extractors",
		)
	}
	seen := make(map[int]struct{})
	for _, group := range p.cfg.equivalentGroups {
		if len(group) < 2 {
			return fmt.Errorf("status-equivalence group requires at least two statuses")
		}
		for _, status := range group {
			if status < 100 || status > 599 {
				return fmt.Errorf("invalid equivalent HTTP status %d", status)
			}
			if _, ok := seen[status]; ok {
				return fmt.Errorf("HTTP status %d occurs more than once in equivalence groups", status)
			}
			seen[status] = struct{}{}
		}
	}
	return nil
}

func (p *decisionParityPolicy) compareSuccessDecision(
	ctx context.Context,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
	statusEquivalent bool,
) (Comparison, error) {
	if !p.cfg.compareSuccessHeaders && !p.cfg.compareSuccessBody {
		comparison := Comparison{}
		if !statusEquivalent {
			comparison.Differences = p.appendDifference(
				comparison.Differences,
				statusDifference(authoritative.statusCode, shadow.statusCode),
			)
		}
		if authoritative.truncated || shadow.truncated {
			comparison.Differences = p.appendDifference(
				comparison.Differences,
				Difference{Dimension: "body", Kind: "comparison_incomplete"},
			)
		}
		return comparison, nil
	}
	successPolicy := *p.response
	if !p.cfg.compareSuccessHeaders {
		successPolicy.cfg.headerMode = headersDisabled
	}
	if !p.cfg.compareSuccessBody {
		successPolicy.cfg.bodyMode = bodyDisabled
	}
	return successPolicy.compare(
		ctx,
		authoritative,
		shadow,
		!statusEquivalent,
	)
}

func (p *decisionParityPolicy) compareRejectedDecision(
	ctx context.Context,
	authoritative ResponseSnapshot,
	shadow ResponseSnapshot,
	statusEquivalent bool,
) (Comparison, error) {
	rejectionPolicy := p.response
	if p.cfg.compareRejectedCode {
		withoutBody := *p.response
		withoutBody.cfg.bodyMode = bodyDisabled
		rejectionPolicy = &withoutBody
	}
	comparison, err := rejectionPolicy.compare(
		ctx,
		authoritative,
		shadow,
		!statusEquivalent,
	)
	if err != nil {
		return Comparison{}, err
	}
	authoritativeCode := callExtractor(
		ctx,
		p.cfg.authoritativeExtractor,
		authoritative,
	)
	shadowCode := callExtractor(
		ctx,
		p.cfg.shadowExtractor,
		shadow,
	)
	comparison.RejectionCodes = &RejectionCodeEvidence{
		Authoritative: authoritativeCode.evidence(p.response.cfg.maxValueBytes),
		Shadow:        shadowCode.evidence(p.response.cfg.maxValueBytes),
	}
	switch {
	case authoritativeCode.result == RejectionCodeExtractorPanic ||
		shadowCode.result == RejectionCodeExtractorPanic:
		comparison.Differences = p.appendDifference(
			comparison.Differences,
			Difference{
				Dimension: "rejection",
				Kind:      "extractor_panic",
			},
		)
	case authoritativeCode.failed() || shadowCode.failed():
		comparison.Differences = p.appendDifference(
			comparison.Differences,
			Difference{
				Dimension: "rejection",
				Kind:      "extractor_error",
			},
		)
	case p.cfg.compareRejectedCode && (authoritativeCode.found != shadowCode.found ||
		authoritativeCode.code != shadowCode.code):
		comparison.Differences = p.appendDifference(
			comparison.Differences,
			Difference{
				Dimension: "rejection",
				Path:      "code",
				Kind:      "different",
				Expected: extractedCode(
					authoritativeCode.code,
					authoritativeCode.found,
				),
				Actual: extractedCode(
					shadowCode.code,
					shadowCode.found,
				),
			},
		)
	}
	return comparison, nil
}

func (p *decisionParityPolicy) appendDifference(
	differences []Difference,
	difference Difference,
) []Difference {
	return appendBoundedDifference(
		differences,
		difference,
		p.response.cfg.maxDifferences,
		p.response.cfg.maxValueBytes,
	)
}

func (p *decisionParityPolicy) compareRejectedCodeEnabled() bool {
	return p.cfg.compareRejectedCode ||
		p.cfg.authoritativeExtractor != nil ||
		p.cfg.shadowExtractor != nil
}

func (p *decisionParityPolicy) hasRejectedCodeExtractors() bool {
	return p.cfg.authoritativeExtractor != nil &&
		p.cfg.shadowExtractor != nil
}

type extractedRejectionCode struct {
	code   string
	found  bool
	result RejectionCodeResult
}

func (c extractedRejectionCode) failed() bool {
	switch c.result {
	case RejectionCodeExtractorError, RejectionCodeExtractorPanic,
		RejectionCodeExtractorCanceled:
		return true
	default:
		return false
	}
}

func (c extractedRejectionCode) evidence(maxCodeBytes int) ExtractedRejectionCode {
	evidence := ExtractedRejectionCode{Result: c.result}
	if c.result == RejectionCodeFound {
		evidence.Code = boundDifferenceValue(c.code, maxCodeBytes)
	}
	return evidence
}

func callExtractor(
	ctx context.Context,
	extractor ErrorExtractor,
	snapshot ResponseSnapshot,
) extractedRejectionCode {
	var result extractedRejectionCode
	func() {
		defer func() {
			if recover() != nil {
				result = extractedRejectionCode{
					result: RejectionCodeExtractorPanic,
				}
			}
		}()
		if extractor == nil {
			result = extractedRejectionCode{result: RejectionCodeExtractorError}
			return
		}
		code, found, err := extractor(ctx, snapshot)
		if err != nil {
			result = extractedRejectionCode{result: RejectionCodeExtractorError}
			return
		}
		if ctx.Err() != nil {
			result = extractedRejectionCode{result: RejectionCodeExtractorCanceled}
			return
		}
		if !found {
			result = extractedRejectionCode{result: RejectionCodeNotFound}
			return
		}
		result = extractedRejectionCode{
			code:   code,
			found:  true,
			result: RejectionCodeFound,
		}
	}()
	return result
}

func extractedCode(code string, found bool) string {
	if !found {
		return "<none>"
	}
	return code
}

func (p *decisionParityPolicy) statusesEquivalent(first, second int) bool {
	if first == second {
		return true
	}
	for _, group := range p.cfg.equivalentGroups {
		firstFound := false
		secondFound := false
		for _, status := range group {
			firstFound = firstFound || status == first
			secondFound = secondFound || status == second
		}
		if firstFound && secondFound {
			return true
		}
	}
	return false
}

func statusDifference(authoritative, shadow int) Difference {
	return Difference{
		Dimension: "status",
		Kind:      "different",
		Expected:  fmt.Sprintf("%d", authoritative),
		Actual:    fmt.Sprintf("%d", shadow),
	}
}

func is2xx(status int) bool {
	return status >= 200 && status <= 299
}

func is4xx(status int) bool {
	return status >= 400 && status <= 499
}
