package core

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"reflect"
	"regexp"
	"strings"
)

// Policy evaluates two complete response snapshots.
type Policy interface {
	Compare(context.Context, ResponseSnapshot, ResponseSnapshot) (Comparison, error)
}

// Comparison is the bounded result returned by a Policy.
type Comparison struct {
	Differences           []Difference
	RejectionCodes        *RejectionCodeEvidence
	authoritativeCompared BodyRepresentation
	shadowCompared        BodyRepresentation
}

// RejectionCodeResult is one bounded branch extraction outcome.
type RejectionCodeResult string

// RejectionCodeResult values classify branch rejection-code extraction outcomes.
const (
	RejectionCodeFound             RejectionCodeResult = "found"
	RejectionCodeNotFound          RejectionCodeResult = "none"
	RejectionCodeExtractorError    RejectionCodeResult = "error"
	RejectionCodeExtractorPanic    RejectionCodeResult = "panic"
	RejectionCodeExtractorCanceled RejectionCodeResult = "canceled"
)

// ExtractedRejectionCode is bounded observability evidence from one branch.
// Code is populated only when Result is RejectionCodeFound and is bounded by
// the policy's maximum difference-value size. Code remains service data and
// must be allowlisted before use as a metric attribute.
type ExtractedRejectionCode struct {
	Result RejectionCodeResult
	Code   string
}

// RejectionCodeEvidence contains fixed-size authoritative and shadow
// extraction evidence. It does not affect Comparison.Matched.
type RejectionCodeEvidence struct {
	Authoritative ExtractedRejectionCode
	Shadow        ExtractedRejectionCode
}

// Matched reports whether the policy found behavioral parity.
func (c Comparison) Matched() bool {
	return len(c.Differences) == 0
}

// Difference identifies one observable deviation between authoritative and
// shadow outcomes.
type Difference struct {
	Dimension string
	Path      string
	Kind      string
	Expected  string
	Actual    string

	truncation differenceTruncation
}

// SamplingFunc selects a matched request for shadow execution.
type SamplingFunc func(*http.Request) bool

// Rule is an immutable request-selection and comparison policy.
type Rule struct {
	name    string
	policy  Policy
	matcher ruleMatcher
	sampler sampler
}

// Name returns the stable rule name.
func (r *Rule) Name() string { return r.name }

// RuleOption configures an immutable Rule.
type RuleOption func(*ruleBuilder) error

// NewRule validates and constructs an immutable comparison rule.
func NewRule(name string, policy Policy, options ...RuleOption) (*Rule, error) {
	if !ruleNamePattern.MatchString(name) {
		return nil, fmt.Errorf("shadowmw: invalid rule name %q", name)
	}
	if policy == nil || isNilPolicy(policy) {
		return nil, fmt.Errorf("shadowmw: rule %q has no policy", name)
	}
	if validatable, ok := policy.(interface{ validationError() error }); ok {
		if err := validatable.validationError(); err != nil {
			return nil, fmt.Errorf("shadowmw: rule %q policy: %w", name, err)
		}
	}
	var builder ruleBuilder
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("shadowmw: rule %q has a nil option", name)
		}
		if err := option(&builder); err != nil {
			return nil, fmt.Errorf("shadowmw: rule %q: %w", name, err)
		}
	}
	hasMatcher := builder.matcher.matchAll ||
		len(builder.matcher.methods) > 0 ||
		builder.pathCount > 0 ||
		len(builder.matcher.predicates) > 0
	if !hasMatcher {
		return nil, fmt.Errorf("shadowmw: rule %q has no matcher; use MatchAll explicitly", name)
	}
	if builder.sampler == nil {
		return nil, fmt.Errorf("shadowmw: rule %q has no sampling policy", name)
	}
	return &Rule{
		name:    name,
		policy:  policy,
		matcher: builder.matcher,
		sampler: builder.sampler,
	}, nil
}

// MatchMethods matches any listed HTTP method.
func MatchMethods(methods ...string) RuleOption {
	return func(builder *ruleBuilder) error {
		if len(methods) == 0 {
			return fmt.Errorf("MatchMethods requires at least one method")
		}
		if builder.matcher.methods == nil {
			builder.matcher.methods = make(map[string]struct{}, len(methods))
		}
		for _, method := range methods {
			if method == "" {
				return fmt.Errorf("MatchMethods contains an empty method")
			}
			builder.matcher.methods[method] = struct{}{}
		}
		return nil
	}
}

// MatchPath matches an exact URL path.
func MatchPath(path string) RuleOption {
	return setPathMatcher(func(matcher *ruleMatcher) { matcher.path = path }, path)
}

// MatchPathPrefix matches a URL path prefix.
func MatchPathPrefix(prefix string) RuleOption {
	return setPathMatcher(func(matcher *ruleMatcher) { matcher.pathPrefix = prefix }, prefix)
}

// MatchPathRegexp matches a URL path with a compiled regular expression.
func MatchPathRegexp(expression string) RuleOption {
	return func(builder *ruleBuilder) error {
		if expression == "" {
			return fmt.Errorf("path matcher cannot be empty")
		}
		if builder.pathCount != 0 {
			return fmt.Errorf("only one path matcher is allowed")
		}
		compiled, err := regexp.Compile(expression)
		if err != nil {
			return fmt.Errorf("invalid path regexp: %w", err)
		}
		builder.matcher.pathRegexp = compiled
		builder.pathCount++
		return nil
	}
}

// MatchWhen adds a custom predicate. Multiple predicates are ANDed.
func MatchWhen(predicate func(*http.Request) bool) RuleOption {
	return func(builder *ruleBuilder) error {
		if predicate == nil {
			return fmt.Errorf("MatchWhen requires a predicate")
		}
		builder.matcher.predicates = append(builder.matcher.predicates, predicate)
		return nil
	}
}

// MatchAll explicitly matches every request.
func MatchAll() RuleOption {
	return func(builder *ruleBuilder) error {
		builder.matcher.matchAll = true
		return nil
	}
}

// SampleAll selects every matched request for shadow execution.
func SampleAll() RuleOption {
	return func(builder *ruleBuilder) error {
		if builder.sampler != nil {
			return fmt.Errorf("only one sampling policy is allowed")
		}
		builder.sampler = func(*http.Request) bool { return true }
		return nil
	}
}

// SampleNone selects no matched requests for shadow execution.
func SampleNone() RuleOption {
	return func(builder *ruleBuilder) error {
		if builder.sampler != nil {
			return fmt.Errorf("only one sampling policy is allowed")
		}
		builder.sampler = func(*http.Request) bool { return false }
		return nil
	}
}

// SamplePercent randomly selects the given percentage per matched request.
func SamplePercent(percent float64) RuleOption {
	return func(builder *ruleBuilder) error {
		if builder.sampler != nil {
			return fmt.Errorf("only one sampling policy is allowed")
		}
		if math.IsNaN(percent) || math.IsInf(percent, 0) ||
			percent < 0 || percent > 100 {
			return fmt.Errorf("SamplePercent must be between 0 and 100")
		}
		builder.sampler = func(*http.Request) bool {
			return rand.Float64()*100 < percent
		}
		return nil
	}
}

// SampleWith installs a caller-provided deterministic or stateful sampler.
// The function must be safe for concurrent use.
func SampleWith(sample SamplingFunc) RuleOption {
	return func(builder *ruleBuilder) error {
		if builder.sampler != nil {
			return fmt.Errorf("only one sampling policy is allowed")
		}
		if sample == nil {
			return fmt.Errorf("SampleWith requires a sampler")
		}
		builder.sampler = sampler(sample)
		return nil
	}
}

type differenceTruncation uint8

const (
	differenceExpectedTruncated differenceTruncation = 1 << iota
	differenceActualTruncated
	differenceCountTruncated
)

type ruleMatcher struct {
	methods    map[string]struct{}
	path       string
	pathPrefix string
	pathRegexp *regexp.Regexp
	matchAll   bool
	predicates []func(*http.Request) bool
}

type sampler func(*http.Request) bool

type ruleBuilder struct {
	matcher   ruleMatcher
	pathCount int
	sampler   sampler
}

var ruleNamePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)

func (r *Rule) matches(request *http.Request) bool {
	matcher := r.matcher
	if len(matcher.methods) > 0 {
		if _, ok := matcher.methods[request.Method]; !ok {
			return false
		}
	}
	switch {
	case matcher.path != "" && request.URL.Path != matcher.path:
		return false
	case matcher.pathPrefix != "" && !strings.HasPrefix(request.URL.Path, matcher.pathPrefix):
		return false
	case matcher.pathRegexp != nil && !matcher.pathRegexp.MatchString(request.URL.Path):
		return false
	}
	for _, predicate := range matcher.predicates {
		if !predicate(request) {
			return false
		}
	}
	return true
}

func isNilPolicy(policy Policy) bool {
	value := reflect.ValueOf(policy)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func setPathMatcher(set func(*ruleMatcher), value string) RuleOption {
	return func(builder *ruleBuilder) error {
		if value == "" {
			return fmt.Errorf("path matcher cannot be empty")
		}
		if builder.pathCount != 0 {
			return fmt.Errorf("only one path matcher is allowed")
		}
		set(&builder.matcher)
		builder.pathCount++
		return nil
	}
}
