package shadowmw_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/sergeybataev/shadowmw"
	"github.com/sergeybataev/shadowmw/internal/render"
)

type fuzzObserverFunc func(context.Context, shadowmw.Outcome) error

func (f fuzzObserverFunc) Observe(ctx context.Context, outcome shadowmw.Outcome) error {
	return f(ctx, outcome)
}

func newAcceptanceEngine(
	t *testing.T,
	maxInFlight int,
	outcomes chan<- shadowmw.Outcome,
) *shadowmw.Engine {
	t.Helper()
	cfg := shadowmw.DefaultEngineConfig()
	cfg.MaxInFlightSessions = maxInFlight
	cfg.ComparisonWorkers = 2
	cfg.SessionTimeout = 200 * time.Millisecond
	cfg.ComparisonTimeout = 200 * time.Millisecond
	if outcomes != nil {
		cfg.Observer = &shadowmw.ObserverDelivery{
			Observer: fuzzObserverFunc(func(_ context.Context, outcome shadowmw.Outcome) error {
				outcomes <- outcome
				return nil
			}),
			QueueCapacity:  32,
			MaxQueuedBytes: 1 << 20,
			Workers:        1,
			Timeout:        200 * time.Millisecond,
			Filter:         shadowmw.ObserveMatches | shadowmw.ObserveMismatches | shadowmw.ObserveSkipped,
			Detail:         shadowmw.OutcomeFull,
		}
	}
	engine, err := shadowmw.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = engine.Shutdown(ctx)
	})
	return engine
}

func newAcceptanceMiddleware(
	t *testing.T,
	engine *shadowmw.Engine,
	authoritative http.Handler,
	cfg shadowmw.MiddlewareConfig,
) *shadowmw.Middleware {
	t.Helper()
	rule, err := shadowmw.NewRule(
		"acceptance.http",
		shadowmw.ResponseParity(),
		shadowmw.MatchAll(),
		shadowmw.SampleAll(),
	)
	if err != nil {
		t.Fatalf("NewRule() error = %v", err)
	}
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, cfg, rule)
	if err != nil {
		t.Fatalf("NewMiddleware() error = %v", err)
	}
	return middleware
}

func waitForOutcome(t *testing.T, outcomes <-chan shadowmw.Outcome) shadowmw.Outcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for outcome")
		return shadowmw.Outcome{}
	}
}

func fuzzComparisonOutcome(
	t *testing.T,
	policy shadowmw.Policy,
	cfg shadowmw.MiddlewareConfig,
	request *http.Request,
	authoritative,
	shadow http.Handler,
) (shadowmw.Outcome, *httptest.ResponseRecorder) {
	t.Helper()
	outcomes := make(chan shadowmw.Outcome, 1)
	engine := newAcceptanceEngine(t, 1, outcomes)
	rule, err := shadowmw.NewRule("fuzz.http", policy, shadowmw.MatchAll(), shadowmw.SampleAll())
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := shadowmw.NewMiddleware(engine, authoritative, cfg, rule)
	if err != nil {
		t.Fatal(err)
	}
	client := httptest.NewRecorder()
	middleware.Wrap(shadow).ServeHTTP(client, request)
	return waitForOutcome(t, outcomes), client
}

func fuzzBodyOutcome(
	t *testing.T,
	policy shadowmw.Policy,
	header http.Header,
	authoritative,
	shadow []byte,
) shadowmw.Outcome {
	t.Helper()
	handler := func(body []byte) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			for name, values := range header {
				w.Header()[name] = append([]string(nil), values...)
			}
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write(body)
		})
	}
	cfg := shadowmw.DefaultMiddlewareConfig()
	cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
	outcome, _ := fuzzComparisonOutcome(
		t,
		policy,
		cfg,
		httptest.NewRequest(http.MethodGet, "/", nil),
		handler(authoritative),
		handler(shadow),
	)
	return outcome
}

func TestOutcomeDifferenceRenderingNeverSplitsUTF8(t *testing.T) {
	expected := render.ResponseInput{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
		Body:       []byte("same\ncafé🙂"),
	}
	actual := render.ResponseInput{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
		Body:       []byte("same\ncafè漢"),
	}

	const contextLines = 3
	full := render.OutcomeDiff(expected, actual, contextLines, 1<<20)
	if !utf8.Valid(full) {
		t.Fatal("unbounded rendering is not valid UTF-8")
	}
	if !bytes.Contains(full, []byte("é")) || !bytes.Contains(full, []byte("🙂")) {
		t.Fatalf("test fixture did not reach rendered output:\n%s", full)
	}

	sawUnfittableRune := false
	for limit := int64(0); limit <= int64(len(full)); limit++ {
		rendered := render.OutcomeDiff(expected, actual, contextLines, limit)
		if int64(len(rendered)) > limit {
			t.Fatalf("limit %d: rendered %d bytes", limit, len(rendered))
		}
		if !utf8.Valid(rendered) {
			t.Fatalf("limit %d: rendering splits UTF-8: %x", limit, rendered)
		}
		if !bytes.Equal(rendered, full[:len(rendered)]) {
			t.Fatalf("limit %d: rendering is not a prefix of the complete output", limit)
		}
		if len(rendered) < int(limit) && limit < int64(len(full)) {
			sawUnfittableRune = true
		}
		if second := render.OutcomeDiff(expected, actual, contextLines, limit); !bytes.Equal(rendered, second) {
			t.Fatalf("limit %d: rendering is nondeterministic", limit)
		}
	}
	if !sawUnfittableRune {
		t.Fatal("test did not exercise a limit too small for the next rune")
	}
}

func FuzzOutcomeDifferenceRendering(f *testing.F) {
	f.Add([]byte("alpha\nold"), []byte("alpha\nnew"), "text/plain", uint8(3), uint16(1024))
	f.Add([]byte{0, 1, 2}, []byte{0, 1, 3}, "application/octet-stream", uint8(1), uint16(64))
	f.Add([]byte(`{"n":1e999999}`), []byte(`{"n":1.0e999999}`), "application/json", uint8(3), uint16(128))
	f.Add([]byte(`{"broken"`), []byte(`{"different"`), "application/json", uint8(0), uint16(96))
	f.Add([]byte("printable private binary payload"), []byte("different private binary payload"), "application/octet-stream", uint8(2), uint16(80))
	f.Add([]byte("café🙂"), []byte("cafè漢"), "text/plain; charset=utf-8", uint8(3), uint16(1))
	f.Add([]byte("old"), []byte("new"), string([]byte{0xff}), uint8(1), uint16(8))
	f.Fuzz(func(
		t *testing.T,
		authoritative []byte,
		shadow []byte,
		contentType string,
		contextLines uint8,
		maximum uint16,
	) {
		limit := int64(maximum) + 1
		expected := render.ResponseInput{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{contentType}},
			Body:       append([]byte(nil), authoritative...),
		}
		actual := render.ResponseInput{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{contentType}},
			Body:       append([]byte(nil), shadow...),
		}
		rendered := render.OutcomeDiff(expected, actual, int(contextLines%8), limit)
		if int64(len(rendered)) > limit {
			t.Fatalf("rendered bytes = %d, limit = %d", len(rendered), limit)
		}
		if !utf8.Valid(rendered) {
			t.Fatalf("rendered output is not valid UTF-8: %x", rendered)
		}
	})
}

func FuzzRequestBodyParsing(f *testing.F) {
	f.Add([]byte("hello"), uint16(4), int8(-1))
	f.Add([]byte{}, uint16(1), int8(0))
	f.Fuzz(func(t *testing.T, body []byte, rawLimit uint16, lengthMode int8) {
		limit := int64(rawLimit%4096) + 1
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Body = io.NopCloser(bytes.NewReader(body))
		switch lengthMode % 3 {
		case 0:
			request.ContentLength = int64(len(body))
		case 1:
			request.ContentLength = -1
		default:
			request.ContentLength = limit + 1
		}
		declaredLength := request.ContentLength
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(w, r.Body)
		})
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.MaxRequestBodyBytes = limit
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		outcome, client := fuzzComparisonOutcome(
			t,
			shadowmw.ResponseParity(shadowmw.CompareBodyAsBytes()),
			cfg,
			request,
			handler,
			handler,
		)
		if !bytes.Equal(client.Body.Bytes(), body) {
			t.Fatal("authoritative request body was not preserved")
		}
		if declaredLength > limit || int64(len(body)) > limit {
			if outcome.Classification() != shadowmw.OutcomeSkipped || outcome.Reason() != shadowmw.ReasonRequestTooLarge {
				t.Fatalf("outcome = %s/%s, want request_too_large skip", outcome.Classification(), outcome.Reason())
			}
		} else if outcome.Classification() != shadowmw.OutcomeMatch {
			t.Fatalf("independent branch bodies did not match: %s/%s", outcome.Classification(), outcome.Reason())
		}
	})
}

func FuzzResponseParsing(f *testing.F) {
	f.Add([]byte("alpha"), "text/plain", uint16(64), uint16(http.StatusOK))
	f.Add([]byte{0, 1, 2}, "application/octet-stream", uint16(2), uint16(http.StatusCreated))
	f.Fuzz(func(t *testing.T, body []byte, contentType string, rawLimit, rawStatus uint16) {
		limit := int64(rawLimit%4096) + 1
		status := int(rawStatus%400) + 200
		if status == http.StatusNoContent || status == http.StatusNotModified {
			body = nil
		}
		handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(status)
			_, _ = w.Write(body)
		})
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.MaxResponseBodyBytes = limit
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		outcome, client := fuzzComparisonOutcome(
			t,
			shadowmw.ResponseParity(),
			cfg,
			httptest.NewRequest(http.MethodGet, "/", nil),
			handler,
			handler,
		)
		if !bytes.Equal(client.Body.Bytes(), body) {
			t.Fatal("response capture changed the authoritative client body")
		}
		if outcome.Classification() == shadowmw.OutcomeSkipped {
			return
		}
		for _, snapshot := range []shadowmw.ResponseSnapshot{outcome.AuthoritativeResponse(), outcome.ShadowResponse()} {
			if int64(len(snapshot.Body())) > limit {
				t.Fatalf("captured bytes exceed limit %d", limit)
			}
			if snapshot.Truncated() != (int64(len(body)) > limit) {
				t.Fatalf("truncation = %v for %d bytes at limit %d", snapshot.Truncated(), len(body), limit)
			}
		}
	})
}

func FuzzJSONPointerValidation(f *testing.F) {
	f.Add("/stable/0")
	f.Add("not-a-pointer")
	f.Fuzz(func(t *testing.T, pointer string) {
		policy := shadowmw.ResponseParity(shadowmw.IgnoreJSONPointers(pointer))
		_, _ = shadowmw.NewRule("fuzz.json", policy, shadowmw.MatchAll(), shadowmw.SampleAll())
	})
}

func FuzzFormComparison(f *testing.F) {
	f.Add([]byte("a=1&b=2"), []byte("b=2&a=1"), "a")
	f.Add([]byte("%"), []byte("%zz"), "")
	f.Fuzz(func(t *testing.T, authoritative, shadow []byte, sortKey string) {
		policy := shadowmw.ResponseParity(shadowmw.CompareBodyAsForm(), shadowmw.SortFormValues(sortKey))
		fuzzBodyOutcome(
			t,
			policy,
			http.Header{"Content-Type": {"application/x-www-form-urlencoded"}},
			authoritative,
			shadow,
		)
	})
}

func FuzzContentDecoding(f *testing.F) {
	f.Add([]byte("plain"), "identity", uint16(64))
	f.Add([]byte{0x1f, 0x8b}, "gzip", uint16(8))
	f.Fuzz(func(t *testing.T, body []byte, encoding string, rawLimit uint16) {
		limit := int64(rawLimit%4096) + 1
		outcome := fuzzBodyOutcome(
			t,
			shadowmw.ResponseParity(shadowmw.CompareBodyAsBytes(), shadowmw.WithMaxDecodedBodyBytes(limit)),
			http.Header{"Content-Encoding": {encoding}},
			body,
			body,
		)
		for _, representation := range []shadowmw.BodyRepresentation{outcome.AuthoritativeBodyRepresentation(), outcome.ShadowBodyRepresentation()} {
			if int64(len(representation.Body())) > limit {
				t.Fatalf("compared bytes exceed decoded limit %d", limit)
			}
		}
	})
}

func FuzzInvalidComparisonInput(f *testing.F) {
	f.Add([]byte(`{"broken"`), []byte(`%zz`), "application/json")
	f.Add([]byte{0xff}, []byte{0xfe}, "application/octet-stream")
	f.Fuzz(func(t *testing.T, authoritative, shadow []byte, contentType string) {
		fuzzBodyOutcome(t, shadowmw.ResponseParity(), http.Header{"Content-Type": {contentType}}, authoritative, shadow)
	})
}

func FuzzMiddlewareMalformedAcceptQualityDoesNotBypass(f *testing.F) {
	for form := range uint8(8) {
		f.Add(form, "")
	}
	f.Add(uint8(0), "leading-zero")
	f.Add(uint8(3), "overprecision")
	f.Add(uint8(7), "scientific")

	f.Fuzz(func(t *testing.T, form uint8, suffix string) {
		encodedSuffix := hex.EncodeToString([]byte(suffix))
		var quality string
		switch form % 8 {
		case 0:
			quality = "01" + encodedSuffix
		case 1:
			quality = "+0." + encodedSuffix
		case 2:
			quality = "-0." + encodedSuffix
		case 3:
			quality = "0.0000" + encodedSuffix
		case 4:
			quality = "1.001" + encodedSuffix
		case 5:
			quality = "2" + encodedSuffix
		case 6:
			quality = ".5" + encodedSuffix
		case 7:
			quality = "1e0" + encodedSuffix
		}

		engine := newAcceptanceEngine(t, 1, nil)
		shadowCalled := make(chan struct{}, 1)
		cfg := shadowmw.DefaultMiddlewareConfig()
		cfg.BodyReadErrors = shadowmw.RejectBodyReadErrors()
		handler := newAcceptanceMiddleware(
			t,
			engine,
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}),
			cfg,
		).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			shadowCalled <- struct{}{}
			w.WriteHeader(http.StatusNoContent)
		}))
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set("Accept", "text/event-stream; q="+quality)
		recorder := httptest.NewRecorder()

		handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusNoContent {
			t.Errorf("client status = %d, want %d", recorder.Code, http.StatusNoContent)
		}
		select {
		case <-shadowCalled:
		case <-time.After(time.Second):
			t.Fatalf("malformed q=%q bypassed shadow execution", quality)
		}
	})
}

var ciFuzzTargetLine = regexp.MustCompile(
	`(?m)^\s+(Fuzz[A-Za-z0-9_]+)(?:\s*\\)?\s*$`,
)

func TestCIFuzzSmokeCoversEveryRootFuzzTarget(t *testing.T) {
	discovered := rootFuzzTargets(t)
	workflow, err := os.ReadFile(filepath.Join(".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read CI workflow: %v", err)
	}
	matches := ciFuzzTargetLine.FindAllStringSubmatch(string(workflow), -1)
	configured := make([]string, 0, len(matches))
	for _, match := range matches {
		configured = append(configured, match[1])
	}
	slices.Sort(configured)

	if !slices.Equal(configured, discovered) {
		t.Fatalf(
			"CI fuzz smoke targets = %s, root fuzz targets = %s",
			strings.Join(configured, ", "),
			strings.Join(discovered, ", "),
		)
	}
}

func rootFuzzTargets(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatalf("glob root tests: %v", err)
	}
	targets := make([]string, 0)
	fileset := token.NewFileSet()
	for _, filename := range files {
		file, err := parser.ParseFile(fileset, filename, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", filename, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil ||
				!strings.HasPrefix(function.Name.Name, "Fuzz") {
				continue
			}
			targets = append(targets, function.Name.Name)
		}
	}
	slices.Sort(targets)
	return targets
}
