package render_test

import (
	"bytes"
	"net/http"
	"testing"
	"unicode/utf8"

	"github.com/sergeybataev/shadowmw/internal/render"
)

func TestOutcomeDiffFormatsUnifiedDiffAndRespectsLimit(t *testing.T) {
	expected := render.ResponseInput{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       []byte(`{"b":2,"a":1}`),
	}
	actual := render.ResponseInput{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       []byte(`{"a":1,"b":3}`),
	}
	diff := render.OutcomeDiff(expected, actual, 3, 1024)
	if !utf8.Valid(diff) {
		t.Fatal("OutcomeDiff() returned invalid UTF-8")
	}
	if !bytes.Contains(diff, []byte("--- authoritative\n+++ shadow\n")) {
		t.Fatalf("OutcomeDiff() missing unified diff header:\n%s", diff)
	}
}
