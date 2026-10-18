package httpcap_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sergeybataev/shadowmw/internal/httpcap"
)

func TestCaptureRequestBodyWithinLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/items", bytes.NewBufferString("hello"))
	captured, tooLarge, err := httpcap.CaptureRequestBody(req, 16)
	if err != nil {
		t.Fatalf("CaptureRequestBody() error = %v", err)
	}
	if tooLarge {
		t.Fatal("CaptureRequestBody() tooLarge = true, want false")
	}
	if string(captured) != "hello" {
		t.Fatalf("captured = %q, want hello", captured)
	}
	replayed, err := io.ReadAll(req.Body)
	if err != nil || string(replayed) != "hello" {
		t.Fatalf("replayed = %q, err = %v", replayed, err)
	}
}

func TestSnapshotAndObservingWritersCaptureTrailers(t *testing.T) {
	writer := httpcap.NewSnapshotWriter(64)
	writer.Header().Set("Trailer", "X-Checksum")
	writer.WriteHeader(http.StatusCreated)
	_, _ = writer.Write([]byte("payload"))
	writer.Header().Set("X-Checksum", "abc123")
	snapshot := writer.Snapshot()
	if snapshot.StatusCode != http.StatusCreated {
		t.Fatalf("StatusCode = %d, want %d", snapshot.StatusCode, http.StatusCreated)
	}
	if got := snapshot.Trailer.Get("X-Checksum"); got != "abc123" {
		t.Fatalf("Trailer X-Checksum = %q, want abc123", got)
	}
}

func TestUnsupportedRequestAndEventStream(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set("Accept", "text/event-stream;q=0.5")
	if !httpcap.UnsupportedRequest(req) {
		t.Fatal("UnsupportedRequest() = false for accepted SSE")
	}
	reqZero := httptest.NewRequest(http.MethodGet, "/events", nil)
	reqZero.Header.Set("Accept", "text/event-stream;q=0")
	if httpcap.UnsupportedRequest(reqZero) {
		t.Fatal("UnsupportedRequest() = true for q=0 SSE")
	}
	if !httpcap.IsEventStream("text/event-stream; charset=utf-8") {
		t.Fatal("IsEventStream() = false, want true")
	}
}

func TestContextWithLifetimePropagatesCancel(t *testing.T) {
	type key struct{}
	values := context.WithValue(context.Background(), key{}, "v")
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	merged, cancel := httpcap.ContextWithLifetime(values, lifetime)
	defer cancel()
	if got := merged.Value(key{}); got != "v" {
		t.Fatalf("merged.Value() = %v, want v", got)
	}
	cancelLifetime()
	select {
	case <-merged.Done():
	case <-context.Background().Done():
	}
	if merged.Err() == nil {
		t.Fatal("merged.Err() = nil after lifetime cancel")
	}
}
