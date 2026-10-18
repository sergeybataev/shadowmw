package httpcap

import (
	"bytes"
	"net/http"
	"strings"
	"sync"
)

// CapturedResponse holds the immutable HTTP response state captured from an
// authoritative or shadow handler execution.
type CapturedResponse struct {
	StatusCode int
	Header     http.Header
	Trailer    http.Header
	Body       []byte
	Truncated  bool
}

// SnapshotWriter captures a shadow branch HTTP response without writing to a
// network connection.
type SnapshotWriter struct {
	mu              sync.Mutex
	header          http.Header
	committedHeader http.Header
	status          int
	headerFlushed   bool
	responseBodyCapture
}

// NewSnapshotWriter constructs a SnapshotWriter bounded by limit bytes.
func NewSnapshotWriter(limit int64) *SnapshotWriter {
	return &SnapshotWriter{
		header:              make(http.Header),
		responseBodyCapture: responseBodyCapture{limit: limit},
	}
}

// Header returns the mutable response header map.
func (w *SnapshotWriter) Header() http.Header { return w.header }

// WriteHeader commits the HTTP status code and response headers.
func (w *SnapshotWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status != 0 || informationalStatus(status) {
		return
	}
	w.status = status
	w.committedHeader = captureCommittedHeader(w.header)
}

// Flush commits the response header and sniffs Content-Type if needed.
func (w *SnapshotWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
		w.committedHeader = captureCommittedHeader(w.header)
	}
	if !w.headerFlushed {
		w.inferContentType(w.committedHeader, w.status)
		w.headerFlushed = true
	}
}

// Write appends body bytes up to the configured capture limit.
func (w *SnapshotWriter) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
		w.committedHeader = captureCommittedHeader(w.header)
	}
	w.capture(body)
	return len(body), nil
}

// Snapshot returns the finalized CapturedResponse.
func (w *SnapshotWriter) Snapshot() CapturedResponse {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buildSnapshot(
		w.status,
		w.header,
		&w.committedHeader,
		w.headerFlushed,
	)
}

// ObservingWriter tee-captures an authoritative HTTP response while streaming
// directly to the underlying client ResponseWriter.
type ObservingWriter struct {
	target          http.ResponseWriter
	onCommit        func(http.Header)
	header          http.Header
	status          int
	headerFlushed   bool
	transportFailed bool
	responseBodyCapture
}

// NewObservingWriter wraps target with bounded response capture up to limit
// bytes and invokes onCommit when the final status header is committed.
func NewObservingWriter(
	target http.ResponseWriter,
	limit int64,
	onCommit func(http.Header),
) *ObservingWriter {
	return &ObservingWriter{
		target:              target,
		onCommit:            onCommit,
		responseBodyCapture: responseBodyCapture{limit: limit},
	}
}

// Header returns the underlying writer's header map.
func (w *ObservingWriter) Header() http.Header { return w.target.Header() }

// WriteHeader forwards status to the underlying writer and commits headers for
// non-informational responses.
func (w *ObservingWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if informationalStatus(status) {
		w.target.WriteHeader(status)
		return
	}
	w.status = status
	w.header = captureCommittedHeader(w.target.Header())
	w.target.WriteHeader(status)
	if w.onCommit != nil {
		w.onCommit(w.header)
	}
}

// Write forwards body to the underlying writer and captures written bytes.
func (w *ObservingWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.target.Write(body)
	if err != nil || n != len(body) {
		w.transportFailed = true
	}
	if n > 0 {
		w.capture(body[:min(n, len(body))])
	}
	return n, err
}

// Flush flushes the underlying writer if it implements http.Flusher.
func (w *ObservingWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.target.(http.Flusher); ok {
		if !w.headerFlushed {
			w.inferContentType(w.header, w.status)
			w.headerFlushed = true
		}
		flusher.Flush()
	}
}

// Unwrap lets http.ResponseController reach optional interfaces implemented by
// the client-facing writer.
func (w *ObservingWriter) Unwrap() http.ResponseWriter {
	return w.target
}

// TransportFailed reports whether a write to the underlying client writer
// returned an error or short write.
func (w *ObservingWriter) TransportFailed() bool {
	return w.transportFailed
}

// Snapshot returns the finalized CapturedResponse.
func (w *ObservingWriter) Snapshot() CapturedResponse {
	return w.buildSnapshot(
		w.status,
		w.target.Header(),
		&w.header,
		w.headerFlushed,
	)
}

type responseBodyCapture struct {
	body      bytes.Buffer
	limit     int64
	truncated bool
	// Sniffing has its own fixed bound, even when body capture is smaller.
	prefix []byte
}

func (c *responseBodyCapture) capture(body []byte) {
	if len(c.prefix) < 512 {
		c.prefix = append(c.prefix, body[:min(len(body), 512-len(c.prefix))]...)
	}
	remaining := c.limit - int64(c.body.Len())
	if remaining <= 0 {
		c.truncated = c.truncated || len(body) > 0
		return
	}
	if int64(len(body)) > remaining {
		_, _ = c.body.Write(body[:remaining])
		c.truncated = true
		return
	}
	_, _ = c.body.Write(body)
}

func (c *responseBodyCapture) inferContentType(header http.Header, status int) {
	_, explicit := header["Content-Type"]
	if explicit || len(c.prefix) == 0 || status < 200 ||
		status == http.StatusNoContent || status == http.StatusNotModified ||
		header.Get("Content-Encoding") != "" || header.Get("Transfer-Encoding") != "" {
		return
	}
	header.Set("Content-Type", http.DetectContentType(c.prefix))
}

func (c *responseBodyCapture) buildSnapshot(
	status int,
	liveHeader http.Header,
	committedHeader *http.Header,
	headerFlushed bool,
) CapturedResponse {
	if status == 0 {
		status = http.StatusOK
		*committedHeader = captureCommittedHeader(liveHeader)
	}
	if !headerFlushed {
		c.inferContentType(*committedHeader, status)
	}
	return CapturedResponse{
		StatusCode: status,
		Header:     (*committedHeader).Clone(),
		Trailer:    captureTrailers(liveHeader, *committedHeader),
		Body:       append([]byte(nil), c.body.Bytes()...),
		Truncated:  c.truncated,
	}
}

func informationalStatus(status int) bool {
	return status >= 100 && status <= 199 && status != http.StatusSwitchingProtocols
}

func captureCommittedHeader(header http.Header) http.Header {
	committed := header.Clone()
	for name := range committed {
		if !strings.HasPrefix(name, http.TrailerPrefix) {
			continue
		}
		trailerName := http.CanonicalHeaderKey(strings.TrimPrefix(name, http.TrailerPrefix))
		delete(committed, name)
		if trailerName != "" && !trailerDeclared(committed, trailerName) {
			committed.Add("Trailer", trailerName)
		}
	}
	return committed
}

func trailerDeclared(header http.Header, want string) bool {
	for _, declaration := range header.Values("Trailer") {
		for _, name := range strings.Split(declaration, ",") {
			if http.CanonicalHeaderKey(strings.TrimSpace(name)) == want {
				return true
			}
		}
	}
	return false
}

func captureTrailers(final, committed http.Header) http.Header {
	trailers := make(http.Header)
	for _, declaration := range committed.Values("Trailer") {
		for _, rawName := range strings.Split(declaration, ",") {
			name := http.CanonicalHeaderKey(strings.TrimSpace(rawName))
			if name == "" {
				continue
			}
			if values, ok := final[name]; ok {
				trailers[name] = append([]string(nil), values...)
			}
		}
	}
	for name, values := range final {
		if !strings.HasPrefix(name, http.TrailerPrefix) {
			continue
		}
		trailerName := http.CanonicalHeaderKey(strings.TrimPrefix(name, http.TrailerPrefix))
		if trailerName != "" {
			trailers[trailerName] = append([]string(nil), values...)
		}
	}
	if len(trailers) == 0 {
		return nil
	}
	return trailers
}
