package core

import (
	"fmt"
	"net/http"

	"github.com/sergeybataev/shadowmw/internal/httpcap"
)

// BodyDecodingState reports whether comparison body decoding evidence is
// available, whether decoding ran, and whether it succeeded.
type BodyDecodingState uint8

// BodyDecodingState values classify branch body content-decoding outcomes.
const (
	BodyDecodingUnavailable BodyDecodingState = iota
	BodyDecodingNotAttempted
	BodyDecodingSucceeded
	BodyDecodingFailed
)

// String returns the canonical token for state.
func (state BodyDecodingState) String() string {
	switch state {
	case BodyDecodingUnavailable:
		return "unavailable"
	case BodyDecodingNotAttempted:
		return "not_attempted"
	case BodyDecodingSucceeded:
		return "succeeded"
	case BodyDecodingFailed:
		return "failed"
	default:
		return fmt.Sprintf("BodyDecodingState(%d)", state)
	}
}

// BodyRepresentationKind identifies the bounded body representation retained
// after decoding and optional structured normalization.
type BodyRepresentationKind uint8

// BodyRepresentationKind values classify retained compared body bytes.
const (
	BodyRepresentationNone BodyRepresentationKind = iota
	BodyRepresentationDecoded
	BodyRepresentationNormalized
)

// String returns the canonical token for kind.
func (kind BodyRepresentationKind) String() string {
	switch kind {
	case BodyRepresentationNone:
		return "none"
	case BodyRepresentationDecoded:
		return "decoded"
	case BodyRepresentationNormalized:
		return "normalized"
	default:
		return fmt.Sprintf("BodyRepresentationKind(%d)", kind)
	}
}

// BodyRepresentation is immutable, bounded evidence of the body representation
// used for one branch comparison. Present distinguishes an empty
// representation from no representation.
type BodyRepresentation struct {
	body          []byte
	present       bool
	decodingState BodyDecodingState
	kind          BodyRepresentationKind
}

// Body returns a defensive copy of the compared representation.
func (r BodyRepresentation) Body() []byte {
	if !r.present {
		return nil
	}
	body := make([]byte, len(r.body))
	copy(body, r.body)
	return body
}

// Present reports whether a compared representation exists, including an
// explicitly present empty representation.
func (r BodyRepresentation) Present() bool { return r.present }

// DecodingState reports whether branch body decoding evidence is available,
// whether decoding ran, and whether it succeeded.
func (r BodyRepresentation) DecodingState() BodyDecodingState {
	return r.decodingState
}

// Kind reports whether retained bytes are decoded or structured-normalized.
func (r BodyRepresentation) Kind() BodyRepresentationKind { return r.kind }

// ResponseSnapshot is a detached response representation safe for asynchronous
// comparison.
type ResponseSnapshot struct {
	statusCode int
	header     http.Header
	trailer    http.Header
	body       []byte
	truncated  bool
}

// StatusCode returns the committed HTTP status.
func (s ResponseSnapshot) StatusCode() int { return s.statusCode }

// Header returns a defensive copy of committed response headers.
func (s ResponseSnapshot) Header() http.Header { return s.header.Clone() }

// Trailer returns a defensive copy of response trailers.
func (s ResponseSnapshot) Trailer() http.Header { return s.trailer.Clone() }

// Body returns a defensive copy of captured response bytes.
func (s ResponseSnapshot) Body() []byte { return append([]byte(nil), s.body...) }

// Truncated reports whether response capture exceeded its configured bound.
func (s ResponseSnapshot) Truncated() bool { return s.truncated }

func snapshotFromCaptured(captured httpcap.CapturedResponse) ResponseSnapshot {
	return ResponseSnapshot{
		statusCode: captured.StatusCode,
		header:     captured.Header,
		trailer:    captured.Trailer,
		body:       captured.Body,
		truncated:  captured.Truncated,
	}
}

func (r BodyRepresentation) clone() BodyRepresentation {
	cloned := r
	if r.present {
		cloned.body = make([]byte, len(r.body))
		copy(cloned.body, r.body)
	} else {
		cloned.body = nil
	}
	return cloned
}

func (r BodyRepresentation) retainedBytes() int64 {
	return int64(len(r.body))
}

func decodedBodyRepresentation(
	body []byte,
	kind BodyRepresentationKind,
) BodyRepresentation {
	representation := BodyRepresentation{
		present:       true,
		decodingState: BodyDecodingSucceeded,
		kind:          kind,
		body:          make([]byte, len(body)),
	}
	copy(representation.body, body)
	return representation
}

func failedBodyRepresentation() BodyRepresentation {
	return BodyRepresentation{decodingState: BodyDecodingFailed}
}

func (s ResponseSnapshot) clone() ResponseSnapshot {
	return ResponseSnapshot{
		statusCode: s.statusCode,
		header:     s.header.Clone(),
		trailer:    s.trailer.Clone(),
		body:       append([]byte(nil), s.body...),
		truncated:  s.truncated,
	}
}

func (s ResponseSnapshot) retainedBytes() int64 {
	return 16 +
		headerRetainedBytes(s.header) +
		headerRetainedBytes(s.trailer) +
		int64(len(s.body))
}

func (s ResponseSnapshot) metadataOnly() ResponseSnapshot {
	return ResponseSnapshot{
		statusCode: s.statusCode,
		header:     metadataHeader(s.header),
		trailer:    metadataHeader(s.trailer),
		truncated:  s.truncated,
	}
}

func headerRetainedBytes(header http.Header) int64 {
	var size int64
	for name, values := range header {
		size += int64(len(name))
		for _, value := range values {
			size += int64(len(value))
		}
	}
	return size
}

func metadataHeader(header http.Header) http.Header {
	if len(header) == 0 {
		return nil
	}
	metadata := make(http.Header, len(header))
	for name := range header {
		metadata[name] = nil
	}
	return metadata
}
