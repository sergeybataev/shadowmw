// Package httpcap provides bounded HTTP request/response capture and finite
// exchange classification for shadowmw.
package httpcap

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

// CaptureRequestBody reads up to limit bytes from request.Body while preserving
// replay or stream reconstruction for the authoritative handler.
func CaptureRequestBody(request *http.Request, limit int64) ([]byte, bool, error) {
	if request.Body == nil {
		return nil, false, nil
	}
	if request.ContentLength > limit {
		return nil, true, nil
	}
	source := request.Body
	captured, err := io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		request.Body = &replayReadErrorBody{
			reader: bytes.NewReader(captured),
			err:    err,
			closer: source,
		}
		return captured, false, err
	}
	if int64(len(captured)) > limit {
		request.Body = &reconstructedBody{
			Reader: io.MultiReader(bytes.NewReader(captured), source),
			closer: source,
		}
		return nil, true, nil
	}
	_ = source.Close()
	request.Body = io.NopCloser(bytes.NewReader(captured))
	return captured, false, nil
}

// CloneRequest creates a deep copy of request with ctx and an independent body
// reader backed by body.
func CloneRequest(
	request *http.Request,
	ctx context.Context,
	body []byte,
) *http.Request {
	cloned := request.Clone(ctx)
	cloned.Body = io.NopCloser(bytes.NewReader(body))
	cloned.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	cloned.ContentLength = int64(len(body))
	return cloned
}

// ContextWithLifetime derives a context whose values come from values and whose
// cancellation and deadline come from lifetime.
func ContextWithLifetime(
	values context.Context,
	lifetime context.Context,
) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(values)
	if lifetime.Done() == nil {
		return base, func() {}
	}
	ctx, cancel := context.WithCancelCause(base)
	propagate := func() {
		cancel(context.Cause(lifetime))
	}
	stop := context.AfterFunc(lifetime, propagate)
	if lifetime.Err() != nil {
		propagate()
	}
	return lifetimeDeadlineContext{Context: ctx, lifetime: lifetime}, func() {
		stop()
		cancel(context.Canceled)
	}
}

type reconstructedBody struct {
	io.Reader
	closer io.Closer
}

func (b *reconstructedBody) Close() error {
	if b.closer == nil {
		return nil
	}
	return b.closer.Close()
}

type replayReadErrorBody struct {
	reader *bytes.Reader
	err    error
	closer io.Closer
}

func (b *replayReadErrorBody) Read(destination []byte) (int, error) {
	if b.reader.Len() > 0 {
		return b.reader.Read(destination)
	}
	if b.err != nil {
		err := b.err
		b.err = nil
		return 0, err
	}
	return 0, io.EOF
}

func (b *replayReadErrorBody) Close() error {
	if b.closer == nil {
		return nil
	}
	return b.closer.Close()
}

type lifetimeDeadlineContext struct {
	context.Context
	lifetime context.Context
}

func (c lifetimeDeadlineContext) Deadline() (time.Time, bool) {
	return c.lifetime.Deadline()
}
