package bodycmp

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"strings"
)

// DecodeBody decompresses body according to the comma-separated HTTP
// Content-Encoding header value while enforcing maxBytes and panic isolation.
func DecodeBody(
	body []byte,
	encoding string,
	maxBytes int64,
	openCustom func(name string, reader io.Reader) (io.ReadCloser, bool, error),
	validToken func(string) bool,
) ([]byte, error) {
	var (
		decoded []byte
		err     error
	)
	func() {
		defer func() {
			if recover() != nil {
				decoded = nil
				err = fmt.Errorf("content decoder panicked")
			}
		}()
		decoded, err = decodeBodyEncodings(
			body,
			encoding,
			maxBytes,
			openCustom,
			validToken,
		)
	}()
	return decoded, err
}

func decodeBodyEncodings(
	body []byte,
	encoding string,
	maxBytes int64,
	openCustom func(name string, reader io.Reader) (io.ReadCloser, bool, error),
	validToken func(string) bool,
) ([]byte, error) {
	decoded := append([]byte(nil), body...)
	encodings := strings.Split(encoding, ",")
	for i := len(encodings) - 1; i >= 0; i-- {
		name := strings.ToLower(strings.TrimSpace(encodings[i]))
		if name == "" {
			if len(encodings) == 1 {
				continue
			}
			return nil, fmt.Errorf("malformed content encoding list")
		}
		if validToken != nil && !validToken(name) {
			return nil, fmt.Errorf("invalid content encoding %q", name)
		}
		if name == "identity" {
			continue
		}
		var err error
		decoded, err = decodeSingleEncoding(decoded, name, maxBytes, openCustom)
		if err != nil {
			return nil, err
		}
	}
	if int64(len(decoded)) > maxBytes {
		return nil, fmt.Errorf("decoded body exceeds %d bytes", maxBytes)
	}
	return decoded, nil
}

func decodeSingleEncoding(
	decoded []byte,
	name string,
	maxBytes int64,
	openCustom func(name string, reader io.Reader) (io.ReadCloser, bool, error),
) ([]byte, error) {
	reader, err := openEncodingReader(decoded, name, openCustom)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, fmt.Errorf("content decoder %q returned a nil reader", name)
	}
	output, readErr := readBounded(reader, maxBytes)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return output, nil
}

func openEncodingReader(
	decoded []byte,
	name string,
	openCustom func(name string, reader io.Reader) (io.ReadCloser, bool, error),
) (io.ReadCloser, error) {
	switch name {
	case "gzip":
		return gzip.NewReader(bytes.NewReader(decoded))
	case "deflate":
		reader, err := zlib.NewReader(bytes.NewReader(decoded))
		if err != nil {
			return flate.NewReader(bytes.NewReader(decoded)), nil
		}
		return reader, nil
	default:
		if openCustom != nil {
			reader, ok, err := openCustom(name, bytes.NewReader(decoded))
			if ok {
				return reader, err
			}
		}
		return nil, fmt.Errorf("unsupported content encoding %q", name)
	}
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("decoded body exceeds %d bytes", limit)
	}
	return body, nil
}
