package httpcap

import (
	"mime"
	"net/http"
	"strings"
)

// UnsupportedRequest reports whether request uses a protocol or streaming/upgrade
// negotiation outside finite HTTP/1.x request/response comparison scope.
func UnsupportedRequest(request *http.Request) bool {
	if request.ProtoMajor != 1 {
		return true
	}
	if acceptsEventStream(request.Header.Values("Accept")) {
		return true
	}
	connection := strings.ToLower(request.Header.Get("Connection"))
	return strings.Contains(connection, "upgrade") ||
		request.Header.Get("Upgrade") != ""
}

// IsEventStream reports whether contentType has media type text/event-stream.
func IsEventStream(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && strings.EqualFold(mediaType, "text/event-stream")
}

func acceptsEventStream(values []string) bool {
	for _, value := range values {
		for _, mediaRange := range splitMediaRanges(value) {
			if acceptsEventStreamRange(mediaRange) {
				return true
			}
		}
	}
	return false
}

func acceptsEventStreamRange(mediaRange string) bool {
	mediaType, parameters, err := mime.ParseMediaType(mediaRange)
	if err != nil || !strings.EqualFold(mediaType, "text/event-stream") {
		return false
	}
	quality, present := parameters["q"]
	if !present {
		return true
	}
	rawQuality, present := rawMediaParameter(mediaRange, "q")
	return present && rawQuality == quality && acceptedHTTPQuality(rawQuality)
}

func acceptedHTTPQuality(quality string) bool {
	if quality == "1" {
		return true
	}
	if len(quality) < 2 || len(quality) > 5 || quality[1] != '.' {
		return false
	}
	switch quality[0] {
	case '0':
		accepted := false
		for _, digit := range quality[2:] {
			if digit < '0' || digit > '9' {
				return false
			}
			accepted = accepted || digit != '0'
		}
		return accepted
	case '1':
		for _, digit := range quality[2:] {
			if digit != '0' {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func splitMediaRanges(value string) []string {
	return splitOutsideQuotes(value, ',')
}

func rawMediaParameter(mediaRange, name string) (string, bool) {
	parameters := splitOutsideQuotes(mediaRange, ';')
	for _, parameter := range parameters[1:] {
		separator := strings.IndexByte(parameter, '=')
		if separator < 0 ||
			!strings.EqualFold(strings.TrimSpace(parameter[:separator]), name) {
			continue
		}
		return strings.TrimSpace(parameter[separator+1:]), true
	}
	return "", false
}

func splitOutsideQuotes(value string, separator byte) []string {
	parts := make([]string, 0, strings.Count(value, string(separator))+1)
	start := 0
	quoted := false
	escaped := false
	for index := range len(value) {
		character := value[index]
		switch {
		case escaped:
			escaped = false
		case quoted && character == '\\':
			escaped = true
		case character == '"':
			quoted = !quoted
		case character == separator && !quoted:
			parts = append(parts, strings.TrimSpace(value[start:index]))
			start = index + 1
		}
	}
	return append(parts, strings.TrimSpace(value[start:]))
}
