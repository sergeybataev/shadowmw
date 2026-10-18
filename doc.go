// Package shadowmw compares an authoritative HTTP implementation with a
// non-authoritative replacement implementation without allowing the
// replacement response to reach the client.
//
// Applications construct and explicitly start one long-lived Engine, define
// immutable Rules, and wrap the replacement handler with Middleware. Accepted
// sessions are bounded by EngineConfig.MaxInFlightSessions and comparison work
// is bounded by EngineConfig.ComparisonWorkers. Shutdown stops admission and
// drains accepted comparison and observer work subject to the caller's
// deadline.
//
// v0 compares HTTP/1.x with authoritative and shadow branches executing in
// parallel. Declared unsupported exchanges bypass shadow execution or cancel
// an already-started shadow when detected at response commitment. Generic
// undeclared indefinite HTTP/1.x exchanges remain bounded by the session
// timeout.
//
// The root module has no OpenTelemetry or dependency-injection dependency.
// Optional explicit-provider OpenTelemetry and Fx lifecycle adapters are
// separate modules under contrib/otel and contrib/fx.
package shadowmw
