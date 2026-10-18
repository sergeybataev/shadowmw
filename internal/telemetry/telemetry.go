// Package telemetry isolates collaborator callbacks behind a bounded queue and
// per-callback watchdog timer.
package telemetry

import (
	"sync"
	"sync/atomic"
	"time"
)

// Kind identifies one telemetry collaborator category.
type Kind uint8

// Kind values classify instrumentation, tracing, and logging tasks.
const (
	KindInstrumentation Kind = iota
	KindTracing
	KindLogging
)

// FailureIndex identifies a specific failure counter slot.
type FailureIndex uint8

// FailureIndex values map collaborator panics, timeouts, and drops to atomic counters.
const (
	InstrumentationPanic FailureIndex = iota
	InstrumentationTimeout
	InstrumentationDropped
	TracingPanic
	TracingTimeout
	TracingDropped
	LoggingPanic
	LoggingTimeout
	LoggingDropped
	FailureCount
)

// callbackTimeout bounds collaborator execution after its goroutine has been
// scheduled. The margin tolerates race-detector and host scheduler pauses
// without letting a genuinely stuck telemetry callback block draining.
const callbackTimeout = 100 * time.Millisecond

// Task represents one asynchronous telemetry callback execution.
type Task struct {
	Kind Kind
	Run  func()
}

// Runtime executes telemetry tasks asynchronously with watchdog isolation and
// bounded queue capacity.
type Runtime struct {
	queue chan Task
	wg    sync.WaitGroup

	closedMu sync.RWMutex
	closed   bool
	disabled [3]atomic.Bool
	failures [FailureCount]atomic.Uint64
}

// NewRuntime constructs a bounded telemetry Runtime.
func NewRuntime(capacity int) *Runtime {
	if capacity < 64 {
		capacity = 64
	}
	if capacity > 4096 {
		capacity = 4096
	}
	return &Runtime{
		queue: make(chan Task, capacity),
	}
}

// Start launches the background worker that drains queued telemetry tasks.
func (r *Runtime) Start() {
	r.wg.Add(1)
	go r.run()
}

// Enqueue schedules task for asynchronous execution or records a drop if the
// queue is full.
func (r *Runtime) Enqueue(task Task) {
	if r == nil || r.disabled[task.Kind].Load() {
		return
	}
	r.closedMu.RLock()
	if r.closed {
		r.closedMu.RUnlock()
		return
	}
	select {
	case r.queue <- task:
		r.closedMu.RUnlock()
	default:
		r.closedMu.RUnlock()
		r.RecordFailure(droppedIndex(task.Kind))
	}
}

// Shutdown closes the queue and waits for queued tasks to finish draining.
func (r *Runtime) Shutdown() {
	if r == nil {
		return
	}
	r.closedMu.Lock()
	if !r.closed {
		r.closed = true
		close(r.queue)
	}
	r.closedMu.Unlock()
	r.wg.Wait()
}

// Disabled reports whether collaborator kind has been disabled after a panic or
// timeout.
func (r *Runtime) Disabled(kind Kind) bool {
	if r == nil {
		return true
	}
	return r.disabled[kind].Load()
}

// Disable marks collaborator kind as disabled and increments failureIndex.
func (r *Runtime) Disable(kind Kind, failureIndex FailureIndex) {
	if r == nil {
		return
	}
	r.disabled[kind].Store(true)
	r.RecordFailure(failureIndex)
}

// RecordFailure increments the counter at index.
func (r *Runtime) RecordFailure(index FailureIndex) {
	if r == nil || index >= FailureCount {
		return
	}
	r.failures[index].Add(1)
}

// Failures returns a snapshot of the failure counters.
func (r *Runtime) Failures() [FailureCount]uint64 {
	var counts [FailureCount]uint64
	if r == nil {
		return counts
	}
	for index := range FailureCount {
		counts[index] = r.failures[index].Load()
	}
	return counts
}

// CallWithWatchdog runs fn in a goroutine after scheduling synchronization and
// reports whether it completed, panicked, or timed out.
func CallWithWatchdog(fn func()) (completed bool, panicked bool) {
	status := callTelemetry(fn)
	return status == callCompleted, status == callPanicked
}

func (r *Runtime) run() {
	defer r.wg.Done()
	for task := range r.queue {
		if r.disabled[task.Kind].Load() {
			continue
		}
		status := callTelemetry(task.Run)
		switch status {
		case callPanicked:
			r.Disable(task.Kind, failureIndex(task.Kind, true))
		case callTimedOut:
			r.Disable(task.Kind, failureIndex(task.Kind, false))
		}
	}
}

type callStatus uint8

const (
	callCompleted callStatus = iota
	callPanicked
	callTimedOut
)

func callTelemetry(run func()) callStatus {
	completed := make(chan bool, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		panicked := false
		defer func() {
			if recover() != nil {
				panicked = true
			}
			completed <- panicked
		}()
		run()
	}()
	<-started
	timer := time.NewTimer(callbackTimeout)
	defer timer.Stop()
	select {
	case panicked := <-completed:
		if panicked {
			return callPanicked
		}
		return callCompleted
	case <-timer.C:
		return callTimedOut
	}
}

func failureIndex(kind Kind, panicked bool) FailureIndex {
	switch kind {
	case KindInstrumentation:
		if panicked {
			return InstrumentationPanic
		}
		return InstrumentationTimeout
	case KindTracing:
		if panicked {
			return TracingPanic
		}
		return TracingTimeout
	default:
		if panicked {
			return LoggingPanic
		}
		return LoggingTimeout
	}
}

func droppedIndex(kind Kind) FailureIndex {
	switch kind {
	case KindInstrumentation:
		return InstrumentationDropped
	case KindTracing:
		return TracingDropped
	default:
		return LoggingDropped
	}
}
