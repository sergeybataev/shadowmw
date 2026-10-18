package telemetry_test

import (
	"sync/atomic"
	"testing"

	"github.com/sergeybataev/shadowmw/internal/telemetry"
)

func TestRuntimeDisablesCollaboratorOnPanicAndCountsFailure(t *testing.T) {
	runtime := telemetry.NewRuntime(64)
	runtime.Start()
	var afterPanic atomic.Int32
	runtime.Enqueue(telemetry.Task{
		Kind: telemetry.KindInstrumentation,
		Run: func() {
			panic("boom")
		},
	})
	runtime.Enqueue(telemetry.Task{
		Kind: telemetry.KindInstrumentation,
		Run: func() {
			afterPanic.Add(1)
		},
	})
	runtime.Shutdown()

	if got := afterPanic.Load(); got != 0 {
		t.Fatalf("afterPanic = %d, want 0", got)
	}
	failures := runtime.Failures()
	if failures[telemetry.InstrumentationPanic] != 1 {
		t.Fatalf("InstrumentationPanic = %d, want 1", failures[telemetry.InstrumentationPanic])
	}
}
