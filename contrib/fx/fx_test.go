package shadowmwfx_test

import (
	"context"
	"testing"

	"github.com/sergeybataev/shadowmw"
	shadowmwfx "github.com/sergeybataev/shadowmw/contrib/fx"
	"go.uber.org/fx"
)

func TestModuleManagesOneEngineLifecycle(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1

	var first *shadowmw.Engine
	var second *shadowmw.Engine
	app := fx.New(
		shadowmwfx.Module,
		fx.Supply(cfg),
		fx.Populate(&first, &second),
		fx.NopLogger,
	)
	if err := app.Err(); err != nil {
		t.Fatalf("fx.New() error = %v", err)
	}
	if first == nil {
		t.Fatal("populated engine is nil")
	}
	if first != second {
		t.Fatal("Module constructed more than one engine")
	}
	if got := first.State(); got != shadowmw.EngineNew {
		t.Fatalf("initial State() = %v, want %v", got, shadowmw.EngineNew)
	}

	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got := first.State(); got != shadowmw.EngineRunning {
		t.Fatalf("running State() = %v, want %v", got, shadowmw.EngineRunning)
	}

	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := first.State(); got != shadowmw.EngineStopped {
		t.Fatalf("stopped State() = %v, want %v", got, shadowmw.EngineStopped)
	}
}

func TestModuleForwardsGroupedEngineOptions(t *testing.T) {
	cfg := shadowmw.DefaultEngineConfig()
	cfg.ComparisonWorkers = 1

	app := fx.New(
		shadowmwfx.Module,
		fx.Supply(cfg),
		fx.Provide(
			fx.Annotate(
				func() shadowmw.EngineOption { return nil },
				fx.ResultTags(`group:"shadowmw_engine_options"`),
			),
		),
		fx.Invoke(func(*shadowmw.Engine) {}),
		fx.NopLogger,
	)
	if app.Err() == nil {
		t.Fatal("fx.New() error = nil, want grouped nil option validation error")
	}
}
