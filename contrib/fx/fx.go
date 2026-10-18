// Package shadowmwfx integrates a shadowmw Engine with an Fx lifecycle.
package shadowmwfx

import (
	"github.com/sergeybataev/shadowmw"
	"go.uber.org/fx"
)

// Module constructs and manages one shadowmw Engine.
var Module = fx.Module(
	"shadowmw",
	fx.Provide(NewEngine),
)

// Params contains the dependencies used to construct an Engine.
type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    shadowmw.EngineConfig
	Options   []shadowmw.EngineOption `group:"shadowmw_engine_options"`
}

// NewEngine constructs an Engine and registers its lifecycle hooks.
func NewEngine(params Params) (*shadowmw.Engine, error) {
	engine, err := shadowmw.NewEngine(params.Config, params.Options...)
	if err != nil {
		return nil, err
	}

	params.Lifecycle.Append(fx.Hook{
		OnStart: engine.Start,
		OnStop:  engine.Shutdown,
	})
	return engine, nil
}
