# shadowmw Fx module

This nested module constructs one engine and binds its `Start` and `Shutdown`
methods to `fx.Lifecycle`. The lifecycle's start/stop contexts supply the
deadlines. Routing, rules, middleware, and application configuration remain
service-owned.

Supply one `shadowmw.EngineConfig`, install `shadowmwfx.Module`, and provide
optional grouped `shadowmw.EngineOption` values under
`group:"shadowmw_engine_options"`.
