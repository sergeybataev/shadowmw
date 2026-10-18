# shadowmw OpenTelemetry adapter

This nested module translates core fixed measurements and spans using explicit,
caller-owned providers. It never reads or replaces OpenTelemetry globals and
does not configure readers, exporters, processors, resources, or provider
shutdown.

```go
adapter, err := shadowmwotel.New(meterProvider, tracerProvider)
if err != nil {
	return err
}
engine, err := shadowmw.NewEngine(config, adapter.EngineOptions()...)
```

Both providers are required. Explicit no-op providers are supported.

The adapter creates only shadowmw's fixed instruments, including observer
queue duration and rejection-code evidence. Configure rejection-code metric
allowlisting on the core engine; the adapter never promotes or derives raw
codes. Providers, readers, exporters, processors, and shutdown remain
application-owned.
