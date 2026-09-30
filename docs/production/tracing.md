---
title: "Distributed tracing for the Mercure hub with OpenTelemetry"
description: "Collect OpenTelemetry spans for Mercure publish, subscribe, and history operations by enabling Caddy's tracing directive and the standard OTEL_* variables."
---

# Mercure tracing

The Mercure.rocks Hub emits [OpenTelemetry](https://opentelemetry.io/) spans for its main internal operations:

| Span name                   | Kind     | Description                                                                          |
| --------------------------- | -------- | ------------------------------------------------------------------------------------ |
| `mercure.publish`           | Producer | Covers the publish flow: authorization, validation, and dispatch to the transport    |
| `mercure.subscribe`         | Consumer | Covers subscription setup: authorization, history replay, and transport registration |
| `mercure.subscriptions`     | Internal | Covers requests to the subscription API (`/.well-known/mercure/subscriptions/...`)   |
| `mercure.transport.history` | Internal | Covers history replay from the storage transport (Bolt, Redis...)                    |

Mercure's spans nest under the HTTP request span produced by Caddy's [`tracing`](https://caddyserver.com/docs/caddyfile/directives/tracing) directive, so enable it to start collecting traces:

```caddyfile
route {
	tracing
	mercure {
		# ...
	}
}
```

Exporters, endpoints (OTLP gRPC or HTTP), protocols, resource attributes, and propagators are all configured through the standard [`OTEL_*` environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).
Without a configured OpenTelemetry tracer provider, Mercure uses no-op spans and exports no traces.

## Identity attributes

Once the hub has verified a request's token and every [claim binding](../deployment/configuration.md#claim-bindings) that applies has passed, the handler span of each request (`mercure.publish`, `mercure.subscribe`, `mercure.subscriptions`) carries the caller's identity:

| Attribute                 | Value                                      |
| ------------------------- | ------------------------------------------ |
| `user.id`                 | The token's `sub` claim                    |
| `mercure.client_id`       | The token's `client_id` claim              |
| `mercure.role`            | `publisher` or `subscriber`                |
| `mercure.binding.<claim>` | The value the binding on `<claim>` matched |

Anonymous requests, and requests rejected before verification completes (invalid token, failed binding), carry none of these, and a claim the token does not carry is left out. Values are sanitised and capped as for the [identity placeholders](../deployment/configuration.md#identity-placeholders).

A request refused after verification still carries the identity, for example a `403` from the topic grants on a publish or subscription API request. For a publish, the attributes are on the HTTP handler's `mercure.publish` span only, not on the inner span of the same name that `Hub.Publish` starts.

## Next steps

- [Health checks and monitoring](health-monitoring.md) for Prometheus metrics and health endpoints.
- [Debugging](debugging.md) for the structured logs that complement traces.
