package redistransport

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/lzrf0cuz/mercure/redistransport"

// startSpan starts a span on the TracerProvider carried by the active span in
// ctx — the same lookup the hub uses, so spans nest under Caddy's HTTP server
// span when its `tracing` directive is enabled. With no active provider this
// resolves to the OpenTelemetry no-op tracer (no exporter cost, no globals
// touched). Mirrors the helper in the hub package; kept here so the transport
// remains a standalone module.
func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracer := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName)

	return tracer.Start(ctx, name, opts...)
}

// recordSpanError mirrors the hub's helper in tracer.go: record the error on
// the span and flip its status to codes.Error. Matches the recordSpanError
// call in bolt.go's dispatchHistory failure path so cross-backend trace
// queries that filter on span.Status == ERROR catch Redis history failures
// the same way they catch Bolt ones. Keep in sync with the hub's helper if
// either side evolves; the rt copy exists because rt is a standalone module.
//
// nil err is a no-op so callers don't have to wrap the call in `if err != nil`;
// without this guard, span.SetStatus(codes.Error, "") would flip a successful
// span to ERROR with an empty description.
func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// tagTransportOnParent annotates the active parent span with
// `mercure.transport=redis` so trace queries can filter by transport backend.
// The parent is typically the hub's mercure.publish or mercure.subscribe
// span (whichever entry point the caller is under), but the helper itself
// is parent-agnostic — it stamps whatever span is active in ctx. Cheaper
// than emitting a child span: no wall-clock echo, no exporter pressure, and
// IsRecording gates the SetAttributes allocation away when tracing is off.
// Called from Dispatch and AddSubscriber at the transport boundary; relies
// on the hub preserving span context across the entry call (publish.go and
// subscribe.go use context.WithoutCancel rather than context.Background, so
// span values survive).
func tagTransportOnParent(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	span.SetAttributes(attribute.String("mercure.transport", "redis"))
}
