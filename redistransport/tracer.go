package redistransport

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/lzrf0cuz/mercure/redistransport"

// startSpan starts a span on the TracerProvider of the span active in ctx, as
// the hub does, so spans nest under Caddy's HTTP span when `tracing` is on.
// With no provider this is the no-op tracer.
func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	tracer := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName)

	return tracer.Start(ctx, name, opts...)
}

// recordSpanError records err on span and sets its status to Error, like the
// hub's helper of the same name. A nil err is a no-op.
func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// tagTransportOnParent sets mercure.transport=redis on the span active in ctx
// (the hub's publish or subscribe span), so traces can be filtered by
// transport. It does nothing when the span is not recording.
func tagTransportOnParent(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	span.SetAttributes(attribute.String("mercure.transport", "redis"))
}
