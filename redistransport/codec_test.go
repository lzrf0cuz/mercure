package redistransport

// The JSONCodec/GobCodec/MsgPackCodec types are pure aliases of mercure.*Codec
// (see codec.go). Testing their Marshal/Unmarshal round-trip here would just
// re-test upstream — upstream's codec_test.go already covers those paths
// verbatim. The only code we own in this package is NewCodec's error-wrap, so
// that's all this file tests. Benchmarks are kept because the README's
// codec-choice table is regenerated from them and depends on a local bench
// harness rather than importing upstream's private test helpers.

import (
	"testing"

	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleUpdate is the reference Update used by every benchmark so ns/op and
// bytes/msg are comparable across codecs.
func sampleUpdate() *mercure.Update {
	return &mercure.Update{
		Topics:  []string{"https://example.com/books/1", "https://example.com/authors/2"},
		Private: true,
		Debug:   false,
		Event: mercure.Event{
			ID:    "urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234",
			Data:  `{"title": "The Go Programming Language", "price": 42.99}`,
			Type:  "message",
			Retry: 3000,
		},
	}
}

// largePayloadUpdate is used by the *Large benchmark variants to exercise the
// case where the data field dominates serialization cost.
func largePayloadUpdate() *mercure.Update {
	data := make([]byte, 8192)
	for i := range data {
		data[i] = 'x'
	}

	return &mercure.Update{
		Topics: []string{"https://example.com/large"},
		Event: mercure.Event{
			ID:   "urn:uuid:01952790-0000-7000-8000-000000000004",
			Data: string(data),
		},
	}
}

// TestNewCodec covers newCodec's dispatch + error-wrap — the only code this
// package owns on the codec path (the codec types themselves live upstream).
func TestNewCodec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		encoding string
		wantType mercure.Codec
		wantErr  bool
	}{
		{name: "empty defaults to json", encoding: "", wantType: mercure.JSONCodec{}},
		{name: "explicit json", encoding: "json", wantType: mercure.JSONCodec{}},
		{name: "gob", encoding: "gob", wantType: mercure.GobCodec{}},
		{name: "msgpack", encoding: "msgpack", wantType: mercure.MsgPackCodec{}},
		{name: "unsupported", encoding: "xml", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			codec, err := newCodec(tt.encoding)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "redistransport:",
					"NewCodec must wrap upstream errors with the redistransport: prefix")

				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantType, codec)
		})
	}
}

// Benchmarks report marshaled wire size via b.ReportMetric so both latency AND
// bytes-on-the-wire are captured in a single `go test -bench` invocation. The
// README's codec-choice table is regenerated from these.

func runMarshalBench(b *testing.B, codec mercure.Codec, u *mercure.Update) {
	b.Helper()

	size, _ := codec.Marshal(u)

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Marshal(u)
	}

	b.StopTimer()
	b.ReportMetric(float64(len(size)), "bytes/msg")
}

func runUnmarshalBench(b *testing.B, codec mercure.Codec, u *mercure.Update) {
	b.Helper()

	data, _ := codec.Marshal(u)

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Unmarshal(data)
	}

	b.StopTimer()
	b.ReportMetric(float64(len(data)), "bytes/msg")
}

func BenchmarkJSONCodecMarshal(b *testing.B) { runMarshalBench(b, mercure.JSONCodec{}, sampleUpdate()) }

func BenchmarkJSONCodecUnmarshal(b *testing.B) {
	runUnmarshalBench(b, mercure.JSONCodec{}, sampleUpdate())
}

func BenchmarkGobCodecMarshal(b *testing.B) { runMarshalBench(b, mercure.GobCodec{}, sampleUpdate()) }

func BenchmarkGobCodecUnmarshal(b *testing.B) {
	runUnmarshalBench(b, mercure.GobCodec{}, sampleUpdate())
}

func BenchmarkMsgPackCodecMarshal(b *testing.B) {
	runMarshalBench(b, mercure.MsgPackCodec{}, sampleUpdate())
}

func BenchmarkMsgPackCodecUnmarshal(b *testing.B) {
	runUnmarshalBench(b, mercure.MsgPackCodec{}, sampleUpdate())
}

func BenchmarkJSONCodecMarshalLarge(b *testing.B) {
	runMarshalBench(b, mercure.JSONCodec{}, largePayloadUpdate())
}

func BenchmarkGobCodecMarshalLarge(b *testing.B) {
	runMarshalBench(b, mercure.GobCodec{}, largePayloadUpdate())
}

func BenchmarkMsgPackCodecMarshalLarge(b *testing.B) {
	runMarshalBench(b, mercure.MsgPackCodec{}, largePayloadUpdate())
}
