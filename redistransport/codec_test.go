package redistransport

// The codecs are upstream mercure's, whose round-trip tests live upstream;
// this file covers newCodec, the encoded-size cap and the encode-error path.
// The benchmarks feed the README's codec benchmark table.

import (
	"errors"
	"strings"
	"testing"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
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
		ID:      "urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234",
		Data:    `{"title": "The Go Programming Language", "price": 42.99}`,
		Type:    "message",
		Retry:   3000,
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
		ID:     "urn:uuid:01952790-0000-7000-8000-000000000004",
		Data:   string(data),
	}
}

// TestNewCodec covers newCodec's encoding selection and its error prefix.
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
				assert.Contains(t, err.Error(), "redis transport:",
					"newCodec must wrap upstream errors with the redis transport: prefix")

				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantType, codec)
		})
	}
}

// The benchmarks report marshaled size (bytes/msg) alongside ns/op.

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

// maxEncodedUpdateBytes mirrors the hub codecs' unexported Unmarshal cap: an
// entry one byte over it is rejected as too large, and one at it is not.
func TestMaxEncodedUpdateBytesMatchesCodecCap(t *testing.T) {
	t.Parallel()

	buf := make([]byte, maxEncodedUpdateBytes+1)

	for _, encoding := range []string{"json", "gob", "msgpack"} {
		codec, err := newCodec(encoding)
		require.NoError(t, err)

		_, err = codec.Unmarshal(buf)
		require.ErrorIs(t, err, mercure.ErrCodecPayloadTooLarge, encoding)

		_, err = codec.Unmarshal(buf[:maxEncodedUpdateBytes])
		require.NotErrorIs(t, err, mercure.ErrCodecPayloadTooLarge, encoding)
	}
}

// An update encoding to exactly maxEncodedUpdateBytes is stored; one byte more
// exceeds what every node's Unmarshal accepts, so it is rejected at publish
// rather than stored and then dropped by each consumer. The rejection is a
// client error (the hub counts it as validation), not a publish error.
func TestDispatchRejectsUpdateOverCodecCap(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	codec := *transport.codec.Load()

	// sized returns an update whose encoding is exactly size bytes: the ID is
	// fixed so Dispatch assigns none, and each "a" of data encodes to one byte.
	envelope, err := codec.Marshal(&mercure.Update{Topics: []string{"https://example.com/oversized"}, ID: "urn:uuid:cap-test"})
	require.NoError(t, err)

	sized := func(size int) *mercure.Update {
		return &mercure.Update{
			Topics: []string{"https://example.com/oversized"},
			ID:     "urn:uuid:cap-test",
			Data:   strings.Repeat("a", size-len(envelope)),
		}
	}

	encodedLen := func(u *mercure.Update) int {
		b, err := codec.Marshal(u)
		require.NoError(t, err)

		return len(b)
	}

	atCap := sized(maxEncodedUpdateBytes)
	require.Equal(t, maxEncodedUpdateBytes, encodedLen(atCap), "the fixture must encode to exactly the cap")

	stored := func() int64 {
		n, err := transport.client.XLen(t.Context(), transport.key("")).Result()
		require.NoError(t, err)

		return n
	}

	encodeErrors := func() float64 {
		return counterValueForTest(t, transport.metrics.Load().publishErrors.WithLabelValues(string(publishErrorEncode)))
	}

	require.NoError(t, transport.Dispatch(t.Context(), atCap), "an update at the cap is accepted")
	assert.EqualValues(t, 1, stored())
	assert.Zero(t, encodeErrors())

	err = transport.Dispatch(t.Context(), sized(maxEncodedUpdateBytes+1))
	require.ErrorIs(t, err, mercure.ErrCodecPayloadTooLarge, "an update one byte over the cap is rejected")
	assert.EqualValues(t, 1, stored(), "the rejected update must not be stored")
	assert.Zero(t, encodeErrors(), "a client error is not a publish error: the hub counts it as validation")
}

var errMarshalFailed = errors.New("marshal failed")

// failingMarshalCodec fails every Marshal.
type failingMarshalCodec struct{ mercure.JSONCodec }

func (failingMarshalCodec) Marshal(*mercure.Update) ([]byte, error) { return nil, errMarshalFailed }

// A codec that fails to encode an update is a publish error of kind encode.
func TestDispatchCountsMarshalFailureAsEncodeError(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t, WithPrometheusRegisterer(prometheus.NewRegistry()))
	transport.SetCodec(failingMarshalCodec{})

	err := transport.Dispatch(t.Context(), &mercure.Update{Topics: []string{"https://example.com/marshal"}})
	require.ErrorIs(t, err, errMarshalFailed)
	assert.InDelta(t, 1, counterValueForTest(t, transport.metrics.Load().publishErrors.WithLabelValues(string(publishErrorEncode))), 0)
}
