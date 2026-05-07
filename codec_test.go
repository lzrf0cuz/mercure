package mercure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// codecSampleUpdate returns a representative Update for codec round-trip tests.
func codecSampleUpdate() *Update {
	return &Update{
		Topics:  []string{"https://example.com/books/1", "https://example.com/authors/2"},
		Private: true,
		Debug:   false,
		Event: Event{
			ID:    "urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234",
			Data:  `{"title": "The Go Programming Language", "price": 42.99}`,
			Type:  "message",
			Retry: 3000,
		},
	}
}

// codecSampleUpdateMinimal returns an Update with only required fields.
func codecSampleUpdateMinimal() *Update {
	return &Update{
		Topics: []string{"https://example.com/minimal"},
		Event: Event{
			ID:   "urn:uuid:01952790-0000-7000-8000-000000000001",
			Data: "hello",
		},
	}
}

// assertCodecUpdateEqual compares two updates field-by-field for clear error messages.
func assertCodecUpdateEqual(t *testing.T, expected, actual *Update) {
	t.Helper()
	assert.Equal(t, expected.Topics, actual.Topics, "Topics mismatch")
	assert.Equal(t, expected.Private, actual.Private, "Private mismatch")
	assert.Equal(t, expected.Debug, actual.Debug, "Debug mismatch")
	assert.Equal(t, expected.ID, actual.ID, "Event.ID mismatch")
	assert.Equal(t, expected.Data, actual.Data, "Event.Data mismatch")
	assert.Equal(t, expected.Type, actual.Type, "Event.Type mismatch")
	assert.Equal(t, expected.Retry, actual.Retry, "Event.Retry mismatch")
}

func TestJSONCodecRoundTrip(t *testing.T) {
	t.Parallel()

	codec := JSONCodec{}
	u := codecSampleUpdate()

	data, err := codec.Marshal(u)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := codec.Unmarshal(data)
	require.NoError(t, err)
	assertCodecUpdateEqual(t, u, decoded)
}

func TestJSONCodecRoundTripMinimal(t *testing.T) {
	t.Parallel()

	codec := JSONCodec{}
	u := codecSampleUpdateMinimal()

	data, err := codec.Marshal(u)
	require.NoError(t, err)

	decoded, err := codec.Unmarshal(data)
	require.NoError(t, err)
	assertCodecUpdateEqual(t, u, decoded)
}

func TestJSONCodecStructure(t *testing.T) {
	t.Parallel()

	codec := JSONCodec{}
	u := codecSampleUpdate()

	data, err := codec.Marshal(u)
	require.NoError(t, err)

	// Verify the JSON structure uses lowercase keys and nested event
	j := string(data)
	assert.Contains(t, j, `"topics"`)
	assert.Contains(t, j, `"private"`)
	assert.Contains(t, j, `"event"`)
	assert.Contains(t, j, `"id"`)
	assert.Contains(t, j, `"data"`)
	assert.Contains(t, j, `"type"`)
	assert.Contains(t, j, `"retry"`)
}

func TestJSONCodecUnmarshalError(t *testing.T) {
	t.Parallel()

	codec := JSONCodec{}

	_, err := codec.Unmarshal([]byte("not valid json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "json codec")
}

func TestGobCodecRoundTrip(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	u := codecSampleUpdate()

	data, err := codec.Marshal(u)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := codec.Unmarshal(data)
	require.NoError(t, err)
	assertCodecUpdateEqual(t, u, decoded)
}

func TestGobCodecRoundTripMinimal(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}
	u := codecSampleUpdateMinimal()

	data, err := codec.Marshal(u)
	require.NoError(t, err)

	decoded, err := codec.Unmarshal(data)
	require.NoError(t, err)
	assertCodecUpdateEqual(t, u, decoded)
}

func TestGobCodecUnmarshalError(t *testing.T) {
	t.Parallel()

	codec := GobCodec{}

	_, err := codec.Unmarshal([]byte("not valid gob"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gob codec")
}

func TestMsgPackCodecRoundTrip(t *testing.T) {
	t.Parallel()

	codec := MsgPackCodec{}
	u := codecSampleUpdate()

	data, err := codec.Marshal(u)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := codec.Unmarshal(data)
	require.NoError(t, err)
	assertCodecUpdateEqual(t, u, decoded)
}

func TestMsgPackCodecRoundTripMinimal(t *testing.T) {
	t.Parallel()

	codec := MsgPackCodec{}
	u := codecSampleUpdateMinimal()

	data, err := codec.Marshal(u)
	require.NoError(t, err)

	decoded, err := codec.Unmarshal(data)
	require.NoError(t, err)
	assertCodecUpdateEqual(t, u, decoded)
}

func TestMsgPackCodecUnmarshalError(t *testing.T) {
	t.Parallel()

	codec := MsgPackCodec{}

	_, err := codec.Unmarshal([]byte{0xff, 0xfe, 0xfd})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "msgpack codec")
}

func TestNewCodec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		encoding string
		wantType Codec
		wantErr  bool
	}{
		{name: "empty defaults to json", encoding: "", wantType: JSONCodec{}},
		{name: "explicit json", encoding: "json", wantType: JSONCodec{}},
		{name: "gob", encoding: "gob", wantType: GobCodec{}},
		{name: "msgpack", encoding: "msgpack", wantType: MsgPackCodec{}},
		{name: "unsupported", encoding: "xml", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			codec, err := NewCodec(tt.encoding)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.IsType(t, tt.wantType, codec)
		})
	}
}

func TestCodecEmptyTopics(t *testing.T) {
	t.Parallel()

	codecs := []struct {
		name  string
		codec Codec
	}{
		{"json", JSONCodec{}},
		{"gob", GobCodec{}},
		{"msgpack", MsgPackCodec{}},
	}

	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			u := &Update{
				Topics: []string{},
				Event: Event{
					ID:   "urn:uuid:01952790-0000-7000-8000-000000000002",
					Data: "test",
				},
			}

			data, err := c.codec.Marshal(u)
			require.NoError(t, err)

			decoded, err := c.codec.Unmarshal(data)
			require.NoError(t, err)

			// JSON marshals empty slice as [] which unmarshals to []string{}
			// Gob/MsgPack may unmarshal empty slice as nil
			if len(u.Topics) == 0 {
				assert.Empty(t, decoded.Topics)
			} else {
				assert.Equal(t, u.Topics, decoded.Topics)
			}
		})
	}
}

func TestCodecSpecialCharacters(t *testing.T) {
	t.Parallel()

	codecs := []struct {
		name  string
		codec Codec
	}{
		{"json", JSONCodec{}},
		{"gob", GobCodec{}},
		{"msgpack", MsgPackCodec{}},
	}

	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			u := &Update{
				Topics: []string{"https://example.com/topics/sp\u00e9cial", "https://example.com/topics/\u65e5\u672c\u8a9e"},
				Event: Event{
					ID:   "urn:uuid:01952790-0000-7000-8000-000000000003",
					Data: "line1\nline2\r\nline3\ttab\x00null",
					Type: "custom-event-type",
				},
			}

			data, err := c.codec.Marshal(u)
			require.NoError(t, err)

			decoded, err := c.codec.Unmarshal(data)
			require.NoError(t, err)
			assertCodecUpdateEqual(t, u, decoded)
		})
	}
}

func TestCodecLargePayload(t *testing.T) {
	t.Parallel()

	codecs := []struct {
		name  string
		codec Codec
	}{
		{"json", JSONCodec{}},
		{"gob", GobCodec{}},
		{"msgpack", MsgPackCodec{}},
	}

	// Create a large data payload (~1MB)
	largeData := make([]byte, 1<<20)
	for i := range largeData {
		largeData[i] = byte('A' + (i % 26))
	}

	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			u := &Update{
				Topics: []string{"https://example.com/large"},
				Event: Event{
					ID:   "urn:uuid:01952790-0000-7000-8000-000000000004",
					Data: string(largeData),
				},
			}

			data, err := c.codec.Marshal(u)
			require.NoError(t, err)

			decoded, err := c.codec.Unmarshal(data)
			require.NoError(t, err)
			assert.Equal(t, u.Data, decoded.Data)
		})
	}
}

// TestCodecPayloadSizeCap locks in the defense-in-depth ceiling on Unmarshal
// input length: bytes longer than maxCodecPayloadBytes must short-circuit
// with ErrCodecPayloadTooLarge before the underlying decoder is invoked, so
// pathological gob/msgpack payloads can't balloon allocator pressure.
func TestCodecPayloadSizeCap(t *testing.T) {
	t.Parallel()

	codecs := []struct {
		name  string
		codec Codec
	}{
		{"json", JSONCodec{}},
		{"gob", GobCodec{}},
		{"msgpack", MsgPackCodec{}},
	}

	oversized := make([]byte, maxCodecPayloadBytes+1)

	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.codec.Unmarshal(oversized)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrCodecPayloadTooLarge,
				"expected ErrCodecPayloadTooLarge, got %v", err)
		})
	}
}

func BenchmarkJSONCodecMarshal(b *testing.B) {
	codec := JSONCodec{}
	u := codecSampleUpdate()

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Marshal(u)
	}
}

func BenchmarkJSONCodecUnmarshal(b *testing.B) {
	codec := JSONCodec{}
	data, _ := codec.Marshal(codecSampleUpdate())

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Unmarshal(data)
	}
}

func BenchmarkGobCodecMarshal(b *testing.B) {
	codec := GobCodec{}
	u := codecSampleUpdate()

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Marshal(u)
	}
}

func BenchmarkGobCodecUnmarshal(b *testing.B) {
	codec := GobCodec{}
	data, _ := codec.Marshal(codecSampleUpdate())

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Unmarshal(data)
	}
}

func BenchmarkMsgPackCodecMarshal(b *testing.B) {
	codec := MsgPackCodec{}
	u := codecSampleUpdate()

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Marshal(u)
	}
}

func BenchmarkMsgPackCodecUnmarshal(b *testing.B) {
	codec := MsgPackCodec{}
	data, _ := codec.Marshal(codecSampleUpdate())

	b.ResetTimer()

	for range b.N {
		_, _ = codec.Unmarshal(data)
	}
}
