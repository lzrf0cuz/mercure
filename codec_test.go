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
		Debug:   true,
		ID:      "urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234",
		Data:    `{"title": "The Go Programming Language", "price": 42.99}`,
		Type:    "message",
		Retry:   3000,
	}
}

// codecSampleUpdateMinimal returns an Update with only required fields.
func codecSampleUpdateMinimal() *Update {
	return &Update{
		Topics: []string{"https://example.com/minimal"},
		ID:     "urn:uuid:01952790-0000-7000-8000-000000000001",
		Data:   "hello",
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
				require.ErrorIs(t, err, ErrUnsupportedCodec)

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
				ID:     "urn:uuid:01952790-0000-7000-8000-000000000002",
				Data:   "test",
			}

			data, err := c.codec.Marshal(u)
			require.NoError(t, err)

			decoded, err := c.codec.Unmarshal(data)
			require.NoError(t, err)

			// JSON marshals empty slice as [] which unmarshals to []string{}.
			// Gob unmarshals an empty slice back as nil; MsgPack unmarshals it
			// back as a non-nil empty slice. assert.Empty accepts either form.
			assert.Empty(t, decoded.Topics)
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
				ID:     "urn:uuid:01952790-0000-7000-8000-000000000003",
				Data:   "line1\nline2\r\nline3\ttab\x00null",
				Type:   "custom-event-type",
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
				ID:     "urn:uuid:01952790-0000-7000-8000-000000000004",
				Data:   string(largeData),
			}

			data, err := c.codec.Marshal(u)
			require.NoError(t, err)

			decoded, err := c.codec.Unmarshal(data)
			require.NoError(t, err)
			assert.Equal(t, u.Data, decoded.Data)
		})
	}
}

// TestCodecPayloadSizeCap: bytes longer than maxCodecPayloadBytes return
// ErrCodecPayloadTooLarge before the decoder runs.
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

// Wire-contract golden bytes, as written by the 0.x releases' codecs for
// codecGoldenFull and codecGoldenMinimal. Never regenerate them from the
// current encoders: a mismatch means the wire format changed.
const (
	codecGoldenJSONFull       = "{\"topics\":[\"https://example.com/books/1\",\"https://example.com/authors/2\"],\"private\":true,\"event\":{\"id\":\"urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234\",\"data\":\"{\\\"title\\\": \\\"Go\\\"}\\nline2\",\"type\":\"message\",\"retry\":3000},\"debug\":true}"
	codecGoldenJSONMinimal    = "{\"topics\":[\"https://example.com/minimal\"],\"private\":false,\"event\":{\"id\":\"urn:uuid:01952790-0000-7000-8000-000000000001\",\"data\":\"hello\"}}"
	codecGoldenGobFull        = "[\x7f\x03\x01\x01\x09gobUpdate\x01\xff\x80\x00\x01\x07\x01\x06Topics\x01\xff\x82\x00\x01\x07Private\x01\x02\x00\x01\x05Debug\x01\x02\x00\x01\x02ID\x01\x0c\x00\x01\x04Data\x01\x0c\x00\x01\x04Type\x01\x0c\x00\x01\x05Retry\x01\x06\x00\x00\x00\x16\xff\x81\x02\x01\x01\x08[]string\x01\xff\x82\x00\x01\x0c\x00\x00\xff\x96\xff\x80\x01\x02\x1bhttps://example.com/books/1\x1dhttps://example.com/authors/2\x01\x01\x01\x01\x01-urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234\x01\x15{\"title\": \"Go\"}\x0aline2\x01\x07message\x01\xfe\x0b\xb8\x00"
	codecGoldenGobMinimal     = "[\x7f\x03\x01\x01\x09gobUpdate\x01\xff\x80\x00\x01\x07\x01\x06Topics\x01\xff\x82\x00\x01\x07Private\x01\x02\x00\x01\x05Debug\x01\x02\x00\x01\x02ID\x01\x0c\x00\x01\x04Data\x01\x0c\x00\x01\x04Type\x01\x0c\x00\x01\x05Retry\x01\x06\x00\x00\x00\x16\xff\x81\x02\x01\x01\x08[]string\x01\xff\x82\x00\x01\x0c\x00\x00W\xff\x80\x01\x01\x1bhttps://example.com/minimal\x03-urn:uuid:01952790-0000-7000-8000-000000000001\x01\x05hello\x00"
	codecGoldenMsgPackFull    = "\x87\xa6topics\x92\xbbhttps://example.com/books/1\xbdhttps://example.com/authors/2\xa7private\xc3\xa5debug\xc3\xa2id\xd9-urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234\xa4data\xb5{\"title\": \"Go\"}\x0aline2\xa4type\xa7message\xa5retry\xcf\x00\x00\x00\x00\x00\x00\x0b\xb8"
	codecGoldenMsgPackMinimal = "\x84\xa6topics\x91\xbbhttps://example.com/minimal\xa7private\xc2\xa2id\xd9-urn:uuid:01952790-0000-7000-8000-000000000001\xa4data\xa5hello"
)

func codecGoldenFull() *Update {
	return &Update{
		Topics:  []string{"https://example.com/books/1", "https://example.com/authors/2"},
		Private: true,
		Debug:   true,
		ID:      "urn:uuid:0195278d-8fca-7c11-bfab-deadbeef1234",
		Data:    "{\"title\": \"Go\"}\nline2",
		Type:    "message",
		Retry:   3000,
	}
}

func codecGoldenMinimal() *Update {
	return &Update{
		Topics: []string{"https://example.com/minimal"},
		ID:     "urn:uuid:01952790-0000-7000-8000-000000000001",
		Data:   "hello",
	}
}

// TestCodecGoldenBytes pins the wire contract with 0.x: every codec must decode
// the bytes the 0.x releases wrote, and the JSON and msgpack encoders must still produce
// them byte for byte. Gob output is only decoded: gob type IDs are assigned per
// process in registration order, so the encoded bytes depend on which other
// gob types the process registered first.
func TestCodecGoldenBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		codec         Codec
		update        *Update
		golden        string
		deterministic bool
	}{
		{"json full", JSONCodec{}, codecGoldenFull(), codecGoldenJSONFull, true},
		{"json minimal", JSONCodec{}, codecGoldenMinimal(), codecGoldenJSONMinimal, true},
		{"gob full", GobCodec{}, codecGoldenFull(), codecGoldenGobFull, false},
		{"gob minimal", GobCodec{}, codecGoldenMinimal(), codecGoldenGobMinimal, false},
		{"msgpack full", MsgPackCodec{}, codecGoldenFull(), codecGoldenMsgPackFull, true},
		{"msgpack minimal", MsgPackCodec{}, codecGoldenMinimal(), codecGoldenMsgPackMinimal, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decoded, err := tt.codec.Unmarshal([]byte(tt.golden))
			require.NoError(t, err)
			assertCodecUpdateEqual(t, tt.update, decoded)

			if !tt.deterministic {
				return
			}

			encoded, err := tt.codec.Marshal(tt.update)
			require.NoError(t, err)
			assert.Equal(t, tt.golden, string(encoded))
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
