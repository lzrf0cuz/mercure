package mercure

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// ErrUnsupportedCodec is returned when an unknown codec encoding is requested.
var ErrUnsupportedCodec = errors.New("unsupported codec encoding")

// ErrCodecPayloadTooLarge is returned by Unmarshal when the encoded byte
// length exceeds maxCodecPayloadBytes. A transport returns it, wrapped, from
// Dispatch for an update whose encoding exceeds the cap, since no node could
// read it back; the hub then answers 413 and counts the failure as
// validation. The cap bounds the encoded input only, not the memory a decode
// allocates: a crafted payload under the cap can still make the decoder
// allocate many times its own size.
var ErrCodecPayloadTooLarge = errors.New("codec payload exceeds maximum size")

const maxCodecPayloadBytes = 64 << 20 // 64 MiB

// Codec defines the interface for encoding/decoding Mercure updates
// for transport-level serialization. Three built-in implementations are
// provided: JSONCodec (what NewCodec("") returns, human-readable), GobCodec
// (Go-native binary) and MsgPackCodec (cross-language binary).
type Codec interface {
	// Marshal encodes an Update into a byte slice.
	Marshal(u *Update) ([]byte, error)
	// Unmarshal decodes a byte slice into an Update.
	Unmarshal(data []byte) (*Update, error)
}

// jsonUpdate is the intermediate struct for JSON serialization.
// Uses lowercase field names and a nested event structure, keeping the
// wire format distinct from Go's default PascalCase marshaling.
type jsonUpdate struct {
	Topics  []string  `json:"topics"`
	Private bool      `json:"private"`
	Event   jsonEvent `json:"event"`
	Debug   bool      `json:"debug,omitempty"`
}

type jsonEvent struct {
	ID    string `json:"id"`
	Data  string `json:"data"`
	Type  string `json:"type,omitempty"`
	Retry uint64 `json:"retry,omitempty"`
}

// JSONCodec encodes updates as JSON, readable by consumers in any language.
type JSONCodec struct{}

// Marshal serializes an Update to JSON.
func (JSONCodec) Marshal(u *Update) ([]byte, error) {
	j := jsonUpdate{
		Topics:  u.Topics,
		Private: u.Private,
		Debug:   u.Debug,
		Event: jsonEvent{
			ID:    u.ID,
			Data:  u.Data,
			Type:  u.Type,
			Retry: u.Retry,
		},
	}

	data, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("json codec: marshal failed: %w", err)
	}

	return data, nil
}

// Unmarshal deserializes JSON bytes into an Update.
func (JSONCodec) Unmarshal(data []byte) (*Update, error) {
	if len(data) > maxCodecPayloadBytes {
		return nil, fmt.Errorf("json codec: %w: %d > %d", ErrCodecPayloadTooLarge, len(data), maxCodecPayloadBytes)
	}

	var j jsonUpdate
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("json codec: unmarshal failed: %w", err)
	}

	return &Update{
		Topics:  j.Topics,
		Private: j.Private,
		Debug:   j.Debug,
		ID:      j.Event.ID,
		Data:    j.Event.Data,
		Type:    j.Event.Type,
		Retry:   j.Event.Retry,
	}, nil
}

// gobUpdate is the intermediate struct for gob serialization.
// All fields must be exported for gob encoding.
type gobUpdate struct {
	Topics  []string
	Private bool
	Debug   bool
	ID      string
	Data    string
	Type    string
	Retry   uint64
}

// GobCodec encodes updates using Go's native binary gob encoding.
// Each message carries its own gob type definitions, so for a single Update it
// is slower to encode and decode than JSONCodec (see BenchmarkGobCodecMarshal
// / BenchmarkGobCodecUnmarshal) and larger on the wire: encoding
// codecGoldenFull (codec_test.go) is 267 bytes for Gob against 231 for JSON.
// Only Go can decode it.
type GobCodec struct{}

// Marshal serializes an Update using gob encoding.
func (GobCodec) Marshal(u *Update) ([]byte, error) {
	g := gobUpdate{
		Topics:  u.Topics,
		Private: u.Private,
		Debug:   u.Debug,
		ID:      u.ID,
		Data:    u.Data,
		Type:    u.Type,
		Retry:   u.Retry,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(g); err != nil {
		return nil, fmt.Errorf("gob codec: marshal failed: %w", err)
	}

	return buf.Bytes(), nil
}

// Unmarshal deserializes gob-encoded bytes into an Update.
func (GobCodec) Unmarshal(data []byte) (*Update, error) {
	if len(data) > maxCodecPayloadBytes {
		return nil, fmt.Errorf("gob codec: %w: %d > %d", ErrCodecPayloadTooLarge, len(data), maxCodecPayloadBytes)
	}

	var g gobUpdate
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&g); err != nil {
		return nil, fmt.Errorf("gob codec: unmarshal failed: %w", err)
	}

	return &Update{
		Topics:  g.Topics,
		Private: g.Private,
		Debug:   g.Debug,
		ID:      g.ID,
		Data:    g.Data,
		Type:    g.Type,
		Retry:   g.Retry,
	}, nil
}

// msgpackUpdate is the intermediate struct for msgpack serialization.
// Uses explicit msgpack struct tags.
type msgpackUpdate struct {
	Topics  []string `msgpack:"topics"`
	Private bool     `msgpack:"private"`
	Debug   bool     `msgpack:"debug,omitempty"`
	ID      string   `msgpack:"id"`
	Data    string   `msgpack:"data"`
	Type    string   `msgpack:"type,omitempty"`
	Retry   uint64   `msgpack:"retry,omitempty"`
}

// MsgPackCodec encodes updates using MessagePack binary encoding. It encodes
// and decodes faster than JSONCodec (see the codec benchmarks) and is smaller on
// the wire: encoding codecGoldenFull (codec_test.go) is 188 bytes for MsgPack
// against 231 for JSON. MessagePack libraries exist for most languages.
type MsgPackCodec struct{}

// Marshal serializes an Update using MessagePack encoding.
func (MsgPackCodec) Marshal(u *Update) ([]byte, error) {
	m := msgpackUpdate{
		Topics:  u.Topics,
		Private: u.Private,
		Debug:   u.Debug,
		ID:      u.ID,
		Data:    u.Data,
		Type:    u.Type,
		Retry:   u.Retry,
	}

	data, err := msgpack.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("msgpack codec: marshal failed: %w", err)
	}

	return data, nil
}

// Unmarshal deserializes MessagePack bytes into an Update.
func (MsgPackCodec) Unmarshal(data []byte) (*Update, error) {
	if len(data) > maxCodecPayloadBytes {
		return nil, fmt.Errorf("msgpack codec: %w: %d > %d", ErrCodecPayloadTooLarge, len(data), maxCodecPayloadBytes)
	}

	var m msgpackUpdate
	if err := msgpack.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("msgpack codec: unmarshal failed: %w", err)
	}

	return &Update{
		Topics:  m.Topics,
		Private: m.Private,
		Debug:   m.Debug,
		ID:      m.ID,
		Data:    m.Data,
		Type:    m.Type,
		Retry:   m.Retry,
	}, nil
}

// Encoding names accepted by NewCodec.
const (
	codecJSON    = "json"
	codecGob     = "gob"
	codecMsgpack = "msgpack"
)

// NewCodec returns the Codec for the given encoding name.
// Supported values: "json" (default), "gob", "msgpack".
func NewCodec(encoding string) (Codec, error) {
	switch encoding {
	case "", codecJSON:
		return JSONCodec{}, nil
	case codecGob:
		return GobCodec{}, nil
	case codecMsgpack:
		return MsgPackCodec{}, nil
	default:
		return nil, fmt.Errorf("%w: %q (supported: json, gob, msgpack)", ErrUnsupportedCodec, encoding)
	}
}

// Interface guards.
var (
	_ Codec = JSONCodec{}
	_ Codec = GobCodec{}
	_ Codec = MsgPackCodec{}
)
