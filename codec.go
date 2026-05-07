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
// length exceeds maxCodecPayloadBytes. A sentinel cap protects callers from
// pathological gob/msgpack inputs that can balloon allocator pressure during
// decode (the standard gob library does not bound nested type-definition
// expansion). 64 MiB is well above any realistic SSE Update — single Redis
// Stream entries are 512 MiB max, but legitimate Mercure updates rarely
// exceed a few MB even with large data payloads.
var ErrCodecPayloadTooLarge = errors.New("codec payload exceeds maximum size")

const maxCodecPayloadBytes = 64 << 20 // 64 MiB

// Codec defines the interface for encoding/decoding Mercure updates
// for transport-level serialization. Three built-in implementations are
// provided: JSONCodec (default, human-readable) and GobCodec (Go-native binary).
type Codec interface {
	// Marshal encodes an Update into a byte slice.
	Marshal(u *Update) ([]byte, error)
	// Unmarshal decodes a byte slice into an Update.
	Unmarshal(data []byte) (*Update, error)
}

// jsonUpdate is the intermediate struct for clean JSON serialization.
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

// JSONCodec encodes updates as JSON. Human-readable, compatible with
// external inspection and third-party consumers in any language.
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
		Event: Event{
			ID:    j.Event.ID,
			Data:  j.Event.Data,
			Type:  j.Event.Type,
			Retry: j.Event.Retry,
		},
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
// ~2-3x faster serialization, ~40% smaller payloads than JSON.
// Go-only — not readable by non-Go consumers.
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
		Event: Event{
			ID:    g.ID,
			Data:  g.Data,
			Type:  g.Type,
			Retry: g.Retry,
		},
	}, nil
}

// msgpackUpdate is the intermediate struct for msgpack serialization.
// Uses explicit msgpack struct tags for compact, cross-language binary encoding.
type msgpackUpdate struct {
	Topics  []string `msgpack:"topics"`
	Private bool     `msgpack:"private"`
	Debug   bool     `msgpack:"debug,omitempty"`
	ID      string   `msgpack:"id"`
	Data    string   `msgpack:"data"`
	Type    string   `msgpack:"type,omitempty"`
	Retry   uint64   `msgpack:"retry,omitempty"`
}

// MsgPackCodec encodes updates using MessagePack binary encoding.
// ~3-5x faster than JSON, ~50% smaller payloads. Cross-language compatible —
// MessagePack libraries exist for every major language.
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
		Event: Event{
			ID:    m.ID,
			Data:  m.Data,
			Type:  m.Type,
			Retry: m.Retry,
		},
	}, nil
}

// Encoding names accepted by NewCodec. Kept unexported so the public
// contract continues to be the literal string values documented on
// NewCodec — adding exported consts would expand the API surface for
// no caller benefit (callers configure these via Caddyfile/JSON, not
// Go imports).
const (
	codecJSON    = "json"
	codecGob     = "gob"
	codecMsgpack = "msgpack"
)

// NewCodec returns the Codec for the given encoding name.
// Supported values: "json" (default), "gob", "msgpack".
// Returns the Codec interface deliberately — callers store any of the
// three concrete codecs uniformly.
//
//nolint:ireturn // see godoc above.
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
