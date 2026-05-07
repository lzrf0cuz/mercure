package redistransport

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/gofrs/uuid/v5"
)

// nodeGroupPrefix is the Redis consumer-group naming prefix. Every group
// owned by a Mercure node starts with this prefix; gcZombieGroups uses it
// to distinguish Mercure-managed groups from other consumers on the stream.
const nodeGroupPrefix = "mercure:node:"

// nodeGroupName returns the consumer-group name for the given Mercure node.
func nodeGroupName(nodeID string) string {
	return nodeGroupPrefix + nodeID
}

// publishScriptText is the Lua source for the atomic publish operation.
// KEYS[1] = lastEventID key ({stream}:lastEventID)
// KEYS[2] = stream key ({stream})
// ARGV[1] = event ID (Mercure UUIDv7 URN)
// ARGV[2] = max length (0 = no trim)
// ARGV[3] = encoded update data
// ARGV[4] = node ID (for monitoring/debugging)
//
// XADD is executed first: if it fails, lastEventID is not updated (no phantom ID).
// If SET fails after XADD, lastEventID is stale but the event is in the stream —
// subscribers still receive it via XREADGROUP, and the next publish updates lastEventID.
const publishScriptText = `
local streamID
if tonumber(ARGV[2]) > 0 then
    streamID = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*',
        'eventID', ARGV[1], 'data', ARGV[3], 'nodeID', ARGV[4])
else
    streamID = redis.call('XADD', KEYS[2], '*',
        'eventID', ARGV[1], 'data', ARGV[3], 'nodeID', ARGV[4])
end
redis.call('SET', KEYS[1], ARGV[1])
return streamID
`

// cleanupScriptText is the Lua source for trimming stream entries older than
// a minimum ID.
// KEYS[1] = stream key ({stream})
// ARGV[1] = minimum stream ID to keep (based on current time - TTL)
//
// XTRIM with MINID is available since Redis 6.2. The ~ makes it approximate
// (faster, does not guarantee exact trimming).
const cleanupScriptText = `
redis.call('XTRIM', KEYS[1], 'MINID', '~', ARGV[1])
return 1
`

// key builds a Redis key with a hash tag for Redis Cluster slot consistency.
// All keys for a given stream name map to the same slot, ensuring Lua scripts
// that touch multiple keys don't fail with CROSSSLOT errors.
//
//	key("") → "{mercure}"           (the stream itself)
//	key(":lastEventID") → "{mercure}:lastEventID"
//	key(":presence:abc") → "{mercure}:presence:abc"
func key(stream, suffix string) string {
	return "{" + stream + "}" + suffix
}

// compareStreamIDs returns true if a > b numerically.
// Redis Stream IDs have format "{milliseconds}-{sequence}" where sequence
// numbers have variable digit lengths. Go's native > operator uses
// lexicographic comparison which produces wrong results:
// "1700000000000-10" < "1700000000000-2" because '1' < '2' (ASCII).
//
// Defensive: handles malformed IDs (missing hyphen) by treating sequence as 0.
// On ParseUint failure, the component is treated as 0 — a corrupt entry will
// compare as "0-0", causing it to not be skipped by dedup (fail-open).
func compareStreamIDs(a, b string) bool {
	aParts := strings.SplitN(a, "-", 2)
	bParts := strings.SplitN(b, "-", 2)

	aMs, _ := strconv.ParseUint(aParts[0], 10, 64)
	bMs, _ := strconv.ParseUint(bParts[0], 10, 64)

	if aMs != bMs {
		return aMs > bMs
	}

	var aSeq, bSeq uint64
	if len(aParts) > 1 {
		aSeq, _ = strconv.ParseUint(aParts[1], 10, 64)
	}

	if len(bParts) > 1 {
		bSeq, _ = strconv.ParseUint(bParts[1], 10, 64)
	}

	return aSeq > bSeq
}

// nextStreamID increments a Redis stream ID to create an exclusive lower bound
// for the next XRANGE page. Examples:
//
//	"1700000000000-5" → "1700000000000-6"
//	"1700000000000-0" → "1700000000000-1"
//
// Edge cases:
//   - Missing hyphen: treats as "{ms}-0", returns "{ms}-1"
//   - Sequence overflow at MaxUint64: increments millisecond part, resets sequence to 0
func nextStreamID(id string) string {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) < 2 {
		return parts[0] + "-1"
	}

	seq, _ := strconv.ParseUint(parts[1], 10, 64)
	if seq == math.MaxUint64 {
		ms, _ := strconv.ParseUint(parts[0], 10, 64)

		return strconv.FormatUint(ms+1, 10) + "-0"
	}

	return parts[0] + "-" + strconv.FormatUint(seq+1, 10)
}

// uuidv7ToStreamID extracts the UUIDv7 timestamp and returns an approximate
// Redis stream ID for seeking. The seek position is offset backward by
// clockSkewMargin to handle clock drift between app and Redis servers.
//
// Returns:
//   - streamID: "{timestamp_ms - clockSkewMargin}-0" for XRANGE lower bound
//   - timestampMs: raw UUIDv7 timestamp for logging/debugging
//   - err: if the event ID is not a valid UUIDv7
//
// The "urn:uuid:" prefix (standard Mercure format) is stripped automatically.
func uuidv7ToStreamID(eventID string, clockSkewMargin uint64) (streamID string, timestampMs uint64, err error) {
	raw := strings.TrimPrefix(eventID, "urn:uuid:")

	u, err := uuid.FromString(raw)
	if err != nil {
		return "", 0, fmt.Errorf("redis transport: invalid UUIDv7 event ID: %w", err)
	}

	// UUIDv7: bits 0-47 are Unix timestamp in milliseconds (big-endian)
	// Per RFC 9562 Section 5.7. Zero-alloc bit shift instead of append+Uint64.
	ms := uint64(u[0])<<40 | uint64(u[1])<<32 | uint64(u[2])<<24 | uint64(u[3])<<16 | uint64(u[4])<<8 | uint64(u[5])

	// Explicit comparison avoids uint64 underflow wrapping to ~1.8e19
	var seekMs uint64
	if ms > clockSkewMargin {
		seekMs = ms - clockSkewMargin
	}

	return strconv.FormatUint(seekMs, 10) + "-0", ms, nil
}
