package redistransport

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// TestDecodeStreamEntryDropsForbiddenSSEChars pins the receive-side SSE
// injection guard (CWE-93) in decodeStreamEntry, which both XREADGROUP and
// history replay use: an update whose id or type contains CR, LF or NUL is
// dropped and counted under kind="forbidden_sse_chars". The reserved-topic
// case pins that the guard checks only id and type: subscription events use
// reserved topics that the hub's Validate rejects.
func TestDecodeStreamEntryDropsForbiddenSSEChars(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)

	// The guard runs after codec.Unmarshal, on the decoded *Update, so it is
	// codec-agnostic. Run every case through each supported codec to prove the
	// forbidden bytes survive the round-trip and the guard fires regardless.
	codecs := []struct {
		name  string
		codec mercure.Codec
	}{
		{"json", mercure.JSONCodec{}},
		{"gob", mercure.GobCodec{}},
		{"msgpack", mercure.MsgPackCodec{}},
	}

	cases := []struct {
		name    string
		update  *mercure.Update
		dropped bool
	}{
		{"CR in id", &mercure.Update{ID: "a\rb"}, true},
		{"LF in id", &mercure.Update{ID: "a\nb"}, true},
		{"NUL in id", &mercure.Update{ID: "a\x00b"}, true},
		{"CR in type", &mercure.Update{Type: "a\rb"}, true},
		{"LF in type", &mercure.Update{Type: "a\nb"}, true},
		{"NUL in type", &mercure.Update{Type: "a\x00b"}, true},
		{"forbidden in both id and type", &mercure.Update{ID: "a\nb", Type: "c\nd"}, true},
		{"clean update passes", &mercure.Update{Topics: []string{"https://example.com/x"}, ID: "urn:uuid:1", Type: "message"}, false},
		{"reserved-topic subscription event passes", &mercure.Update{Topics: []string{"https://example.com/.well-known/mercure/subscriptions/x"}, ID: "urn:uuid:2"}, false},
	}

	for _, cc := range codecs {
		for _, tc := range cases {
			t.Run(cc.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				data, err := cc.codec.Marshal(tc.update)
				require.NoError(t, err)

				reg := prometheus.NewRegistry()
				metrics := newMetrics(reg, serverTypeRedis)

				entry := redis.XMessage{ID: "1-0", Values: map[string]any{"data": string(data)}}
				got := decodeStreamEntry(entry, cc.codec, logger, metrics, nil)

				forbidden := forbiddenSSECharCount(t, reg)
				if tc.dropped {
					assert.Nil(t, got, "update with forbidden SSE chars must be dropped, not dispatched")
					assert.InDelta(t, float64(1), forbidden, 0, `kind="forbidden_sse_chars" must be incremented`)

					return
				}

				require.NotNil(t, got, "clean update must pass through the guard")
				assert.Equal(t, tc.update.ID, got.ID)
				assert.InDelta(t, float64(0), forbidden, 0, "clean update must not be metered as forbidden")
			})
		}
	}
}

// TestDecodeStreamEntryLogSampling asserts that the per-drop Error log is
// rate-sampled while the drop counter counts every dropped entry. A burst-2,
// near-zero-refill limiter makes the sample count deterministic.
func TestDecodeStreamEntryLogSampling(t *testing.T) {
	t.Parallel()

	limiter := rate.NewLimiter(rate.Every(time.Hour), 2)

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	reg := prometheus.NewRegistry()
	metrics := newMetrics(reg, serverTypeRedis)

	codec := mercure.JSONCodec{}
	data, err := codec.Marshal(&mercure.Update{ID: "a\nb"}) // forbidden id
	require.NoError(t, err)

	entry := redis.XMessage{ID: "1-0", Values: map[string]any{"data": string(data)}}

	const n = 5
	for range n {
		require.Nil(t, decodeStreamEntry(entry, codec, logger, metrics, limiter))
	}

	assert.InDelta(t, float64(n), forbiddenSSECharCount(t, reg), 0,
		"every dropped entry must be counted")
	assert.Equal(t, 2, strings.Count(logBuf.String(), "dropping stream entry whose id/type is not a valid SSE field value"),
		"drop logs must be rate-sampled to the limiter's budget while the counter counts all")
}

// forbiddenSSECharCount returns the mercure_redis_stream_decode_errors_total
// counter for kind="forbidden_sse_chars" (0 when the series is absent).
func forbiddenSSECharCount(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		if f.GetName() != metricStreamDecodeErrorsTotal {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == labelKeyKind && l.GetValue() == "forbidden_sse_chars" {
					return m.GetCounter().GetValue()
				}
			}
		}
	}

	return 0
}
