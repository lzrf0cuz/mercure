package mercure

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssignUUID(t *testing.T) {
	t.Parallel()

	u := &Update{
		Topics:  []string{"foo"},
		Private: true,
		Event:   Event{Retry: 3},
	}
	u.AssignUUID()

	assert.Equal(t, []string{"foo"}, u.Topics)
	assert.True(t, u.Private)
	assert.Equal(t, uint64(3), u.Retry)
	assert.True(t, strings.HasPrefix(u.ID, "urn:uuid:"))

	_, err := uuid.FromString(strings.TrimPrefix(u.ID, "urn:uuid:"))
	require.NoError(t, err)
}

func TestLogUpdate(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	u := &Update{
		Topics:  []string{"https://example.com/foo"},
		Private: true,
		Debug:   true,
		Event:   Event{ID: "a", Retry: 3, Data: "bar", Type: "baz"},
	}

	logger.Info("test", slog.Any("update", u))

	log := buf.String()
	assert.Contains(t, log, `"id":"a"`)
	assert.Contains(t, log, `"type":"baz"`)
	assert.Contains(t, log, `"retry":3`)
	assert.Contains(t, log, `"topics":["https://example.com/foo"]`)
	assert.Contains(t, log, `"private":true`)
	assert.Contains(t, log, `"data":"bar"`)
}

func TestNewSerializedUpdateCachesSSE(t *testing.T) {
	t.Parallel()

	u := &Update{
		Topics: []string{"https://example.com/test"},
		Event:  Event{ID: "test-id", Data: "hello world", Type: "message"},
	}

	// First call should serialize and cache.
	su1 := newSerializedUpdate(u)
	assert.Contains(t, su1.event, "id: test-id")
	assert.Contains(t, su1.event, "data: hello world")
	assert.Contains(t, su1.event, "event: message")

	// Second call should return the same cached string (same pointer content).
	su2 := newSerializedUpdate(u)
	assert.Equal(t, su1.event, su2.event)
}

func TestNewSerializedUpdateConcurrent(t *testing.T) {
	t.Parallel()

	u := &Update{
		Topics: []string{"https://example.com/concurrent"},
		Event:  Event{ID: "concurrent-id", Data: "concurrent data"},
	}

	// Call from multiple goroutines — must not race.
	done := make(chan string, 100)

	for range 100 {
		go func() {
			su := newSerializedUpdate(u)
			done <- su.event
		}()
	}

	var first string

	for range 100 {
		result := <-done
		if first == "" {
			first = result
		}

		assert.Equal(t, first, result, "all goroutines must get the same cached SSE")
	}
}
