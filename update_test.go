package mercure

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssignUUID(t *testing.T) {
	t.Parallel()

	u := &Update{
		Topics:  []string{"foo"},
		Private: true,
		Retry:   3,
	}
	u.AssignUUID()

	assert.Equal(t, []string{"foo"}, u.Topics)
	assert.True(t, u.Private)
	assert.Equal(t, uint64(3), u.Retry)
	assert.True(t, strings.HasPrefix(u.ID, "urn:uuid:"))

	_, err := uuid.Parse(strings.TrimPrefix(u.ID, "urn:uuid:"))
	require.NoError(t, err)
}

// TestUpdateJSON guards the wire format used by bolt/redis history: the
// canonical topic and its alternates round-trip as a single "Topics" array,
// matching the 0.x shape exactly.
func TestUpdateJSON(t *testing.T) {
	t.Parallel()

	legacy := `{"Data":"d","ID":"i","Type":"t","Retry":3,"Topics":["https://example.com/a","https://example.com/b"],"Private":true,"Debug":false}`

	var u *Update

	require.NoError(t, json.Unmarshal([]byte(legacy), &u))
	assert.Equal(t, []string{"https://example.com/a", "https://example.com/b"}, u.Topics)
	assert.Equal(t, "d", u.Data)
	assert.Equal(t, "i", u.ID)
	assert.Equal(t, "t", u.Type)
	assert.Equal(t, uint64(3), u.Retry)
	assert.True(t, u.Private)

	out, err := json.Marshal(u)
	require.NoError(t, err)
	assert.JSONEq(t, legacy, string(out))
}

func TestLogUpdate(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	u := &Update{
		Topics:  []string{"https://example.com/foo"},
		Private: true,
		Debug:   true,
		ID:      "a", Retry: 3, Data: "bar", Type: "baz",
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

// TestLogUpdateBoundedWithoutDebug pins LogValue's default attr set: without
// Debug, the publisher-controlled type and data stay off the log line and only
// the data length is reported.
func TestLogUpdateBoundedWithoutDebug(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	u := &Update{
		Topics: []string{"https://example.com/foo"},
		ID:     "a", Retry: 3, Data: "bar", Type: "baz",
	}

	logger.Info("test", slog.Any("update", u))

	log := buf.String()
	assert.Contains(t, log, `"id":"a"`)
	assert.Contains(t, log, `"data_bytes":3`)
	assert.NotContains(t, log, `"type"`)
	assert.NotContains(t, log, `"data":`)
	assert.NotContains(t, log, "baz")
}

func TestLogUpdateNil(t *testing.T) {
	t.Parallel()

	var u *Update

	assert.Equal(t, slog.GroupValue(), u.LogValue())
}

func TestNewSerializedUpdateCachesSSE(t *testing.T) {
	t.Parallel()

	u := &Update{
		Topics: []string{"https://example.com/test"},
		ID:     "test-id", Data: "hello world", Type: "message",
	}

	// First call should serialize and cache.
	su1 := newSerializedUpdate(u)
	assert.Contains(t, string(su1.eventBytes), "id: test-id")
	assert.Contains(t, string(su1.eventBytes), "data: hello world")
	assert.Contains(t, string(su1.eventBytes), "event: message")
	assert.Equal(t, u.String(), string(su1.eventBytes))

	// Second call should return the same cached bytes, sharing one array.
	su2 := newSerializedUpdate(u)
	assert.Equal(t, su1.eventBytes, su2.eventBytes)
	require.NotEmpty(t, su2.eventBytes)
	assert.Same(t, &su1.eventBytes[0], &su2.eventBytes[0], "subscribers must share one cached byte slice")

	// The shared slice is capped, so an append by one writer reallocates
	// instead of writing into the array the other subscribers read.
	assert.Equal(t, len(su1.eventBytes), cap(su1.eventBytes))
}

func TestNewSerializedUpdateConcurrent(t *testing.T) {
	t.Parallel()

	u := &Update{
		Topics: []string{"https://example.com/concurrent"},
		ID:     "concurrent-id", Data: "concurrent data",
	}

	// Call from multiple goroutines; must not race.
	done := make(chan []byte, 100)

	for range 100 {
		go func() {
			su := newSerializedUpdate(u)
			done <- su.eventBytes
		}()
	}

	var first []byte

	for range 100 {
		result := <-done
		if first == nil {
			first = result
		}

		assert.Equal(t, first, result, "all goroutines must get the same cached SSE")
	}
}

// TestSubscribeHandlerWritesCachedSSE pins that the subscriber write path goes
// through newSerializedUpdate: once SubscribeHandler has written the update,
// its serializedOnce must already have fired.
func TestSubscribeHandlerWritesCachedSSE(t *testing.T) {
	t.Parallel()

	hub := createAnonymousDummy(t)
	u := &Update{
		Topics: []string{"https://example.com/books/1"},
		Data:   "Hello World", ID: "b",
	}

	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequest(http.MethodGet, defaultHubURL+"?match=https://example.com/books/1", nil).WithContext(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		hub.SubscribeHandler(&responseTester{
			expectedStatusCode: http.StatusOK,
			expectedBody:       ":\nid: b\ndata: Hello World\n\n",
			tb:                 t,
			cancel:             cancel,
		}, req)
	}()

	waitSubscribers(t, hub.transport.(*LocalTransport), 1)
	require.NoError(t, hub.transport.Dispatch(t.Context(), u))
	<-done

	notCached := false

	u.serializedOnce.Do(func() { notCached = true })
	assert.False(t, notCached, "the subscriber write must use the update's cached serialization")
}
