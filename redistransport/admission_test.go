package redistransport

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dunglas/mercure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTryAdmitDefaultOffAdmitsEverything: with no ceiling and no rate limit
// (the default), TryAdmit admits, counts the live slot, and the release
// closure is idempotent.
func TestTryAdmitDefaultOffAdmitsEverything(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(t)

	_, release, err := transport.TryAdmit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, release)
	assert.Equal(t, int64(1), transport.admissionLeasesHeld.Load())

	release()
	assert.Equal(t, int64(0), transport.admissionLeasesHeld.Load())

	release() // a second call is a no-op
	assert.Equal(t, int64(0), transport.admissionLeasesHeld.Load())
}

// TestTryAdmitCapacityCeiling: at max_count, the next admit is shed with
// AdmissionCapacity + a positive Retry-After + the metric; releasing frees room.
func TestTryAdmitCapacityCeiling(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(
		t,
		WithSubscriberMaxCount(2),
		WithPrometheusRegisterer(prometheus.NewRegistry()),
	)
	ctx := context.Background()

	_, r1, err := transport.TryAdmit(ctx)
	require.NoError(t, err)
	_, _, err = transport.TryAdmit(ctx)
	require.NoError(t, err)

	_, r3, err := transport.TryAdmit(ctx)
	require.Error(t, err)
	require.Nil(t, r3, "a shed admit returns no release")

	var ae *mercure.AdmissionError
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, mercure.AdmissionCapacity, ae.Reason)
	assert.Positive(t, ae.RetryAfter)

	m := transport.metrics.Load()
	require.NotNil(t, m)
	assert.InDelta(t, 1.0,
		counterValueForTest(t, m.subscriberAdmissionRejected.WithLabelValues(admissionReasonCapacity)), 0.001)

	// Free a slot → the next admit succeeds again.
	r1()

	_, r4, err := transport.TryAdmit(ctx)
	require.NoError(t, err)
	require.NotNil(t, r4)
}

// TestTryAdmitNoOverAdmitUnderConcurrency: a burst of concurrent admits past the
// ceiling never exceeds max_count.
func TestTryAdmitNoOverAdmitUnderConcurrency(t *testing.T) {
	t.Parallel()

	const maxCount = 50

	transport, _ := newTestTransport(
		t,
		WithSubscriberMaxCount(maxCount),
		WithPrometheusRegisterer(prometheus.NewRegistry()),
	)
	ctx := context.Background()

	var admitted atomic.Int64

	var wg sync.WaitGroup
	for range maxCount * 4 {
		wg.Go(func() {
			if _, release, err := transport.TryAdmit(ctx); err == nil {
				admitted.Add(1)

				_ = release // hold the slot for the duration of the test
			}
		})
	}

	wg.Wait()

	assert.Equal(t, int64(maxCount), admitted.Load(), "must never over-admit past the ceiling")
	assert.Equal(t, int64(maxCount), transport.admissionLeasesHeld.Load())
}

// TestTryAdmitRateReject: with the rate budget exhausted and fail-fast
// (admission_timeout 0), the next admit is shed with AdmissionRate.
func TestTryAdmitRateReject(t *testing.T) {
	t.Parallel()

	transport, _ := newTestTransport(
		t,
		WithSubscriberRateLimit(1),
		WithSubscriberRateBurst(1),
		WithPrometheusRegisterer(prometheus.NewRegistry()),
	)
	ctx := context.Background()

	_, _, err := transport.TryAdmit(ctx) // consumes the single burst token
	require.NoError(t, err)

	_, r2, err := transport.TryAdmit(ctx) // no token, fail-fast → shed
	require.Error(t, err)
	require.Nil(t, r2)

	var ae *mercure.AdmissionError
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, mercure.AdmissionRate, ae.Reason)

	m := transport.metrics.Load()
	require.NotNil(t, m)
	assert.InDelta(t, 1.0,
		counterValueForTest(t, m.subscriberAdmissionRejected.WithLabelValues(admissionReasonRate)), 0.001)
}
