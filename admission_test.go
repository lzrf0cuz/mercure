package mercure

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errNotAdmission is a plain error: it must not match *AdmissionError, so it
// keeps its 503 path.
var errNotAdmission = errors.New("backend down")

func TestAdmissionReasonString(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		reason AdmissionReason
		want   string
	}{
		{AdmissionRate, "rate"},
		{AdmissionCapacity, "capacity"},
		{AdmissionReason(99), "unknown"},
	} {
		assert.Equal(t, tc.want, tc.reason.String())
	}
}

func TestAdmissionErrorCarriesReasonAndRetryAfter(t *testing.T) {
	t.Parallel()

	err := &AdmissionError{Reason: AdmissionRate, RetryAfter: 2 * time.Second}

	assert.Contains(t, err.Error(), "rate")
	assert.Contains(t, err.Error(), "2s")
}

// TestAdmissionErrorIsExtractableThroughWrap: a transport may wrap the
// *AdmissionError, and SubscribeHandler must still find it with errors.As
// (→ 429 + Retry-After).
func TestAdmissionErrorIsExtractableThroughWrap(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("redis transport: %w", &AdmissionError{
		Reason:     AdmissionCapacity,
		RetryAfter: 1500 * time.Millisecond,
	})

	var ae *AdmissionError
	require.ErrorAs(t, wrapped, &ae, "an *AdmissionError must survive wrapping")
	assert.Equal(t, AdmissionCapacity, ae.Reason)
	assert.Equal(t, 1500*time.Millisecond, ae.RetryAfter)

	// A non-admission error must NOT match (so it keeps its 503 mapping).
	require.NotErrorAs(t, errNotAdmission, &ae)
}
