package mercure

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errNotAdmission is a static non-admission error used as the negative control
// for the errors.As discrimination test (a plain error must keep its 503 path).
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

// TestAdmissionErrorIsExtractableThroughWrap pins the handler's discrimination
// contract: the gate wraps the sentinel, and SubscribeHandler must recover it
// (→ 429 + Retry-After) via errors.As rather than substring-matching.
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

// admitEverything is the trivial Admitter used to pin the interface shape.
type admitEverything struct{}

func (admitEverything) TryAdmit(ctx context.Context) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func TestAdmitterInterfaceShape(t *testing.T) {
	t.Parallel()

	var a Admitter = admitEverything{}

	ctx, release, err := a.TryAdmit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ctx)
	require.NotNil(t, release)
	release() // must be safe to call
}
