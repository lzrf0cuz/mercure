package redistransport

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTestClose = errors.New("close failed")

// closeCountingClient counts Close calls on the wrapped client.
type closeCountingClient struct {
	redis.UniversalClient

	closes   atomic.Int32
	closeErr error
}

func (c *closeCountingClient) Close() error {
	c.closes.Add(1)

	return errors.Join(c.closeErr, c.UniversalClient.Close())
}

// NewRedisTransport takes ownership of the client: every error return closes it
// exactly once. The caller (the Caddy module) therefore must not close it
// again, and the client's pool does not leak.
func TestNewRedisTransportClosesClientOnceOnInitError(t *testing.T) {
	t.Parallel()

	// miniredis does not answer INFO server, so the version check fails. The
	// startup case skips that check and fails the initial presence SET.
	mr := miniredis.RunT(t)

	for _, tc := range []struct {
		name    string
		opts    []Option
		hook    *cmdErrHook
		wantErr error
	}{
		{name: "option validation", opts: []Option{WithStreamName("}x")}, wantErr: ErrInvalidOptions},
		{name: "version check", opts: nil},
		{
			name:    "startup",
			opts:    []Option{withSkipVersionCheck()},
			hook:    &cmdErrHook{errs: map[string]error{"set": errOtherWriteReply}},
			wantErr: errOtherWriteReply,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inner := redis.NewClient(&redis.Options{Addr: mr.Addr()})

			if tc.hook != nil {
				tc.hook.on.Store(true)
				inner.AddHook(tc.hook)
			}

			client := &closeCountingClient{UniversalClient: inner}

			transport, err := NewRedisTransport(client, tc.opts...)
			require.Error(t, err)
			assert.Nil(t, transport)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}

			assert.EqualValues(t, 1, client.closes.Load(), "the client must be closed exactly once")
		})
	}
}

// A failing Close is reported with the original error, not swallowed.
func TestNewRedisTransportJoinsCloseErrorOnOptionError(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	closeErr := errTestClose
	client := &closeCountingClient{UniversalClient: redis.NewClient(&redis.Options{Addr: mr.Addr()}), closeErr: closeErr}

	_, err := NewRedisTransport(client, WithStreamName("}x"))
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.ErrorIs(t, err, closeErr)
	assert.EqualValues(t, 1, client.closes.Load())
}
