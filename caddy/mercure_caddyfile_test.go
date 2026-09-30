package caddy

import (
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddytest"
	"github.com/dunglas/mercure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnmarshalCaddyfileSubscriberOutBuffer covers the subscriber_out_buffer
// directive's parser: 0 and values at or above the floor set the field, a
// negative, sub-floor or non-numeric value is rejected, and an omitted
// directive leaves the field nil (so the hub default applies).
func TestUnmarshalCaddyfileSubscriberOutBuffer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		input    string
		wantSize *int
		wantNil  bool
		wantErr  bool
	}{
		{
			name:     "valid size",
			input:    "mercure {\n\tsubscriber_out_buffer 64\n}",
			wantSize: new(64),
		},
		{
			name:    "negative rejected",
			input:   "mercure {\n\tsubscriber_out_buffer -1\n}",
			wantErr: true,
		},
		{
			name:    "positive below the floor rejected",
			input:   "mercure {\n\tsubscriber_out_buffer 8\n}",
			wantErr: true,
		},
		{
			name:     "explicit zero means default",
			input:    "mercure {\n\tsubscriber_out_buffer 0\n}",
			wantSize: new(0),
		},
		{
			name:    "non-numeric rejected",
			input:   "mercure {\n\tsubscriber_out_buffer not-a-number\n}",
			wantErr: true,
		},
		{
			name:    "omitted leaves field nil",
			input:   "mercure {\n}",
			wantNil: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := caddyfile.NewTestDispenser(tc.input)

			var m Mercure

			err := m.UnmarshalCaddyfile(d)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tc.wantNil {
				assert.Nil(t, m.SubscriberOutBuffer)

				return
			}

			require.NotNil(t, m.SubscriberOutBuffer)
			assert.Equal(t, *tc.wantSize, *m.SubscriberOutBuffer)
		})
	}
}

// TestUnmarshalCaddyfilePublishTimeout covers the publish_timeout directive: a
// valid duration sets the field, a malformed one surfaces the parse error, and
// an omitted directive leaves it nil (so the timeout stays disabled).
func TestUnmarshalCaddyfilePublishTimeout(t *testing.T) {
	t.Parallel()

	t.Run("valid duration", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("mercure {\n\tpublish_timeout 30s\n}")

		var m Mercure

		require.NoError(t, m.UnmarshalCaddyfile(d))
		require.NotNil(t, m.PublishTimeout)
		assert.Equal(t, caddy.Duration(30*time.Second), *m.PublishTimeout)
	})

	t.Run("malformed rejected", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("mercure {\n\tpublish_timeout not-a-duration\n}")

		var m Mercure

		require.Error(t, m.UnmarshalCaddyfile(d))
	})

	t.Run("omitted leaves field nil", func(t *testing.T) {
		t.Parallel()

		d := caddyfile.NewTestDispenser("mercure {\n}")

		var m Mercure

		require.NoError(t, m.UnmarshalCaddyfile(d))
		assert.Nil(t, m.PublishTimeout)
	})
}

// TestSubscriberOutBufferReachesHub proves Provision passes subscriber_out_buffer
// to the hub: a JSON config skips the Caddyfile floor check, so a sub-minimum
// value must be refused by mercure.WithSubscriberOutBuffer when the hub is built.
func TestSubscriberOutBufferReachesHub(t *testing.T) {
	caddytest.AssertLoadError(t, `{
		"admin": {"listen": "localhost:2999"},
		"apps": {
			"http": {
				"http_port": 9080,
				"https_port": 9443,
				"servers": {
					"srv0": {
						"listen": [":9080"],
						"routes": [{
							"handle": [{
								"handler": "mercure",
								"anonymous": true,
								"resource_identifier": "https://example.com/.well-known/mercure",
								"issuers": [{"identifier": "https://example.com", "publisher": {"jwt": {"key": "!ChangeMe!", "alg": "HS256"}}}],
								"subscriber_out_buffer": 8,
								"transport": {"name": "local"}
							}]
						}]
					}
				}
			},
			"pki": {"certificate_authorities": {"local": {"install_trust": false}}}
		}
	}`, "json", mercure.ErrInvalidSubscriberOutBuffer.Error())
}
