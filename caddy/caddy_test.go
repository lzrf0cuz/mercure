package caddy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	bearerPrefix    = "Bearer "
	publisherJWT    = "eyJhbGciOiJIUzI1NiJ9.eyJtZXJjdXJlIjp7InB1Ymxpc2giOlsiKiJdfX0.vhMwOaN5K68BTIhWokMLOeOJO4EPfT64brd8euJOA4M"
	publisherJWTRSA = "eyJhbGciOiJSUzI1NiJ9.eyJtZXJjdXJlIjp7InB1Ymxpc2giOlsiKiJdLCJzdWJzY3JpYmUiOlsiaHR0cHM6Ly9leGFtcGxlLmNvbS9teS1wcml2YXRlLXRvcGljIiwie3NjaGVtZX06Ly97K2hvc3R9L2RlbW8vYm9va3Mve2lkfS5qc29ubGQiLCIvLndlbGwta25vd24vbWVyY3VyZS9zdWJzY3JpcHRpb25zey90b3BpY317L3N1YnNjcmliZXJ9Il0sInBheWxvYWQiOnsidXNlciI6Imh0dHBzOi8vZXhhbXBsZS5jb20vdXNlcnMvZHVuZ2xhcyIsInJlbW90ZUFkZHIiOiIxMjcuMC4wLjEifX19.iwryQ5k-CWNCNQLPg7CtgTdDWbG_CurSxDK8kMjTZfprGhh7Yli1SFt8WB3U4zbZ2wxUO7UfprZq3hnl8nSrozO9KDTCDwCYhMgRlcrdwm6XL1uXFwMJt4VSmp1srCQotv0FgT11jF8Km1vMQQOnUC27Va9fbfRtITVsjxsveYeMJqusVWO6F3vAvkM35oL8E8qgBbfrG_lnuhb_9Ws6RIq4YOslkOar_gopEs00CITxmV_aHVHRYzeW7QpycxjC7m8Mp-lKzaUewvJuKWI5HsM134xfaH8RAHSvh6H9pVQAiJ9tyc17bAx46M98WMsHFokVwz3rd7PoGGou6A7y5RzeGpiSxykTWCPPcBnxJ1gwUYqEYGTnRjl9JmhHY_VfQP4edyU-zhmMCCSie8rvkRDilAQGd5kj5m1voSn-EqA13sSe69evXxVUIB2nO70qHCcHBBHxunLqTIIerpc3F9_WWM4_Q_0j9CoTd2aFyuq_sdc6RcmAE3uTznp2DyKNQkT1EfpY7xCCe1MR-Webez5Ioa1EMDP0KrvLdnNRmuM3THSu1pqcvPV7Di7dJci5QWsYEmaP8cLuuZXdAhy_UoSgzbvfT_8mlDoJ9VvDXLJ39OwGYIyZiZ9VTNXm8mxre993cqg7boZRS8x70VRxnjmNxm40SgEvb6CHYO0lSBU"
	subscriberJWT   = "eyJhbGciOiJIUzI1NiJ9.eyJtZXJjdXJlIjp7InN1YnNjcmliZSI6WyIqIl19fQ.g3w81T7YQLKLrgovor9uEKUiOCAx6DmAAbq18qmDwsY"
)

func TestMercure(t *testing.T) {
	boltPath := filepath.Join(t.TempDir(), "bolt.db")

	data := []struct {
		name            string
		transportConfig string
	}{
		{"bolt", `transport bolt {
			path ` + boltPath + `
		}`},
		{"local", "transport local\n"},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			if d.name == "bolt" {
				t.Cleanup(func() {
					require.NoError(t, os.Remove(boltPath))
				})
			}

			tester := caddytest.NewTester(t)
			tester.InitServer(fmt.Sprintf(`{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
}

localhost:9080 {
	route {
		mercure {
			anonymous
			publisher_jwt !ChangeMe!
			%[1]s
		}

		respond 404
	}
}

example.com:9080 {
	route {
		mercure {
			anonymous
			publisher_jwt !ChangeMe!
			%[1]s
		}

		respond 404
	}
}`, d.transportConfig), "caddyfile")

			var connected, received sync.WaitGroup

			connected.Add(1)
			received.Go(func() {
				cx, cancel := context.WithCancel(t.Context())
				req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/.well-known/mercure?topic=https%3A%2F%2Fexample.com%2Ffoo%2F1", nil)
				req = req.WithContext(cx)
				resp := tester.AssertResponseCode(req, http.StatusOK)

				connected.Done()

				var receivedBody strings.Builder

				buf := make([]byte, 1024)
				for {
					_, err := resp.Body.Read(buf)
					require.NoError(t, err)

					receivedBody.Write(buf)

					if strings.Contains(receivedBody.String(), "data: bar\n") {
						cancel()

						break
					}
				}

				assert.NoError(t, resp.Body.Close())
			})

			connected.Wait()

			body := url.Values{"topic": {"https://example.com/foo/1"}, "data": {"bar"}, "id": {"bar"}}
			req, err := http.NewRequest(http.MethodPost, "http://localhost:9080/.well-known/mercure", strings.NewReader(body.Encode()))
			require.NoError(t, err)
			req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Add("Authorization", bearerPrefix+publisherJWT)

			resp := tester.AssertResponseCode(req, http.StatusOK)
			require.NoError(t, resp.Body.Close())

			received.Wait()

			if d.name != "bolt" {
				assert.NoFileExists(t, boltPath)
			}
		})
	}
}

func TestJWTPlaceholders(t *testing.T) {
	k, _ := os.ReadFile("../fixtures/jwt/RS256.key.pub")
	t.Setenv("TEST_JWT_KEY", string(k))
	t.Setenv("TEST_JWT_ALG", "RS256")

	tester := caddytest.NewTester(t)
	tester.InitServer(`
	{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
	}

	localhost:9080 {
		route {
			mercure {
				anonymous
				publisher_jwt {env.TEST_JWT_KEY} {env.TEST_JWT_ALG}
				transport local
			}
	
			respond 404
		}
	}
	`, "caddyfile")

	var connected, received sync.WaitGroup

	connected.Add(1)
	received.Go(func() {
		cx, cancel := context.WithCancel(t.Context())
		req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/.well-known/mercure?topic=https%3A%2F%2Fexample.com%2Ffoo%2F1", nil)
		req = req.WithContext(cx)
		resp := tester.AssertResponseCode(req, http.StatusOK)

		connected.Done()

		var receivedBody strings.Builder

		buf := make([]byte, 1024)
		for {
			_, err := resp.Body.Read(buf)
			require.NoError(t, err)

			receivedBody.Write(buf)

			if strings.Contains(receivedBody.String(), "data: bar\n") {
				cancel()

				break
			}
		}

		assert.NoError(t, resp.Body.Close())
	})

	connected.Wait()

	body := url.Values{"topic": {"https://example.com/foo/1"}, "data": {"bar"}, "id": {"bar"}}
	req, err := http.NewRequest(http.MethodPost, "http://localhost:9080/.well-known/mercure", strings.NewReader(body.Encode()))
	require.NoError(t, err)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+publisherJWTRSA)

	resp := tester.AssertResponseCode(req, http.StatusOK)
	require.NoError(t, resp.Body.Close())

	received.Wait()
}

func TestSubscriptionAPI(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`
	{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
	}

	localhost:9080 {
		route {
			mercure {
				anonymous
				subscriptions
				publisher_jwt !ChangeMe!
			}
	
			respond 404
		}
	}
	`, "caddyfile")

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/.well-known/mercure/subscriptions", nil)
	resp := tester.AssertResponseCode(req, http.StatusOK)
	require.NoError(t, resp.Body.Close())
}

func TestCookieName(t *testing.T) {
	tester := caddytest.NewTester(t)
	tester.InitServer(`
	{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
	}
	localhost:9080 {
		route {
			mercure {
				publisher_jwt !ChangeMe!
				subscriber_jwt !ChangeMe!
				cookie_name foo
				publish_origins http://localhost:9080
			}
	
			respond 404
		}
	}
	`, "caddyfile")

	var connected, received sync.WaitGroup

	connected.Add(1)
	received.Go(func() {
		cx, cancel := context.WithCancel(t.Context())
		req, _ := http.NewRequest(http.MethodGet, "http://localhost:9080/.well-known/mercure?topic=https%3A%2F%2Fexample.com%2Ffoo%2F1", nil)
		req.Header.Add("Origin", "http://localhost:9080")
		req.AddCookie(&http.Cookie{Name: "foo", Value: subscriberJWT, HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})
		req = req.WithContext(cx)
		resp := tester.AssertResponseCode(req, http.StatusOK)

		connected.Done()

		var receivedBody strings.Builder

		buf := make([]byte, 1024)
		for {
			_, err := resp.Body.Read(buf)
			require.NoError(t, err)

			receivedBody.Write(buf)

			if strings.Contains(receivedBody.String(), "data: bar\n") {
				cancel()

				break
			}
		}

		assert.NoError(t, resp.Body.Close())
	})

	connected.Wait()

	body := url.Values{"topic": {"https://example.com/foo/1"}, "data": {"bar"}, "id": {"bar"}, "private": {"1"}}
	req, err := http.NewRequest(http.MethodPost, "http://localhost:9080/.well-known/mercure", strings.NewReader(body.Encode()))
	require.NoError(t, err)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Origin", "http://localhost:9080")
	req.AddCookie(&http.Cookie{Name: "foo", Value: publisherJWT, HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})

	resp := tester.AssertResponseCode(req, http.StatusOK)
	require.NoError(t, resp.Body.Close())

	received.Wait()
}

func TestAllowNoPublish(t *testing.T) {
	AllowNoPublish = true

	t.Cleanup(func() {
		AllowNoPublish = false
	})

	tester := caddytest.NewTester(t)
	tester.InitServer(`
	{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
	}
	localhost:9080 {
		route {
			mercure {
				subscriber_jwt !ChangeMe!
			}
	
			respond 404
		}
	}
	`, "caddyfile")

	req, _ := http.NewRequest(http.MethodPost, "http://localhost:9080/.well-known/mercure", nil)
	r := tester.AssertResponseCode(req, http.StatusMethodNotAllowed)
	require.NoError(t, r.Body.Close())
}

func TestBoltConfig(t *testing.T) {
	t.Cleanup(func() {
		require.NoError(t, os.Remove("test.db"))
	})

	tester := caddytest.NewTester(t)
	tester.InitServer(`
{
	skip_install_trust
	admin localhost:2999
	http_port     9080
	https_port    9443
}

localhost:9080 {
	route {
		mercure {
			anonymous
			publisher_jwt !ChangeMe!
			transport bolt {
				path test.db
				bucket_name foo
				size 20
				cleanup_frequency 0.2
			}
		}

		respond 404
	}
}`, "caddyfile")

	assert.FileExists(t, "test.db")
}

func TestAdaptBoltConfig(t *testing.T) {
	caddytest.AssertAdapt(t, `http://

mercure {
	publisher_jwt !ChangeMe!
	transport bolt {
		path test.db
		bucket_name foo
		size 20
		cleanup_frequency 0.2
	}
}
`, "caddyfile", `{
	"apps": {
		"http": {
			"servers": {
				"srv0": {
					"listen": [
						":80"
					],
					"routes": [
						{
							"handle": [
								{
									"handler": "mercure",
									"publisher_jwt": {
										"key": "!ChangeMe!"
									},
									"transport": {
										"bucket_name": "foo",
										"cleanup_frequency": 0.2,
										"name": "bolt",
										"path": "test.db",
										"size": 20
									}
								}
							]
						}
					]
				}
			}
		}
	}
}`)
}

func TestAdaptLocalConfig(t *testing.T) {
	caddytest.AssertAdapt(t, `http://

mercure {
	publisher_jwt !ChangeMe!
	transport local
}
`, "caddyfile", `{
	"apps": {
		"http": {
			"servers": {
				"srv0": {
					"listen": [
						":80"
					],
					"routes": [
						{
							"handle": [
								{
									"handler": "mercure",
									"publisher_jwt": {
										"key": "!ChangeMe!"
									},
									"transport": {
										"name": "local"
									}
								}
							]
						}
					]
				}
			}
		}
	}
}`)
}

func TestJWKSURLFile(t *testing.T) {
	jwksPath, err := filepath.Abs("testdata/RS256.jwks.json")
	require.NoError(t, err)

	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`
	{
		skip_install_trust
		admin localhost:2999
		http_port     9080
		https_port    9443
	}

	localhost:9080 {
		route {
			mercure {
				anonymous
				publisher_jwks_url file://%s
				transport local
			}

			respond 404
		}
	}
	`, jwksPath), "caddyfile")

	body := url.Values{"topic": {"https://example.com/foo/1"}, "data": {"bar"}, "id": {"bar"}}
	req, err := http.NewRequest(http.MethodPost, "http://localhost:9080/.well-known/mercure", strings.NewReader(body.Encode()))
	require.NoError(t, err)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Authorization", bearerPrefix+publisherJWTRSA)

	resp := tester.AssertResponseCode(req, http.StatusOK)
	require.NoError(t, resp.Body.Close())
}

func TestNewJWKSetKeyfunc(t *testing.T) {
	jwksPath, err := filepath.Abs("testdata/RS256.jwks.json")
	require.NoError(t, err)

	t.Run("file URL with empty host", func(t *testing.T) {
		k, err := newJWKSetKeyfunc(t.Context(), "file://"+jwksPath)
		require.NoError(t, err)
		assert.NotNil(t, k)
	})

	t.Run("file URL with localhost host", func(t *testing.T) {
		k, err := newJWKSetKeyfunc(t.Context(), "file://localhost"+jwksPath)
		require.NoError(t, err)
		assert.NotNil(t, k)
	})

	t.Run("file URL with rejected host", func(t *testing.T) {
		_, err := newJWKSetKeyfunc(t.Context(), "file://example.com"+jwksPath)
		require.ErrorIs(t, err, errJWKSetFileHost)
		assert.Contains(t, err.Error(), `"example.com"`)
	})

	// Negative paths added to lock loadFileJWKSet's hardening: the size cap,
	// non-regular-file rejection, missing file, and malformed JSON each have
	// a distinct error message that operators rely on for diagnosis. Without
	// these, a refactor that drops any one of them would not be caught by the
	// existing positive cases.
	t.Run("file URL pointing at non-regular file rejected", func(t *testing.T) {
		// /dev/zero is a character special file; Size()==0, so a Size-cap
		// alone would let ReadFile drain it until OOM. The IsRegular guard
		// rejects it.
		_, err := newJWKSetKeyfunc(t.Context(), "file:///dev/zero")
		require.ErrorIs(t, err, errJWKSetFileNotRegular)
	})

	t.Run("file URL pointing at FIFO is rejected without blocking", func(t *testing.T) {
		// A blocking open(2) on a read-only FIFO hangs until a writer
		// appears; loadFileJWKSet opens with O_NONBLOCK so the open returns
		// immediately and the IsRegular Fstat check rejects it. Without the
		// O_NONBLOCK flag this call would block forever (there is no writer),
		// so the test runs it under a watchdog: a hang is a regression, not a
		// pass. /dev/zero (the sibling case above) cannot catch this — a
		// character device opens immediately regardless of O_NONBLOCK.
		if runtime.GOOS == "windows" {
			t.Skip("POSIX FIFO semantics; no syscall.Mkfifo on Windows")
		}

		fifoPath := filepath.Join(t.TempDir(), "jwks.fifo")
		require.NoError(t, syscall.Mkfifo(fifoPath, 0o600))

		errCh := make(chan error, 1)

		go func() {
			_, err := newJWKSetKeyfunc(t.Context(), "file://"+fifoPath)
			errCh <- err
		}()

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, errJWKSetFileNotRegular)
		case <-time.After(5 * time.Second):
			t.Fatal("loadFileJWKSet blocked on a FIFO open — O_NONBLOCK regression (a read-only FIFO open hangs until a writer appears)")
		}
	})

	t.Run("file URL pointing at missing file surfaces open error", func(t *testing.T) {
		_, err := newJWKSetKeyfunc(t.Context(), "file://"+filepath.Join(t.TempDir(), "missing-jwks.json"))
		require.Error(t, err)
		// "failed to read JWK Set file" prefix is the operator log-grep
		// contract; the "(open)" qualifier identifies which step failed.
		// Open-then-Fstat ordering means a missing file surfaces here, not
		// at the post-open Fstat call.
		assert.Contains(t, err.Error(), "failed to read JWK Set file")
		assert.Contains(t, err.Error(), "(open)")
	})

	t.Run("file URL pointing at oversized file is rejected at fstat", func(t *testing.T) {
		bigPath := filepath.Join(t.TempDir(), "big.jwks.json")
		bigData := make([]byte, maxJWKSetFileBytes+1)

		require.NoError(t, os.WriteFile(bigPath, bigData, 0o600))

		_, err := newJWKSetKeyfunc(t.Context(), "file://"+bigPath)
		require.ErrorIs(t, err, errJWKSetFileTooLarge)
	})

	// loadFileJWKSet has two independent size gates: the Fstat-time
	// fi.Size() > cap check (covered above) AND the post-read
	// len(b) > cap check after io.LimitReader(f, cap+1). The post-read gate
	// is defense-in-depth against a file that grows between Fstat and the
	// io.ReadAll call. Without this test the post-read gate is dead from a
	// regression-detection perspective — a refactor could drop it and the
	// existing test suite would still pass.
	//
	// Simulating "grows between Fstat and Read" deterministically is hard
	// (sparse files, race timing). Instead we lock the SHAPE of the
	// post-read gate by reading the source and asserting both the
	// io.LimitReader cap+1 idiom and the len(b) > maxJWKSetFileBytes
	// post-check are present. The error message "exceeded %d byte cap
	// during read" is the operator-visible signal we'd see in production if
	// the post-gate ever fires.
	t.Run("loadFileJWKSet post-read size gate present", func(t *testing.T) {
		src, err := os.ReadFile("mercure.go")
		require.NoError(t, err)

		body := string(src)
		assert.Contains(t, body, "io.LimitReader(f, maxJWKSetFileBytes+1)",
			"loadFileJWKSet must read via io.LimitReader at cap+1 — without it, a file that grows between Fstat and Read can exceed the cap unbounded")
		assert.Contains(t, body, "len(b) > maxJWKSetFileBytes",
			"loadFileJWKSet must post-check len(b) > maxJWKSetFileBytes — without it, the io.LimitReader cap+1 read could be silently truncated and parsed as a valid JWKS")
		assert.Contains(t, body, "grew past",
			"loadFileJWKSet post-read overflow must surface an operator-readable 'grew past ... during read' error (errJWKSetFileTooLarge)")
	})

	t.Run("file URL pointing at malformed JSON surfaces parse error", func(t *testing.T) {
		badPath := filepath.Join(t.TempDir(), "bad.jwks.json")

		require.NoError(t, os.WriteFile(badPath, []byte("not-json"), 0o600))

		_, err := newJWKSetKeyfunc(t.Context(), "file://"+badPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse JWK Set file")
	})

	t.Run("unreachable http URL fails fast at provision (not fail-open)", func(t *testing.T) {
		// keyfunc's NewDefaultCtx defaults NoErrorReturnFirstHTTPReq=true,
		// which would return a usable-but-empty key set (hub boots, then
		// rejects every JWT). newJWKSetKeyfunc overrides that to false so an
		// unreachable JWKS URL surfaces the fetch error at Provision time.
		// 127.0.0.1:1 refuses immediately on loopback — no slow DNS/timeout.
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		_, err := newJWKSetKeyfunc(ctx, "http://127.0.0.1:1/jwks.json")
		require.Error(t, err, "an unreachable JWKS URL must fail Provision, not boot with an empty key set (fail-open)")
	})

	t.Run("file URL with .. in path is filepath.Clean'd", func(t *testing.T) {
		// Verify that the error path reports the CLEANED path, not the raw one.
		// If filepath.Clean were dropped, the error message would echo the raw
		// `/tmp/foo/sub/../missing.json` form rather than `/tmp/foo/missing.json`.
		//
		// String-concatenate the path rather than filepath.Join — Join already
		// calls Clean, which would normalise `..` away BEFORE it reached
		// loadFileJWKSet and make this test vacuous (passing against any
		// implementation, including one that drops filepath.Clean).
		base := t.TempDir()
		raw := base + "/sub/../missing-jwks.json"
		want := filepath.Clean(raw)

		// Positive control: the test setup must produce a path that
		// filepath.Clean actually rewrites. If raw == want the assertions
		// below pass against any implementation including a regression that
		// drops filepath.Clean — exactly the "test the test" failure mode.
		require.NotEqual(t, want, raw, "test setup must produce a path containing '..' that filepath.Clean rewrites; otherwise the filepath.Clean assertion is vacuous")

		_, err := newJWKSetKeyfunc(t.Context(), "file://"+raw)
		require.Error(t, err)
		assert.Contains(t, err.Error(), want, "loadFileJWKSet must filepath.Clean the path so error messages report the normalised form")
		assert.NotContains(t, err.Error(), "/sub/../", "raw '..' segments must not appear in the error message — filepath.Clean was dropped")
	})
}
