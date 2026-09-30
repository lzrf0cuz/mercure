package caddy

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
)

// TestMain moves everything Caddy persists during the tests (autosave.json,
// instance.uuid, locks, pki, and the default Bolt file, mercure.db) from the
// user's Caddy directories to a temp directory. XDG_CONFIG_HOME and
// XDG_DATA_HOME are the only variables caddy.AppConfigDir and caddy.AppDataDir
// read before their per-user fallbacks, so they cover every platform. Caddy
// computes caddy.DefaultStorage and caddy.ConfigAutosavePath at package init,
// before this runs, so both are reset here as well.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "mercure-caddy-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create temp dir:", err)

		return 1
	}

	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			fmt.Fprintln(os.Stderr, "remove temp dir:", err)
		}
	}()

	for name, value := range map[string]string{
		"XDG_CONFIG_HOME": filepath.Join(dir, "config"),
		"XDG_DATA_HOME":   filepath.Join(dir, "data"),
	} {
		if err := os.Setenv(name, value); err != nil {
			fmt.Fprintln(os.Stderr, "set", name+":", err)

			return 1
		}
	}

	caddy.DefaultStorage = &certmagic.FileStorage{Path: caddy.AppDataDir()}
	caddy.ConfigAutosavePath = filepath.Join(caddy.AppConfigDir(), "autosave.json")

	return m.Run()
}
