package caddy

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	caddyv2 "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
)

// TestMain moves everything Caddy persists during the tests (instance.uuid,
// locks, autosave.json) from the user's Caddy directories to a temp
// directory. XDG_CONFIG_HOME and XDG_DATA_HOME are the only variables
// caddyv2.AppConfigDir and caddyv2.AppDataDir read before their per-user
// fallbacks, so they cover every platform. Caddy computes caddyv2.DefaultStorage
// and caddyv2.ConfigAutosavePath at package init, before this runs, so both are
// reset here as well.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "mercure-redistransport-caddy-test-")
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

	caddyv2.DefaultStorage = &certmagic.FileStorage{Path: caddyv2.AppDataDir()}
	caddyv2.ConfigAutosavePath = filepath.Join(caddyv2.AppConfigDir(), "autosave.json")

	return m.Run()
}
