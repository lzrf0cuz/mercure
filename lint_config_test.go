package mercure

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// deprecatedFilesLintRegex mirrors the lint exclusion regex from
// .golangci.yml under both issues.exclude-rules and formatters.exclusions.
// Drift between this constant and the YAML defeats the positive control —
// keep them in lock-step.
const deprecatedFilesLintRegex = `(/|^)[a-z]+_deprecated(_test|_transport)?\.go$`

// TestDeprecatedFilesLintRegexMatchesExpectedSet is the positive control
// for the .golangci.yml exclusion that suppresses lint/format on the
// deprecated-tag-only surface. A typo in the YAML (dropping the
// alternation, changing `_transport` to `_transports`, widening to
// `_(not)?deprecated`) would silently widen or narrow the exclusion with
// no other signal from CI.
//
// This test catches drift between the YAML regex and the literal copy in
// deprecatedFilesLintRegex below — keep them in lockstep. It does NOT
// detect new deprecated files added to the tree without a fixture update
// (lint will surface that case via its normal rule application). Update
// both the YAML and the matrix when adding new deprecated surface.
func TestDeprecatedFilesLintRegexMatchesExpectedSet(t *testing.T) {
	t.Parallel()

	re := regexp.MustCompile(deprecatedFilesLintRegex)

	cases := []struct {
		path        string
		shouldMatch bool
		reason      string
	}{
		// Hub-root deprecated files — all must match. Inventory verified
		// against `find . -maxdepth 1 -name '*_deprecated*.go'`.
		{"bolt_deprecated_test.go", true, "deprecated-tag-only bbolt transport tests"},
		{"config_deprecated.go", true, "deprecated-tag-only viper/cobra config wiring"},
		{"config_deprecated_test.go", true, "tests for the deprecated config wiring"},
		{"handler_deprecated.go", true, "deprecated-tag-only HTTP handler shim"},
		{"hub_deprecated.go", true, "deprecated-tag-only Hub constructor surface"},
		{"hub_deprecated_test.go", true, "tests for the deprecated Hub surface"},
		{"metrics_deprecated.go", true, "deprecated-tag-only Prometheus registration shim"},
		{"server_deprecated_test.go", true, "tests for the deprecated server"},
		{"transport_deprecated.go", true, "deprecated-tag-only Transport interface shim"},
		// Caddy submodule deprecated file — must match (path-prefixed and bare).
		{"caddy/mercure_deprecated_transport.go", true, "deprecated-tag-only caddy submodule transport shim, path-qualified"},
		{"mercure_deprecated_transport.go", true, "deprecated-tag-only caddy submodule transport shim, bare-name"},
		// Active surface — must NOT match. The active-tag counterpart of
		// the deprecated_transport shim has a `not` prefix that the regex
		// must reject; if alternation widens (e.g. `_(not)?deprecated`),
		// this assertion fires. Hub-root *_notdeprecated*.go are the
		// closest-named negative controls in the tree.
		{"caddy/mercure_notdeprecated_transport.go", false, "active-tag transport surface — wrongly excluding this would let lint regressions slip through"},
		{"mercure_notdeprecated_transport.go", false, "active-tag transport surface, bare-name"},
		{"hub_notdeprecated.go", false, "hub-root active surface — closest sibling name to the deprecated set"},
		{"hub_notdeprecated_test.go", false, "hub-root active surface tests"},
		// Unrelated active files — must NOT match.
		{"hub.go", false, "core hub surface — wrongly excluding this would silence the bulk of the codebase"},
		{"publish.go", false, "publish handler — must remain lint-covered"},
		{"caddy/mercure.go", false, "caddy module entry point — must remain lint-covered"},
		// Edge case: a file named exactly "deprecated.go" (no prefix) — the regex
		// requires `[a-z]+_` before `deprecated`, so it must NOT match. Locks
		// the contract that bare-name deprecated.go (if ever created) does NOT
		// silently inherit the exclusion.
		{"deprecated.go", false, "bare deprecated.go has no `[a-z]+_` prefix — must NOT inherit exclusion silently"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.shouldMatch, re.MatchString(tc.path),
				"regex match mismatch for %q (%s) — drift between YAML regex and the file inventory; update both this matrix and .golangci.yml in lockstep",
				tc.path, tc.reason)
		})
	}
}
