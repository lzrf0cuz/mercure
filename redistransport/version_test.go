package redistransport

import (
	"runtime/debug"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	t.Parallel()

	got := Version()
	require.NotEmpty(t, got, "Version() must not return an empty string")

	// In `go test` builds without -ldflags, the source's "dev" sentinel
	// remains because debug.ReadBuildInfo reports Main.Version="(devel)"
	// for in-tree compilation. The dev path is the most-exercised one in
	// CI and must still produce a usable label value.
	if got == devVersion {
		return
	}

	// When ldflag-injected (e.g., during a `task go:build` or Docker
	// build), Version() returns bare semver: leading "v" stripped, three
	// dot-separated parts. Mirrors common.AppVersion.Version's shape so
	// the build_info gauge label and mercure_version_info label join
	// cleanly without a parse step.
	assert.False(t, strings.HasPrefix(got, "v"),
		"Version() %q must NOT carry a leading 'v' — labels join on bare semver", got)

	parts := strings.Split(got, ".")
	require.Len(t, parts, 3, "Version() %q must look like MAJOR.MINOR.PATCH", got)

	for i, part := range parts {
		assert.NotEmpty(t, part, "Version() %q part %d is empty", got, i)
	}
}

func TestLookupBuildInfo(t *testing.T) {
	t.Parallel()

	bi := LookupBuildInfo()

	assert.Equal(t, Version(), bi.Version,
		"BuildInfo.Version must equal Version()'s resolved value")
	assert.True(t, strings.HasPrefix(bi.GoVersion, "go"),
		"BuildInfo.GoVersion %q must start with 'go' (debug.ReadBuildInfo / runtime.Version format)", bi.GoVersion)

	// Revision is populated only in `go build` artifacts (vcs.revision is
	// omitted by `go test` and `go run`). When it IS present, the
	// truncation contract is ≤ vcsRevisionShortLen chars — assert that
	// bound regardless of the active build mode so a future regression
	// surfaces here.
	assert.LessOrEqual(t, len(bi.Revision), vcsRevisionShortLen,
		"BuildInfo.Revision %q exceeds %d-char truncation contract", bi.Revision, vcsRevisionShortLen)
}

// TestResolveVersion_LdflagInjected covers the build-time injection path
// directly: with `version = "v1.2.3"`, resolveVersion strips the leading
// "v" without consulting debug.ReadBuildInfo. Mutates the package-level
// `version` var. Go runs non-parallel tests serially before resuming
// parallel tests, so the mutation cannot race Version() callers in
// TestVersion / TestLookupBuildInfo / TestVersionFromBuildInfo. The
// t.Cleanup restores the sentinel.
func TestResolveVersion_LdflagInjected(t *testing.T) {
	original := version

	t.Cleanup(func() { version = original })

	cases := []struct {
		name, injected, want string
	}{
		{"with leading v", "v1.2.3", "1.2.3"},
		{"already bare", "1.2.3", "1.2.3"},
		{"prerelease", "v1.0.0-rc.1", "1.0.0-rc.1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			version = tc.injected
			assert.Equal(t, tc.want, resolveVersion(),
				"resolveVersion injected=%q", tc.injected)
		})
	}
}

func TestIsRealVersion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		// Literal "(devel)" canary: catches a typo in develVersionSentinel.
		// runtime/debug.ReadBuildInfo emits this exact string for in-tree builds —
		// if the constant value drifts, this assertion catches the regression
		// where production silently misclassifies real (devel) builds as releases.
		{"(devel)", false},
		{"v0.0.1", true},
		{"0.0.1", true},
		{"v1.2.3-rc.4", true},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.want, isRealVersion(tc.in), "isRealVersion(%q)", tc.in)
	}
}

// TestResolveVersion_EmptyLdflag covers the build-pipeline accident where
// RT_VERSION is unset / empty and Compose substitutes "" into the
// Dockerfile ARG, injecting an empty ldflag value. Without the
// isRealVersion guard, version="" would short-circuit and the metric
// would emit version="" — bypassing the debug.ReadBuildInfo fallback.
func TestResolveVersion_EmptyLdflag(t *testing.T) {
	original := version

	t.Cleanup(func() { version = original })

	version = ""

	assert.NotEmpty(t, resolveVersion(),
		"empty-string ldflag injection must fall through to fallback chain")
}

func TestVersionFromBuildInfo(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		di   *debug.BuildInfo
		want string
	}{
		{
			name: "main matches with real version",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: moduleImportPath, Version: "v1.2.3"},
			},
			want: "1.2.3",
		},
		{
			name: "main is (devel) — falls through to deps",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: moduleImportPath, Version: develVersionSentinel},
				Deps: []*debug.Module{
					{Path: moduleImportPath, Version: "v2.0.0"},
				},
			},
			want: "2.0.0",
		},
		{
			name: "deps matches with real version",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/dunglas/mercure"},
				Deps: []*debug.Module{
					{Path: moduleImportPath, Version: "v1.2.3"},
				},
			},
			want: "1.2.3",
		},
		{
			name: "deps with local-path replace — synthetic v0.0.0 filtered",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/dunglas/mercure"},
				Deps: []*debug.Module{
					{
						Path:    moduleImportPath,
						Version: "v0.0.0",
						Replace: &debug.Module{Path: "../redistransport"},
					},
				},
			},
			want: devVersion,
		},
		{
			name: "deps with registry-versioned replace — also filtered (ldflag is canonical)",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/dunglas/mercure"},
				Deps: []*debug.Module{
					{
						Path:    moduleImportPath,
						Version: "v0.0.0",
						Replace: &debug.Module{
							Path:    "github.com/someone/redistransport-fork",
							Version: "v2.0.0",
						},
					},
				},
			},
			want: devVersion,
		},
		{
			name: "no matching dep — devVersion sentinel",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/dunglas/mercure"},
				Deps: []*debug.Module{
					{Path: "github.com/some/other/dep", Version: "v1.0.0"},
				},
			},
			want: devVersion,
		},
		{
			name: "deps matches but version empty",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/dunglas/mercure"},
				Deps: []*debug.Module{
					{Path: moduleImportPath, Version: ""},
				},
			},
			want: devVersion,
		},
		{
			name: "multiple deps with our path — first real-version match wins",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/dunglas/mercure"},
				Deps: []*debug.Module{
					{Path: moduleImportPath, Version: "v1.0.0"},
					{Path: moduleImportPath, Version: "v2.0.0"},
				},
			},
			want: "1.0.0",
		},
		{
			name: "main is (devel) and deps is nil — falls through to devVersion",
			di: &debug.BuildInfo{
				Main: debug.Module{Path: moduleImportPath, Version: develVersionSentinel},
				Deps: nil,
			},
			want: devVersion,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, versionFromBuildInfo(tc.di))
		})
	}
}

func TestRevisionFromSettings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		settings []debug.BuildSetting
		want     string
	}{
		{"no vcs.revision setting", []debug.BuildSetting{{Key: "GOOS", Value: "darwin"}}, ""},
		{"empty vcs.revision", []debug.BuildSetting{{Key: vcsRevisionKey, Value: ""}}, ""},
		{"revision exactly vcsRevisionShortLen chars (boundary, not truncated)", []debug.BuildSetting{{Key: vcsRevisionKey, Value: "abc1234"}}, "abc1234"},
		{"long revision truncated to vcsRevisionShortLen chars", []debug.BuildSetting{{Key: vcsRevisionKey, Value: "abc1234deadbeef"}}, "abc1234"},
		{"multiple vcs.revision entries — first wins", []debug.BuildSetting{
			{Key: vcsRevisionKey, Value: "aaaaaaa"},
			{Key: vcsRevisionKey, Value: "bbbbbbb"},
		}, "aaaaaaa"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, revisionFromSettings(tc.settings))
		})
	}
}
