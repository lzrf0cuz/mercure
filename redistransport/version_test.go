package redistransport

import (
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	t.Parallel()

	got := Version()
	require.NotEmpty(t, got, "Version() must not return an empty string")

	// Without -ldflags, go test builds report Main.Version "(devel)", so
	// Version() stays at the "dev" sentinel.
	if got == devVersion {
		return
	}

	// A release Version() is bare numeric MAJOR.MINOR.PATCH with an optional
	// semver prerelease (1.0.0-rc.1) and build metadata (1.0.0+build.5), and no
	// leading "v". Both are cut off before the part count, as their own dots
	// would add parts.
	assert.False(t, strings.HasPrefix(got, "v"),
		"Version() %q must not carry a leading 'v'", got)

	core, _, _ := strings.Cut(got, "+")
	core, _, _ = strings.Cut(core, "-")
	parts := strings.Split(core, ".")
	require.Len(t, parts, 3, "Version() %q must look like MAJOR.MINOR.PATCH", got)

	for i, part := range parts {
		_, err := strconv.Atoi(part)
		assert.NoError(t, err, "Version() %q part %d (%q) is not numeric", got, i, part)
	}
}

func TestLookupBuildInfo(t *testing.T) {
	t.Parallel()

	bi := LookupBuildInfo()

	assert.Equal(t, Version(), bi.Version,
		"BuildInfo.Version must equal Version()'s resolved value")
	assert.True(t, strings.HasPrefix(bi.GoVersion, "go"),
		"BuildInfo.GoVersion %q must start with 'go' (debug.ReadBuildInfo / runtime.Version format)", bi.GoVersion)

	// Revision is set only in go build artifacts; when set, it is truncated to
	// vcsRevisionShortLen.
	assert.LessOrEqual(t, len(bi.Revision), vcsRevisionShortLen,
		"BuildInfo.Revision %q exceeds %d-char truncation contract", bi.Revision, vcsRevisionShortLen)
}

// TestResolveVersion_LdflagInjected covers the build-time injection path
// directly: with `version = "v1.2.3"`, resolveVersion strips the leading
// "v" without consulting debug.ReadBuildInfo. Mutates the package-level
// `version` var. Go runs non-parallel tests serially before resuming
// parallel tests. The mutation therefore cannot race any parallel reader of
// `version` (TestVersion, TestLookupBuildInfo, the build_info check in
// metrics_test.go, and every transport built with metrics). The t.Cleanup
// restores the original value.
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
		// "(devel)" is what debug.ReadBuildInfo reports for in-tree builds; the
		// literal guards develVersionSentinel's value.
		{"(devel)", false},
		{"v0.0.1", true},
		{"0.0.1", true},
		{"v1.2.3-rc.4", true},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.want, isRealVersion(tc.in), "isRealVersion(%q)", tc.in)
	}
}

// TestResolveVersion_EmptyLdflag pins that an empty injected version falls
// back to debug.ReadBuildInfo instead of emitting version="".
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
