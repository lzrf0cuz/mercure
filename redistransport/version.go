package redistransport

import (
	"runtime"
	"runtime/debug"
	"strings"
)

// version is set at link time with
// `-X github.com/lzrf0cuz/mercure/redistransport.version=<value>`; see
// resolveVersion for the fallback order.
var version = devVersion

const (
	// vcsRevisionShortLen matches git's default short-SHA length.
	vcsRevisionShortLen = 7
	// moduleImportPath is redistransport's import path; matched against
	// debug.ReadBuildInfo's Main / Deps entries when resolving the module's
	// own version.
	moduleImportPath = "github.com/lzrf0cuz/mercure/redistransport"
	// devVersion is the sentinel returned when no real version is resolvable.
	devVersion = "dev"
	// vcsRevisionKey is the debug.BuildSetting.Key carrying the build's
	// short VCS revision.
	vcsRevisionKey = "vcs.revision"
	// develVersionSentinel is the placeholder runtime/debug.ReadBuildInfo
	// emits for replace-directive builds and in-tree (`go run`) builds.
	develVersionSentinel = "(devel)"
)

// Version returns the redistransport module's release version.
//
// Resolution order: build-time ldflag injection > debug.ReadBuildInfo
// (when redistransport is a non-replaced dependency or the test main
// module) > devVersion. A leading "v" is stripped so labels join as bare
// semver.
func Version() string {
	return resolveVersion()
}

// BuildInfo describes redistransport build provenance.
//
// Surfaced as the mercure_redis_build_info Prometheus gauge (constant 1
// with these fields as labels). Lets operators identify a node lagging
// behind a transport rollout.
type BuildInfo struct {
	// Version is the resolved module version (see Version).
	Version string
	// Revision is the short VCS revision (≤ vcsRevisionShortLen chars)
	// embedded by `go build` via runtime/debug.ReadBuildInfo. Empty in
	// `go test` and `go run` builds, which do not embed VCS info.
	Revision string
	// GoVersion is the Go toolchain that built the binary
	// (debug.BuildInfo.GoVersion when available, else runtime.Version()).
	// These normally match, but BuildInfo's value reflects the build-time
	// toolchain even when the binary runs on a different runtime.
	GoVersion string
}

// LookupBuildInfo returns the module's resolved Version, its short VCS
// revision (empty in `go test` and `go run` builds) and the Go version that
// built the binary.
func LookupBuildInfo() BuildInfo {
	bi := BuildInfo{
		Version:   resolveVersion(),
		GoVersion: runtime.Version(),
	}

	di, ok := debug.ReadBuildInfo()
	if !ok {
		return bi
	}

	bi.GoVersion = di.GoVersion
	bi.Revision = revisionFromSettings(di.Settings)

	return bi
}

func resolveVersion() string {
	if isRealVersion(version) && version != devVersion {
		return strings.TrimPrefix(version, "v")
	}

	di, ok := debug.ReadBuildInfo()
	if !ok {
		return devVersion
	}

	return versionFromBuildInfo(di)
}

// versionFromBuildInfo returns the module version recorded in di: from Main
// when redistransport is the main module, else from its non-replaced Deps
// entry. A replaced dependency yields devVersion.
func versionFromBuildInfo(di *debug.BuildInfo) string {
	if di.Main.Path == moduleImportPath && isRealVersion(di.Main.Version) {
		return strings.TrimPrefix(di.Main.Version, "v")
	}

	for _, dep := range di.Deps {
		if dep.Path != moduleImportPath {
			continue
		}

		if dep.Replace != nil {
			continue
		}

		if isRealVersion(dep.Version) {
			return strings.TrimPrefix(dep.Version, "v")
		}
	}

	return devVersion
}

// revisionFromSettings extracts and truncates the vcs.revision build
// setting. Returns the empty string when no vcs.revision setting is
// present (`go test` and `go run` builds do not embed VCS info).
func revisionFromSettings(settings []debug.BuildSetting) string {
	for _, s := range settings {
		if s.Key != vcsRevisionKey {
			continue
		}

		rev := s.Value
		if len(rev) > vcsRevisionShortLen {
			rev = rev[:vcsRevisionShortLen]
		}

		return rev
	}

	return ""
}

// isRealVersion filters out the "(devel)" sentinel and the empty string
// that runtime/debug.ReadBuildInfo emits for in-tree builds and replaced
// dependencies.
func isRealVersion(v string) bool {
	return v != "" && v != develVersionSentinel
}
