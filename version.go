package mercure

import (
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// devVersion is the placeholder version when no `-X github.com/dunglas/mercure.version=…`
// was supplied (in-tree `go test`, ad-hoc `go run`).
const devVersion = "dev"

// The ldflag targets, set at link time by the Taskfile and the Dockerfile:
//
//	-X 'github.com/dunglas/mercure.version=<fork version>'
//	-X 'github.com/dunglas/mercure.upstreamVersion=<dunglas/mercure tag>'
//
// A build without these stamps, such as a plain `go build` in caddy/mercure, reports the Go
// module's VCS pseudo-version. Builds made through the Taskfile or the Dockerfile are stamped.
var (
	version         = devVersion
	upstreamVersion = ""
)

// AppVersionInfo describes the running binary. It backs the mercure_version_info metric.
type AppVersionInfo struct {
	Version string
	// BuildDate and Commit are not stamped by the fork's build pipeline: they stay empty and
	// only keep the built_at and commit labels of mercure_version_info.
	BuildDate    string
	Commit       string
	GoVersion    string
	OS           string
	Architecture string
	// UpstreamVersion is the dunglas/mercure tag this fork was built against. Empty when built
	// outside the fork's build pipeline.
	UpstreamVersion string
}

// currentAppVersion returns the version info of the running binary.
func currentAppVersion() AppVersionInfo {
	info, ok := debug.ReadBuildInfo()

	return AppVersionInfo{
		Version:         resolveVersion(version, info, ok),
		GoVersion:       runtime.Version(),
		OS:              runtime.GOOS,
		Architecture:    runtime.GOARCH,
		UpstreamVersion: upstreamVersion,
	}
}

// resolveVersion returns the stamped version, or when none was stamped the main module's
// version from the build info, with any leading "v" removed.
func resolveVersion(stamped string, info *debug.BuildInfo, ok bool) string {
	if stamped == devVersion && ok && info.Main.Version != "(devel)" && info.Main.Version != "" {
		stamped = info.Main.Version
	}

	return strings.TrimPrefix(stamped, "v")
}

// NewMetricsCollector returns the mercure_version_info gauge: a constant 1 labeled with the
// build fields.
func (v *AppVersionInfo) NewMetricsCollector() *prometheus.GaugeVec {
	labels := map[string]string{
		"version":          v.Version,
		"built_at":         v.BuildDate,
		"commit":           v.Commit,
		"go_version":       v.GoVersion,
		"os":               v.OS,
		"architecture":     v.Architecture,
		"upstream_version": v.UpstreamVersion,
	}

	labelNames := make([]string, 0, len(labels))
	for n := range labels {
		labelNames = append(labelNames, n)
	}

	buildInfo := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mercure_version_info",
			Help: "A metric with a constant '1' value labeled by different build stats fields.",
		},
		labelNames,
	)
	buildInfo.With(labels).Set(1)

	return buildInfo
}
