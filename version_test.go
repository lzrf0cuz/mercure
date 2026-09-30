package mercure

import (
	"runtime/debug"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveVersion(t *testing.T) {
	t.Parallel()

	withMain := func(v string) *debug.BuildInfo { return &debug.BuildInfo{Main: debug.Module{Version: v}} }

	tests := []struct {
		name    string
		stamped string
		info    *debug.BuildInfo
		ok      bool
		want    string
	}{
		{"stamped, v stripped", "v0.0.1", withMain("v9.9.9"), true, "0.0.1"},
		{"stamped without v", "0.0.1", nil, false, "0.0.1"},
		{"unstamped falls back to the module version", devVersion, withMain("v1.2.3"), true, "1.2.3"},
		{"unstamped, devel module version", devVersion, withMain("(devel)"), true, devVersion},
		{"unstamped, empty module version", devVersion, withMain(""), true, devVersion},
		{"unstamped, no build info", devVersion, nil, false, devVersion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, resolveVersion(tt.stamped, tt.info, tt.ok))
		})
	}
}

func TestVersionMetricsCollectorInitialization(t *testing.T) {
	t.Parallel()

	v := AppVersionInfo{
		Version:         "1.0.0",
		BuildDate:       "2020-05-03T18:42:44Z",
		Commit:          "96ee2b9",
		GoVersion:       "go1.14.2",
		OS:              "linux",
		Architecture:    "amd64",
		UpstreamVersion: "v0.23.5",
	}

	c := v.NewMetricsCollector()

	m, err := c.GetMetricWith(map[string]string{
		"version":          v.Version,
		"built_at":         v.BuildDate,
		"commit":           v.Commit,
		"go_version":       v.GoVersion,
		"os":               v.OS,
		"architecture":     v.Architecture,
		"upstream_version": v.UpstreamVersion,
	})
	require.NoError(t, err)

	assertGaugeValue(t, 1.0, m)
}

// NewPrometheusMetrics registers mercure_version_info with the seven build labels, and a second
// instance on the same registry adopts it rather than panicking or duplicating the series.
func TestPrometheusMetricsRegistersVersionInfo(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()
	NewPrometheusMetrics(reg)
	NewPrometheusMetrics(reg)

	families, err := reg.Gather()
	require.NoError(t, err)

	var found bool

	for _, f := range families {
		if f.GetName() != "mercure_version_info" {
			continue
		}

		found = true

		require.Len(t, f.GetMetric(), 1)

		labels := map[string]string{}
		for _, l := range f.GetMetric()[0].GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}

		want := currentAppVersion()
		assert.Equal(t, map[string]string{
			"version":          want.Version,
			"built_at":         want.BuildDate,
			"commit":           want.Commit,
			"go_version":       want.GoVersion,
			"os":               want.OS,
			"architecture":     want.Architecture,
			"upstream_version": want.UpstreamVersion,
		}, labels)
		assert.InDelta(t, 1.0, f.GetMetric()[0].GetGauge().GetValue(), 0)
	}

	assert.True(t, found, "mercure_version_info must be registered")
}

// upstream_version is the only label the caller stamps besides version, so it has to reach the
// scraped series. Not parallel: it sets a package variable, which the parallel tests only read
// after every sequential test has finished.
func TestUpstreamVersionLabel(t *testing.T) {
	old := upstreamVersion
	upstreamVersion = "v9.8.7-test"

	t.Cleanup(func() { upstreamVersion = old })

	assert.Equal(t, "v9.8.7-test", currentAppVersion().UpstreamVersion)

	reg := prometheus.NewPedanticRegistry()
	NewPrometheusMetrics(reg)

	families, err := reg.Gather()
	require.NoError(t, err)

	var got string

	for _, f := range families {
		if f.GetName() != "mercure_version_info" {
			continue
		}

		for _, l := range f.GetMetric()[0].GetLabel() {
			if l.GetName() == "upstream_version" {
				got = l.GetValue()
			}
		}
	}

	assert.Equal(t, "v9.8.7-test", got)
}
