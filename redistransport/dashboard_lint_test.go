package redistransport

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Dashboard-lint invariants enforced at `go test` time so the bundled
// Grafana dashboard JSON can't silently regress. Runs under `task rt:test`
// (Tier 1 CI), no external tooling required. General Grafana best-practice
// checks (templated datasource, $job/$instance presence, $__rate_interval,
// uneditable, etc.) are gated separately by `task rt:lint:dashboard` via
// grafana/dashboard-linter — that binary IS external tooling.
//
// Six invariants live here — domain-specific contracts that
// dashboard-linter doesn't know about. Numbered in file-order so
// CHANGELOG/CONTRIBUTING references stay monotonic.
//
// Invariant 1 — legendFormat ↔ aggregator by-clause (backend_type):
//
//	Every target whose legendFormat contains `{{backend_type}}` MUST
//	have its expression aggregators preserve the backend_type label.
//	An aggregator drops backend_type when (a) it has no by/without
//	clause, (b) its `by` clause omits backend_type, or (c) its
//	`without` clause includes backend_type. Without this, Prometheus
//	renders legends with a leading-space `" foo"` and collapses
//	multi-backend deployments into a single value.
//
// Invariant 2 — guarded template variables present and defaulted:
//
//	Template variables `job`, `instance`, and `backend_type` MUST
//	exist AND have `current: {selected: false, text: "All",
//	value: "$__all"}`. Grafana UI save round-trips can rewrite this
//	to whatever was selected at save time, silently breaking operator
//	bookmarks. Removing a guarded variable entirely is also flagged.
//	Runs on BOTH dashboards (hub.json's guarded set excludes
//	`backend_type`, which is transport-only).
//
// Invariant 3 — backend_type selector on hub-side queries:
//
//	Every target whose expr references `mercure_redis_*` metrics MUST
//	include `backend_type=~"$backend_type"` in its selector.
//	dashboard-linter doesn't know about the custom $backend_type
//	variable — this is the only gate that catches a hand-edit
//	dropping the multi-tenant filter while leaving the
//	`{{backend_type}}` legend in place.
//
// Invariant 4 — .lint exclusion drift detection:
//
//	Every panel-name string in grafana/dashboards/.lint MUST match a
//	real panel in transport.json. dashboard-linter silently no-ops on
//	stale exclusion entries — without this gate, renaming a redis_exporter
//	panel breaks the exclusion contract silently.
//
// Invariant 5 — legendFormat ↔ aggregator by-clause (instance):
//
//	Analogous to Invariant 1 but for the `instance` label. Gates on
//	`{{instance}}` legend presence rather than universal application
//	because the dashboards intentionally mix per-instance timeseries
//	with fleet-aggregate stat/histogram/piechart panels. Runs on BOTH
//	transport.json and hub.json. See CONTRIBUTING.md § Per-instance vs
//	fleet-aggregate panel design for the rationale.
//
// Invariant 6 — `{{instance}}` legend prefix shape:
//
//	Every legendFormat containing `{{instance}}` MUST start with
//	`{{instance}}` followed by EOL, space, or `|`. Catches
//	`{{backend_type}} {{instance}} shard {{shard}}` regressions
//	(instance buried in the middle) and `{{instance}}garbage` shapes
//	(no separator). Runs on BOTH dashboards.
//
// If a future change intentionally violates an invariant, update the
// test rather than deleting it.

const (
	transportDashboardPath = "grafana/dashboards/transport.json"
	// hubDashboardPath traverses up to the root Go module so the
	// `instance`-pattern invariants cover both dashboards from one
	// test target (root has no Go test that could load it without
	// re-implementing the unmarshalling boilerplate).
	hubDashboardPath = "../grafana/dashboards/hub.json"
)

// labelDroppingAggregators are PromQL aggregators that drop labels by
// default (group everything when invoked without a by/without clause).
// `topk` / `bottomk` are deliberately excluded — they preserve the
// labels of the individual samples they pick, so a `{{backend_type}}`
// legend renders correctly even with a bare `topk(5, foo)`.
var labelDroppingAggregators = map[string]struct{}{
	"sum":          {},
	"min":          {},
	"max":          {},
	"count":        {},
	"count_values": {},
	"avg":          {},
	"stddev":       {},
	"stdvar":       {},
	"group":        {},
	"quantile":     {},
}

// aggregatorRe matches a PromQL aggregator invocation in either form:
//
//	sum(...)                  — captures: ("sum", "",       "")
//	sum by (a, b) (...)       — captures: ("sum", "by",     "a, b")
//	sum without (a, b) (...)  — captures: ("sum", "without","a, b")
//
// `\s+` (not `\s*`) between the aggregator and the by/without modifier is
// intentional: PromQL's lexer treats identifiers as one token, so `sumby`
// lexes as a single identifier and is NOT a valid `sum by` form — there
// has to be whitespace between the two keywords for the parser to see
// them as separate tokens. Accepting `\s*` here would only allow false
// matches against syntactically invalid input that no real dashboard
// could carry.
//
// The match also fires on non-aggregator function calls like `rate(`
// or `histogram_quantile(`; the labelDroppingAggregators set filters
// those out by name.
var aggregatorRe = regexp.MustCompile(
	`\b(\w+)\b(?:\s+(by|without)\s*\(([^)]*)\))?\s*\(`,
)

var backendTypeSelectorRe = regexp.MustCompile(
	`backend_type\s*=~\s*"\$backend_type"`,
)

var hubSideMetricRe = regexp.MustCompile(`\bmercure_redis_\w+`)

type dashboardTarget struct {
	Expr         string `json:"expr"`
	LegendFormat string `json:"legendFormat"`
	RefID        string `json:"refId"`
}

type dashboardPanel struct {
	ID          int               `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Targets     []dashboardTarget `json:"targets"`
	Panels      []dashboardPanel  `json:"panels"`
}

type dashboardVariable struct {
	Name    string         `json:"name"`
	Current map[string]any `json:"current"`
}

type dashboardTemplating struct {
	List []dashboardVariable `json:"list"`
}

type dashboard struct {
	Panels     []dashboardPanel    `json:"panels"`
	Templating dashboardTemplating `json:"templating"`
}

// loadTransportDashboard reads the dashboard JSON. Resolution is anchored
// to the package directory via resolvePackagePath, so the test works under
// any `go test` invocation cwd including a pre-compiled binary run from
// elsewhere.
func loadTransportDashboard(t *testing.T) *dashboard {
	t.Helper()

	return loadDashboardJSON(t, resolvePackagePath(t, transportDashboardPath))
}

// loadHubDashboard reads hub.json from the root module via the same
// resolvePackagePath anchor as the transport loader. Earlier inline
// reimplementations used different shapes (hub stripped `../`, transport
// added `redistransport/`) which happened to resolve to the same files.
func loadHubDashboard(t *testing.T) *dashboard {
	t.Helper()

	return loadDashboardJSON(t, resolvePackagePath(t, hubDashboardPath))
}

func loadDashboardJSON(t *testing.T, path string) *dashboard {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err, "load dashboard JSON from %q", path)

	var d dashboard
	require.NoError(t, json.Unmarshal(raw, &d), "parse dashboard JSON from %q", path)
	require.NotEmpty(t, d.Panels, "dashboard %q has no panels — schema drift?", path)
	require.NotEmpty(t, d.Templating.List, "dashboard %q has no template variables", path)

	return &d
}

// walkTargets visits every (panel, target) pair in the panel tree.
// Recurses into nested panels (collapsed rows have children under
// panel.panels).
func walkTargets(panels []dashboardPanel, visit func(panel dashboardPanel, target dashboardTarget)) {
	for _, p := range panels {
		for _, target := range p.Targets {
			visit(p, target)
		}

		walkTargets(p.Panels, visit)
	}
}

// dropsLabel reports whether expr contains any aggregator that strips
// the given label. An aggregator drops a label when (a) it has no
// by/without clause (bare `sum(x)` drops everything), (b) its `by`
// clause omits the label, or (c) its `without` clause includes the
// label. `topk`/`bottomk` are NOT in labelDroppingAggregators because
// they preserve the labels of the picked samples.
func dropsLabel(expr, label string) bool {
	for _, m := range aggregatorRe.FindAllStringSubmatch(expr, -1) {
		name, clauseType, clauseLabels := m[1], m[2], m[3]
		if _, isAgg := labelDroppingAggregators[name]; !isAgg {
			continue
		}

		switch clauseType {
		case "":
			return true
		case "by":
			if !clauseContainsLabel(clauseLabels, label) {
				return true
			}
		case "without":
			if clauseContainsLabel(clauseLabels, label) {
				return true
			}
		}
	}

	return false
}

// dropsBackendType is the backend_type-specific shorthand for dropsLabel.
// New label invariants should call dropsLabel directly.
func dropsBackendType(expr string) bool {
	return dropsLabel(expr, "backend_type")
}

// clauseContainsLabel reports whether a `by/without (A, B, C)` body
// contains the given label as a whole token.
func clauseContainsLabel(clauseLabels, label string) bool {
	for part := range strings.SplitSeq(clauseLabels, ",") {
		if strings.TrimSpace(part) == label {
			return true
		}
	}

	return false
}

// TestDashboardLegendByClauseInvariant: every {{backend_type}} legend
// pairs with a query whose aggregators preserve backend_type. Also
// asserts a lower-bound floor on the {{backend_type}}-bearing legend
// count so wholesale-stripping doesn't pass vacuously.
func TestDashboardLegendByClauseInvariant(t *testing.T) {
	t.Parallel()

	d := loadTransportDashboard(t)

	var (
		violations         []string
		backendTypeLegends int
	)

	walkTargets(d.Panels, func(panel dashboardPanel, target dashboardTarget) {
		if !strings.Contains(target.LegendFormat, "{{backend_type}}") {
			return
		}

		backendTypeLegends++

		if dropsBackendType(target.Expr) {
			violations = append(violations, fmt.Sprintf(
				"panel %d %q refId %s: legend %q but aggregator drops backend_type — expr: %s",
				panel.ID, panel.Title, target.RefID, target.LegendFormat, target.Expr,
			))
		}
	})

	assert.Empty(t, violations,
		"add `by (backend_type, …)` to the aggregator or drop {{backend_type}} from the legend")

	// Current dashboard has ~53 {{backend_type}} legends; floor at 35
	// to allow legitimate panel removal but trip if a third disappear.
	assert.GreaterOrEqual(t, backendTypeLegends, 35,
		"{{backend_type}} legend count fell below floor — Invariant 1 is now vacuous; restore legends or update the floor with concrete justification")
}

// TestDashboardVariableDefaults: bookmark-stable $__all defaults on
// every guarded template variable, AND every guarded variable is
// present (removing one would otherwise pass vacuously). Runs on both
// dashboards; hub.json's guarded set excludes `backend_type` (which is
// transport-specific). The `datasource` template variable is gated
// separately by TestDashboardDatasourceVariablePresent because its
// `current` shape differs ({text: "Prometheus", value: "prometheus"}
// instead of $__all).
func TestDashboardVariableDefaults(t *testing.T) {
	t.Parallel()

	expectedAll := map[string]any{
		"selected": false,
		"text":     "All",
		"value":    "$__all",
	}

	for _, dc := range guardedVarDashboards() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			d := dc.load(t)

			seen := make(map[string]bool, len(dc.guarded))
			for _, v := range d.Templating.List {
				if _, guarded := dc.guarded[v.Name]; !guarded {
					continue
				}

				seen[v.Name] = true

				assert.Equal(t, expectedAll, v.Current,
					"template variable %q `current` must be the $__all default — Grafana UI save rewrites this; see CONTRIBUTING § Grafana dashboard JSON edits", v.Name)
			}

			for name := range dc.guarded {
				assert.True(t, seen[name],
					"guarded template variable %q is missing from templating.list — Invariant 2 would be vacuous; restore it or update guardedVarDashboards with concrete justification", name)
			}
		})
	}
}

// guardedVarDashboard pairs a dashboard loader with its per-dashboard
// guarded variable set. transport.json includes `backend_type`;
// hub.json doesn't (backend_type is transport-only).
type guardedVarDashboard struct {
	name    string
	load    func(*testing.T) *dashboard
	guarded map[string]struct{}
}

func guardedVarDashboards() []guardedVarDashboard {
	return []guardedVarDashboard{
		{
			"transport.json",
			loadTransportDashboard,
			map[string]struct{}{"job": {}, "instance": {}, "backend_type": {}},
		},
		{
			"hub.json",
			loadHubDashboard,
			map[string]struct{}{"job": {}, "instance": {}},
		},
	}
}

// datasourceVarStatus reports whether a dashboard has a properly-shaped
// `datasource` template variable. Returns the variable + ok=true when
// present with value="prometheus", else nil+false. Extracted so
// TestDatasourceVarStatus_PositiveControl can exercise the contract
// against synthetic inputs.
func datasourceVarStatus(d *dashboard) (*dashboardVariable, bool) {
	for i := range d.Templating.List {
		v := &d.Templating.List[i]
		if v.Name != "datasource" {
			continue
		}

		if v.Current["value"] == "prometheus" {
			return v, true
		}

		return v, false
	}

	return nil, false
}

// TestDashboardDatasourceVariablePresent gates the `datasource`
// template variable both dashboards rely on (so $datasource-templated
// panel uids actually resolve at import time). Distinct from
// TestDashboardVariableDefaults because its `current` shape carries a
// concrete datasource value, not the $__all sentinel.
func TestDashboardDatasourceVariablePresent(t *testing.T) {
	t.Parallel()

	for _, dc := range guardedVarDashboards() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			d := dc.load(t)

			v, ok := datasourceVarStatus(d)
			require.NotNil(t, v, "datasource template variable missing — panels reference ${datasource} uids that won't resolve")
			assert.True(t, ok, "datasource template variable `current.value` must be \"prometheus\"; Grafana UI save can rewrite this if an operator picks a non-Prometheus datasource")
		})
	}
}

// TestDatasourceVarStatus_PositiveControl proves datasourceVarStatus
// returns the right verdict on synthetic dashboards. Without this, the
// outer presence check could pass vacuously today and silently start
// missing regressions if the helper is later refactored.
func TestDatasourceVarStatus_PositiveControl(t *testing.T) {
	t.Parallel()

	mkVar := func(name, value string) dashboardVariable {
		return dashboardVariable{
			Name:    name,
			Current: map[string]any{"value": value},
		}
	}

	cases := []struct {
		name      string
		vars      []dashboardVariable
		wantFound bool
		wantOK    bool
	}{
		{
			"datasource present with prometheus value",
			[]dashboardVariable{mkVar("job", "$__all"), mkVar("datasource", "prometheus")},
			true,
			true,
		},
		{
			"datasource missing entirely",
			[]dashboardVariable{mkVar("job", "$__all"), mkVar("instance", "$__all")},
			false,
			false,
		},
		{
			"datasource present with non-prometheus value",
			[]dashboardVariable{mkVar("datasource", "loki")},
			true,
			false,
		},
		{
			"empty template list",
			nil,
			false,
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := &dashboard{Templating: dashboardTemplating{List: tc.vars}}

			v, ok := datasourceVarStatus(d)

			if tc.wantFound {
				require.NotNil(t, v, "expected datasource var to be found")
			} else {
				assert.Nil(t, v, "expected datasource var to be absent")
			}

			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

// TestInstanceLegendCountSentinel locks the exact {{instance}}-bearing
// legend count per dashboard. Literal-canary pattern: when panel
// inventory changes, the diff trips this sentinel AND the floor in
// TestDashboardLegendByClauseInvariant_Instance, forcing the author
// to update both with concrete justification rather than silently
// drifting toward a vacuous floor.
func TestInstanceLegendCountSentinel(t *testing.T) {
	t.Parallel()

	expected := map[string]int{
		// transport.json delta from the prior 27 → 36 (+9) covers four
		// new per-instance panels:
		//   - panels 41, 42, 43 (consumer_groups, history_window,
		//     dispatch_zero_match rate timeseries): +1 legend each = +3
		//   - panel 44 (Per-instance p99 outliers): +6 legends across
		//     dispatch_duration, xreadgroup_latency, history_replay_duration,
		//     publish_duration, add_subscriber_duration, remove_subscriber_duration.
		// 36 → 37 (+1): panel 45 (History replay truncated rate), legend
		// "{{instance}} | {{backend_type}} truncated/sec".
		// 37 → 39 (+2): panels 111 (Subscribers shed by admission gate, legend
		// "{{instance}} | {{backend_type}} {{reason}}") and 112 (Presence reads
		// capped, legend "{{instance}} | {{backend_type}} capped/sec") — the
		// previously-undashboarded admission-rejected + presence-read-capped metrics.
		// 39 → 40 (+1): the Operational errors panel gained a target for
		// history_cursor_rejected_total, legend
		// "{{instance}} | {{backend_type}} history_cursor_rejected".
		"transport.json": 40,
		"hub.json":       26,
	}

	for _, dc := range instanceDashboards() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			d := dc.load(t)

			count := 0

			walkTargets(d.Panels, func(_ dashboardPanel, target dashboardTarget) {
				if strings.Contains(target.LegendFormat, "{{instance}}") {
					count++
				}
			})

			assert.Equal(t, expected[dc.name], count,
				"exact {{instance}}-bearing legend count for %s changed — update BOTH this sentinel AND the floor in TestDashboardLegendByClauseInvariant_Instance with concrete justification", dc.name)
		})
	}

	// Positive control: any text-match gate needs a known-bad input
	// proving it fires. Here we synthesize a mutated dashboard with one
	// less {{instance}} legend and assert the counter returns
	// expected-1. Without this control, a future regression that breaks
	// the counting walk would silently match the sentinel.
	t.Run("counter logic detects deliberate corruption", func(t *testing.T) {
		t.Parallel()

		for _, dc := range instanceDashboards() {
			d := dc.load(t)

			countFunc := func(d *dashboard) int {
				c := 0

				walkTargets(d.Panels, func(_ dashboardPanel, target dashboardTarget) {
					if strings.Contains(target.LegendFormat, "{{instance}}") {
						c++
					}
				})

				return c
			}

			baseline := countFunc(d)
			assert.Equal(t, expected[dc.name], baseline,
				"baseline must equal sentinel for the corruption assertion to be meaningful")

			// Mutate: strip the {{instance}} substring from the first
			// matching target. This proves the counter logic responds
			// to a deliberate change.
			mutated := false

			for i := range d.Panels {
				if mutateFirstInstanceLegend(&d.Panels[i]) {
					mutated = true

					break
				}
			}

			require.True(t, mutated, "test must find at least one {{instance}} legend to mutate")

			assert.Equal(t, baseline-1, countFunc(d),
				"after stripping ONE {{instance}} legend, count must drop by exactly 1 — if equal to baseline, the counter is vacuous")
		}
	})
}

// mutateFirstInstanceLegend strips the literal "{{instance}}" substring
// from the first target whose legend contains it, walking panels and
// nested panels in order. Returns true if a mutation was applied.
// Used only by the TestInstanceLegendCountSentinel positive control.
func mutateFirstInstanceLegend(p *dashboardPanel) bool {
	for i := range p.Targets {
		if strings.Contains(p.Targets[i].LegendFormat, "{{instance}}") {
			p.Targets[i].LegendFormat = strings.Replace(
				p.Targets[i].LegendFormat, "{{instance}}", "", 1,
			)

			return true
		}
	}

	for i := range p.Panels {
		if mutateFirstInstanceLegend(&p.Panels[i]) {
			return true
		}
	}

	return false
}

// walkPanels visits every panel (including nested) for description /
// metadata checks. Distinct from walkTargets because the panel-level
// fields (title, description) aren't carried on dashboardTarget.
func walkPanels(panels []dashboardPanel, visit func(panel dashboardPanel)) {
	for _, p := range panels {
		visit(p)
		walkPanels(p.Panels, visit)
	}
}

// panelRefRe matches the “ `<Name>` panel “ pattern operators use
// when one description points at a sibling panel for cross-correlation.
// Single-word backticked tokens (metric names, config keys, CLI tasks)
// are not implicitly excluded; the trailing literal "panel"/"panel's"
// keyword is what distinguishes a panel reference. See
// TestPanelRefRe_PositiveControl for the regex contract.
var panelRefRe = regexp.MustCompile("`([^`]+)`\\s+panel(?:'s)?\\b")

// TestPanelRefRe_PositiveControl proves panelRefRe fires on the shapes
// TestDashboardPanelNameReferences claims to catch. Any regex-based
// gate needs a known-bad positive control proving it would have fired
// on the bug class it claims to prevent.
func TestPanelRefRe_PositiveControl(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		desc      string
		wantMatch string // first capture group, "" if no match expected
	}{
		{
			"name + literal panel keyword",
			"pair with the `Disconnects by reason (rate/sec)` panel for triage",
			"Disconnects by reason (rate/sec)",
		},
		{
			"name + possessive panel's",
			"see the `Write+flush rate by outcome` panel's outcome=failed series",
			"Write+flush rate by outcome",
		},
		{
			"multi-word with parens",
			"compare against the `Subscribers connected (per instance)` panel",
			"Subscribers connected (per instance)",
		},
		{
			"backticked metric without panel keyword does not match",
			"see metric `mercure_subscribers_connected` for raw values",
			"",
		},
		{
			"backticked path without panel keyword does not match",
			"edit `grafana/dashboards/hub.json` and re-provision",
			"",
		},
		{
			"backticked CLI task without panel keyword does not match",
			"run `task dashboard:lint` to validate",
			"",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := panelRefRe.FindStringSubmatch(tc.desc)

			if tc.wantMatch == "" {
				assert.Empty(t, m, "expected no match, got: %v", m)

				return
			}

			require.NotEmpty(t, m, "expected match for desc: %s", tc.desc)
			assert.Equal(t, tc.wantMatch, m[1])
		})
	}
}

// TestDashboardPanelNameReferences: any backticked panel-name
// reference inside a panel description MUST resolve to a real panel
// title. Renaming a panel without updating cross-references leaves the
// runbook trail stale; this test trips the diff so the author notices.
func TestDashboardPanelNameReferences(t *testing.T) {
	t.Parallel()

	for _, dc := range guardedVarDashboards() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			d := dc.load(t)

			titles := map[string]bool{}

			walkPanels(d.Panels, func(p dashboardPanel) {
				if p.Title != "" {
					titles[p.Title] = true
				}
			})

			var missing []string

			walkPanels(d.Panels, func(p dashboardPanel) {
				if p.Description == "" {
					return
				}

				for _, m := range panelRefRe.FindAllStringSubmatch(p.Description, -1) {
					name := m[1]
					if !titles[name] {
						missing = append(missing, fmt.Sprintf(
							"panel %d %q description references `%s` panel — no such panel title in this dashboard",
							p.ID, p.Title, name,
						))
					}
				}
			})

			assert.Empty(t, missing,
				"rename the dangling reference, fix the typo, or pluralize/dequalify the wording so it no longer matches the `<name>` panel pattern")
		})
	}
}

// TestDashboardHubExprsHaveBackendTypeSelector: every hub-side target
// (one whose expr references a mercure_redis_* metric) must include
// `backend_type=~"$backend_type"` in its selector. Catches the case
// where someone strips the multi-tenant filter but leaves the
// {{backend_type}} legend intact.
func TestDashboardHubExprsHaveBackendTypeSelector(t *testing.T) {
	t.Parallel()

	d := loadTransportDashboard(t)

	var missing []string

	walkTargets(d.Panels, func(panel dashboardPanel, target dashboardTarget) {
		if !hubSideMetricRe.MatchString(target.Expr) {
			return
		}

		if !backendTypeSelectorRe.MatchString(target.Expr) {
			missing = append(missing, fmt.Sprintf(
				`panel %d %q refId %s: hub expr missing backend_type=~"$backend_type" — expr: %s`,
				panel.ID, panel.Title, target.RefID, target.Expr,
			))
		}
	})

	assert.Empty(t, missing,
		`hub-side queries must filter by $backend_type so the variable dropdown actually scopes the view`)
}

// TestDashboardLintExclusionsMatchRealPanels: every panel name in
// grafana/dashboards/.lint must match a real panel in transport.json.
// dashboard-linter silently no-ops on stale exclusion entries — this
// test catches rename drift.
func TestDashboardLintExclusionsMatchRealPanels(t *testing.T) {
	t.Parallel()

	d := loadTransportDashboard(t)

	titles := map[string]bool{}

	walkTargets(d.Panels, func(panel dashboardPanel, _ dashboardTarget) {
		titles[panel.Title] = true
	})

	lintPath := filepath.Join("grafana", "dashboards", ".lint")
	if _, err := os.Stat(lintPath); err != nil {
		lintPath = filepath.Join("redistransport", lintPath)
	}

	raw, err := os.ReadFile(lintPath)
	require.NoError(t, err, "load .lint from %q", lintPath)

	// .lint is YAML, but every panel reference is a single line of
	// the form `<indent>panel: <name>`. We don't need a full YAML
	// parser to extract panel names for drift detection.
	panelLineRe := regexp.MustCompile(`(?m)^\s*panel:\s*(.+?)\s*$`)
	matches := panelLineRe.FindAllStringSubmatch(string(raw), -1)
	require.NotEmpty(t, matches, ".lint has no `panel:` entries — schema drift?")

	for _, m := range matches {
		name := strings.Trim(m[1], `"'`)
		assert.True(t, titles[name],
			`.lint references panel %q which does not exist in transport.json — rename drift? Either restore the panel name or update .lint`, name)
	}
}

// TestDropsBackendType_TableDriven exercises the dropsBackendType
// helper against a fixed set of expressions. If the regex regresses,
// at least one case will fail loudly — catching the class of bug that
// led to the original Invariant-1 check being vacuous (the old regex
// only matched postfix-by forms like `sum(`, missing every prefix-by
// `sum by (` form the dashboard actually uses).
func TestDropsBackendType_TableDriven(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		expr    string
		violate bool
	}{
		{
			"bare metric read with no aggregator",
			`mercure_redis_healthy{job=~"$job",backend_type=~"$backend_type"}`,
			false,
		},
		{
			"prefix-by includes backend_type",
			`sum by (backend_type, le) (rate(foo[5m]))`,
			false,
		},
		{
			"prefix-by missing backend_type",
			`sum by (instance, le) (rate(foo[5m]))`,
			true,
		},
		{
			"prefix-without omits backend_type",
			`sum without (le) (rate(foo[5m]))`,
			false,
		},
		{
			"prefix-without includes backend_type drops it",
			`sum without (backend_type) (rate(foo[5m]))`,
			true,
		},
		{
			"aggregator with no clause drops all labels",
			`sum(mercure_redis_publish_total)`,
			true,
		},
		{
			"histogram_quantile wraps sum by backend_type",
			`histogram_quantile(0.99, sum by (backend_type, le) (rate(foo[5m])))`,
			false,
		},
		{
			"spaced by clause still matches",
			`sum by ( backend_type ) (foo)`,
			false,
		},
		{
			"count with no clause drops labels",
			`count(mercure_redis_consumer_lag == -1) or vector(0)`,
			true,
		},
		{
			"topk preserves labels of picked samples",
			`topk(5, mercure_redis_shard_subscribers{backend_type=~"$backend_type"})`,
			false,
		},
		{
			"rate() is not an aggregator",
			`rate(mercure_redis_publish_total[5m])`,
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.violate, dropsBackendType(tc.expr),
				"expr: %s", tc.expr)
		})
	}
}

// instanceDashboard pairs a dashboard loader with its {{instance}}
// legend-count floor. Both per-instance invariants (Invariant 5 and 6)
// iterate the same pair, so the slice lives in one helper.
type instanceDashboard struct {
	name        string
	load        func(*testing.T) *dashboard
	legendFloor int
}

// instanceDashboards returns the dashboards exercised by the per-instance
// invariants. Floors are tuned per dashboard: transport timeseries panels
// carry `{{instance}}` (~28; floor 25 catches a quarter disappearing);
// hub mixes timeseries + multi-row stats (~26; floor 18 catches a third
// disappearing). Stat/histogram/piechart panels intentionally omit
// `{{instance}}` so an "every aggregator preserves instance" rule would
// over-fire.
func instanceDashboards() []instanceDashboard {
	return []instanceDashboard{
		{"transport.json", loadTransportDashboard, 25},
		{"hub.json", loadHubDashboard, 18},
	}
}

// TestDashboardLegendByClauseInvariant_Instance is the per-instance
// analogue of Invariant 1: every target whose legendFormat contains
// `{{instance}}` MUST have its expression aggregators preserve the
// `instance` label. Without this, Prometheus renders legends with a
// leading-space `" foo"` (empty `{{instance}}` placeholder), and a
// per-replica timeseries silently collapses to fleet-aggregate.
func TestDashboardLegendByClauseInvariant_Instance(t *testing.T) {
	t.Parallel()

	for _, dc := range instanceDashboards() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			d := dc.load(t)

			var (
				violations      []string
				instanceLegends int
			)

			walkTargets(d.Panels, func(panel dashboardPanel, target dashboardTarget) {
				if !strings.Contains(target.LegendFormat, "{{instance}}") {
					return
				}

				instanceLegends++

				if dropsLabel(target.Expr, "instance") {
					violations = append(violations, fmt.Sprintf(
						"panel %d %q refId %s: legend %q but aggregator drops instance — expr: %s",
						panel.ID, panel.Title, target.RefID, target.LegendFormat, target.Expr,
					))
				}
			})

			assert.Empty(t, violations,
				"add `by (instance, …)` to the aggregator or drop {{instance}} from the legend")

			assert.GreaterOrEqual(t, instanceLegends, dc.legendFloor,
				"{{instance}} legend count fell below floor — instance invariant is now vacuous; restore legends or update the floor with concrete justification")
		})
	}
}

// TestDropsLabel_TableDriven exercises the generalized helper across
// BOTH labels — backend_type and instance. Catches the parameterized
// vacuous-regex pattern: a regression where the helper silently breaks
// one label's detection while the other continues to work.
func TestDropsLabel_TableDriven(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		expr    string
		label   string
		violate bool
	}{
		{
			"prefix-by includes instance",
			`sum by (instance, le) (rate(foo[5m]))`,
			"instance",
			false,
		},
		{
			"prefix-by missing instance",
			`sum by (backend_type, le) (rate(foo[5m]))`,
			"instance",
			true,
		},
		{
			"prefix-by includes both labels",
			`sum by (instance, backend_type, le) (rate(foo[5m]))`,
			"instance",
			false,
		},
		{
			"bare aggregator drops instance",
			`sum(mercure_subscribers_connected{job=~"$job"})`,
			"instance",
			true,
		},
		{
			"prefix-without omits instance",
			`sum without (le) (rate(foo[5m]))`,
			"instance",
			false,
		},
		{
			"prefix-without includes instance drops it",
			`sum without (instance) (rate(foo[5m]))`,
			"instance",
			true,
		},
		{
			"histogram_quantile fleet-aggregate drops instance",
			`histogram_quantile(0.99, sum by (backend_type, le) (rate(foo[5m])))`,
			"instance",
			true,
		},
		{
			"histogram_quantile per-instance preserves instance",
			`histogram_quantile(0.99, sum by (instance, backend_type, le) (rate(foo[5m])))`,
			"instance",
			false,
		},
		{
			"topk preserves labels of picked samples (instance)",
			`topk(5, mercure_redis_shard_subscribers{instance=~"$instance"})`,
			"instance",
			false,
		},
		{
			"bare-metric read with instance selector preserves instance",
			`mercure_subscribers_connected{job=~"$job",instance=~"$instance"}`,
			"instance",
			false,
		},
		{
			"count with fallback or vector(0) drops labels",
			`count(mercure_redis_consumer_lag == -1) or vector(0)`,
			"instance",
			true,
		},
		// Cross-label sanity: same expressions, opposite label, opposite
		// verdict — proves the helper actually parameterizes over `label`
		// instead of hard-coding one value.
		{
			"by (instance, …) drops backend_type",
			`sum by (instance, le) (rate(foo[5m]))`,
			"backend_type",
			true,
		},
		{
			"by (backend_type, …) preserves backend_type",
			`sum by (backend_type, le) (rate(foo[5m]))`,
			"backend_type",
			false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.violate, dropsLabel(tc.expr, tc.label),
				"label=%s expr: %s", tc.label, tc.expr)
		})
	}
}

// instancePrefix is the literal token Invariant 6 requires as a
// legendFormat prefix. Extracted so the separator-shape check below
// reads len(instancePrefix) rather than the magic number 12.
const instancePrefix = "{{instance}}"

// legendInstancePrefixOK reports whether a legend satisfies the
// Invariant 6 prefix-shape contract: the legend starts with
// `{{instance}}` AND (if anything follows) the next character is a
// space or pipe separator. Extracted as a helper so
// TestLegendInstancePrefixOK_PositiveControl can exercise the contract
// directly with known-good and known-bad inputs.
func legendInstancePrefixOK(legend string) bool {
	if !strings.HasPrefix(legend, instancePrefix) {
		return false
	}

	rest := legend[len(instancePrefix):]
	if rest == "" {
		return true
	}

	// Rune-decode rather than byte-index so the comparison stays correct
	// if the allowed-separator set is ever expanded beyond ASCII.
	r, _ := utf8.DecodeRuneInString(rest)

	return r == ' ' || r == '|'
}

// TestDashboardInstanceLegendPrefixShape: when a legend contains
// `{{instance}}`, the placeholder MUST be the first token AND the
// next character (if any) must be a space or `|` separator. Catches
// the `{{backend_type}} {{instance}} shard {{shard}}` shape regression
// (instance buried in the middle), the `foo {{instance}}` shape
// (instance at the end), AND the `{{instance}}garbage` shape (no
// separator) which renders as a concatenated mess.
func TestDashboardInstanceLegendPrefixShape(t *testing.T) {
	t.Parallel()

	for _, dc := range instanceDashboards() {
		t.Run(dc.name, func(t *testing.T) {
			t.Parallel()

			d := dc.load(t)

			var violations []string

			walkTargets(d.Panels, func(panel dashboardPanel, target dashboardTarget) {
				legend := target.LegendFormat
				if !strings.Contains(legend, instancePrefix) {
					return
				}

				if !legendInstancePrefixOK(legend) {
					violations = append(violations, fmt.Sprintf(
						"panel %d %q refId %s: legend %q must start with %s and be followed by space, `|`, or end-of-string",
						panel.ID, panel.Title, target.RefID, legend, instancePrefix,
					))
				}
			})

			assert.Empty(t, violations,
				"reorder legend tokens so {{instance}} is first and followed by a space or `|` separator")
		})
	}
}

// TestLegendInstancePrefixOK_PositiveControl proves
// legendInstancePrefixOK fires on the shapes Invariant 6 claims to
// catch. Without this, the prefix-shape check could pass vacuously
// against any real dashboard that happens to be clean today.
func TestLegendInstancePrefixOK_PositiveControl(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		legend string
		want   bool
	}{
		{"bare instance placeholder", "{{instance}}", true},
		{"prefix with space + single suffix", "{{instance}} alloc", true},
		{"prefix with pipe separator", "{{instance}} | {{backend_type}} foo", true},
		{"instance in middle drops the contract", "{{backend_type}} {{instance}} shard {{shard}}", false},
		{"instance at end drops the contract", "foo {{instance}}", false},
		{"no separator after instance drops the contract", "{{instance}}garbage", false},
		{"placeholder concatenation drops the contract", "{{instance}}{{backend_type}}", false},
		// Multi-byte separator cases — pin the rune-decode behavior so a
		// regression back to byte-indexing (which would compare against the
		// leading UTF-8 continuation byte) doesn't silently regress when the
		// allowed-separator set is expanded.
		{"em-dash separator drops the contract", "{{instance}}— alloc", false}, // U+2014 (3 bytes)
		{"NBSP separator drops the contract", "{{instance}} alloc", false},     // U+00A0 (2 bytes)
		{"invalid UTF-8 byte after prefix drops the contract", "{{instance}}\xc3garbage", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, legendInstancePrefixOK(tc.legend), "legend: %q", tc.legend)
		})
	}
}

// TestTracingDashboardStructure guards transport-tracing.json, which sits
// OUTSIDE the Prometheus-oriented lint scope on purpose (rt:lint:dashboard +
// the invariants above only cover transport.json/hub.json) because its panels
// query Jaeger, not Prometheus. Without this the trace dashboard had zero
// automated coverage: a JSON typo, a wrong datasource uid (which silently
// resolves to nothing in Grafana), an accidental Prometheus panel inside a
// row, editable drift, or a renamed Jaeger entry in datasources.yml would all
// ship undetected. Asserts valid JSON, uneditable, every data-panel/target
// (including those nested under `row` panels) uses the Jaeger datasource
// (type=uid="jaeger"), no Prometheus `expr` queries, at least one
// `mercure-hub` search target attested by a Jaeger datasource on the
// witnessing panel/target, and the uid is provisioned on EXACTLY ONE entry
// in datasources.yml (parsed as YAML, not substring).
//
// The scanning logic is factored into pure scanTraceDashboard so the
// positive-control test below can drive it with synthetic input.
func TestTracingDashboardStructure(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(resolvePackagePath(t, "grafana/dashboards/transport-tracing.json"))
	require.NoError(t, err)

	var d map[string]any
	require.NoError(t, json.Unmarshal(raw, &d), "transport-tracing.json must be valid JSON")

	// `editable` must be present AND boolean-false. A missing key would read as
	// nil; type-asserting surfaces that distinctly from `editable: true`.
	editable, ok := d["editable"].(bool)
	require.Truef(t, ok, "trace dashboard must set a boolean `editable` field; got %T", d["editable"])
	assert.False(t, editable, "trace dashboard must set editable:false (matches transport.json/hub.json + the dashboard-linter uneditable contract)")

	panels, _ := d["panels"].([]any)
	require.NotEmpty(t, panels, "trace dashboard must have panels")

	scan := scanTraceDashboard(panels)

	// Any finding is a contract violation; assert each individually so the
	// failure message identifies which violation tripped.
	for _, f := range scan.Findings {
		assert.Failf(t, "trace dashboard contract violation",
			"kind=%s, where=%s, detail=%s — the trace dashboard must be Jaeger-only (no Prometheus expr, no string-form datasources, every datasource object must carry both `type` and `uid` matching the provisioned Jaeger datasource)",
			f.Kind, f.Where, f.Detail)
	}

	assert.True(t, scan.SawMercureHubSearchOnJaeger,
		"trace dashboard must have at least one `queryType: search` target against service=mercure-hub, attested by a Jaeger datasource on the target (or inherited from its panel) — the dashboard's whole purpose")

	assertJaegerDatasourceProvisioned(t)
}

// TestScanTraceDashboard_RecursesIntoRowPanels is the positive control for
// scanTraceDashboard's recursion. transport-tracing.json today has no `row`
// panels, so the recursion path in scanTraceDashboard is exercised by zero
// production data — a bug in the recursive call (e.g. recursing into the
// wrong key, flipping the OR-fold, missing the nested-len guard) would not be
// caught by TestTracingDashboardStructure. This test drives the same scanner
// with a synthetic dashboard containing a Prometheus panel buried under a
// `type: "row"` parent and asserts the scanner surfaces it.
func TestScanTraceDashboard_RecursesIntoRowPanels(t *testing.T) {
	t.Parallel()

	synthetic := []any{
		// Valid top-level Jaeger search panel (the "happy path" that should
		// satisfy SawMercureHubSearchOnJaeger).
		map[string]any{
			"title":      "Recent mercure-hub traces",
			"type":       "table",
			"datasource": jaegerDS,
			"targets": []any{
				map[string]any{
					"datasource": jaegerDS,
					"queryType":  "search",
					"service":    "mercure-hub",
				},
			},
		},
		// Row-wrapped Prometheus panel — the contamination the recursion must catch.
		map[string]any{
			"title": "Collapsed row",
			"type":  "row",
			"panels": []any{
				map[string]any{
					"title":      "Hidden Prometheus",
					"type":       "timeseries",
					"datasource": prometheusDS,
					"targets": []any{
						map[string]any{
							"datasource": prometheusDS,
							"expr":       "up",
						},
					},
				},
			},
		},
	}

	scan := scanTraceDashboard(synthetic)

	// Must surface BOTH the wrong-type panel/target datasources AND the
	// Prometheus expr — three findings at the nested location prove the
	// recursion + datasource + expr checks all fire on row-nested content.
	var sawWrongTypePanel, sawWrongTypeTarget, sawPrometheusExpr bool

	const nestedPath = "Collapsed row/Hidden Prometheus"

	for _, f := range scan.Findings {
		switch {
		case f.Kind == findingWrongType && f.Where == "panel "+nestedPath:
			sawWrongTypePanel = true
		case f.Kind == findingWrongType && f.Where == "target in panel "+nestedPath:
			sawWrongTypeTarget = true
		case f.Kind == findingPrometheusExpr && f.Where == "target in panel "+nestedPath:
			sawPrometheusExpr = true
		}
	}

	assert.Truef(t, sawWrongTypePanel, "scanner must flag wrong_type on the row-nested Prometheus panel; findings=%v", scan.Findings)
	assert.Truef(t, sawWrongTypeTarget, "scanner must flag wrong_type on the row-nested Prometheus target; findings=%v", scan.Findings)
	assert.Truef(t, sawPrometheusExpr, "scanner must flag prometheus_expr on the row-nested target; findings=%v", scan.Findings)
	assert.True(t, scan.SawMercureHubSearchOnJaeger, "scanner must still surface the valid top-level mercure-hub Jaeger search")
}

// TestScanTraceDashboard_PanelLevelInheritance covers the two inheritance
// paths the scanner must enforce: (1) target with no datasource inheriting
// from a Jaeger-attested parent panel (legitimate; must NOT trip the gate
// and MUST count toward SawMercureHubSearchOnJaeger); (2) target with no
// datasource AND parent with no datasource (silent wrong-backend query;
// MUST trip findingNoJaegerInheritance).
func TestScanTraceDashboard_PanelLevelInheritance(t *testing.T) {
	t.Parallel()

	synthetic := []any{
		// Case 1 — panel-level Jaeger inheritance; the target witnesses
		// mercure-hub even though it has no explicit datasource.
		map[string]any{
			"title":      "Inheriting panel",
			"type":       "table",
			"datasource": jaegerDS,
			"targets": []any{
				map[string]any{
					"queryType": "search",
					"service":   "mercure-hub",
				},
			},
		},
		// Case 2 — neither panel nor target carries a datasource; the
		// inheritance-gap finding must fire.
		map[string]any{
			"title": "Orphan panel",
			"type":  "table",
			"targets": []any{
				map[string]any{
					"queryType": "search",
					"service":   "mercure-hub",
				},
			},
		},
	}

	scan := scanTraceDashboard(synthetic)

	var (
		sawInheritanceGapOnOrphan bool
		sawInheritanceGapOnValid  bool
	)

	for _, f := range scan.Findings {
		if f.Kind != findingNoJaegerInheritance {
			continue
		}

		switch f.Where {
		case "target in panel Orphan panel":
			sawInheritanceGapOnOrphan = true
		case "target in panel Inheriting panel":
			sawInheritanceGapOnValid = true
		}
	}

	assert.Truef(t, sawInheritanceGapOnOrphan,
		"scanner must flag no_jaeger_inheritance on a target with no datasource AND a panel with no datasource; findings=%v", scan.Findings)
	assert.Falsef(t, sawInheritanceGapOnValid,
		"scanner must NOT flag no_jaeger_inheritance when the parent panel carries a Jaeger datasource (Grafana resolves via inheritance); findings=%v", scan.Findings)
	assert.Truef(t, scan.SawMercureHubSearchOnJaeger,
		"panel-level Jaeger inheritance must count toward SawMercureHubSearchOnJaeger; findings=%v", scan.Findings)
}

// TestScanTraceDashboard_FindingKinds is the matched-omission positive
// control for the datasource-shape finding kinds not already covered by
// the row-recursion + inheritance tests: findingStringDatasource,
// findingMissingDatasourceKey, findingWrongUID, findingUnexpectedDatasource,
// findingMalformedTargets.
func TestScanTraceDashboard_FindingKinds(t *testing.T) {
	t.Parallel()

	// Cases 1-4 each isolate ONE datasource-shape finding kind. Targets
	// carry an explicit Jaeger datasource so findingNoJaegerInheritance
	// doesn't incidentally fire — the per-kind assertions below stay
	// focused on the case under test, and the findings=%v failure dump
	// stays readable instead of being padded with inheritance noise.
	// Coverage for findingNoJaegerInheritance lives in
	// TestScanTraceDashboard_PanelLevelInheritance.
	jaegerTarget := map[string]any{"datasource": jaegerDS}

	synthetic := []any{
		// Legacy string-form datasource.
		map[string]any{
			"title":      "Legacy string",
			"datasource": "Jaeger",
			"targets":    []any{jaegerTarget},
		},
		// Datasource map missing both `type` and `uid`.
		map[string]any{
			"title":      "Missing keys",
			"datasource": map[string]any{"name": "Jaeger"},
			"targets":    []any{jaegerTarget},
		},
		// Datasource carrying the wrong uid (right type, different namespacing).
		map[string]any{
			"title":      "Wrong uid",
			"datasource": map[string]any{"type": "jaeger", "uid": "mercure-jaeger"},
			"targets":    []any{jaegerTarget},
		},
		// Datasource is a number — neither string nor map.
		map[string]any{
			"title":      "Unexpected type",
			"datasource": float64(42),
			"targets":    []any{jaegerTarget},
		},
		// Malformed targets value — string instead of array. Without the
		// findingMalformedTargets gate, the type assertion in scanPanel
		// would silently no-op and the panel would pass every Jaeger-only
		// invariant vacuously.
		map[string]any{
			"title":      "Malformed targets",
			"datasource": jaegerDS,
			"targets":    "TODO — fill in targets",
		},
	}

	scan := scanTraceDashboard(synthetic)

	kinds := make(map[string]bool, len(scan.Findings))
	for _, f := range scan.Findings {
		kinds[f.Kind] = true
	}

	assert.Truef(t, kinds[findingStringDatasource], "scanner must flag string_datasource on the legacy string-form panel; findings=%v", scan.Findings)
	assert.Truef(t, kinds[findingMissingDatasourceKey], "scanner must flag missing_datasource_key when the datasource map lacks `type`/`uid`; findings=%v", scan.Findings)
	assert.Truef(t, kinds[findingWrongUID], "scanner must flag wrong_uid when type=jaeger but uid differs; findings=%v", scan.Findings)
	assert.Truef(t, kinds[findingUnexpectedDatasource], "scanner must flag unexpected_datasource on a non-string non-map datasource; findings=%v", scan.Findings)
	assert.Truef(t, kinds[findingMalformedTargets], "scanner must flag malformed_targets on a non-array targets value; findings=%v", scan.Findings)
}

// TestScanTraceDashboard_MalformedTargetsStillRecurses: scanPanel emits
// findingMalformedTargets AND keeps recursing into nested children. A
// refactor that early-returned on malformed-targets would silently swallow
// every finding from the nested subtree.
func TestScanTraceDashboard_MalformedTargetsStillRecurses(t *testing.T) {
	t.Parallel()

	synthetic := []any{
		map[string]any{
			"title":      "Outer with malformed targets",
			"datasource": jaegerDS,
			"targets":    "TODO — schema regression at the parent level",
			"panels": []any{
				map[string]any{
					"title":      "Nested Prometheus child",
					"type":       "timeseries",
					"datasource": prometheusDS,
					"targets": []any{
						map[string]any{
							"datasource": prometheusDS,
							"expr":       "up",
						},
					},
				},
			},
		},
	}

	scan := scanTraceDashboard(synthetic)

	var (
		sawMalformedOnParent      bool
		sawWrongTypeOnNestedPanel bool
		sawPrometheusExprOnNested bool
	)

	for _, f := range scan.Findings {
		switch {
		case f.Kind == findingMalformedTargets && f.Where == "panel Outer with malformed targets":
			sawMalformedOnParent = true
		case f.Kind == findingWrongType && f.Where == "panel Outer with malformed targets/Nested Prometheus child":
			sawWrongTypeOnNestedPanel = true
		case f.Kind == findingPrometheusExpr && f.Where == "target in panel Outer with malformed targets/Nested Prometheus child":
			sawPrometheusExprOnNested = true
		}
	}

	assert.Truef(t, sawMalformedOnParent, "scanner must flag malformed_targets on the outer panel; findings=%v", scan.Findings)
	assert.Truef(t, sawWrongTypeOnNestedPanel, "scanner must still recurse into nested children and flag wrong_type — early-return on malformed targets would silently swallow this; findings=%v", scan.Findings)
	assert.Truef(t, sawPrometheusExprOnNested, "scanner must still surface prometheus_expr on nested targets despite malformed parent targets; findings=%v", scan.Findings)
}

// TestAllTraceDashboardFindingKindsHaveCoverage walks
// traceDashboardCoverageFixtures (a kind → panels map) and runs each fixture
// through scanTraceDashboard. It asserts two invariants:
//
//  1. Forward: every key in the map produces its named kind. A dead fixture
//     (refactored panel that no longer trips its assigned kind) fails fast.
//  2. Reverse: every kind any fixture emits is registered in the map. A new
//     scanner kind landing in `scanTraceDashboard` without a map entry trips
//     the audit, closing the drift loop the older slice-based check missed.
//
// The per-kind positive-control tests (TestScanTraceDashboard_FindingKinds,
// _RecursesIntoRowPanels, _PanelLevelInheritance) carry the behavioral
// assertions; this audit only enforces reachability + completeness of the
// kind enumeration. Residual gap: a brand-new kind that no existing fixture
// happens to trip will still slip past — when adding a scanner kind, the
// contributor MUST add a fixture entry here.
func TestAllTraceDashboardFindingKindsHaveCoverage(t *testing.T) {
	t.Parallel()

	for kind, panels := range traceDashboardCoverageFixtures {
		scan := scanTraceDashboard(panels)

		emitted := make(map[string]bool, len(scan.Findings))
		for _, f := range scan.Findings {
			emitted[f.Kind] = true
		}

		assert.Truef(t, emitted[kind],
			"traceDashboardCoverageFixtures[%q] no longer trips its named kind; emitted=%v", kind, emitted)

		// Reverse drift: every emitted kind must be registered in the map.
		// A new scanner kind landing without a fixture entry trips here.
		for emittedKind := range emitted {
			_, registered := traceDashboardCoverageFixtures[emittedKind]
			assert.Truef(t, registered,
				"scanner emitted kind %q from fixture %q but no map entry registers it — add traceDashboardCoverageFixtures[%q]",
				emittedKind, kind, emittedKind)
		}
	}
}

// TestScannerKindsAllRegisteredInCoverageMap closes the residual exhaustive-
// enumeration gap that the runtime audit above can't cover by itself: a new
// scanner kind added WITHOUT a fixture entry is invisible at runtime because
// no fixture trips it. This test parses this file's own source and:
//
//  1. Collects every `findingX = "value"` declaration from the package's
//     `const` blocks (auto-built — no manual list to drift).
//  2. Collects every `Kind: ...` emission site inside `traceDashboardFinding`
//     composite literals — accepts either `Kind: findingX` (identifier) OR
//     `Kind: "value"` (bare string literal). Both routes are checked so a
//     contributor who forgets to use the const can't silently bypass.
//  3. Asserts (a) every emitted ident resolves to a declared const, and
//     (b) the resolved value (or direct literal) is a key in
//     traceDashboardCoverageFixtures.
//  4. Asserts no positional `traceDashboardFinding{...}` composites exist
//     anywhere in this file — those would skip the KeyValueExpr walker
//     and bypass the audit entirely.
//
// Adding a new finding kind now requires three coordinated edits or the
// audit fails: (1) const declaration, (2) emission site, (3) fixture entry.
// The runtime audit catches per-fixture drift; this static audit catches
// the case where steps 1+2 land without step 3.
//
// Single-file scope: the walker only parses dashboard_lint_test.go via
// runtime.Caller(0). If the scanner is ever extracted to its own file, this
// walker must be updated to parse the whole package via parser.ParseDir.
func TestScannerKindsAllRegisteredInCoverageMap(t *testing.T) {
	t.Parallel()

	file := parseTestSource(t)
	assertNoPositionalFindingComposites(t, file)

	constValues := collectFindingConsts(t, file)
	require.NotEmpty(t, constValues, "AST walker found no finding* const declarations — walker is broken")

	kindIdents, kindLiterals := collectKindEmissions(file)
	require.True(t, len(kindIdents) > 0 || len(kindLiterals) > 0,
		"AST walker found no Kind: finding* sites — walker is broken")

	for ident := range kindIdents {
		value, declared := constValues[ident]
		require.Truef(t, declared,
			"scanner emits Kind: %s but no finding* const declaration was found — declare it before emitting", ident)

		_, registered := traceDashboardCoverageFixtures[value]
		assert.Truef(t, registered,
			"scanner emits Kind: %s (value %q) but no traceDashboardCoverageFixtures entry registers it — add a fixture", ident, value)
	}

	for literal := range kindLiterals {
		_, registered := traceDashboardCoverageFixtures[literal]
		assert.Truef(t, registered,
			"scanner emits Kind: %q (bare string literal — prefer a finding* const for grep-ability) but no traceDashboardCoverageFixtures entry registers it — add a fixture", literal)
	}
}

// parseTestSource parses this test file's own source via runtime.Caller(0)
// + parser.ParseFile. Returned to both AST audits so they share the parse
// without duplicating the boilerplate.
func parseTestSource(t *testing.T) *ast.File {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller(0) failed — cannot locate test source for AST walk")

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, thisFile, nil, parser.AllErrors)
	require.NoError(t, err, "AST parse of test source failed")

	return file
}

// collectFindingConsts scans top-level `const` blocks for `findingX = "literal"`
// declarations and returns name → unquoted value. Shape-drift (iota-style
// implicit values, non-string literals, broken quoting) surfaces via t.Logf
// in collectFindingValueSpec so silent drops are observable in test output.
func collectFindingConsts(t *testing.T, file *ast.File) map[string]string {
	t.Helper()

	out := make(map[string]string)

	for _, decl := range file.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}

		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}

			collectFindingValueSpec(t, vs, out)
		}
	}

	return out
}

// collectFindingValueSpec extracts `findingX = "literal"` declarations from a
// single ValueSpec into `out`. Further split out so collectFindingConsts stays
// shallow enough for the per-function cognitive-complexity budget. Non-string
// shapes (iota, references, broken literals) surface via t.Logf so silent
// drift is observable in test output.
func collectFindingValueSpec(t *testing.T, vs *ast.ValueSpec, out map[string]string) {
	t.Helper()

	for i, name := range vs.Names {
		if !strings.HasPrefix(name.Name, "finding") {
			continue
		}

		if i >= len(vs.Values) {
			t.Logf("collectFindingValueSpec: skipping %s — no explicit value (iota/implicit-repeat?)", name.Name)

			continue
		}

		lit, ok := vs.Values[i].(*ast.BasicLit)
		if !ok {
			t.Logf("collectFindingValueSpec: skipping %s — value is not a basic literal (%T)", name.Name, vs.Values[i])

			continue
		}

		if lit.Kind != token.STRING {
			t.Logf("collectFindingValueSpec: skipping %s — value literal is not a string (kind=%s)", name.Name, lit.Kind)

			continue
		}

		unquoted, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Logf("collectFindingValueSpec: skipping %s — Unquote failed: %v", name.Name, err)

			continue
		}

		out[name.Name] = unquoted
	}
}

// assertNoPositionalFindingComposites fails the test if any
// `traceDashboardFinding{...}` composite literal uses positional form
// instead of keyed (`Kind: ..., Where: ...`). Positional composites have
// no KeyValueExpr nodes, which would cause collectKindEmissions to silently
// skip the emission — bypassing the entire coverage audit.
func assertNoPositionalFindingComposites(t *testing.T, file *ast.File) {
	t.Helper()

	ast.Inspect(file, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok || cl.Type == nil {
			return true
		}

		typeIdent, ok := cl.Type.(*ast.Ident)
		if !ok || typeIdent.Name != "traceDashboardFinding" {
			return true
		}

		for i, elt := range cl.Elts {
			if _, isKV := elt.(*ast.KeyValueExpr); !isKV {
				t.Errorf("traceDashboardFinding composite literal at element [%d] uses positional form (%T) — switch to keyed form (Kind: ..., Where: ...) so the static coverage audit can see the emission", i, elt)
			}
		}

		return false
	})
}

// TestTraceDashboardPositiveControlsPresent closes the positive-control
// deletion gap: the runtime + AST coverage audits enforce kind reachability
// but neither one notices if a per-kind positive-control test is DELETED
// or RENAMED — at that point the kind stays reachable (via the coverage
// fixture) but the BEHAVIORAL assertions for that kind vanish silently.
//
// This audit walks the same source AST for declared `TestScanTraceDashboard_*`
// functions and asserts each named contract test is still declared. The
// expected list is intentionally explicit: a renamed test fails this audit
// AND surfaces the rename in the diff (the maintainer must update both),
// which beats a silent loss of behavioral coverage.
//
// NOT caught: `t.Skip("...")` calls added inside a positive-control test
// — the function still declares, so the existence check passes. Spotting
// silently-skipped tests is `go test -v`'s job (skips show as SKIP lines);
// adding `ast.Inspect` for skip calls would catch unconditional skips but
// not skip-on-condition shapes, so we explicitly scope this audit to
// presence-only.
func TestTraceDashboardPositiveControlsPresent(t *testing.T) {
	t.Parallel()

	expected := []string{
		"TestScanTraceDashboard_FindingKinds",
		"TestScanTraceDashboard_RecursesIntoRowPanels",
		"TestScanTraceDashboard_PanelLevelInheritance",
		"TestScanTraceDashboard_MalformedTargetsStillRecurses",
	}

	file := parseTestSource(t)

	declared := make(map[string]bool)

	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}

		declared[fd.Name.Name] = true
	}

	for _, name := range expected {
		assert.Truef(t, declared[name],
			"expected positive-control test %q not found — was it renamed or deleted? Update the `expected` list or restore the test", name)
	}
}

// collectKindEmissions scans for every `Kind: ...` KeyValueExpr in this file
// and returns two sets:
//   - idents: emission sites of the form `Kind: findingX` (identifier value)
//   - literals: emission sites of the form `Kind: "some_value"` (bare string)
//
// Identifier values must start with `finding` to be collected (matches the
// scanner's naming convention). String literals are unconditionally
// collected so a contributor who bypasses the const layer doesn't bypass
// the coverage audit. Positional composite literals are caught separately
// by assertNoPositionalFindingComposites.
//
// Patterns NOT covered (currently unused by the scanner):
//   - `Kind: someFunc()` — function-call values
//   - `Kind: string(findingX)` — explicit type conversions
//   - `f.Kind = findingX` — assignment statements (not composite literals)
//
// If the scanner adopts any of these patterns, extend this walker. The
// audit's scope is documented in TestScannerKindsAllRegisteredInCoverageMap's
// docstring.
func collectKindEmissions(file *ast.File) (idents, literals map[string]bool) {
	idents = make(map[string]bool)
	literals = make(map[string]bool)

	ast.Inspect(file, func(n ast.Node) bool {
		kv, ok := n.(*ast.KeyValueExpr)
		if !ok {
			return true
		}

		keyIdent, ok := kv.Key.(*ast.Ident)
		if !ok || keyIdent.Name != "Kind" {
			return true
		}

		switch v := kv.Value.(type) {
		case *ast.Ident:
			if strings.HasPrefix(v.Name, "finding") {
				idents[v.Name] = true
			}
		case *ast.BasicLit:
			if v.Kind == token.STRING {
				if unquoted, err := strconv.Unquote(v.Value); err == nil {
					literals[unquoted] = true
				}
			}
		}

		return true
	})

	return idents, literals
}

// traceDashboardCoverageFixtures pairs each enumerated finding kind with a
// minimal synthetic panel tree designed to emit it. The map is the single
// source of truth for the kind enumeration (no separate slice to drift).
// Adding a new kind to the scanner requires adding a fixture entry here,
// or TestAllTraceDashboardFindingKindsHaveCoverage's reverse-drift check
// will fail when the new kind shows up in `emitted` without registration.
var traceDashboardCoverageFixtures = map[string][]any{
	findingStringDatasource: {
		map[string]any{"title": "Legacy string", "datasource": "Jaeger", "targets": []any{map[string]any{"datasource": jaegerDS}}},
	},
	findingMissingDatasourceKey: {
		map[string]any{"title": "Missing keys", "datasource": map[string]any{"name": "Jaeger"}, "targets": []any{map[string]any{"datasource": jaegerDS}}},
	},
	findingWrongUID: {
		map[string]any{"title": "Wrong uid", "datasource": map[string]any{"type": "jaeger", "uid": "other"}, "targets": []any{map[string]any{"datasource": jaegerDS}}},
	},
	findingUnexpectedDatasource: {
		map[string]any{"title": "Unexpected type", "datasource": float64(42), "targets": []any{map[string]any{"datasource": jaegerDS}}},
	},
	findingMalformedTargets: {
		map[string]any{"title": "Malformed targets", "datasource": jaegerDS, "targets": "not-an-array"},
	},
	findingWrongType: {
		// Row recursion + non-Jaeger child panel — produces wrong_type AND
		// prometheus_expr (covered separately below). Target carries Jaeger
		// explicitly so findingNoJaegerInheritance stays scoped to the orphan
		// fixture.
		map[string]any{
			"title": "row", "type": "row",
			"panels": []any{
				map[string]any{
					"title": "child", "type": "timeseries", "datasource": prometheusDS,
					"targets": []any{map[string]any{"datasource": jaegerDS, "expr": "up"}},
				},
			},
		},
	},
	findingPrometheusExpr: {
		// Minimal Jaeger-typed panel whose target carries a Prometheus
		// `expr` — trips prometheus_expr without also emitting wrong_type
		// or no_jaeger_inheritance, so the reverse-drift error message
		// stays precise when this entry is the load-bearing fixture.
		map[string]any{
			"title": "Jaeger panel with stray prometheus expr", "datasource": jaegerDS,
			"targets": []any{map[string]any{"datasource": jaegerDS, "expr": "up"}},
		},
	},
	findingNoJaegerInheritance: {
		map[string]any{
			"title":   "Orphan",
			"targets": []any{map[string]any{"queryType": "search", "service": "mercure-hub"}},
		},
	},
}

// jaegerDatasource is the type AND uid both transport-tracing.json's panels
// target and datasources.yml provisions today. If the uid is ever namespaced
// (e.g. "mercure-jaeger"), split into separate type+uid constants then.
const jaegerDatasource = "jaeger"

// Shared synthetic-fixture values for the TestScanTraceDashboard_* tests.
// Captured as package-private vars so each test stays a single map literal
// without inline rebuilds of the same datasource shape.
var (
	jaegerDS     = map[string]any{"type": jaegerDatasource, "uid": jaegerDatasource}
	prometheusDS = map[string]any{"type": "prometheus", "uid": "prometheus"}
)

// traceDashboardFinding kinds. Matched against by the positive-control test +
// the assertion driver; centralized so a rename doesn't drift the two callers.
const (
	findingStringDatasource     = "string_datasource"
	findingMissingDatasourceKey = "missing_datasource_key"
	findingWrongType            = "wrong_type"
	findingWrongUID             = "wrong_uid"
	findingUnexpectedDatasource = "unexpected_datasource"
	findingPrometheusExpr       = "prometheus_expr"
	// findingNoJaegerInheritance — target has no Jaeger datasource AND no
	// parent panel provides one to inherit. Distinct from findingMissingDatasourceKey,
	// which is the keys-absent case (a Prometheus datasource is fully-formed
	// but still trips this gate).
	findingNoJaegerInheritance = "no_jaeger_inheritance"
	// findingMalformedTargets — panel's `targets` is present but not a JSON
	// array (string/null/object). Closes a silent no-op in scanTargets's
	// type assertion that would let the panel pass every Jaeger invariant.
	findingMalformedTargets = "malformed_targets"
)

// traceDashboardFinding is a single trace-dashboard contract violation.
// Findings are accumulated into traceDashboardScan rather than asserted in
// place, so the scanner is reusable from the positive-control test without
// `*testing.T` side effects.
type traceDashboardFinding struct {
	Kind   string
	Where  string
	Detail string
}

// traceDashboardScan is the result of walking a trace-dashboard JSON tree.
// Pure data — no testing.T involved. TestTracingDashboardStructure converts
// the findings into assertions; TestScanTraceDashboard_RecursesIntoRowPanels
// inspects them directly.
type traceDashboardScan struct {
	Findings                    []traceDashboardFinding
	SawMercureHubSearchOnJaeger bool
}

// scanTraceDashboard walks the panel tree of a trace dashboard (parsed JSON,
// generic map[string]any shape) and returns every contract violation it found
// plus whether at least one mercure-hub search target was attested by a
// Jaeger datasource on either the target itself or its parent panel.
// Panel-level inheritance is how Grafana resolves a target with no
// datasource, so the inheritance gate is part of the scanner's contract —
// see TestScanTraceDashboard_PanelLevelInheritance for the positive control.
func scanTraceDashboard(panels []any) traceDashboardScan {
	var s traceDashboardScan
	s.scanPanels(panels, "")

	return s
}

func (s *traceDashboardScan) scanPanels(panels []any, parent string) {
	for _, p := range panels {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}

		s.scanPanel(pm, parent)
	}
}

func (s *traceDashboardScan) scanPanel(pm map[string]any, parent string) {
	// Use a typed read for the title; fall back to id-based naming if the
	// panel omits it (rows/text panels often do) so error messages don't
	// surface the misleading "<nil>" string.
	title, _ := pm["title"].(string)
	if title == "" {
		title = fmt.Sprintf("id=%v", pm["id"])
	}

	panelPath := title
	if parent != "" {
		panelPath = parent + "/" + title
	}

	panelIsJaeger := s.checkDatasource(pm["datasource"], "panel "+panelPath)

	// Targets must be an array when present. A hand-edit writing
	// `"targets": "TODO"` (string), `"targets": null` (after a Grafana UI
	// save that emptied the slice but left the key), or any other
	// non-array shape would silently no-op every Jaeger-only invariant on
	// this panel — zero targets scanned, zero findings, panel passes.
	// Emit findingMalformedTargets so the scanner catches the schema
	// regression instead of accepting it.
	if rawTargets, present := pm["targets"]; present {
		if targets, ok := rawTargets.([]any); ok {
			s.scanTargets(targets, panelIsJaeger, panelPath)
		} else {
			s.Findings = append(s.Findings, traceDashboardFinding{
				Kind:   findingMalformedTargets,
				Where:  "panel " + panelPath,
				Detail: fmt.Sprintf("targets is %T, want []any", rawTargets),
			})
		}
	}

	// Recurse into nested panels (collapsed-row pattern: `type: "row"` panels
	// carry children under `panels`). Without this, a future Prometheus panel
	// hidden under a row would silently bypass every Jaeger-only invariant —
	// covered by TestScanTraceDashboard_RecursesIntoRowPanels.
	if nested, ok := pm["panels"].([]any); ok && len(nested) > 0 {
		s.scanPanels(nested, panelPath)
	}
}

func (s *traceDashboardScan) scanTargets(targets []any, panelIsJaeger bool, panelPath string) {
	for _, tg := range targets {
		tm, ok := tg.(map[string]any)
		if !ok {
			continue
		}

		targetIsJaeger := s.checkDatasource(tm["datasource"], "target in panel "+panelPath)

		// Symmetric to the sawMercureHubSearch gate below: every data target
		// must be Jaeger-attested, either explicitly on the target or
		// inherited from a Jaeger-validated parent panel. Without this, a
		// non-witnessing target with no datasource (Grafana resolves to the
		// environment default — Prometheus in our dev stack) would silently
		// query the wrong backend, while the existing valid mercure-hub
		// search keeps SawMercureHubSearchOnJaeger=true and masks the
		// violation. checkDatasource's nil branch is intentionally
		// finding-free for panels (text/link panels legitimately have no
		// datasource); the lack is only a contract break at the target level
		// when the panel also doesn't supply one to inherit.
		if !targetIsJaeger && !panelIsJaeger {
			s.Findings = append(s.Findings, traceDashboardFinding{
				Kind:   findingNoJaegerInheritance,
				Where:  "target in panel " + panelPath,
				Detail: "target has no Jaeger datasource and parent panel doesn't provide one to inherit",
			})
		}

		if exprAny, hasExpr := tm["expr"]; hasExpr {
			expr, _ := exprAny.(string)
			s.Findings = append(s.Findings, traceDashboardFinding{
				Kind:   findingPrometheusExpr,
				Where:  "target in panel " + panelPath,
				Detail: expr,
			})
		}

		if qt, _ := tm["queryType"].(string); qt != "search" {
			continue
		}

		if svc, _ := tm["service"].(string); svc != "mercure-hub" {
			continue
		}

		// The witnessing search target must itself be attested by a Jaeger
		// datasource — either explicit on the target OR inherited from the
		// panel. Without this, a target with no datasource (Grafana falls
		// back to the environment default, usually Prometheus) silently
		// satisfies the "have a mercure-hub search" gate.
		if targetIsJaeger || panelIsJaeger {
			s.SawMercureHubSearchOnJaeger = true
		}
	}
}

// checkDatasource validates a Grafana datasource value. Returns true if the
// datasource is the object form {type:jaeger, uid:jaeger}. Any deviation
// produces a finding (nil/absent is the one exception — caller decides
// whether absence at the call site is acceptable).
func (s *traceDashboardScan) checkDatasource(ds any, where string) bool {
	switch v := ds.(type) {
	case nil:
		return false
	case string:
		s.Findings = append(s.Findings, traceDashboardFinding{
			Kind:   findingStringDatasource,
			Where:  where,
			Detail: v,
		})

		return false
	case map[string]any:
		typ, typOK := v["type"].(string)
		uid, uidOK := v["uid"].(string)

		if !typOK || !uidOK {
			s.Findings = append(s.Findings, traceDashboardFinding{
				Kind:   findingMissingDatasourceKey,
				Where:  where,
				Detail: fmt.Sprintf("type string present: %v, uid string present: %v", typOK, uidOK),
			})

			return false
		}

		if typ != jaegerDatasource {
			s.Findings = append(s.Findings, traceDashboardFinding{
				Kind:   findingWrongType,
				Where:  where,
				Detail: typ,
			})

			return false
		}

		if uid != jaegerDatasource {
			s.Findings = append(s.Findings, traceDashboardFinding{
				Kind:   findingWrongUID,
				Where:  where,
				Detail: uid,
			})

			return false
		}

		return true
	default:
		s.Findings = append(s.Findings, traceDashboardFinding{
			Kind:   findingUnexpectedDatasource,
			Where:  where,
			Detail: fmt.Sprintf("%T", ds),
		})

		return false
	}
}

// assertJaegerDatasourceProvisioned parses datasources.yml properly (not via
// substring) and asserts EXACTLY ONE entry has both type=jaeger AND uid=jaeger
// on it. Substring matching would pass on a comment line ("# was uid: jaeger")
// or a stale `tracesToMetrics.datasourceUid` reference even after the actual
// datasource was renamed; a >1 match would let Grafana provision two
// datasources with the same uid (warns at boot, hides which one a panel
// query actually targets).
func assertJaegerDatasourceProvisioned(t *testing.T) {
	t.Helper()

	dsRaw, err := os.ReadFile(resolvePackagePath(t, "grafana/datasources.yml"))
	require.NoError(t, err)

	var dsFile struct {
		Datasources []struct {
			Name string `yaml:"name"`
			Type string `yaml:"type"`
			UID  string `yaml:"uid"`
		} `yaml:"datasources"`
	}
	require.NoError(t, yaml.Unmarshal(dsRaw, &dsFile), "datasources.yml must be valid YAML")
	require.NotEmptyf(t, dsFile.Datasources, "datasources.yml has zero datasources entries — file may be truncated or schema-regressed; check the file before debugging Jaeger provisioning")

	var matches int

	for _, ds := range dsFile.Datasources {
		if ds.Type == jaegerDatasource && ds.UID == jaegerDatasource {
			matches++
		}
	}

	require.Equalf(t, 1, matches,
		"datasources.yml must provision EXACTLY ONE datasource with type=%q AND uid=%q; got %d match(es) across %d total datasources",
		jaegerDatasource, jaegerDatasource, matches, len(dsFile.Datasources))
}

// TestDashboardPanelsNoGridPosOverlap asserts that no two panels in
// transport.json or hub.json have overlapping gridPos rectangles AND that no
// panel exceeds the 24-column grid width. A row re-tile (e.g. the History
// replay row's 12+12 → 8+8+8 reshuffle) is exactly where an off-by-one x or
// w slips past visual inspection — the dashboard renders, but Grafana stacks
// the overlapping panels and hides one. Pairwise O(n²) is fine here (~50
// panels per dashboard); the check covers row + non-row panels uniformly.
func TestDashboardPanelsNoGridPosOverlap(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, path string
	}{
		{"transport.json", transportDashboardPath},
		{"hub.json", hubDashboardPath},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(st *testing.T) {
			st.Parallel()

			raw, err := os.ReadFile(resolvePackagePath(st, tc.path))
			require.NoError(st, err)

			var d map[string]any
			require.NoError(st, json.Unmarshal(raw, &d))

			panels, _ := d["panels"].([]any)
			collected, missingGridPos := collectPanelsWithGridPos(panels)

			assert.Emptyf(st, missingGridPos,
				"dashboard %q has non-row panel(s) without gridPos: %v — schema regression that would let the overlap+fit checks silently skip these panels",
				tc.name, missingGridPos)
			require.NotEmptyf(st, collected, "dashboard %q must have panels with gridPos", tc.name)

			// Collect every violation rather than fail-fast on the first invalid
			// panel — a bulk JSON-rewrite that breaks several panels surfaces in
			// ONE CI run with the full list, not one panel per re-run.
			var (
				invalidPanels []string
				gridOverflows []string
				panelOverlaps []string
			)

			for i := range collected {
				a := collected[i]
				if !a.fieldsValid {
					invalidPanels = append(invalidPanels, fmt.Sprintf("panel %q (id=%d): %s", a.title, a.id, a.invalidReason))

					continue
				}

				if a.x+a.w > 24 {
					gridOverflows = append(gridOverflows, fmt.Sprintf("panel %q (id=%d): x=%d w=%d → x+w=%d > 24", a.title, a.id, a.x, a.w, a.x+a.w))
				}

				for j := i + 1; j < len(collected); j++ {
					b := collected[j]
					if !b.fieldsValid {
						continue
					}

					if rangesOverlap(a.x, a.x+a.w, b.x, b.x+b.w) && rangesOverlap(a.y, a.y+a.h, b.y, b.y+b.h) {
						panelOverlaps = append(panelOverlaps,
							fmt.Sprintf("%q (id=%d) at x=%d w=%d y=%d h=%d AND %q (id=%d) at x=%d w=%d y=%d h=%d",
								a.title, a.id, a.x, a.w, a.y, a.h, b.title, b.id, b.x, b.w, b.y, b.h))
					}
				}
			}

			assert.Emptyf(st, invalidPanels,
				"dashboard %q has invalid gridPos on panel(s) — schema regression: %v", tc.name, invalidPanels)
			assert.Emptyf(st, gridOverflows,
				"dashboard %q has panel(s) exceeding Grafana's 24-column grid: %v", tc.name, gridOverflows)
			assert.Emptyf(st, panelOverlaps,
				"dashboard %q has overlapping panel(s): %v", tc.name, panelOverlaps)
		})
	}
}

// TestRangesOverlap_TableDriven is the positive control for rangesOverlap.
// Without it, the parent TestDashboardPanelsNoGridPosOverlap would pass
// vacuously if the helper were mutated (always-false, off-by-one, swapped
// args) — the real dashboards happen to be clean today.
func TestRangesOverlap_TableDriven(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		a1, a2, b1, b2 int
		want           bool
	}{
		{"disjoint left", 0, 5, 5, 10, false},           // half-open [0,5) and [5,10) touch at 5; non-overlapping
		{"disjoint right", 5, 10, 0, 5, false},          // symmetric
		{"identical", 0, 5, 0, 5, true},                 // [0,5) and [0,5) — full overlap
		{"a contains b", 0, 10, 2, 5, true},             // a ⊃ b
		{"b contains a", 2, 5, 0, 10, true},             // b ⊃ a
		{"partial left", 0, 5, 3, 8, true},              // a ends inside b
		{"partial right", 3, 8, 0, 5, true},             // b ends inside a
		{"zero-width a at b start", 5, 5, 5, 10, false}, // empty interval at b's start — half-open
		{"single column overlap", 0, 4, 3, 7, true},     // x=0,w=4 (cols 0,1,2,3) overlaps x=3,w=4 (cols 3,4,5,6) on col 3
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := rangesOverlap(tc.a1, tc.a2, tc.b1, tc.b2)
			assert.Equalf(t, tc.want, got, "rangesOverlap(%d,%d,%d,%d) = %v, want %v", tc.a1, tc.a2, tc.b1, tc.b2, got, tc.want)
		})
	}
}

// TestCollectPanelsWithGridPos_PositiveControl drives collectPanelsWithGridPos
// with synthetic input covering: a row-nested overlap, a grid-width violation,
// a panel without gridPos (must surface in missingGridPos), and a row panel
// that legitimately omits gridPos (must NOT surface).
func TestCollectPanelsWithGridPos_PositiveControl(t *testing.T) {
	t.Parallel()

	gp := func(x, y, w, h int) map[string]any {
		return map[string]any{
			"x": float64(x), "y": float64(y),
			"w": float64(w), "h": float64(h),
		}
	}

	synthetic := []any{
		// Panel A — well-formed, top-left.
		map[string]any{
			"id":      float64(1),
			"title":   "Panel A",
			"gridPos": gp(0, 0, 12, 8),
		},
		// Row panel — legitimately omits gridPos, nested children carry it.
		map[string]any{
			"id":    float64(2),
			"title": "Row",
			"type":  "row",
			"panels": []any{
				// Nested panel B — overlaps with A on cols 0-11.
				map[string]any{
					"id":      float64(3),
					"title":   "Nested overlap",
					"gridPos": gp(0, 0, 12, 8),
				},
				// Nested panel C — exceeds 24-column grid (x=20, w=8 → 28).
				map[string]any{
					"id":      float64(4),
					"title":   "Grid overflow",
					"gridPos": gp(20, 8, 8, 4),
				},
			},
		},
		// Panel D — non-row, no gridPos. MUST surface in missingGridPos.
		map[string]any{
			"id":    float64(5),
			"title": "Orphan no-gridPos",
		},
	}

	collected, missing := collectPanelsWithGridPos(synthetic)

	require.Lenf(t, collected, 3, "expected 3 panels with gridPos (A, B-nested, C-nested); collected=%v", collected)
	assert.Equalf(t, []string{"Orphan no-gridPos"}, missing,
		"expected Orphan no-gridPos in missingGridPos; the row panel must NOT surface here")

	// Confirm overlap is detectable on the synthetic input.
	var sawOverlap, sawOverflow bool

	for i := range collected {
		a := collected[i]
		require.Truef(t, a.fieldsValid, "synthetic input is well-formed; got invalid: %s", a.invalidReason)

		if a.x+a.w > 24 {
			sawOverflow = true
		}

		for j := i + 1; j < len(collected); j++ {
			b := collected[j]
			if rangesOverlap(a.x, a.x+a.w, b.x, b.x+b.w) && rangesOverlap(a.y, a.y+a.h, b.y, b.y+b.h) {
				sawOverlap = true
			}
		}
	}

	assert.True(t, sawOverlap, "collectPanelsWithGridPos + rangesOverlap must detect the deliberate Panel A vs Nested overlap")
	assert.True(t, sawOverflow, "Panel C is x=20 w=8 → x+w=28, must trip the 24-column ceiling")
}

// TestBuildPanelGridPos_TableDriven covers the validity matrix that
// buildPanelGridPos enforces. The overlap test gates on fieldsValid before
// running the 24-column / overlap checks; without this table-driven control
// a mutation to the validity rules (e.g. dropping the `w <= 0` branch, which
// would let a zero-width panel evade overlap detection silently) would pass
// every existing test.
func TestBuildPanelGridPos_TableDriven(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		gp          map[string]any
		wantValid   bool
		wantContain string // substring expected in invalidReason when !wantValid
	}{
		{
			name:      "valid origin",
			gp:        map[string]any{"x": 0.0, "y": 0.0, "w": 12.0, "h": 8.0},
			wantValid: true,
		},
		{
			// Forward-compat: if a future caller switches to json.Decoder.UseNumber()
			// (recommended for big-integer safety), every field comes back as
			// json.Number instead of float64. intOfOK supports both shapes; this
			// case pins that contract.
			name:      "json.Number values from UseNumber decoder",
			gp:        map[string]any{"x": json.Number("0"), "y": json.Number("0"), "w": json.Number("12"), "h": json.Number("8")},
			wantValid: true,
		},
		{
			name:        "json.Number that can't fit in int64",
			gp:          map[string]any{"x": json.Number("not-a-number"), "y": 0.0, "w": 12.0, "h": 8.0},
			wantValid:   false,
			wantContain: "xOK=false",
		},
		{
			name:        "missing x",
			gp:          map[string]any{"y": 0.0, "w": 12.0, "h": 8.0},
			wantValid:   false,
			wantContain: "xOK=false",
		},
		{
			name:        "string-typed w",
			gp:          map[string]any{"x": 0.0, "y": 0.0, "w": "12", "h": 8.0},
			wantValid:   false,
			wantContain: "wOK=false",
		},
		{
			name:        "negative x",
			gp:          map[string]any{"x": -1.0, "y": 0.0, "w": 12.0, "h": 8.0},
			wantValid:   false,
			wantContain: "negative coordinate",
		},
		{
			name:        "zero width — the silent-evasion case",
			gp:          map[string]any{"x": 0.0, "y": 0.0, "w": 0.0, "h": 8.0},
			wantValid:   false,
			wantContain: "non-positive size",
		},
		{
			name:        "zero height",
			gp:          map[string]any{"x": 0.0, "y": 0.0, "w": 12.0, "h": 0.0},
			wantValid:   false,
			wantContain: "non-positive size",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := buildPanelGridPos(tc.gp, "synthetic", 0)
			assert.Equalf(t, tc.wantValid, got.fieldsValid, "fieldsValid mismatch; invalidReason=%q", got.invalidReason)

			if !tc.wantValid {
				assert.Containsf(t, got.invalidReason, tc.wantContain,
					"invalidReason should explain the failure; got %q", got.invalidReason)
			}
		})
	}
}

type panelGridPos struct {
	title          string
	id, x, y, w, h int
	// fieldsValid records whether gridPos x/y/w/h were all present + numeric
	// + non-negative (w/h non-zero). The overlap test fails fast on invalid
	// gridPos rather than silently coercing missing fields to zero (which
	// would make a zero-width panel evade overlap detection AND satisfy
	// x+w<=24 trivially — a quiet schema regression).
	fieldsValid bool
	// invalidReason carries diagnostics when fieldsValid=false.
	invalidReason string
}

// collectPanelsWithGridPos flattens a panel tree (recursing into row-nested
// children with ABSOLUTE gridPos, not relative to the row) and reports
// non-row panels lacking gridPos. Recursion runs regardless of the parent's
// gridPos so a malformed/collapsed parent can't hide its nested children.
// missingGridPos catches schema regressions where a non-row panel drops its
// gridPos and would otherwise evade the overlap + fit-to-24 invariants.
func collectPanelsWithGridPos(panels []any) (withGridPos []panelGridPos, missingGridPos []string) {
	for _, p := range panels {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}

		id, _ := pm["id"].(float64)
		title, _ := pm["title"].(string)
		panelType, _ := pm["type"].(string)

		if gp, ok := pm["gridPos"].(map[string]any); ok {
			withGridPos = append(withGridPos, buildPanelGridPos(gp, title, int(id)))
		} else if panelType != "row" {
			label := title
			if label == "" {
				label = fmt.Sprintf("id=%d", int(id))
			}

			missingGridPos = append(missingGridPos, label)
		}

		if nested, ok := pm["panels"].([]any); ok && len(nested) > 0 {
			nestedWith, nestedMissing := collectPanelsWithGridPos(nested)
			withGridPos = append(withGridPos, nestedWith...)
			missingGridPos = append(missingGridPos, nestedMissing...)
		}
	}

	return withGridPos, missingGridPos
}

func buildPanelGridPos(gp map[string]any, title string, id int) panelGridPos {
	x, xOK := intOfOK(gp["x"])
	y, yOK := intOfOK(gp["y"])
	w, wOK := intOfOK(gp["w"])
	h, hOK := intOfOK(gp["h"])

	p := panelGridPos{title: title, id: id, x: x, y: y, w: w, h: h}

	switch {
	case !xOK || !yOK || !wOK || !hOK:
		p.invalidReason = fmt.Sprintf("gridPos field(s) missing/non-numeric: xOK=%v yOK=%v wOK=%v hOK=%v", xOK, yOK, wOK, hOK)
	case x < 0 || y < 0:
		p.invalidReason = fmt.Sprintf("gridPos negative coordinate: x=%d y=%d", x, y)
	case w <= 0 || h <= 0:
		// w/h <= 0 lets a panel evade overlap detection silently — flag it.
		p.invalidReason = fmt.Sprintf("gridPos non-positive size: w=%d h=%d", w, h)
	default:
		p.fieldsValid = true
	}

	return p
}

// intOfOK returns (int(v), true) for a JSON-decoded number — both `float64`
// (default from json.Unmarshal) and `json.Number` (from UseNumber) — and
// (0, false) otherwise. Distinguishes missing/non-numeric from explicit
// zero so the gridPos validity check fails on schema regressions instead
// of coercing missing fields to a value that often avoids overlap.
func intOfOK(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}

		return int(i), true
	}

	return 0, false
}

// rangesOverlap reports whether the half-open intervals [a1,a2) and [b1,b2)
// share any value. Same semantics as Grafana's gridPos (x..x+w / y..y+h are
// inclusive-start, exclusive-end).
func rangesOverlap(a1, a2, b1, b2 int) bool {
	return a1 < b2 && b1 < a2
}

// resolvePackagePath joins a relative path with the package directory so
// resolution is cwd-independent — required because the redistransport
// `grafana/datasources.yml` shares its name with a Prometheus-only file
// at the project root and the wrong one would otherwise resolve from a
// root-cwd invocation. Logs (but does not fail) when the resolved path
// is missing or is a directory; the caller's ReadFile surfaces the error.
func resolvePackagePath(t *testing.T, relPath string) string {
	t.Helper()

	dir := packageDir(t)
	abs := filepath.Join(dir, relPath)

	if info, err := os.Stat(abs); err != nil {
		t.Logf("resolvePackagePath: %q (resolved to %q) stat error: %v — caller's ReadFile will surface the failure", relPath, abs, err)
	} else if info.IsDir() {
		t.Logf("resolvePackagePath: %q (resolved to %q) is a directory; caller probably wanted a file", relPath, abs)
	}

	return abs
}

// packageDir returns the directory containing this source file via
// runtime.Caller(0). Used to anchor relative paths against the package
// dir rather than the test-invocation cwd.
func packageDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.Truef(t, ok, "runtime.Caller(0) failed — cannot anchor package-relative paths")

	return filepath.Dir(file)
}
