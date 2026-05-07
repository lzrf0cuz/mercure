package caddy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProvisionCapturesMetricsRegistryBeforeWithValue locks the capture-order
// invariant for the `caddy.Context.WithValue` quirk documented at the head of
// Mercure.Provision: every call to ctx.WithValue returns a fresh Context that
// drops the embedded metricsRegistry, so ctx.GetMetricsRegistry() must be
// captured BEFORE the first WithValue or the transport binds against a typed
// nil registerer and every mercure_redis_* series silently goes missing.
//
// The end-to-end path is already covered by
// TestBindTransportMetricsRealRedisTransport against a real *RedisTransport,
// but that test bypasses Provision and calls bindTransportMetrics directly.
// A refactor that moved the GetMetricsRegistry() capture below ctx.WithValue
// (or removed it entirely) would not fail the e2e test, only the production
// scrape. This guard catches the structural regression at compile time.
//
// Source inspection mirrors the rt/caddy
// TestBuildOptionsWiresPrometheusRegisterer pattern — constructing a real
// caddy.Context is heavy and brittle relative to the precise invariant being
// guarded (the capture must precede the first WithValue call).
func TestProvisionCapturesMetricsRegistryBeforeWithValue(t *testing.T) {
	t.Parallel()

	src, err := os.ReadFile("mercure.go")
	require.NoError(t, err)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "mercure.go", src, 0)
	require.NoError(t, err, "mercure.go must parse")

	provision := findMercureProvision(file)
	require.NotNil(t, provision, "(*Mercure).Provision must exist")
	require.NotNil(t, provision.Body, "Provision body must not be nil")

	ctxParam := firstParamName(provision)
	require.NotEmpty(t, ctxParam, "Provision must declare a named first parameter for the caddy.Context — an anonymous (unnamed or `_`) first parameter cannot be referenced inside the body, so the body-level AST guard below has nothing to match against")

	pos := findCaptureOrderPositions(provision.Body, ctxParam)
	require.Positive(t, pos.GetMetricsRegistry, "Provision must call %s.GetMetricsRegistry() — moving the capture into bindTransportMetrics re-introduces the silent-metrics bug", ctxParam)
	require.Positive(t, pos.WithValue, "Provision must call %s.WithValue at least once — the test premise assumes the WithValue chain still exists", ctxParam)
	require.Less(t, pos.GetMetricsRegistry, pos.WithValue,
		"%s.GetMetricsRegistry() capture must precede the first %s.WithValue call; caddy.Context.WithValue drops the embedded metricsRegistry, so capturing afterward yields a typed-nil *prometheus.Registry and bindTransportMetrics silently no-ops",
		ctxParam, ctxParam)
}

// captureOrderPositions names the two AST anchors the capture-order guard
// asserts on. Naming the fields prevents an accidental positional swap at the
// call site from inverting the require.Less comparison into a vacuous pass.
type captureOrderPositions struct {
	GetMetricsRegistry token.Pos
	WithValue          token.Pos
}

// findMercureProvision returns the *Mercure receiver's Provision method, or
// nil if absent. Matching on both receiver type and method name guards
// against a future free function or sibling receiver also named "Provision"
// silently displacing the test target.
func findMercureProvision(file *ast.File) *ast.FuncDecl {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Provision" || !hasMercureReceiver(fn) {
			continue
		}

		return fn
	}

	return nil
}

// hasMercureReceiver reports whether fn is a method on *Mercure (the only
// receiver type whose Provision method this guard cares about).
func hasMercureReceiver(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}

	star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}

	ident, ok := star.X.(*ast.Ident)

	return ok && ident.Name == "Mercure"
}

// firstParamName returns the source-level name of fn's first parameter, or
// "" if the function is parameterless, the first parameter is unnamed, or
// the first parameter is the blank identifier `_`. The guard tracks the
// actual parameter name (`ctx`, `c`, …) rather than hardcoding it — a
// rename refactor surfaces a meaningful "no matching call" failure instead
// of a silent false-pass. The blank-identifier case is treated as anonymous
// because `_` cannot be referenced inside the body, so a body-level AST
// guard has nothing to anchor against.
func firstParamName(fn *ast.FuncDecl) string {
	if fn.Type == nil || fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
		return ""
	}

	first := fn.Type.Params.List[0]
	if len(first.Names) == 0 {
		return ""
	}

	name := first.Names[0].Name
	if name == "_" {
		return ""
	}

	return name
}

// findCaptureOrderPositions walks the function body and records the position
// of the FIRST `<ctxParam>.GetMetricsRegistry()` and the FIRST
// `<ctxParam>.WithValue(...)` calls. token.NoPos in either field means the
// corresponding call was not found, which the caller surfaces via
// require.Positive.
//
// Known limitation: this matcher anchors on the literal identifier ctxParam.
// An alias-shadow refactor like `c := ctx; c.WithValue(...)` (or one that
// extracts the captures into a helper accepting a `caddy.Context` parameter)
// makes the calls disappear from this function's body, surfacing as a
// require.Positive failure with a "missing call" diagnostic rather than the
// real "you broke the alias/helper" cause. Acceptable trade-off given the
// helper's single purpose; document the alias-rename in the PR description
// when it happens.
func findCaptureOrderPositions(body *ast.BlockStmt, ctxParam string) captureOrderPositions {
	var pos captureOrderPositions

	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		ident, ok := sel.X.(*ast.Ident)
		if !ok || ident.Name != ctxParam {
			return true
		}

		switch sel.Sel.Name {
		case "GetMetricsRegistry":
			if pos.GetMetricsRegistry == token.NoPos {
				pos.GetMetricsRegistry = call.Pos()
			}
		case "WithValue":
			if pos.WithValue == token.NoPos {
				pos.WithValue = call.Pos()
			}
		}

		return true
	})

	return pos
}

// TestProvisionWorkaroundCommentSurvives keeps the load-bearing comment block
// from being dropped by a future cleanup. The block explains *why* the
// capture order matters (caddy v2.11.x context.go quirk); without it, a
// reviewer who sees an "unused" early capture might reorder it back into the
// silent-failure shape. Matched on two anchored phrases — the canonical name
// of the framework quirk (`caddy.Context.WithValue`) and the load-bearing
// constraint phrase (`BEFORE`) — so wordsmithing the rest of the block is
// fine but deleting the warning is not.
func TestProvisionWorkaroundCommentSurvives(t *testing.T) {
	t.Parallel()

	src, err := os.ReadFile("mercure.go")
	require.NoError(t, err)

	body := string(src)
	require.Contains(t, body, "caddy.Context.WithValue",
		"Provision must keep the comment naming the caddy.Context.WithValue quirk — dropping it invites a refactor that re-introduces the silent-metrics bug")
	require.Contains(t, body, "BEFORE any WithValue",
		"Provision must keep the comment phrase 'BEFORE any WithValue' anchoring the capture-order constraint — the AST guard alone reports symptoms, not the why")
}
