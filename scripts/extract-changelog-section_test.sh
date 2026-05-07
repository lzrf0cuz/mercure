#!/usr/bin/env bash
#
# Tests for scripts/extract-changelog-section.awk.
#
# Usage: bash scripts/extract-changelog-section_test.sh
# Or via task: task scripts:test
#
# Each test sets up a CHANGELOG fixture and asserts the awk script
# extracts (or rejects) the expected output. Adds 1 line per test so
# regressions point at the failing assertion number.

set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/extract-changelog-section.awk"
test -f "$SCRIPT" || {
	echo "FAIL: $SCRIPT not found"
	exit 1
}

FIXTURE=$(mktemp -t extract-changelog-test.XXXXXX)
trap 'rm -f "$FIXTURE"' EXIT

pass=0
fail=0
assert() {
	local name="$1" expected="$2" actual="$3"
	if [ "$expected" = "$actual" ]; then
		pass=$((pass + 1))
	else
		fail=$((fail + 1))
		printf "FAIL [%s]\n  expected: %q\n  actual:   %q\n" "$name" "$expected" "$actual" >&2
	fi
}
assert_rc() {
	local name="$1" expected="$2" actual="$3"
	if [ "$expected" = "$actual" ]; then
		pass=$((pass + 1))
	else
		fail=$((fail + 1))
		echo "FAIL [$name] expected rc=$expected, got rc=$actual" >&2
	fi
}

# ---- Fixture 1: multi-section CHANGELOG with reference definitions at EOF.
cat >"$FIXTURE" <<'EOF'
# Changelog

## [0.0.2] — 2026-06-01

### Added
- New feature

## [0.0.1] — 2026-04-27

### Added
- Old feature

[0.0.2]: https://example.com/0.0.2
[0.0.1]: https://example.com/0.0.1
EOF

# Test 1: check mode succeeds for existing version
set +e
awk -v v=v0.0.1 -v mode=check -f "$SCRIPT" "$FIXTURE"
rc=$?
set -e
assert_rc "1: check existing returns 0" 0 "$rc"

# Test 2: check mode fails for missing version
set +e
awk -v v=v9.9.9 -v mode=check -f "$SCRIPT" "$FIXTURE" 2>/dev/null
rc=$?
set -e
assert_rc "2: check missing returns 1" 1 "$rc"

# Test 3: extract final section — must NOT include trailing reference defs
out=$(awk -v v=v0.0.1 -f "$SCRIPT" "$FIXTURE")
case "$out" in *"[0.0.1]: https"*)
	fail=$((fail + 1))
	echo "FAIL [3]: ref-def bled into final section" >&2
	;;
*) pass=$((pass + 1)) ;; esac

# Test 4: extract middle section — must NOT bleed into next ## heading
out=$(awk -v v=v0.0.2 -f "$SCRIPT" "$FIXTURE")
case "$out" in *"Old feature"*)
	fail=$((fail + 1))
	echo "FAIL [4]: extract bled into next section" >&2
	;;
*) pass=$((pass + 1)) ;; esac
case "$out" in *"New feature"*) pass=$((pass + 1)) ;; *)
	fail=$((fail + 1))
	echo "FAIL [4b]: extract missed expected content" >&2
	;;
esac

# Test 5: missing -v v errors out with rc=2
set +e
awk -f "$SCRIPT" "$FIXTURE" >/dev/null 2>&1
rc=$?
set -e
assert_rc "5: missing -v errors rc=2" 2 "$rc"

# Test 6: unknown mode errors out with rc=2
set +e
awk -v v=v0.0.1 -v mode=invalid -f "$SCRIPT" "$FIXTURE" >/dev/null 2>&1
rc=$?
set -e
assert_rc "6: unknown mode errors rc=2" 2 "$rc"

# ---- Fixture 2: pre-release version (regex-metachar escape coverage).
cat >"$FIXTURE" <<'EOF'
## [1.0.0-rc.1] — 2026-06-01

### Added
- Pre-release feature

## [0.9.0]

### Added
- Old feature
EOF

# Test 7: pre-release version extracts the right section (regex escape)
out=$(awk -v v=v1.0.0-rc.1 -f "$SCRIPT" "$FIXTURE")
case "$out" in *"Pre-release feature"*) pass=$((pass + 1)) ;; *)
	fail=$((fail + 1))
	echo "FAIL [7]: pre-release version not extracted" >&2
	;;
esac
case "$out" in *"Old feature"*)
	fail=$((fail + 1))
	echo "FAIL [7b]: pre-release extract bled" >&2
	;;
*) pass=$((pass + 1)) ;; esac

# Test 8: dot in version doesn't match unrelated chars (e.g., v1.0.0 must NOT match v1X0X0)
cat >"$FIXTURE" <<'EOF'
## [1X0X0] — 2026-06-01

### Added
- Wrong section
EOF
set +e
awk -v v=v1.0.0 -v mode=check -f "$SCRIPT" "$FIXTURE" 2>/dev/null
rc=$?
set -e
assert_rc "8: dot regex-escape prevents false match" 1 "$rc"

# ---- Fixture 3: build metadata version.
cat >"$FIXTURE" <<'EOF'
## [0.0.1+build.123] — 2026-06-01

### Added
- Build metadata version
EOF

# Test 9: build metadata version extracts (+ is regex metachar, must be escaped)
out=$(awk -v v=v0.0.1+build.123 -f "$SCRIPT" "$FIXTURE")
case "$out" in *"Build metadata version"*) pass=$((pass + 1)) ;; *)
	fail=$((fail + 1))
	echo "FAIL [9]: build-metadata version not extracted" >&2
	;;
esac

# ---- Fixture 4: blank section body.
cat >"$FIXTURE" <<'EOF'
## [0.0.1] — 2026-06-01

## [0.0.0] — older

### Added
- Has content
EOF

# Test 10: blank section produces empty output (test -s in caller would catch)
out=$(awk -v v=v0.0.1 -f "$SCRIPT" "$FIXTURE")
assert "10: blank section yields empty" "" "$out"

# ---- Fixture 5: leading + trailing blank-line trim.
cat >"$FIXTURE" <<'EOF'
## [0.0.1] — 2026-06-01



- First content
- Second content


## [0.0.0] — older
EOF

# Test 11: leading blanks (between heading and first content) are trimmed
out=$(awk -v v=v0.0.1 -f "$SCRIPT" "$FIXTURE")
expected=$(printf -- '- First content\n- Second content')
assert "11: leading+trailing blanks trimmed" "$expected" "$out"

# ---- Fixture 6: section terminated by reference-definition (final section).
cat >"$FIXTURE" <<'EOF'
## [0.0.1] — 2026-06-01

### Added
- Real content

[0.0.1]: https://example.com/0.0.1
EOF

# Test 12: ref-def exit rule fires (final section with no subsequent ## heading)
out=$(awk -v v=v0.0.1 -f "$SCRIPT" "$FIXTURE")
case "$out" in *"[0.0.1]: https"*)
	fail=$((fail + 1))
	echo "FAIL [12]: ref-def not excluded from final section" >&2
	;;
*) pass=$((pass + 1)) ;; esac
case "$out" in *"Real content"*) pass=$((pass + 1)) ;; *)
	fail=$((fail + 1))
	echo "FAIL [12b]: content missing" >&2
	;;
esac

# ---- Summary
echo
echo "extract-changelog-section.awk: $pass passed, $fail failed."
if [ "$fail" -gt 0 ]; then exit 1; fi
