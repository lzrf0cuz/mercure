#!/usr/bin/env bash
#
# Tests for scripts/unwrap-markdown-paragraphs.awk.
#
# Usage: bash scripts/unwrap-markdown-paragraphs_test.sh

set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/unwrap-markdown-paragraphs.awk"
test -f "$SCRIPT" || {
	echo "FAIL: $SCRIPT not found"
	exit 1
}

pass=0
fail=0
assert() {
	local name="$1" expected="$2" actual="$3"
	if [ "$expected" = "$actual" ]; then
		pass=$((pass + 1))
	else
		fail=$((fail + 1))
		printf "FAIL [%s]\n  expected:\n%s\n  actual:\n%s\n" "$name" "$expected" "$actual" >&2
	fi
}

run() {
	printf '%s' "$1" | awk -f "$SCRIPT"
}

# 1. Plain paragraph join
assert "single-paragraph soft-break join" \
	"foo bar baz" \
	"$(run "foo
bar
baz")"

# 2. Two paragraphs separated by blank line
assert "blank-line preserves paragraph break" \
	"foo bar

baz qux" \
	"$(run "foo
bar

baz
qux")"

# 3. ATX header stays separate from following paragraph
assert "header preserved" \
	"## Install

mercure goes here" \
	"$(run "## Install

mercure
goes here")"

# 4. Code fence preserved verbatim (no joining inside)
assert "code fence preserves internal newlines" \
	'```caddyfile
mercure {
    transport redis
}
```

trailing paragraph' \
	"$(run '```caddyfile
mercure {
    transport redis
}
```

trailing paragraph')"

# 5. Sibling list items stay separate
assert "list siblings stay on separate lines" \
	"- alpha
- beta
- gamma" \
	"$(run "- alpha
- beta
- gamma")"

# 6. List item with continuation joins into the bullet
assert "list-item continuation joins" \
	"- first item continuation text" \
	"$(run "- first item
  continuation text")"

# 7. Horizontal rule preserved
assert "horizontal rule stays alone" \
	"above

---

below" \
	"$(run "above

---

below")"

# 8. Numbered list siblings stay separate, continuations join
assert "numbered list with continuation" \
	"1. one continued
2. two" \
	"$(run "1. one
   continued
2. two")"

# 9. Multiple paragraphs with mixed content
assert "mixed prelude with header" \
	"Intro sentence one. Intro sentence two.

## Section

- bullet alpha
- bullet beta with continuation" \
	"$(run "Intro sentence one.
Intro sentence two.

## Section

- bullet alpha
- bullet beta
  with continuation")"

# 10. Empty input → empty output (rc=0)
empty_output=$(printf '' | awk -f "$SCRIPT")
assert "empty input produces empty output" "" "$empty_output"

# 11. CRLF input is normalized — no \r embedded in joined lines
crlf_in=$(printf 'line one\r\nline two\r\n\r\npara two\r\n')
crlf_out=$(printf '%s' "$crlf_in" | awk -f "$SCRIPT")
if printf '%s' "$crlf_out" | grep -q $'\r'; then
	printf 'FAIL [CRLF normalization]: output contains a CR\n' >&2
	fail=$((fail + 1))
else
	pass=$((pass + 1))
fi

# 12. Tilde fence preserved verbatim (parallel to backtick fence)
assert "tilde code fence preserved" \
	'~~~go
package main
func main() {}
~~~' \
	"$(run '~~~go
package main
func main() {}
~~~')"

# 13. Tilde and backtick fences don't cross-close
tilde_then_backtick=$(printf '%s\n' '~~~' 'in tilde' '```' 'still in tilde' '~~~')
assert "mismatched closer does not end fence" \
	'~~~
in tilde
```
still in tilde
~~~' \
	"$(printf '%s' "$tilde_then_backtick" | awk -f "$SCRIPT")"

# 14. Unclosed fence exits 2 (loud failure)
unclosed_rc=0
printf '%s\n' '```bash' 'open but no close' | awk -f "$SCRIPT" >/dev/null 2>&1 || unclosed_rc=$?
if [ "$unclosed_rc" -eq 2 ]; then
	pass=$((pass + 1))
else
	printf 'FAIL [unclosed fence]: expected exit 2, got %s\n' "$unclosed_rc" >&2
	fail=$((fail + 1))
fi

# 15. Nested list bullet preserves its leading indent — visual nesting
# survives. Inner-item CONTINUATION (the third source line) flattens into the
# nested bullet (documented limitation), but the nested bullet does NOT
# collapse into the parent's line.
assert "nested list bullet keeps indent" \
	"- top item
  - nested item with continuation
- another top item" \
	"$(run "- top item
  - nested item
    with continuation
- another top item")"

# 16-17. END-rule byte contract: output ends with exactly one trailing
# newline. `out=$(cmd; printf x); out=${out%x}` preserves the trailing \n
# that `$(...)` would otherwise strip — so a regression to zero or two
# trailing newlines is detectable. Tests 1-15 use the strip-trailing run()
# helper; these two go around it.
nl_out=$({
	printf '%s' "single line" | awk -f "$SCRIPT"
	printf x
})
nl_out=${nl_out%x}
assert "single-line output has exactly one trailing newline" \
	$'single line\n' \
	"$nl_out"

multi_out=$({
	printf '%s' "para one
line two

para two" | awk -f "$SCRIPT"
	printf x
})
multi_out=${multi_out%x}
assert "multi-paragraph output ends with one trailing newline" \
	$'para one line two\n\npara two\n' \
	"$multi_out"

# Reject-loud tests: each shape exits 2 AND the stderr message contains the
# expected diagnostic substring. Asserting the message protects against
# regressions that mangle the operator-facing recovery advice.
reject() {
	local name="$1" input="$2" expected_substr="$3"
	local rc=0 stderr
	stderr=$(printf '%s\n' "$input" | awk -f "$SCRIPT" 2>&1 >/dev/null) || rc=$?
	if [ "$rc" -eq 2 ] && printf '%s' "$stderr" | grep -q -- "$expected_substr"; then
		pass=$((pass + 1))
	else
		printf 'FAIL [%s]: rc=%s, stderr=%q\n' "$name" "$rc" "$stderr" >&2
		fail=$((fail + 1))
	fi
}

# 18. Blockquote rejected (with space after `>` per CommonMark)
reject "blockquote rejected" "> quoted line" "blockquote"

# 18b. `>=` NOT rejected (would be a false-positive — common in prose)
ge_out=$(printf '%s\n' "Latency >= 100ms is now common." | awk -f "$SCRIPT" 2>&1)
if printf '%s' "$ge_out" | grep -q "blockquote"; then
	printf 'FAIL [>= operator not falsely rejected as blockquote]\n' >&2
	fail=$((fail + 1))
else
	pass=$((pass + 1))
fi

# 19. Table rejected
reject "table rejected" "| col1 | col2 |" "GFM table"

# 20. Hard line break (trailing two spaces) rejected
reject "hard line break rejected" "line with trailing two spaces  " "hard-break"

# 21. Setext H1 underline rejected
reject "setext H1 underline rejected" "Title
===" "setext H1"

# 21b. Setext H2 / HR ambiguity rejected (`---` after non-blank line)
reject "setext H2 / HR ambiguity rejected" "Title
---" "ambiguous"

# 21c. Block-level HTML rejected (would break disclosure widget on flatten)
reject "block-level HTML rejected" "<details>
<summary>hello</summary>
content
</details>" "block-level HTML"

# 22. Indented code fence (1-3 spaces) treated as fence, not continuation
assert "fence with 2-space indent recognized" \
	"  \`\`\`
  inside code
  \`\`\`" \
	"$(run "  \`\`\`
  inside code
  \`\`\`")"

# 23. Rejected shapes ARE allowed inside code fences (documentation use).
# Four assertions covering all reject-rule types — guards against a
# rule-ordering regression that moves rejects before `in_code { print; next }`.
assert "blockquote inside code fence passes through" \
	'```markdown
> quote example
```' \
	"$(run '```markdown
> quote example
```')"

assert "table inside code fence passes through" \
	'```markdown
| col1 | col2 |
|------|------|
```' \
	"$(run '```markdown
| col1 | col2 |
|------|------|
```')"

assert "setext H1 inside code fence passes through" \
	'```markdown
Title
===
```' \
	"$(run '```markdown
Title
===
```')"

assert "block HTML inside code fence passes through" \
	'```html
<details><summary>X</summary>Y</details>
```' \
	"$(run '```html
<details><summary>X</summary>Y</details>
```')"

echo ""
echo "unwrap-markdown-paragraphs.awk: $pass passed, $fail failed."
test "$fail" -eq 0
