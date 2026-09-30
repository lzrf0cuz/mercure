#!/usr/bin/env bash
#
# Fail when a tracked Caddyfile is not in `caddy fmt` form.
# Usage: scripts/check-caddyfile-fmt.sh <path-to-mercure-binary>

set -euo pipefail

bin="${1:?usage: check-caddyfile-fmt.sh <mercure-binary>}"
cd "$(git rev-parse --show-toplevel)"

bad=()
while IFS= read -r file; do
	if ! "$bin" fmt "$file" 2>/dev/null | diff -u "$file" -; then
		bad+=("$file")
	fi
done < <(git ls-files '*Caddyfile*')

if ((${#bad[@]} > 0)); then
	printf 'Not in caddy fmt form: %s\n' "${bad[*]}" >&2
	printf 'Fix: %s fmt --overwrite <file>\n' "$bin" >&2
	exit 1
fi
