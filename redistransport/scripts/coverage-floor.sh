#!/bin/sh
# Fail when the total statement coverage in a Go coverage profile is below a
# floor, used by `task rt:test` and `task rt:test:caddy`. Run it from the module
# directory the profile was written in, so `go tool cover` can resolve the
# profile's packages. Prints the measured total and the floor either way.
#
# Usage: coverage-floor.sh <coverprofile> <floor-percent>
set -eu

[ "$#" -eq 2 ] || {
	echo "usage: $0 <coverprofile> <floor-percent>" >&2
	exit 2
}
profile=$1
floor=$2
printf '%s\n' "$floor" | grep -Eq '^[0-9]+(\.[0-9]+)?$' || {
	echo "coverage: floor must be a number of percent such as 90.0, got '$floor'" >&2
	exit 2
}

report=$(go tool cover -func="$profile")
total=$(printf '%s\n' "$report" | awk '$1 == "total:" { sub(/%$/, "", $NF); print $NF }')
[ -n "$total" ] || {
	echo "coverage: no total line in the go tool cover -func report for $profile" >&2
	exit 1
}

if awk -v total="$total" -v floor="$floor" 'BEGIN { exit !(total + 0 >= floor + 0) }'; then
	echo "coverage: $total% of statements (floor $floor%)"
else
	echo "coverage: $total% of statements is below the $floor% floor" >&2
	exit 1
fi
