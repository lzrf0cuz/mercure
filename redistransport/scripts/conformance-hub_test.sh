#!/bin/sh
# Tests for conformance-hub.sh — the start/stop lifecycle that `task rt:conformance`
# relies on. Regression guard for the go-task `$!`-is-"g1" foot-gun: the hub PID
# must be captured by a real shell (numeric) and the teardown must reap the hub
# even when the pidfile is non-numeric.
set -eu

here=$(
	unset CDPATH
	cd "$(dirname "$0")" && pwd
)
script="$here/conformance-hub.sh"
pidfile=$(mktemp)
rm -f "$pidfile" # start is responsible for creating it

httpd=""
cleanup() {
	pkill -f 'sleep 31847' 2>/dev/null || true # unique marker procs, if any survived
	pkill -f 'sleep 31848' 2>/dev/null || true
	if [ -n "$httpd" ]; then
		kill "$httpd" 2>/dev/null || true
		wait "$httpd" 2>/dev/null || true
	fi
	rm -f "$pidfile"
}
trap cleanup EXIT

fail() {
	echo "FAIL: $1" >&2
	exit 1
}

# --- start captures a REAL numeric PID and the process is alive ---
"$script" start /tmp "$pidfile" sleep 31847
[ -f "$pidfile" ] || fail "start did not create the pidfile"
pid=$(cat "$pidfile")
case "$pid" in
'' | *[!0-9]*) fail "pidfile is not a numeric PID: '$pid' (the go-task g1 bug)" ;;
esac
kill -0 "$pid" 2>/dev/null || fail "started process $pid is not alive"
echo "ok: start captured numeric PID $pid (alive)"

# --- start fails fast (non-zero) VIA THE LIVENESS CHECK when the hub dies ---
deathpf=$(mktemp)
rm -f "$deathpf"
if out=$("$script" start /tmp "$deathpf" ./conformance-hub-no-such-binary 2>&1); then
	fail "start should fail when the hub binary exits immediately"
fi
case "$out" in
*"exited immediately"*) : ;; # proves the failure came from the liveness check, not some other error
*) fail "start failed but NOT via the liveness check (vacuous): $out" ;;
esac
rm -f "$deathpf" "${deathpf%.pid}.log"
echo "ok: start fails fast (via liveness check) when the hub dies immediately"

# --- stop kills by PID and removes the pidfile ---
"$script" stop "$pidfile"
kill -0 "$pid" 2>/dev/null && fail "process $pid still alive after stop"
[ -f "$pidfile" ] && fail "pidfile not removed after stop"
echo "ok: stop killed PID $pid by pidfile and cleaned up"

# --- stop tolerates a non-numeric pidfile (the 'g1' case) without aborting,
#     and the pkill fallback reaps a hub whose PID was lost ---
"$script" start /tmp "$pidfile" sleep 31848
escaped=$(cat "$pidfile")
printf 'g1' >"$pidfile" # simulate go-task's broken $! capture
"$script" stop "$pidfile" 'sleep 31848'
kill -0 "$escaped" 2>/dev/null && fail "pkill fallback did not reap escaped hub $escaped"
[ -f "$pidfile" ] && fail "pidfile not removed on non-numeric stop"
echo "ok: stop tolerates non-numeric pidfile and pkill-fallback reaps the hub"

# --- stop honours the identity pattern: kills a PID whose cmdline matches ---
"$script" start /tmp "$pidfile" sleep 31849
idpid=$(cat "$pidfile")
"$script" stop "$pidfile" 'sleep 31849'
kill -0 "$idpid" 2>/dev/null && fail "stop did not kill the identity-matched hub $idpid"
echo "ok: stop kills a PID whose command line matches the identity pattern"

# --- PID-reuse safety: stop must NOT kill a numeric PID whose cmdline does
#     NOT match the pattern (the recycled-PID hazard) ---
sleep 300 &
innocent=$!
printf '%s' "$innocent" >"$pidfile" # pidfile names an unrelated live process
"$script" stop "$pidfile" 'mercure run -c .*conformance'
if ! kill -0 "$innocent" 2>/dev/null; then
	fail "stop killed an unrelated PID $innocent (PID-reuse hazard not guarded)"
fi
kill "$innocent" 2>/dev/null || true
wait "$innocent" 2>/dev/null || true # reap quietly
echo "ok: stop spares a reused PID whose command line does not match"

# --- wait: fails fast (non-zero) when nothing serves the URL ---
if "$script" wait "http://127.0.0.1:1/" 200 2 2>/dev/null; then
	fail "wait should have failed against an unreachable endpoint"
fi
echo "ok: wait fails fast on an unreachable endpoint"

# --- wait: with a pidfile, fails IMMEDIATELY when the hub PID is already dead
#     (don't poll a corpse for the full budget) ---
sleep 300 &
deadpid=$!
kill "$deadpid" 2>/dev/null || true
wait "$deadpid" 2>/dev/null || true
printf '%s' "$deadpid" >"$pidfile"
start_s=$(date +%s)
if "$script" wait "http://127.0.0.1:1/" 200 30 "$pidfile" 2>/dev/null; then
	fail "wait should fail when the hub PID is dead"
fi
took=$(($(date +%s) - start_s))
[ "$took" -le 3 ] || fail "wait did not fail fast on a dead hub PID (took ${took}s)"
echo "ok: wait fails fast (${took}s) when the captured hub PID is dead"

# --- wait: keeps polling then fails when the endpoint serves the WRONG code ---
if command -v python3 >/dev/null 2>&1; then
	wport=18472
	python3 -m http.server "$wport" --bind 127.0.0.1 >/dev/null 2>&1 &
	httpd=$!
	sleep 1
	kill -0 "$httpd" 2>/dev/null || fail "test http.server did not start on :$wport"

	"$script" wait "http://127.0.0.1:$wport/" 200 30 || fail "wait did not detect a ready endpoint"
	echo "ok: wait detects a ready endpoint (HTTP 200)"

	if "$script" wait "http://127.0.0.1:$wport/" 404 2 2>/dev/null; then
		fail "wait should fail when the endpoint returns the wrong code"
	fi
	echo "ok: wait fails when the endpoint serves the wrong HTTP code"

	kill "$httpd" 2>/dev/null || true
	wait "$httpd" 2>/dev/null || true # reap quietly (no job-control "Terminated" noise)
	httpd=""
else
	echo "skip: python3 unavailable — wait ready-path + wrong-code cases not exercised"
fi

echo "PASS"
