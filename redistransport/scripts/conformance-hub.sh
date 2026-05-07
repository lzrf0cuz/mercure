#!/bin/sh
# Start/stop/wait for the background hub used by `task rt:conformance`.
#
# Why a script instead of an inline `cmd:`? go-task's embedded shell (mvdan/sh)
# reports `$!` as a job id ("g1"), not the OS PID, so a hub backgrounded from a
# go-task `cmd:` cannot have its PID captured there — the teardown then can't
# kill it and a hub leaks holding :80/:443/:2019 into later runs. A real /bin/sh
# (this script) reports a numeric `$!`, making the kill deterministic. The
# process-name pkill is kept only as a fallback for a stale/non-numeric pidfile.
#
# Usage:
#   conformance-hub.sh start <workdir> <pidfile> <binary> [args...]
#   conformance-hub.sh stop  <pidfile> [identity_pattern]
#   conformance-hub.sh wait  <url> <want_http_code> [tries] [pidfile]
#
# `start` records the PID and does a brief liveness check that catches a hub
# dying immediately (bad binary / instant bind failure), printing its log tail.
# `wait` is the steady-state readiness gate: it polls <url> until it returns
# <want_http_code> and, when given <pidfile>, fails immediately if the captured
# PID dies — so a hub that crashes on bind (e.g. :443/:80/:2019 already held by
# another local stack) fails fast instead of polling a corpse for the full
# budget. The conformance hub answers a bare GET on /.well-known/mercure (the
# anonymous subscribe handler) with HTTP 400 (missing "topic"); an auth-gated
# hub answers 401 before that check — so 400 marks a serving anonymous hub.
set -eu

action=${1:-}
case "$action" in
start)
	[ "$#" -ge 4 ] || {
		echo "start: need <workdir> <pidfile> <binary> [args...]" >&2
		exit 2
	}
	workdir=$2
	pidfile=$3
	shift 3
	cd "$workdir" || {
		echo "start: workdir not found: $workdir" >&2
		exit 1
	}
	# Redirect the hub's output to a file, NOT the inherited (go-task) pipe.
	# go-task closes a cmd's stdout pipe when it advances to the next cmd; a
	# backgrounded hub still wired to that pipe takes SIGPIPE on its next log
	# write and dies before the tests run. A file FD survives the cmd boundary.
	"$@" >"${pidfile%.pid}.log" 2>&1 &
	pid=$!
	echo "$pid" >"$pidfile"
	# Surface an immediate death (bad binary, instant "address already in use")
	# AT THE SOURCE with the hub's own log, instead of leaving the whole burden
	# to `wait` (which only fires when wired with the pidfile and can race).
	sleep 1
	if ! kill -0 "$pid" 2>/dev/null; then
		echo "start: hub exited immediately — tail of ${pidfile%.pid}.log:" >&2
		tail -n 15 "${pidfile%.pid}.log" >&2 || true
		exit 1
	fi
	;;
stop)
	pidfile=${2:-}
	pattern=${3:-}
	killed=0
	if [ -n "$pidfile" ] && [ -f "$pidfile" ]; then
		pid=$(cat "$pidfile" 2>/dev/null || true)
		case "$pid" in
		'' | *[!0-9]*) : ;; # non-numeric (e.g. go-task "g1") — skip the pid kill
		*)
			# Guard against PID reuse on the fixed-path pidfile: kill only if the
			# PID is alive AND (when an identity pattern is given) its command line
			# still matches the hub — a recycled PID could be any process.
			if kill -0 "$pid" 2>/dev/null &&
				{ [ -z "$pattern" ] || ps -p "$pid" -o command= 2>/dev/null | grep -Eq "$pattern"; }; then
				kill "$pid" 2>/dev/null || true
				killed=1
			fi
			;;
		esac
		rm -f "$pidfile"
	fi
	# Fallback ONLY when the pid kill did not fire (stale/non-numeric pidfile):
	# reap a hub that escaped the pidfile, matched by its command-line pattern.
	if [ "$killed" -eq 0 ] && [ -n "$pattern" ]; then
		pkill -f "$pattern" 2>/dev/null || true
	fi
	;;
wait)
	[ "$#" -ge 3 ] || {
		echo "wait: need <url> <want_http_code> [tries] [pidfile]" >&2
		exit 2
	}
	url=$2
	want=$3
	tries=${4:-30}
	wpidfile=${5:-}
	# Surface the hub's log (start redirects its output there) in failure
	# diagnostics — it's the only place a startup/config error is recorded.
	hublog=""
	[ -n "$wpidfile" ] && hublog=" (hub log: ${wpidfile%.pid}.log)"
	start_ts=$(date +%s 2>/dev/null || echo 0)
	i=0
	code=""
	while [ "$i" -lt "$tries" ]; do
		# Fail fast if the hub we backgrounded already died (don't poll a corpse).
		if [ -n "$wpidfile" ] && [ -f "$wpidfile" ]; then
			wpid=$(cat "$wpidfile" 2>/dev/null || true)
			case "$wpid" in
			'' | *[!0-9]*) : ;;
			*)
				if ! kill -0 "$wpid" 2>/dev/null; then
					echo "wait: hub (pid $wpid) exited before becoming ready at $url$hublog" >&2
					exit 1
				fi
				;;
			esac
		fi
		# --connect-timeout bounds a wedged connect (a degraded loopback can hang
		# past --max-time otherwise); both keep a single poll from blocking.
		code=$(curl -sk -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 2 "$url" 2>/dev/null || true)
		if [ "$code" = "$want" ]; then
			exit 0
		fi
		i=$((i + 1))
		sleep 0.5 2>/dev/null || sleep 1 # fractional sleep is GNU/BSD/macOS; fall back to integer
	done
	elapsed=$(($(date +%s 2>/dev/null || echo 0) - start_ts))
	echo "wait: $url did not return HTTP $want after ${elapsed}s ($tries tries; last: ${code:-none});" \
		"hub failed to start, :443/:80/:2019 held by another stack," \
		"or a TLS/config error — re-run: curl -v $url$hublog" >&2
	exit 1
	;;
*)
	echo "usage: $0 {start <workdir> <pidfile> <binary> [args...]|stop <pidfile> [pattern]|wait <url> <code> [tries] [pidfile]}" >&2
	exit 2
	;;
esac
