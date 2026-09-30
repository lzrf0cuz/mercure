#!/usr/bin/env bash
#
# Tests for scripts/tls-trust.sh.
#
# Usage: bash scripts/tls-trust_test.sh
#
# Runs the script with a stub `security` on PATH and a temporary HOME, so the
# real keychain and ~/.caddy-trust-* files are never touched. The stub logs its
# calls, prints the listing in $STUB_LISTING for `find-certificate`, and exits
# with $STUB_ADD_RC (default 0) for `add-trusted-cert`. `delete-certificate`
# exits with $STUB_DELETE_RC (default 0); on success it drops the hash from the
# listing.

set -euo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/tls-trust.sh"
test -f "$SCRIPT" || {
	echo "FAIL: $SCRIPT not found"
	exit 1
}

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$TMP/bin" "$TMP/home"
cat >"$TMP/bin/security" <<'STUB'
#!/bin/sh
echo "$*" >>"$STUB_LOG"
case "$1" in
find-certificate)
	# security exits 44 when nothing matches.
	[ -f "$STUB_LISTING" ] || exit 44
	cat "$STUB_LISTING"
	;;
add-trusted-cert) exit "${STUB_ADD_RC:-0}" ;;
delete-certificate)
	[ "${STUB_DELETE_RC:-0}" -eq 0 ] || exit "$STUB_DELETE_RC"
	# $3 is the hash after `-Z`.
	[ ! -f "$STUB_LISTING" ] || {
		grep -vxF "SHA-1 hash: $3" "$STUB_LISTING" >"$STUB_LISTING.new" || true
		mv "$STUB_LISTING.new" "$STUB_LISTING"
	}
	exit 0
	;;
esac
exit 0
STUB
chmod +x "$TMP/bin/security"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
	-keyout "$TMP/key.pem" -out "$TMP/cert.crt" -days 1 \
	-subj "/CN=Caddy Local Authority - test" 2>/dev/null
HASH=$(openssl x509 -in "$TMP/cert.crt" -noout -fingerprint -sha1 | sed 's/.*=//' | tr -d ':')
OTHER_HASH=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
SIBLING_HASH=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

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
# reset <hash>...: clear HOME and the call log, and list these hashes as the
# keychain contents (none given: an empty keychain).
reset() {
	rm -f "$TMP"/home/.caddy-trust-* "$STUB_LISTING" "$STUB_LOG"
	: >"$STUB_LOG"
	local h
	for h in "$@"; do
		printf 'SHA-1 hash: %s\n' "$h" >>"$STUB_LISTING"
	done
}
# run <args>...: run the script; sets $out and $rc.
run() {
	rc=0
	out=$(HOME="$TMP/home" PATH="$TMP/bin:$PATH" bash "$SCRIPT" "$@" 2>&1) || rc=$?
}
calls() { grep -c "^$1" "$STUB_LOG" || true; }

export STUB_LOG="$TMP/log" STUB_LISTING="$TMP/listing"

# (a) Hash file matches and the keychain holds the cert: skip.
reset "$HASH"
echo "$HASH" >"$TMP/home/.caddy-trust-local-sha1"
run trust local "$TMP/cert.crt"
assert "a: rc" 0 "$rc"
assert "a: skip message" "Certificate unchanged and already in the keychain, skipping the keychain update." "$out"
assert "a: no add" 0 "$(calls add-trusted-cert)"
assert "a: no delete" 0 "$(calls delete-certificate)"

# (b) Hash file matches but the keychain lacks the cert: delete the tracked
# certificate, add, and record it again.
reset "$OTHER_HASH"
echo "$HASH" >"$TMP/home/.caddy-trust-local-sha1"
run trust local "$TMP/cert.crt"
assert "b: rc" 0 "$rc"
assert "b: delete tracked" 1 "$(calls "delete-certificate -Z $HASH ")"
assert "b: add" 1 "$(calls add-trusted-cert)"
assert "b: hash file records the cert again" "$HASH" "$(cat "$TMP/home/.caddy-trust-local-sha1")"

# (c) No hash file and an untracked cert: NOTE lists it, add, rc 0, no delete.
reset "$OTHER_HASH"
run trust local "$TMP/cert.crt"
assert "c: rc" 0 "$rc"
assert "c: note lists the untracked cert" yes "$([[ $out == "NOTE: "*"(SHA-1: $OTHER_HASH)"* ]] && echo yes || echo no)"
assert "c: add" 1 "$(calls add-trusted-cert)"
assert "c: no delete" 0 "$(calls delete-certificate)"
assert "c: hash file" "$HASH" "$(cat "$TMP/home/.caddy-trust-local-sha1")"

# (d) The sibling's tracked hash and the new cert are not listed in the NOTE.
reset "$SIBLING_HASH" "$OTHER_HASH" "$HASH"
echo "$SIBLING_HASH" >"$TMP/home/.caddy-trust-docker-sha1"
run trust local "$TMP/cert.crt"
assert "d: note lists only the untracked cert" yes "$([[ $out == *"(SHA-1: $OTHER_HASH)"* ]] && echo yes || echo no)"

# (e) The add fails: the script exits 0 and writes no hash file.
reset
STUB_ADD_RC=1 run trust local "$TMP/cert.crt"
assert "e: rc" 0 "$rc"
assert "e: no hash file" no "$([ -f "$TMP/home/.caddy-trust-local-sha1" ] && echo yes || echo no)"

# (e2) The hash file records an older cert (the root was rotated) and the add
# succeeds: delete the recorded cert, then record the new one.
reset "$OTHER_HASH"
echo "$OTHER_HASH" >"$TMP/home/.caddy-trust-local-sha1"
run trust local "$TMP/cert.crt"
assert "e2: rc" 0 "$rc"
assert "e2: delete recorded" 1 "$(calls "delete-certificate -Z $OTHER_HASH ")"
assert "e2: hash file" "$HASH" "$(cat "$TMP/home/.caddy-trust-local-sha1")"

# (e3) The hash file records an older cert and the add fails: delete the old
# cert, exit 0, keep the old hash in the hash file, and report that the new cert
# is not trusted.
reset "$OTHER_HASH"
echo "$OTHER_HASH" >"$TMP/home/.caddy-trust-local-sha1"
STUB_ADD_RC=1 run trust local "$TMP/cert.crt"
assert "e3: rc" 0 "$rc"
assert "e3: delete recorded" 1 "$(calls "delete-certificate -Z $OTHER_HASH ")"
assert "e3: hash file still names the old cert" "$OTHER_HASH" "$(cat "$TMP/home/.caddy-trust-local-sha1")"
assert "e3: message" yes "$([[ $out == *"The new certificate is not trusted."* ]] && echo yes || echo no)"

# (e4) The hash file records an older cert and the delete fails, leaving it in
# the keychain: stop before the add, exit 0, leave the hash file unchanged, and
# report the failed delete.
reset "$OTHER_HASH"
echo "$OTHER_HASH" >"$TMP/home/.caddy-trust-local-sha1"
STUB_DELETE_RC=1 run trust local "$TMP/cert.crt"
assert "e4: rc" 0 "$rc"
assert "e4: no add" 0 "$(calls add-trusted-cert)"
assert "e4: hash file unchanged" "$OTHER_HASH" "$(cat "$TMP/home/.caddy-trust-local-sha1")"
assert "e4: message" yes "$([[ $out == *"Could not delete the certificate this task trusted before"* ]] && echo yes || echo no)"

# (f) check: recorded and in keychain, recorded but not in keychain, no hash
# file, and an extra argument (usage error, exit 2).
reset "$HASH"
echo "$HASH" >"$TMP/home/.caddy-trust-local-sha1"
run check
assert "f: recorded and in keychain" 0 "$rc"
reset "$OTHER_HASH"
echo "$HASH" >"$TMP/home/.caddy-trust-local-sha1"
run check
assert "f: recorded but not in keychain" 1 "$rc"
reset "$HASH"
run check
assert "f: no hash file" 1 "$rc"
run check extra
assert "f: extra argument" 2 "$rc"

# (g) trust docker writes the docker hash file. The local hash file is the
# sibling, and its hash is not listed in the NOTE.
reset "$SIBLING_HASH" "$OTHER_HASH"
echo "$SIBLING_HASH" >"$TMP/home/.caddy-trust-local-sha1"
run trust docker "$TMP/cert.crt"
assert "g: rc" 0 "$rc"
assert "g: note lists only the untracked cert, then Done" yes "$([[ $out == "NOTE: "*"(SHA-1: $OTHER_HASH)"*"Done. Docker"* ]] && echo yes || echo no)"
assert "g: docker hash file" "$HASH" "$(cat "$TMP/home/.caddy-trust-docker-sha1")"
assert "g: local hash file untouched" "$SIBLING_HASH" "$(cat "$TMP/home/.caddy-trust-local-sha1")"

echo "tls-trust_test: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
