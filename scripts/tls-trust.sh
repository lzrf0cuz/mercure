#!/usr/bin/env bash
#
# Trust a Caddy root CA in the macOS login keychain, or check that the local one
# is still there.
# Usage:
#   scripts/tls-trust.sh trust <local|docker> <cert.crt>
#   scripts/tls-trust.sh check
#
# `trust` records the certificate's SHA-1 in its hash file,
# ~/.caddy-trust-<kind>-sha1, after a successful add. It skips the keychain
# update only when that file matches and the keychain still holds the
# certificate, so a certificate deleted from the keychain is added again.
#
# Before the add, `trust` deletes the certificate recorded in the hash file. If
# that certificate is still in the keychain afterwards, `trust` stops before the
# add and leaves the hash file unchanged. `trust` reports that stop and a failed
# add on stderr, and exits 0 in both cases.
#
# `check` succeeds when the local certificate is recorded and still in the
# keychain. It does not read trust settings or compare the hash with the current
# certificate file.

set -euo pipefail

KEYCHAIN="$HOME/Library/Keychains/login.keychain-db"

# in_keychain <hash>: succeed when the keychain holds a Caddy Local Authority
# certificate with this SHA-1. The -Z flag of `security find-certificate` only
# prints hashes and cannot select by one, so list them and compare the exact
# line.
in_keychain() {
	local listing
	listing=$(security find-certificate -a -Z -c "Caddy Local Authority" "$KEYCHAIN" 2>/dev/null) || return 1
	grep -qxF "SHA-1 hash: $1" <<<"$listing"
}

# note_untracked_certs <new-hash> <sibling-hash-file>: print a note listing
# Caddy Local Authority certificates in the keychain other than the new
# certificate and the sibling task's tracked certificate.
note_untracked_certs() {
	local skip="$1" listing others
	[ -f "$2" ] && skip="$skip"$'\n'"$(cat "$2")"
	listing=$(security find-certificate -a -Z -c "Caddy Local Authority" "$KEYCHAIN" 2>/dev/null) || return 0
	others=$(sed -n 's/^SHA-1 hash: //p' <<<"$listing" | grep -vxF "$skip" | paste -sd ' ' -) || true
	if [ -n "$others" ]; then
		echo "NOTE: the keychain holds Caddy Local Authority certificates that neither tls:trust nor tls:trust:docker recorded (SHA-1: $others). They may belong to another project and are left in place."
	fi
}

usage() {
	echo "usage: tls-trust.sh trust <local|docker> <cert.crt>" >&2
	echo "       tls-trust.sh check" >&2
}

hash_file() {
	printf '%s/.caddy-trust-%s-sha1' "$HOME" "$1"
}

trust() {
	local kind="$1" cert="$2" other tag label file new_hash recorded
	case "$kind" in
	local)
		other=docker
		tag="tls:trust"
		label=Local
		;;
	docker)
		other=local
		tag="tls:trust:docker"
		label=Docker
		;;
	*)
		echo "unknown kind '$kind': expected local or docker" >&2
		return 2
		;;
	esac
	file=$(hash_file "$kind")
	new_hash=$(openssl x509 -in "$cert" -noout -fingerprint -sha1 | sed 's/.*=//' | tr -d ':')

	recorded=
	[ ! -f "$file" ] || recorded=$(cat "$file")

	if [ "$recorded" = "$new_hash" ] && in_keychain "$new_hash"; then
		echo "Certificate unchanged and already in the keychain, skipping the keychain update."
		return 0
	fi

	# Delete only the certificate recorded in this kind's hash file. Caddy names
	# every root "Caddy Local Authority - <year> ECC Root" by default, so another
	# project's CA may be trusted on purpose.
	if [ -f "$file" ]; then
		# The delete fails both when the recorded certificate is already gone and
		# when the keychain refuses it. Only in the first case does the add go
		# ahead. If the certificate is still there, stop, because a successful add
		# would overwrite the hash file and leave the old certificate trusted and
		# untracked.
		security delete-certificate -Z "$recorded" -t "$KEYCHAIN" 2>/dev/null || true
		if in_keychain "$recorded"; then
			echo "Could not delete the certificate this task trusted before (SHA-1 $recorded; headless or locked keychain?). $file is unchanged and the new certificate was not added. Run 'task $tag' from a GUI session." >&2
			return 0
		fi
	else
		note_untracked_certs "$new_hash" "$(hash_file "$other")"
	fi

	# Record the new hash only if the add succeeds. After a failed add, an
	# existing hash file still names the certificate that the delete above
	# removed, so the next `trust` run adds again and `check` fails.
	if security add-trusted-cert -r trustRoot -k "$KEYCHAIN" "$cert"; then
		echo "$new_hash" >"$file"
		echo "Done. $label Caddy root CA is now trusted in the login keychain."
	else
		echo "Could not add the trust setting (headless or locked keychain?). The new certificate is not trusted. Run 'task $tag' from a GUI session." >&2
	fi
}

check() {
	local file hash
	file=$(hash_file local)
	if [ ! -f "$file" ]; then
		echo "$file not found: the local Caddy root CA has not been trusted. Run 'task tls:trust'." >&2
		return 1
	fi
	hash=$(cat "$file")
	if ! in_keychain "$hash"; then
		echo "The local Caddy root CA recorded in $file (SHA-1 $hash) is not in the login keychain. Run 'task tls:trust'." >&2
		return 1
	fi
}

case "${1:-}" in
trust)
	[ "$#" -eq 3 ] || {
		usage
		exit 2
	}
	trust "$2" "$3"
	;;
check)
	[ "$#" -eq 1 ] || {
		usage
		exit 2
	}
	check
	;;
*)
	usage
	exit 2
	;;
esac
