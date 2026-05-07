#!/usr/bin/env bash
#
# Redistransport release lifecycle. Subcommands: tag | publish | create |
# update | delete. Required env: V (vX.Y.Z[-suffix]), REMOTE (default origin).
#
# Release body = optional `redistransport/releases/$V.pre.md` prelude +
# awk-extracted `## [$V]` section from `redistransport/CHANGELOG.md`. The
# CHANGELOG section is REQUIRED; the prelude is optional polish for first
# releases, major bumps, breaking changes, deprecations.
#
# `tag` and `publish` are split so CI can `publish` on a tag-push event
# without signing material — operator signs locally, CI just publishes.
#
# `--latest=false` is intentional: the fork doesn't publish the hub via
# GitHub releases, so nothing competes for "Latest"; preventing rt from
# claiming it keeps the repo's releases page from showing rt point releases
# as the canonical anchor. Audit this flag if the hub ever gets a GH release.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXTRACT_AWK="$SCRIPT_DIR/extract-changelog-section.awk"
UNWRAP_AWK="$SCRIPT_DIR/unwrap-markdown-paragraphs.awk"
CHANGELOG="$REPO_ROOT/redistransport/CHANGELOG.md"

: "${V:?V environment variable required}"
: "${REMOTE:=origin}"

# `[[ =~ ]]` for whole-string match. `grep -Eq` (per-line) lets
# `V=$'v0.0.1\nbad'` through. `git check-ref-format` catches `..`,
# embedded `@{`, control chars.
if ! [[ "$V" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.+-]+)?$ ]]; then
	echo "release.sh: V must be vX.Y.Z[-suffix] (got: $V)" >&2
	exit 1
fi
if ! git check-ref-format "refs/tags/redistransport/$V"; then
	echo "release.sh: V does not form a valid git ref: redistransport/$V" >&2
	exit 1
fi

# A REMOTE starting with `--` is parsed by `git ls-remote` as a flag, and
# `--upload-pack=<cmd>` would execute <cmd> locally.
if ! [[ "$REMOTE" =~ ^[A-Za-z][A-Za-z0-9._/-]*$ ]]; then
	echo "release.sh: REMOTE must be a simple git remote name (got: $REMOTE)" >&2
	exit 1
fi

TAG="redistransport/$V"
PRELUDE_FILE="$REPO_ROOT/redistransport/releases/$V.pre.md"

# Derive `owner/name` from $REMOTE so every `gh` call can pass `--repo`.
# Without it, gh's default-repo heuristic picks `upstream` on a fork.
# Strip `.git` BEFORE the regex: the owner/name char class includes `.`
# (legitimate in `golang.org`-style names), so an in-regex `(\.git)?$`
# alternative used to greedily capture `.git` as part of the name.
REPO=$(
	url=$(git remote get-url "$REMOTE" 2>/dev/null) || {
		echo "release.sh: cannot read URL for remote '$REMOTE'" >&2
		exit 1
	}
	url="${url%.git}"
	printf '%s' "$url" | sed -nE \
		-e 's#^git@github\.com:([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)$#\1#p' \
		-e 's#^ssh://git@github\.com(:[0-9]+)?/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)$#\2#p' \
		-e 's#^https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)$#\1#p'
)
if [[ ! "$REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
	echo "release.sh: $REMOTE is not a recognized github.com remote URL" >&2
	exit 1
fi

# Detect prereleases per SemVer: a `-` BEFORE any `+`. So `v1.2.3-rc.1`
# and `v1.2.3-rc.1+build.5` are prereleases; `v1.2.3+build-5` is NOT.
is_prerelease() {
	local before_plus="${V%%+*}"
	case "$before_plus" in *-*) return 0 ;; *) return 1 ;; esac
}

# Distinguish "release not found (404)" from "could not determine state".
# Sets `gh_release_view_state` to: present | absent | unknown
gh_release_view() {
	local out rc
	out=$(gh release view --repo "$REPO" "$1" 2>&1) && {
		gh_release_view_state=present
		return
	}
	rc=$?
	case "$out" in
	*"release not found"* | *"could not find any release"* | *"HTTP 404"* | *"Not Found"*)
		gh_release_view_state=absent
		;;
	*)
		echo "release.sh: gh release view '$1' failed unexpectedly (rc=$rc): $out" >&2
		gh_release_view_state=unknown
		;;
	esac
}

# Trap covers both tmpfiles (primary $NOTES + the unwrap pass's $UNWRAP_NOTES)
# so a SIGINT mid-awk doesn't leak either. CLOBBERS any prior EXIT trap; if
# you add one above, refactor to a composable cleanup function.
make_notes_file() {
	NOTES=""
	UNWRAP_NOTES=""
	trap '[ -n "${NOTES:-}" ] && rm -f "$NOTES"; [ -n "${UNWRAP_NOTES:-}" ] && rm -f "$UNWRAP_NOTES"' EXIT
	NOTES=$(mktemp "${TMPDIR:-/tmp}/rt-release-notes.XXXXXX")
}

# Splice $NOTES = optional prelude + awk-extracted CHANGELOG section.
# Track awk-output size independently so a present prelude can't mask a
# missing CHANGELOG section. awk runs inline (not via $()) so `set -e`
# catches partial-write failures.
splice_notes() {
	if [ -f "$PRELUDE_FILE" ]; then
		# Block tracking pixels / phishing vectors that could ride a prelude
		# PR into a published GitHub release. Markdown handles formatting
		# natively; legitimate preludes don't need raw HTML. Code-fence
		# interiors are allowed (operators may document HTML embedding).
		local html_hits
		html_hits=$(awk '
      /^(```|~~~)/ { in_code = !in_code; next }
      !in_code && $0 ~ /<(script|iframe|object|embed|form|img|svg|video|audio)/ {
        printf "  %d: %s\n", NR, $0
      }' "$PRELUDE_FILE")
		if [ -n "$html_hits" ]; then
			echo "release.sh: prelude $PRELUDE_FILE contains disallowed raw HTML tag(s) outside code fences:" >&2
			printf '%s\n' "$html_hits" >&2
			exit 1
		fi
		cat "$PRELUDE_FILE" >"$NOTES" || {
			echo "release.sh: failed to read prelude $PRELUDE_FILE" >&2
			exit 1
		}
		# Ensure newline so awk's `## [...]` header doesn't glue onto a
		# no-trailing-newline last line of the prelude.
		[ "$(tail -c 1 "$NOTES" | wc -l)" -eq 1 ] || printf '\n' >>"$NOTES"
	else
		: >"$NOTES"
	fi
	local before_size after_size
	before_size=$(wc -c <"$NOTES")
	awk -v v="$V" -f "$EXTRACT_AWK" "$CHANGELOG" >>"$NOTES"
	after_size=$(wc -c <"$NOTES")
	if [ "$before_size" -eq "$after_size" ]; then
		echo "release.sh: CHANGELOG section for $V is missing or empty in $CHANGELOG" >&2
		exit 1
	fi
	if ! grep -q '[^[:space:]]' "$NOTES"; then
		echo "release.sh: release body for $V is empty/whitespace-only" >&2
		exit 1
	fi
	# Unwrap source 72-col line breaks (GitHub renders `\n` as `<br>` so source
	# line-wrapping shows mid-sentence breaks on the release page). $UNWRAP_NOTES
	# is in the EXIT trap so a SIGINT mid-awk doesn't leak the tmpfile.
	UNWRAP_NOTES=$(mktemp "${TMPDIR:-/tmp}/rt-release-unwrapped.XXXXXX") || {
		echo "release.sh: mktemp for unwrap pass failed" >&2
		exit 1
	}
	if ! awk -f "$UNWRAP_AWK" "$NOTES" >"$UNWRAP_NOTES"; then
		# Preserve the spliced source so the operator can match the awk error's
		# line numbers against actual content. Without this the tmpfile gets
		# cleaned by the EXIT trap before the operator can `cat` it.
		local debug_copy="${TMPDIR:-/tmp}/rt-unwrap-fail-$V.md"
		cp "$NOTES" "$debug_copy" 2>/dev/null && {
			echo "release.sh: paragraph-unwrap pass failed; spliced source preserved at: $debug_copy" >&2
			echo "release.sh: line numbers in the awk error above reference that file." >&2
		} || echo "release.sh: paragraph-unwrap pass failed on $NOTES (also failed to preserve a debug copy)" >&2
		exit 1
	fi
	if ! mv "$UNWRAP_NOTES" "$NOTES"; then
		echo "release.sh: failed to swap unwrapped notes into $NOTES" >&2
		exit 1
	fi
	UNWRAP_NOTES=""
	# Re-check after unwrap: a system-level awk write truncation (ENOSPC mid-
	# flush, etc.) could leave $NOTES empty and silently blank a published
	# release on `release:update`.
	if ! grep -q '[^[:space:]]' "$NOTES"; then
		echo "release.sh: unwrap pass produced empty/whitespace-only output for $V" >&2
		exit 1
	fi
}

do_tag() {
	git tag -as "$TAG" -m "redistransport $V" || {
		echo "release.sh: git tag failed for $TAG. Nothing to clean up." >&2
		exit 1
	}
	git push "$REMOTE" "refs/tags/$TAG:refs/tags/$TAG" || {
		echo "release.sh: git push failed for $TAG. Local tag exists but nothing was pushed." >&2
		echo "release.sh: recover with: task release:delete V=$V REMOTE=$REMOTE" >&2
		exit 1
	}
	echo "Tagged and pushed $TAG (remote: $REMOTE)"
}

# Publish from a pre-spliced $NOTES. Caller runs make_notes_file +
# splice_notes upfront so a notes failure can't strand a pushed tag.
do_publish_with_notes() {
	local flags=(--title "redistransport $V" --verify-tag --latest=false --notes-file "$NOTES")
	if is_prerelease; then flags+=(--prerelease); fi

	gh release create --repo "$REPO" "$TAG" "${flags[@]}" || {
		echo "release.sh: gh release create failed for $TAG." >&2
		echo "release.sh: if the tag is good and only publish needs retry:" >&2
		echo "release.sh:   task release:publish V=$V REMOTE=$REMOTE" >&2
		echo "release.sh: if a partial release exists and you want a full reset:" >&2
		echo "release.sh:   task release:delete V=$V REMOTE=$REMOTE && task release:create V=$V REMOTE=$REMOTE" >&2
		exit 1
	}
	echo "Published $TAG (remote: $REMOTE)"
}

case "${1:-}" in
tag)
	do_tag
	;;

publish)
	make_notes_file
	splice_notes
	do_publish_with_notes
	;;

create)
	# Build + validate notes FIRST so a notes failure doesn't leave a
	# signed tag on the remote with no release.
	make_notes_file
	splice_notes
	do_tag
	do_publish_with_notes
	;;

update)
	make_notes_file
	splice_notes
	# --latest=false re-asserted on each edit so a UI toggle doesn't drift.
	gh release edit --repo "$REPO" "$TAG" --notes-file "$NOTES" --latest=false || {
		echo "release.sh: gh release edit failed for $TAG. The release on GitHub is unchanged." >&2
		echo "release.sh: retry with: task release:update V=$V REMOTE=$REMOTE" >&2
		exit 1
	}
	echo "Updated $TAG notes (prelude + CHANGELOG)"
	;;

delete)
	rc=0
	deleted=()
	gh_release_view "$TAG"
	case "$gh_release_view_state" in
	present)
		gh release delete --repo "$REPO" "$TAG" --yes --cleanup-tag || {
			echo "release.sh: gh release delete failed for $TAG — aborting before tag cleanup to avoid orphaning the release." >&2
			echo "release.sh: manual cleanup:" >&2
			echo "  gh release view --repo $REPO $TAG       # check release state" >&2
			echo "  gh release delete --repo $REPO $TAG --yes  # retry release delete" >&2
			echo "  git tag -d $TAG                            # then local tag" >&2
			echo "  git push $REMOTE :refs/tags/$TAG           # then remote tag" >&2
			exit 1
		}
		deleted+=(release)
		# --cleanup-tag also removes the remote tag atomically. Local
		# tag still needs explicit delete below.
		;;
	absent) ;; # nothing to delete on the release side
	unknown)
		echo "release.sh: cannot determine release state for $TAG — aborting" >&2
		exit 1
		;;
	esac

	if git rev-parse "refs/tags/$TAG" >/dev/null 2>&1; then
		if git tag -d "$TAG" >/dev/null; then
			deleted+=(local-tag)
		else
			echo "release.sh: local tag delete failed for $TAG" >&2
			rc=1
		fi
	fi

	# Use `if x=$(); then` so the rc is captured under set -e without
	# depending on a fragile `$?` line ordering.
	if remote_listing=$(git ls-remote --tags "$REMOTE" "refs/tags/$TAG" 2>&1); then
		if [ -n "$remote_listing" ]; then
			if push_out=$(git push "$REMOTE" ":refs/tags/$TAG" 2>&1); then
				deleted+=(remote-tag)
			else
				echo "release.sh: remote tag delete failed for $TAG on $REMOTE: $push_out" >&2
				rc=1
			fi
		fi
	else
		ls_rc=$?
		echo "release.sh: git ls-remote $REMOTE failed (rc=$ls_rc): $remote_listing" >&2
		rc=1
	fi

	if [ "$rc" -eq 0 ]; then
		if [ "${#deleted[@]}" -eq 0 ]; then
			echo "Nothing to delete for $TAG (was already absent)"
		else
			echo "Deleted $TAG: ${deleted[*]} (on $REMOTE)"
		fi
	else
		exit "$rc"
	fi
	;;

"")
	echo "release.sh: missing subcommand" >&2
	echo "usage: $0 {tag|publish|create|update|delete}" >&2
	exit 2
	;;

*)
	echo "release.sh: unknown subcommand: $1" >&2
	echo "usage: $0 {tag|publish|create|update|delete}" >&2
	exit 2
	;;
esac
