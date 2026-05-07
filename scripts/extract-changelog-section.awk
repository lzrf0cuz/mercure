# Extract a Keep-a-Changelog section from a CHANGELOG.md file.
#
# Usage:
#   awk -v v="<version>" [-v mode="<mode>"] -f extract-changelog-section.awk <CHANGELOG.md>
#
# Modes:
#   extract  — print the section body, trimming both leading and trailing
#              blank lines. Stops on the next `## [` heading or on a
#              `[link-id]: <url>` reference-definition block (Keep-a-Changelog
#              convention places these at the end of the document, so they
#              must not bleed into the final section's notes).
#              This is the default mode.
#   check    — exit 0 if a section anchored on `## [<v>]` exists, 1 otherwise.
#
# The version is normalized: a leading `v` is stripped (so `v0.0.1` matches
# `## [0.0.1] — date`), and regex metacharacters are escaped (semver allows
# `.` `+` `-`).

BEGIN {
  if (v == "") {
    print "extract-changelog-section.awk: -v v=<version> is required" > "/dev/stderr"
    exit 2
  }
  sub(/^v/, "", v)
  gsub(/[-.+*?()\[\]{}\\^$|]/, "\\\\&", v)
  if (mode == "") { mode = "extract" }
  if (mode != "extract" && mode != "check") {
    print "extract-changelog-section.awk: unknown mode '" mode "' (expected extract or check)" > "/dev/stderr"
    exit 2
  }
  buffer = ""
  seen_content = 0
}

$0 ~ "^## \\[" v "\\]" {
  if (mode == "check") { hit = 1; exit }
  capture = 1
  next
}

capture && /^## \[/ { exit }
capture && /^\[[^\]]+\]:/ { exit }

capture && mode == "extract" {
  if ($0 == "") {
    if (seen_content) buffer = buffer "\n"
    next
  }
  seen_content = 1
  printf "%s%s\n", buffer, $0
  buffer = ""
}

END {
  if (mode == "check") { exit hit ? 0 : 1 }
}
