# Join soft-break lines within paragraphs and list items so GitHub's release
# renderer (treats every `\n` as `<br>`) shows flowing paragraphs instead of
# mid-sentence wraps from the source's 72-col convention.
#
# Preserves backtick + tilde fences (≤3 spaces opener indent), blank-line
# paragraph breaks, ATX headers, horizontal rules (only after a blank line),
# sibling list items including nested bullets.
#
# Rejects loudly (exit 2) any shape that would silently corrupt on flatten:
# blockquotes, GFM tables, hard line breaks, setext H1 underline, setext-H2 /
# HR ambiguity, block-level HTML, unclosed fences.
#
# Not handled (silently joined): indented code blocks (4-space style), nested
# multi-paragraph list items. Revisit if a CHANGELOG introduces them.

BEGIN { in_code = 0; fence = ""; buffer = "" }

# Strip CRLF first — sources saved on Windows leave `\r` that would otherwise
# end up embedded mid-paragraph after joining.
{ sub(/\r$/, "") }

# Code fence open/close. CommonMark allows up to 3 leading spaces. Track which
# marker opened so a mismatched closer (`~~~` after a ``` open) doesn't
# prematurely end the block.
/^[ \t]{0,3}(```|~~~)/ {
  if (buffer != "") { print buffer; buffer = "" }
  if (match($0, /(```|~~~)/)) {
    marker = substr($0, RSTART, 3)
  }
  if (in_code) {
    if (marker == fence) { in_code = 0; fence = "" }
  } else {
    in_code = 1; fence = marker
  }
  print $0
  next
}
in_code { print; next }

# Reject shapes that would silently corrupt on flatten — fail before publish.

# CommonMark blockquote: `>` MUST be followed by whitespace or EOL. Excludes
# `>=`, `>>`, `> file`, and wrap accidents that land `>` at column 0.
/^[ \t]*>([ \t]|$)/ {
  printf "unwrap: line %d uses blockquote (`> `) — not supported, would render as inline prose. Convert to plain text or extend the awk.\n", NR > "/dev/stderr"
  exit 2
}
/^[ \t]*\|.*\|/ {
  printf "unwrap: line %d uses GFM table — not supported, would collapse to a single line. Convert to a bulleted list or extend the awk.\n", NR > "/dev/stderr"
  exit 2
}
/  $/ {
  printf "unwrap: line %d ends with markdown hard-break (trailing two spaces) — not supported, would silently flatten. Use a blank-line paragraph break instead.\n", NR > "/dev/stderr"
  exit 2
}
/^=+[ \t]*$/ {
  printf "unwrap: line %d uses setext H1 underline (`===`) — not supported. Use ATX `# heading` instead.\n", NR > "/dev/stderr"
  exit 2
}
# Block-level HTML needs its own lines for GitHub to render correctly (the
# `<details>` widget, html tables, etc.). Flatten would break rendering
# silently. Alternation `( |>|/)` instead of `[ />]` because awk's regex
# parser is unhappy with `/` inside a character class.
/^[ \t]*<\/?(details|summary|table|tr|td|th|div|section|article|figure|aside|nav|header|footer)( |>|\/)/ {
  printf "unwrap: line %d uses block-level HTML — not supported, would silently flatten and break rendering. Use markdown alternatives, or move inside a code fence for documentation.\n", NR > "/dev/stderr"
  exit 2
}

# Blank line: paragraph break.
/^[[:space:]]*$/ {
  if (buffer != "") { print buffer; buffer = "" }
  print ""
  next
}

# ATX header.
/^#{1,6}[ \t]/ {
  if (buffer != "") { print buffer; buffer = "" }
  print $0
  next
}

# Horizontal rule (3+ of -, *, or _). Only fires when buffer is empty (i.e.,
# the prior line was blank). A non-empty buffer means this `---` / `===`-like
# sequence is a setext-H2 underline for the buffered text — silently
# rendering as HR would lose the heading semantics.
/^[ \t]*(-{3,}|\*{3,}|_{3,})[ \t]*$/ {
  if (buffer != "") {
    printf "unwrap: line %d: ambiguous `---` / `***` / `___` after non-blank line (setext-H2 vs HR). Use ATX `## heading` or insert a blank line before HR.\n", NR > "/dev/stderr"
    exit 2
  }
  print $0
  next
}

# New list item: flush prior buffer, start new buffer with the bullet line.
/^[ \t]*([-*+]|[0-9]+\.)[ \t]/ {
  if (buffer != "") { print buffer; buffer = "" }
  buffer = $0
  next
}

# Continuation: strip the source's hanging-indent / 72-col-wrap indent — it
# carries no semantic meaning here because code fences took the verbatim path
# above. Join with a single space.
{
  cont = $0
  sub(/^[ \t]+/, "", cont)
  if (buffer == "") { buffer = cont } else { buffer = buffer " " cont }
}

END {
  if (buffer != "") print buffer
  if (in_code) {
    print "unwrap: unclosed " fence " code fence — markdown source is malformed" > "/dev/stderr"
    exit 2
  }
}
