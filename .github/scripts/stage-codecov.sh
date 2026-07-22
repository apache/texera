#!/usr/bin/env bash
# Stage coverage + test-result reports for the deferred Codecov upload
# (.github/workflows/codecov-upload.yml). Fork PRs get no CODECOV_TOKEN on the
# pull_request event, so the build stages its reports as a `codecov-<flag>`
# artifact instead of uploading inline; the workflow_run job re-uploads them with
# a token (see codecov-upload.yml / #6685).
#
# Produces ./cc/coverage/ (coverage reports), ./cc/results/ (JUnit XMLs), and the
# pr/sha/branch identifier files. Directory structure is preserved so same-basename
# reports (e.g. amber's per-module jacoco.xml across ~8 modules) do not collide;
# the deferred job points `directory:` at cc/coverage and cc/results so Codecov's
# recursive search finds every file under each.
#
# Inputs (env):
#   CC_COVERAGE  space-separated list of coverage sources — each a literal file or
#                a `find -path` glob (use * which matches across /), e.g.
#                "*/target/scala-2.13/jacoco/report/jacoco.xml". May be empty for a
#                test-results-only flag.
#   CC_RESULTS   same, for JUnit test-result XMLs.
#   CC_PR        PR number   (empty on push builds)
#   CC_SHA       head commit sha
#   CC_BRANCH    head branch
set -uo pipefail

mkdir -p cc/coverage cc/results

copy_into() {
  dest="$1"; shift
  for src in "$@"; do
    if [ -e "$src" ]; then
      # literal existing path
      cp --parents "$src" "$dest/" 2>/dev/null || cp "$src" "$dest/" 2>/dev/null || true
    else
      # treat as a find -path glob (matches across / since find's * does)
      find . -path "./$src" -o -path "$src" 2>/dev/null \
        | while IFS= read -r f; do
            [ -f "$f" ] && cp --parents "$f" "$dest/" 2>/dev/null || true
          done
    fi
  done
}

# shellcheck disable=SC2086  # intentional word-splitting into separate globs
copy_into cc/coverage ${CC_COVERAGE:-}
# shellcheck disable=SC2086
copy_into cc/results ${CC_RESULTS:-}

printf '%s' "${CC_PR:-}"     > cc/pr-number.txt
printf '%s' "${CC_SHA:-}"    > cc/commit-sha.txt
printf '%s' "${CC_BRANCH:-}" > cc/branch.txt

echo "Staged coverage files:"
find cc/coverage -type f 2>/dev/null | sed 's/^/  /' || true
echo "Staged result files:"
find cc/results -type f 2>/dev/null | sed 's/^/  /' || true
