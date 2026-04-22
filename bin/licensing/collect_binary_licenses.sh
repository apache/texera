#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Extract META-INF/LICENSE* and META-INF/NOTICE* from every .jar in <jar-dir>
# and emit LICENSE-skeleton / NOTICE-skeleton for curating LICENSE-binary /
# NOTICE-binary by hand. Skeletons are raw concatenations, not shippable.
#
# JVM jars only. Frontend (npm) is covered by the Angular CLI's
# 3rdpartylicenses.txt; Python is covered by pip-licenses.
#
# Usage:
#   ./bin/licensing/collect_binary_licenses.sh <jar-dir> [<out-dir>]
#
# Example:
#   sbt 'project WorkflowExecutionService' dist
#   unzip amber/target/universal/amber-*.zip -d /tmp/dist
#   ./bin/licensing/collect_binary_licenses.sh /tmp/dist/amber-*/lib /tmp/license-staging

set -euo pipefail

JAR_DIR="${1:?Usage: $0 <jar-dir> [<out-dir>]}"
OUT_DIR="${2:-license-staging}"

if [ ! -d "$JAR_DIR" ]; then
  echo "error: $JAR_DIR is not a directory" >&2
  exit 1
fi

TMPDIR_ROOT=$(mktemp -d)
trap 'rm -rf "$TMPDIR_ROOT"' EXIT

mkdir -p "$OUT_DIR/bundled"

total=0
with_license=0
with_notice=0

for jar in "$JAR_DIR"/*.jar; do
  [ -f "$jar" ] || continue
  total=$((total + 1))

  jar_basename=$(basename "$jar" .jar)
  tmp_extract="$TMPDIR_ROOT/$jar_basename"
  mkdir -p "$tmp_extract"

  # Extract META-INF/LICENSE* and META-INF/NOTICE* (case-insensitive globs).
  unzip -q -o -j "$jar" 'META-INF/LICENSE*' 'META-INF/NOTICE*' \
    -d "$tmp_extract" 2>/dev/null || true

  jar_out="$OUT_DIR/bundled/$jar_basename"

  # Copy first matching LICENSE.
  for f in "$tmp_extract"/LICENSE* "$tmp_extract"/license*; do
    if [ -f "$f" ]; then
      mkdir -p "$jar_out"
      cp "$f" "$jar_out/LICENSE"
      with_license=$((with_license + 1))
      break
    fi
  done

  # Copy first matching NOTICE.
  for f in "$tmp_extract"/NOTICE* "$tmp_extract"/notice*; do
    if [ -f "$f" ]; then
      mkdir -p "$jar_out"
      cp "$f" "$jar_out/NOTICE"
      with_notice=$((with_notice + 1))
      break
    fi
  done
done

# Emit a deduped skeleton: group jars whose LICENSE (or NOTICE) content is
# byte-identical, print each unique content once with the list of jars that
# share it. Typical 100:1 reduction for LICENSE (boilerplate Apache 2.0
# repeated across many jars), 10:1 for NOTICE (standard ASF header).
emit_deduped_skeleton() {
  kind="$1"   # "LICENSE" or "NOTICE"
  header="$2"
  out="$OUT_DIR/${kind}-skeleton"

  index="$TMPDIR_ROOT/${kind}.index"
  : > "$index"
  for jar_dir in "$OUT_DIR"/bundled/*; do
    [ -d "$jar_dir" ] || continue
    f="$jar_dir/$kind"
    [ -f "$f" ] || continue
    hash=$(shasum "$f" | awk '{print $1}')
    printf '%s\t%s\n' "$hash" "$(basename "$jar_dir")" >> "$index"
  done
  sort -o "$index" "$index"

  {
    echo "# ${kind}-skeleton (deduped by content hash)"
    echo "$header"
    echo ""

    last_hash=""
    jars=""
    while IFS=$'\t' read -r hash jar_name; do
      if [ "$hash" != "$last_hash" ]; then
        if [ -n "$last_hash" ]; then
          printf '\n%s\n\n%s\n' "# ---- shared by: $jars ----" "$(cat "$TMPDIR_ROOT/${kind}-$last_hash")"
        fi
        last_hash="$hash"
        jars="$jar_name"
        cp "$OUT_DIR/bundled/$jar_name/$kind" "$TMPDIR_ROOT/${kind}-$hash"
      else
        jars="$jars, $jar_name"
      fi
    done < "$index"
    if [ -n "$last_hash" ]; then
      printf '\n%s\n\n%s\n' "# ---- shared by: $jars ----" "$(cat "$TMPDIR_ROOT/${kind}-$last_hash")"
    fi
  } > "$out"

  unique=$(awk '{print $1}' "$index" | sort -u | wc -l | tr -d ' ')
  total_files=$(wc -l < "$index" | tr -d ' ')
  echo "  ${kind}-skeleton: $unique unique / $total_files files"
}

echo ""
echo "Done. Scanned $total jars."
echo "  with META-INF/LICENSE: $with_license"
echo "  with META-INF/NOTICE:  $with_notice"
echo ""
emit_deduped_skeleton LICENSE \
  "# Drop Apache-2.0 boilerplate (already covered by LICENSE-binary's top-level ALv2 text); curate per-license attribution sections."
emit_deduped_skeleton NOTICE \
  "# Keep only legally-required attribution; drop MIT/BSD/ISC copyright lines per https://infra.apache.org/licensing-howto.html."
echo ""
echo "Output:"
echo "  per-jar extracts:   $OUT_DIR/bundled/<jar-name>/{LICENSE,NOTICE}"
echo "  LICENSE skeleton:   $OUT_DIR/LICENSE-skeleton"
echo "  NOTICE skeleton:    $OUT_DIR/NOTICE-skeleton"
