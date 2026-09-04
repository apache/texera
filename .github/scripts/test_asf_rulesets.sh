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

# Invariants over the CI configuration that a plain YAML parse cannot see.
#
# 1. "Merge Queue" and "Merge Queue (release)" in .asf.yaml must carry
#    identical rules: they are one policy split across two rulesets only so
#    the release half can hold an Actions bypass that must not reach main.
#    Nothing else keeps the copies from drifting apart.
# 2. .asf.yaml and every workflow must parse with a duplicate-key-strict
#    loader. PyYAML silently keeps the last duplicate, but GitHub's loader
#    rejects the file, so a duplicated trigger key passes local checks and
#    then stops the workflow from ever starting.

set -uo pipefail

command -v python3 >/dev/null || { echo "python3 is required to run these tests" >&2; exit 1; }

cd "$(git rev-parse --show-toplevel)"

python3 - <<'EOF'
import glob
import sys

import yaml


class StrictLoader(yaml.SafeLoader):
    pass


def no_duplicates(loader, node, deep=False):
    seen = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise yaml.YAMLError(
                f"duplicate key {key!r} at line {key_node.start_mark.line + 1}"
            )
        seen.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep)


StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates
)

failures = []

files = sorted(glob.glob(".github/workflows/*.yml")) + [".asf.yaml"]
for path in files:
    with open(path) as fh:
        try:
            yaml.load(fh, StrictLoader)
        except yaml.YAMLError as exc:
            failures.append(f"{path}: {exc}")

with open(".asf.yaml") as fh:
    rulesets = {
        r.get("name"): r
        for r in yaml.safe_load(fh)["github"]["rulesets"]
        if isinstance(r, dict)
    }
main_rs = rulesets.get("Merge Queue")
release_rs = rulesets.get("Merge Queue (release)")
if main_rs is None or release_rs is None:
    failures.append(
        ".asf.yaml: expected rulesets named 'Merge Queue' and 'Merge Queue (release)'"
    )
elif main_rs["rules"] != release_rs["rules"]:
    failures.append(
        ".asf.yaml: 'Merge Queue' and 'Merge Queue (release)' rules differ -- "
        "these are one policy in two rulesets; change both or neither"
    )

for failure in failures:
    print(f"FAIL: {failure}")
if failures:
    sys.exit(1)
print(f"OK: {len(files)} files duplicate-key clean; Merge Queue rules identical")
EOF
