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

# Every YAML file GitHub reads from this repository -- .asf.yaml and
# everything under .github/ -- must parse with a duplicate-key-strict loader.
#
# A duplicate mapping key is invalid YAML, but PyYAML keeps the last one and
# says nothing, so a local parse cannot see the mistake:
#
#   >>> yaml.safe_load("jobs:\n  a: {}\njobs:\n  b: {}")
#   {'jobs': {'b': {}}}
#
# GitHub's loader rejects the file instead. The shapes that follow are the
# expensive ones: a workflow that never starts again, a labeler rule that
# quietly stops matching, or a release branch dropped from
# release-branches.yml so its backports are never even nominated -- each one
# arriving as silence rather than as a failure.

set -uo pipefail

command -v python3 >/dev/null || { echo "python3 is required to run these tests" >&2; exit 1; }
# Runners ship python3 but not necessarily PyYAML (see release_branches.py);
# CI installs it via amber/dev-requirements.txt.
python3 -c 'import yaml' 2>/dev/null || { echo "PyYAML is required (pip install pyyaml)" >&2; exit 1; }

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

files = sorted(
    {
        path
        for pattern in (".github/**/*.yml", ".github/**/*.yaml", ".asf.yaml")
        for path in glob.glob(pattern, recursive=True)
    }
)
if not files:
    print("FAIL: no YAML files found to check -- run this from the repository root")
    sys.exit(1)

failures = []
for path in files:
    with open(path) as fh:
        try:
            yaml.load(fh, StrictLoader)
        except yaml.YAMLError as exc:
            failures.append(f"{path}: {exc}")

for failure in failures:
    print(f"FAIL: {failure}")
if failures:
    sys.exit(1)
print(f"OK: {len(files)} YAML files parse, none with a duplicate key")
EOF
