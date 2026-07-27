# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Reads .github/release-branches.yml and emits the release-branch -> manager
# mapping as JSON, so the backport workflows have one parser instead of each
# hand-rolling YAML handling in shell/JS. Uses only the standard library
# (GitHub runners have python3 but not necessarily PyYAML), parsing the small,
# fixed schema this file uses:
#
#     targets:
#       - branch: release/v1.2   # inline comments allowed
#         manager: xuang7        # optional
#
# Usage:
#   release_branches.py [path]              -> JSON array [{"branch","manager"}]
#   release_branches.py [path] --targets    -> JSON array of branch names
#   release_branches.py [path] --manager B  -> manager for branch B (or empty)

import json
import sys

DEFAULT_PATH = ".github/release-branches.yml"


def _strip(value):
    # Drop an inline "# ..." comment (the values here never contain '#'),
    # surrounding whitespace, and optional quotes.
    value = value.split("#", 1)[0].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    return value


def parse(path):
    entries = []
    current = None
    in_targets = False
    with open(path, encoding="utf-8") as handle:
        for raw in handle:
            line = raw.rstrip("\n")
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            indent = len(line) - len(line.lstrip())
            if indent == 0:
                # A new top-level key ends the targets block.
                in_targets = stripped.split(":", 1)[0].strip() == "targets"
                current = None
                continue
            if not in_targets:
                continue

            item = stripped
            if item.startswith("-"):
                current = {"branch": "", "manager": ""}
                entries.append(current)
                item = item[1:].strip()
                if not item:
                    continue
            if current is None or ":" not in item:
                continue
            key, value = item.split(":", 1)
            key = key.strip()
            if key in ("branch", "manager"):
                current[key] = _strip(value)

    return [e for e in entries if e["branch"]]


def main(argv):
    args = list(argv)
    manager_of = None
    mode = "entries"
    if "--targets" in args:
        args.remove("--targets")
        mode = "targets"
    if "--manager" in args:
        idx = args.index("--manager")
        manager_of = args[idx + 1]
        del args[idx : idx + 2]
        mode = "manager"
    path = args[0] if args else DEFAULT_PATH

    entries = parse(path)
    if mode == "targets":
        print(json.dumps([e["branch"] for e in entries]))
    elif mode == "manager":
        match = next((e for e in entries if e["branch"] == manager_of), None)
        print(match["manager"] if match else "")
    else:
        print(json.dumps(entries))


if __name__ == "__main__":
    main(sys.argv[1:])
