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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Every value a template reads must exist in values.yaml, or be listed as deliberately
# absent below.
#
# Helm does not fail on a missing value, it renders an empty string -- so a chart with a
# typo, or one whose template was added without its values, installs and then misbehaves
# at runtime with nothing pointing at the cause.
#
# Needs no helm and no cluster, so it runs anywhere the repo does.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

python3 - "$CHART_DIR" <<'PY'
import fnmatch
import os
import re
import sys

chart_dir = sys.argv[1]

try:
    import yaml
except ImportError:
    # Skipping here would turn this suite into a green no-op the moment the dependency
    # goes missing, which is worse than not having the check.
    print(
        "FAIL: PyYAML is required by this check but is not installed.\n"
        "  Install it with: python -m pip install -r amber/dev-requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

with open(os.path.join(chart_dir, "values.yaml"), encoding="utf-8") as handle:
    values = yaml.safe_load(handle) or {}

# Read by a template but deliberately not defined here: a value behind a guard a default
# deployment never enters, or a subchart default this chart inherits. Every entry needs
# its reason -- an unexplained one is indistinguishable from a suppressed break.
ALLOWED_ABSENT = {
    # Read only inside `if eq .Values....type "NodePort"`, so a deployment that does not
    # use NodePort services leaves them unset.
    "fileService.service.nodePort",
    "webserver.service.nodePort",
    "workflowCompilingService.service.nodePort",
    "workflowComputingUnitManager.service.nodePort",
}

# Helm renders everything under templates/ that .helmignore does not drop, so scan by
# exclusion: an extension allowlist skips _helpers.tpl, where the shared naming logic
# lives, and a rename there would go through unnoticed.
#
# Read the chart's own .helmignore rather than keeping a second copy of it here. A
# hardcoded list drifts the moment either side is edited, and then the check either fails
# on a file Helm never renders or stops looking at one it does.
def helmignore(root):
    path = os.path.join(root, ".helmignore")
    if not os.path.exists(path):
        # Helm with no .helmignore renders every file under templates/. Match that rather
        # than inventing exclusions the chart did not ask for.
        return []
    with open(path, encoding="utf-8") as handle:
        lines = (line.strip() for line in handle)
        return [line for line in lines if line and not line.startswith("#")]


def helmignored(relative, is_dir, patterns):
    # helm's own format (pkg/ignore): a pattern with no separator matches the base name,
    # one with a separator matches the chart-relative path, a leading "/" anchors to the
    # chart root, and a trailing "/" restricts the pattern to directories. Negation and
    # "**" are not part of it.
    for pattern in patterns:
        if pattern.endswith("/") and not is_dir:
            continue
        pattern = pattern.rstrip("/")
        anchored = pattern.startswith("/")
        pattern = pattern.lstrip("/")
        target = relative if anchored or "/" in pattern else os.path.basename(relative)
        if fnmatch.fnmatchcase(target, pattern):
            return True
    return False


# Both accessors reach the same values; `index` is the only form for keys a dotted path
# cannot express.
DOTTED = re.compile(r"\.Values\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)")
INDEXED = re.compile(r"index\s+\$?\.Values\s+((?:\"[^\"]*\"\s*)+)")
QUOTED = re.compile(r"\"([^\"]*)\"")
DOTTABLE = re.compile(r"[A-Za-z0-9_]+\Z")
# `with .Values.x` rebinds the dot, so the keys read inside it appear as bare `.field`
# and drop out of the reference set -- the check would go on passing while no longer
# seeing them. `include "h" .Values.x` opens the same hole through a helper, where the
# rebound dot is read in another file entirely. Neither is resolvable from the file the
# reference is written in, so both are rejected; the parentheses Go templates allow
# around the argument are part of the form, not a way around this.
REBINDING = (
    re.compile(r"\{\{-?\s*with\s+\(*\s*\$?\.Values\b[^}]*"),
    re.compile(r"(?:include|template)\s+\"[^\"]*\"\s+\(*\s*\$?\.Values\b[^}]*"),
)
# A variable *is* resolvable -- the assignment spells the full path out in the same file
# -- so follow it instead of rejecting it: aliasing a subtree to avoid repeating it is
# what the persistence templates already do, and `$p := .Values.a.b` followed by
# `$p.storageClass` would otherwise hide that key exactly as `with` does. Anchored at
# `{{` so `range $k, $v := .Values.m` is not mistaken for an alias of the map: `$v` is an
# element, and its fields are data rather than chart keys.
ALIAS = re.compile(
    r"\{\{-?\s*\$([A-Za-z0-9_]+)\s*:=\s*\$?\.Values\.([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)"
)
ALIAS_CHAIN = re.compile(
    r"\{\{-?\s*\$([A-Za-z0-9_]+)\s*:=\s*\$([A-Za-z0-9_]+)((?:\.[A-Za-z0-9_]+)+)"
)
ALIAS_READ = re.compile(r"\$([A-Za-z0-9_]+)((?:\.[A-Za-z0-9_]+)+)")

# Held as tuples of key segments: a key may itself contain a dot, so joining and
# re-splitting on "." would take `index .Values "a" "b.c"` apart at the wrong place.
references = set()
rebound = []
scanned = 0
patterns = helmignore(chart_dir)
templates_dir = os.path.join(chart_dir, "templates")
for directory, dirnames, filenames in os.walk(templates_dir):
    dirnames[:] = sorted(
        name
        for name in dirnames
        if not helmignored(
            os.path.relpath(os.path.join(directory, name), chart_dir), True, patterns
        )
    )
    for filename in sorted(filenames):
        path = os.path.join(directory, filename)
        relative = os.path.relpath(path, chart_dir)
        if helmignored(relative, False, patterns):
            continue
        scanned += 1
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        references.update(tuple(match.split(".")) for match in DOTTED.findall(text))
        references.update(tuple(QUOTED.findall(match)) for match in INDEXED.findall(text))
        # Variables are file-scoped, so resolve them per file. An alias of an alias
        # ($storageClass := $persistence.storageClass) needs the first resolved before the
        # second can be, and nothing orders the assignments, so iterate to a fixpoint.
        aliases = {name: tuple(read.split(".")) for name, read in ALIAS.findall(text)}
        while True:
            resolved = {
                name: aliases[source] + tuple(suffix.strip(".").split("."))
                for name, source, suffix in ALIAS_CHAIN.findall(text)
                if name not in aliases and source in aliases
            }
            if not resolved:
                break
            aliases.update(resolved)
        for name, suffix in ALIAS_READ.findall(text):
            if name in aliases:
                references.add(aliases[name] + tuple(suffix.strip(".").split(".")))
        for pattern in REBINDING:
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                rebound.append(f"{relative}:{line}: {match.group().strip()}")

if not scanned:
    # Otherwise a moved or renamed templates/ reports "0 references, all fine".
    print(f"FAIL: no template files found under {templates_dir}", file=sys.stderr)
    sys.exit(1)

if rebound:
    # On its own: every verdict below reads the reference set, and the keys hidden
    # inside these blocks are missing from it.
    print(f"FAIL: {len(rebound)} rebinding(s) of the dot hide the keys read inside:")
    for location in rebound:
        print(f"  {location}")
    print("  Spell the .Values path out at each use, or bind it to a variable")
    print("  (`$p := .Values.a.b`, then `$p.key`), which this check follows.")
    sys.exit(1)


def render(reference):
    if all(DOTTABLE.match(part) for part in reference):
        return ".Values." + ".".join(reference)
    return "index .Values " + " ".join(f'"{part}"' for part in reference)


def resolve(reference):
    node = values
    for part in reference:
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


allowed = {tuple(reference.split(".")) for reference in ALLOWED_ABSENT}
missing = sorted(r for r in references if r not in allowed and not resolve(r))
# An exemption stops earning its place either when no template reads it or when
# values.yaml starts defining it. Left in, it goes on suppressing that key, so a later
# rename of the value it covers would pass unnoticed.
stale = []
for reference in sorted(allowed):
    if reference not in references:
        stale.append((reference, "no template reads it"))
    elif resolve(reference):
        stale.append((reference, "values.yaml now defines it"))

if missing or stale:
    if missing:
        print(f"FAIL: {len(missing)} template value(s) missing from values.yaml:")
        for reference in missing:
            print(f"  {render(reference)}")
        print("  Define each in values.yaml, or add it to ALLOWED_ABSENT with the reason.")
    if stale:
        print(f"FAIL: {len(stale)} ALLOWED_ABSENT entr(y/ies) no longer earning a place:")
        for reference, reason in stale:
            print(f"  {render(reference)} -- {reason}")
    sys.exit(1)

print(
    f"PASS: {len(references) - len(allowed)} template value reference(s) checked "
    f"across {scanned} file(s); {len(allowed)} exempt"
)
PY
