<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Binary distribution licensing tools

Utilities for maintaining `LICENSE-binary` and the per-license text files
under `licenses/`. Modeled after Apache Flink's
[`tools/releasing/collect_license_files.sh`](https://github.com/apache/flink/blob/master/tools/releasing/collect_license_files.sh)
— it is explicitly a skeleton helper, not a full generator.

## collect_binary_licenses.py

Walks every `.jar` in a given directory and extracts each jar's
`META-INF/LICENSE`, `META-INF/NOTICE`, and `META-INF/maven/*/pom.xml` into
a staging directory. Produces a per-jar summary and a concatenated
`NOTICE-binary-skeleton`.

### Typical flow (from a built Docker image)

```bash
# 1. Extract the bundled lib/ from a Docker image
docker create --name tmp-tx texera/workflow-execution-coordinator:latest
docker cp tmp-tx:/texera/amber/lib /tmp/texera-lib
docker rm tmp-tx

# 2. Collect licensing info
python3 tools/licensing/collect_binary_licenses.py /tmp/texera-lib /tmp/texera-licenses

# 3. Review the output
less /tmp/texera-licenses/summary.tsv
less /tmp/texera-licenses/NOTICE-binary-skeleton
ls /tmp/texera-licenses/bundled/
```

### What to do with the output

- `summary.tsv` — a per-jar overview showing which jars declare their
  license in `META-INF/LICENSE` vs. in their POM `<licenses>` section.
  Use it to identify jars that need manual investigation.
- `bundled/<jar-basename>/` — raw extracts, useful for reading the
  upstream LICENSE / NOTICE text verbatim when drafting attribution.
- `NOTICE-binary-skeleton` — concatenated NOTICE files with one block
  per source jar, suitable as a starting point for the binary NOTICE.

The final `LICENSE-binary` at the repo root is still hand-maintained:
group deps by license family, dedupe copyright notices, and verify that
the set of listed jars matches what each Docker image actually ships.

### Limitations

- Many jars omit `META-INF/LICENSE`; their license can usually be found
  in the embedded `pom.xml` `<licenses>` section, but some POMs don't
  declare licenses explicitly (they inherit from a parent POM this
  script does not follow).
- The concatenated `NOTICE-binary-skeleton` does not deduplicate common
  blocks across jars (e.g. the ASF header appears once per Apache jar).
- Apache-2.0 licensed dependencies are left as-is; per ASF policy they
  don't require an entry in the binary LICENSE beyond the Apache 2.0
  text itself, though listing them for completeness is encouraged.
