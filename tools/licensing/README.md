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

Helper script for maintaining `LICENSE-binary` and the per-license text
files under `licenses/`. Modeled after Apache Flink's
[`tools/releasing/collect_license_files.sh`](https://github.com/apache/flink/blob/master/tools/releasing/collect_license_files.sh).

## collect_binary_licenses.sh

Walks every `.jar` in a given directory and extracts each jar's
`META-INF/LICENSE` and `META-INF/NOTICE` into a staging directory.
Produces a concatenated `NOTICE-skeleton` from all extracted NOTICE files.

Only requires `bash` and `unzip` (standard on any Unix system).

### Usage

```bash
./tools/licensing/collect_binary_licenses.sh <jar-dir> [<out-dir>]
```

### Typical flow (from sbt dist output)

```bash
# 1. Build the dist zip
sbt 'project WorkflowExecutionService' dist

# 2. Unzip it
unzip amber/target/universal/amber-*.zip -d /tmp/dist

# 3. Collect licensing artefacts from the bundled jars
./tools/licensing/collect_binary_licenses.sh /tmp/dist/amber-*/lib /tmp/license-staging

# 4. Review the output
ls /tmp/license-staging/bundled/
cat /tmp/license-staging/NOTICE-skeleton
```

### What it produces

```
<out-dir>/
  bundled/
    <jar-basename>/
      LICENSE          # extracted from jar's META-INF/LICENSE (if present)
      NOTICE           # extracted from jar's META-INF/NOTICE (if present)
  NOTICE-skeleton      # all extracted NOTICE files concatenated
```

### How to use the output

- **`bundled/<jar>/LICENSE`** — read the upstream license text verbatim
  when drafting attribution entries in `LICENSE-binary`.
- **`bundled/<jar>/NOTICE`** — ASF policy requires carrying forward
  upstream NOTICE content for Apache-licensed deps. Use this as the raw
  source when updating the binary NOTICE file.
- **Jars with no output directory** — the script prints these to stdout.
  They need manual investigation (check the project's website or Maven
  Central for license info).

### When to run it

- **At release time**: verify that `LICENSE-binary` and `NOTICE` still
  match the actual set of bundled jars.
- **When dependencies change**: re-run and diff the output against the
  previous run to identify what licensing entries need to be added or
  removed from `LICENSE-binary`.

### Limitations

- Many jars omit `META-INF/LICENSE` or `META-INF/NOTICE`. Their license
  is typically declared in the POM on Maven Central — this script does
  not fetch POMs.
- The `NOTICE-skeleton` is a raw concatenation with no deduplication.
  Human curation is required before shipping.
