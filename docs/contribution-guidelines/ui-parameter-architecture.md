<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

---
title: "UI Parameter Architecture"
weight: 25
---

UI parameters use one metadata contract with language-specific syntax and runtime adapters.

| Layer | Owner | Responsibility |
| --- | --- | --- |
| Type manifest | `common/workflow-operator/src/main/resources/udf/ui-parameter-types.json` | Supported type aliases (canonical token first) and resource kinds |
| Serialized model and validator | `udf.UiUDFParameter`, `UiUdfParameterContract` | Stable wire fields, metadata validation, duplicate names, and resource/type compatibility |
| Frontend model and policy | `ui-udf-parameter.ts`, parser service | Read the manifest; create rows; validate names, additions, and duplicates once before dispatch |
| Declaration parsers | `python-ui-udf-parameters-parser.ts`, `r-ui-udf-parameters-parser.ts` | Read and edit each language's syntax; delegate metadata policy |
| Editor synchronization | `UiUdfParametersSyncService` | Observe Yjs and preserve user values by name and resource kind |
| Compilation | Python/R injector adapters | Validate the shared model and encode values using the appropriate runtime API |
| Resource binding | `UdfUiParameterSupport` | Resolve selected versions into mount directories at execution time |
| Runtime | PyTexera SDK / `texera-rudf` SDK | Construct native values and maintain per-operator state |

```text
code declaration -> language parser -> shared parameter rows -> saved workflow
                                                           |
                                       common validation + resource binding
                                                           |
                                       language compiler -> runtime SDK
```

The R compiler emits a call to `texera_ui_parameters` with encoded text and the shared type/resource mappings. Its implementation belongs to the R plugin, which evaluates each parameterized script in a fresh environment. The compiler does not embed the SDK implementation. The runtime reports capability version 1; Amber gives an explicit upgrade error when a parameterized script is used with a plugin lacking that capability. Scripts without UI parameters keep the existing plugin path.

Native conversions remain language-specific. For example, R uses `bit64::integer64` for long values and reserves its minimum signed value for `NA`. Common value cases preserve the existing Python boolean contract: true/false and numeric text, where nonzero is true. R's tuple bridge converts `integer64` through its decimal representation to preserve all available bits.

## Contract tests

`common/workflow-operator/src/test/resources/udf/ui-parameter-contract.json` supplies declaration, metadata, and runtime value cases. Frontend tests run the declaration cases against both parsers. Scala runs the metadata cases against both injectors. Python tests check the same values against the Python SDK and the compiled Python/R code.

`UiParameterIntegrationFixtures` generates code through the actual Scala injectors. The integration suite passes that code to Amber's `ExecutorManager` and the actual `texera-rudf` table/tuple source/processing executors. It checks Arrow round-trips, exact long values, Unicode and escaped strings, booleans, timestamps, resource directories, invalid values, and Windows line endings. SDK tests in the plugin cover operator isolation and legacy script compatibility.

Parameterized R scripts require `texera-rudf` 0.2.0 with SDK v1 support. Generate compiler fixtures with `WorkflowOperator/Test/runMain org.apache.texera.amber.operator.udf.UiParameterIntegrationFixtures common/workflow-operator/target/ui-parameter-fixtures.json`, then run `amber/src/test/python/pytexera/udf/test_ui_parameter_contract.py` with `TEXERA_R_INTEGRATION=1` to require the actual plugin and Arrow path.
