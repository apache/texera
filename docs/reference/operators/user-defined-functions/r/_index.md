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
title: "R"
description: "Operators in the R category"
weight: 3
categories: [Operators]
tags: [user-defined-functions, r]
---

[Home](../../) > [User-defined Functions](../) > R

## Operators

| Operator | Description |
|----------|-------------|
| [R UDF](r-udf/) | User-defined function operator in R script |
| [1-out R UDF](1-out-r-udf/) | User-defined function operator in R script |

**Total**: 2 operators

## UI parameters

R UDF and 1-out R UDF operators expose values in the properties panel through `UiParameter(...)` declarations. Use **Add parameter** in the panel to insert a declaration, or write one in the script:

```r
count <- UiParameter("count", AttributeType$INT)$value
function(table, port) {
  head(table, count)
}
```

The script must still end with the function the operator executes. The same declarations work before a `coro::generator(function(...) { ... })` for the Tuple API, or inside the function body. Names and types are inferred from active declarations; comments and string examples are ignored. Values are saved with the workflow and preserved during code edits while the parameter name and resource kind stay the same.

| Type | R value |
| --- | --- |
| `AttributeType$STRING` | Character string; an empty string is allowed |
| `AttributeType$INT` / `$INTEGER` | Integer (the minimum 32-bit value uses an exact numeric scalar because R reserves that integer for `NA`) |
| `AttributeType$LONG` | `bit64::integer64`; requires the R package `bit64` |
| `AttributeType$DOUBLE` | Numeric scalar, including `NaN` and positive/negative infinity |
| `AttributeType$BOOL` / `$BOOLEAN` | Logical scalar; accepts true/false or numeric text (zero is false, nonzero is true), matching Python UDF parameters |
| `AttributeType$TIMESTAMP` | `POSIXct`; ISO 8601 timestamp or date, with UTC used when no timezone is supplied |

Non-string values must be non-empty and valid for their declared type. Duplicate names and unsupported binary types are rejected. R's `integer64` reserves the minimum 64-bit value for `NA`, so that value is rejected. Each operator receives its own parameter environment.

To select a model or dataset version, declare a string parameter with `value=Resource$MODEL` or `value=Resource$DATASET`:

```r
data_dir <- UiParameter("data", AttributeType$STRING, value=Resource$DATASET)$value
function() {
  read.csv(file.path(data_dir, "data.csv"))
}
```

The panel provides the resource selector, and the script receives the selected version's mounted directory when execution starts. UI parameters require `texera-rudf` 0.2.0 or newer with UI parameter SDK v1. Scripts without UI parameters remain compatible with older plugin versions.
