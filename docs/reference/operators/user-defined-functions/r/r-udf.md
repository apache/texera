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
title: "R UDF"
description: "User-defined function operator in R script"
category: "R"
operator_type: "RUDF"
tags: [user-defined-functions, r]
---

[Home](../../../) > [User Defined Functions](../../) > [R](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| R UDF Script | ✓ | Code (r) | `See template below` | Input your code here |
| Worker count | ✓ | Integer | 1 | Specify how many parallel workers to launch |
| Use Tuple API? | ✓ | Boolean | false | Check this box to use Tuple API, leave unchecked<br>to use Table API |
| Retain input columns | ✓ | Boolean | true | Keep the original input columns? |
| Extra output column(s) |  | List<Attribute> | - | Name of the newly added output columns that the<br>UDF will produce, if any |
| ↳ Attribute Name | ✓ | String | - |  |
| ↳ Attribute Type | ✓ | string, integer, long, double, boolean,<br>timestamp, binary, large_binary | - |  |

#### Default Code Template

**R UDF Script**

```r
# If using Table API:
# function(table, port) { 
#   return (table) 
# }

# If using Tuple API:
# library(coro)
# coro::generator(function(tuple, port) {
#   yield (tuple)
# })
```

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
