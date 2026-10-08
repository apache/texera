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
title: "2-in Python UDF"
description: "User-defined function operator in Python script"
category: "Python"
operator_type: "DualInputPortsPythonUDFV2"
tags: [user-defined-functions, python]
---

[Home](../../../) > [User Defined Functions](../../) > [Python](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Python script | ✓ | Code (python) | `See template below` | Input your code here |
| Worker count | ✓ | Integer | 1 | Specify how many parallel workers to launch |
| Retain input columns | ✓ | Boolean | true | Keep the original input columns? |
| Extra output column(s) |  | List<Attribute> | - | Name of the newly added output columns that the<br>UDF will produce, if any |
| ↳ Attribute Name | ✓ | String | - |  |
| ↳ Attribute Type | ✓ | string, integer, long, double, boolean,<br>timestamp, binary, large_binary | - |  |
| Parameters |  | List<UiUDFParameter> | - | Values inferred from active `self.UiParameter(...)` calls.<br>See [UI parameters](../#ui-parameters). |

#### Default Code Template

**Python script**

```python
# Keep exactly ONE class below: uncomment it and delete the others.
# Its method must match its base class:
#   UDFOperatorV2 -> process_tuple, UDFBatchOperator -> process_batch,
#   UDFTableOperator -> process_table
# Always `yield` results (never `return` a value). Put setup code in open();
# if you override __init__, call super().__init__() first.
# 
# Port 0 (model) is read completely before port 1 (tuples) starts.
# Use the `port` argument to tell them apart, e.g. `if port == 0:`.
# 
# Define UiParameter inside open() of ProcessTupleOperator, ProcessBatchOperator, or ProcessTableOperator.
# Example: self.count = self.UiParameter("count", AttributeType.INT).value
# Add value=Resource.MODEL or Resource.DATASET to pick a version; the value is its mount directory.
# See the Python UDF operator documentation for supported types and behavior.

from pytexera import *

# class ProcessTupleOperator(UDFOperatorV2):
#     
#     @overrides
#     def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
#         yield tuple_
# 
# class ProcessBatchOperator(UDFBatchOperator):
#     BATCH_SIZE = 10 # must be a positive integer
# 
#     @overrides
#     def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:
#         yield batch
# 
# class ProcessTableOperator(UDFTableOperator):
# 
#     @overrides
#     def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
#         yield table

```

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
