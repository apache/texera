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
title: "Python"
description: "Operators in the Python category"
weight: 1
categories: [Operators]
tags: [user-defined-functions, python]
---

[Home](../../) > [User-defined Functions](../) > Python

## Operators

| Operator | Description |
|----------|-------------|
| [2-in Python UDF](2-in-python-udf/) | User-defined function operator in Python script |
| [Python Lambda Function](python-lambda-function/) | Modify or add a new column with more ease |
| [Python Table Reducer](python-table-reducer/) | Reduce Table to Tuple |
| [1-out Python UDF](1-out-python-udf/) | User-defined function operator in Python script |
| [Python UDF](python-udf/) | User-defined function operator in Python script |

**Total**: 5 operators

## UI parameters

The Python UDF, 2-in Python UDF, and 1-out Python UDF operators can expose values in the property panel. Declare each value with `self.UiParameter(...)` inside the UDF class's `open()` method, then use its `.value` in later methods.

```python
from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):
    @overrides
    def open(self):
        self.count = self.UiParameter("count", AttributeType.INT).value

    @overrides
    def process_tuple(self, tuple_: Tuple, port: int):
        # self.count contains the value entered in the property panel.
        yield tuple_
```

Active calls are inferred from the script; commented-out calls are ignored. The supported classes are `ProcessTupleOperator`, `ProcessBatchOperator`, `ProcessTableOperator`, and `GenerateOperator`. Supported types are `STRING`, `INT`/`LONG`, `DOUBLE`, `BOOL`, and `TIMESTAMP`. Empty strings are valid for `STRING`; all other types require a non-empty value before execution.
