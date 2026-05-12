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

#### Default Code Template

**Python script**

```python
# Choose from the following templates:
# 
# from pytexera import *
# 
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
