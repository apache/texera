---
title: "1-out Python UDF"
description: "User-defined function operator in Python script"
category: "Python"
operator_type: "PythonUDFSourceV2"
tags: [user-defined-functions, python]
---

[Home](../../../) > [User Defined Functions](../../) > [Python](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Python script | ✓ | Code (python) | `See template below` | Input your code here |
| Worker count | ✓ | Integer | 1 | Specify how many parallel workers to launch |
| Columns |  | List<Attribute> | - | The columns of the source |
| ↳ Attribute Name | ✓ | String | - |  |
| ↳ Attribute Type | ✓ | string, integer, long, double, boolean,<br>timestamp, binary, large_binary | - |  |

#### Default Code Template

**Python script**

```python
# from pytexera import *
# class GenerateOperator(UDFSourceOperator):
# 
#     @overrides
#     
#     def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
#         yield

```

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
