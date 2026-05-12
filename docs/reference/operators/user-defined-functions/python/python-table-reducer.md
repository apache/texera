---
title: "Python Table Reducer"
description: "Reduce Table to Tuple"
category: "Python"
operator_type: "PythonTableReducer"
tags: [user-defined-functions, python]
---

[Home](../../../) > [User Defined Functions](../../) > [Python](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Output columns |  | List<Lambda Attribute Unit> | - |  |
| ↳ Attribute Name | ✓ | String | - |  |
| ↳ Expression | ✓ | String | - |  |
| ↳ Attribute Type | ✓ | string, integer, long, double, boolean,<br>timestamp, binary, large_binary | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
