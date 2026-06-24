---
title: "Aggregate"
description: "Calculate different types of aggregation values"
category: "Aggregate"
operator_type: "Aggregate"
tags: [data-cleaning, aggregate]
---

[Home](../../../) > [Data Cleaning](../../) > [Aggregate](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Aggregations | ✓ | List<Aggregation> | - | Multiple aggregation functions (min: 1,<br>aggregations cannot be empty) |
| ↳ Aggregate Func | ✓ | sum, count, count(*), average, min, max, concat | - | Sum, count, count(*), average, min, max, or concat |
| ↳ Attribute | ✓ (hidden for `count(*)`) | String | - | Column to aggregate on. Required for every function except `count(*)`, which counts all rows and hides this field |
| ↳ Result Attribute | ✓ | String | - | Column name of the aggregation result |
| Group By Keys |  | List | - | Group by columns |

> **Counting rows**: use `count(*)` to count every row (including rows with nulls) without selecting a column. Use `count` with a column to count only that column's non-null values.

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
