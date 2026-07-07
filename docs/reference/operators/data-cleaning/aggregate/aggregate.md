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
| ↳ Aggregate Func | ✓ | sum, count, average, min, max, concat | - | Sum, count, average, min, max, or concat |
| ↳ Attribute | ✓ (optional for `count`) | String | - | Column to aggregate on. Required for every function except `count`: leave it empty with `count` to count all rows (`COUNT(*)`), or pick a column to count its non-null values |
| ↳ Result Attribute | ✓ | String | - | Column name of the aggregation result |
| Group By Keys |  | List | - | Group by columns |

> **Counting rows**: with the `count` function, leave **Attribute** empty to count every row (`COUNT(*)`, including rows with nulls), or choose a column to count only that column's non-null values.

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
