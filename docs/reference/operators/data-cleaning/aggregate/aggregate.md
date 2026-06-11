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
| ↳ Attribute | ✓ | String | - | Column to calculate average value |
| ↳ Result Attribute | ✓ | String | - | Column name of average result |
| Group By Keys |  | List | - | Group by columns |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
