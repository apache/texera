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
