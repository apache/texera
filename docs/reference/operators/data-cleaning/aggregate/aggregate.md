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
| ↳ Attribute | ✓ | String | - | Column to calculate average value |
| ↳ Result Attribute | ✓ | String | - | Column name of average result |
| Group By Keys |  | List | - | Group by columns |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
