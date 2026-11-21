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
title: "Filter"
description: "Performs a filter operation using AND or OR between multiple predicates"
category: "Data Cleaning"
operator_type: "Filter"
tags: [data-cleaning]
---

[Home](../../) > [Data Cleaning](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Combine Predicates With |  | any (OR), all (AND) | any (OR) | How to combine multiple predicates |
| Predicates | ✓ | List<Filter Predicate> | - | Multiple predicates to be combined |
| ↳ Attribute | ✓ | String | - |  |
| ↳ Condition | ✓ | =, >, >=, <, <=, !=, is null,<br>is not null | - |  |
| ↳ Value |  | String | - |  |

A Filter with no predicates keeps no rows in either mode, including "all (AND)".

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
