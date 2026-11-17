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
title: "Continuous Error Bands"
description: "Visualize error or uncertainty along a continuous line"
category: "Statistical"
operator_type: "ContinuousErrorBands"
tags: [visualization, statistical]
---

[Home](../../../) > [Visualization](../../) > [Statistical](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| X Label |  | String | X Axis | Label used for x axis |
| Y Label |  | String | Y Axis | Label used for y axis |
| Bands | ✓ | List<Band> | - |  |
| ↳ Y-Axis Upper Bound | ✓ | String | - | Represents upper bound error of y-values |
| ↳ Y-Axis Lower Bound | ✓ | String | - | Represents lower bound error of y-values |
| ↳ Fill Color |  | String | - | Must be a valid CSS color or hex color string |
| ↳ Y Value | ✓ | String | - | Value for y axis |
| ↳ X Value | ✓ | String | - | Value for x axis |
| ↳ Line Mode | ✓ | line, dots, line with dots | line with dots |  |
| ↳ Line Name |  | String | - |  |
| ↳ Line Color |  | String | - | Must be a valid CSS color or hex color string |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
