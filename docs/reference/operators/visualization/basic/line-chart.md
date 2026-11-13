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
title: "Line Chart"
description: "View the result in line chart"
category: "Basic"
operator_type: "LineChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Y Label |  | String | Y Axis | The label for y axis |
| X Label |  | String | X Axis | The label for x axis |
| Lines | ✓ | List<Line> | - |  |
| ↳ Y Value | ✓ | String | - | Value for y axis |
| ↳ X Value | ✓ | String | - | Value for x axis |
| ↳ Line Mode | ✓ | line, dots, line with dots | line with dots |  |
| ↳ Line Name |  | String | - |  |
| ↳ Line Color |  | String | - | Must be a valid CSS color or hex color string |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
