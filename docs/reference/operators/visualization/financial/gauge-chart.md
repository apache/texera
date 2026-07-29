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
title: "Gauge Chart"
description: "Visualize a single value with a radial gauge chart, showing progress towards a goal with optional steps, threshold, and delta."
category: "Financial"
operator_type: "GaugeChart"
tags: [visualization, financial]
---

[Home](../../../) > [Visualization](../../) > [Financial](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Gauge Value | ✓ | String | - | The primary value displayed on the gauge chart |
| Delta |  | String | - | The baseline value used to calculate the delta<br>from the gauge value |
| Threshold Value |  | String | - | Defines a boundary or target value shown on the<br>gauge chart |
| Steps |  | List<Step> | - | List of step ranges for the gauge |
| ↳ Start |  | String | - |  |
| ↳ End |  | String | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
