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
title: "Empirical Cumulative Distribution Plot"
description: "Visualize the empirical cumulative distribution of a numeric column."
category: "Statistical"
operator_type: "ECDFPlot"
tags: [visualization, statistical]
---

[Home](../../../) > [Visualization](../../) > [Statistical](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Value Column | ✓ | String (integer, long, double) | - | Numeric column used to compute the empirical<br>cumulative distribution |
| Color Column |  | String | - | Optional column for coloring ECDF lines by group |
| Separate By Column |  | String | - | Optional column for splitting ECDF plots into<br>subplots |
| Y Axis Mode |  | String | probability | Display cumulative probability, raw count, or<br>cumulative sum |
| CDF Mode |  | String | standard | 'standard' shows P(X ≤ x), 'reversed' shows P(X ≥<br>x), 'complementary' shows 1 - P(X ≤ x) |
| Orientation |  | String | vertical | Plot ECDF vertically or horizontally |
| Show Markers |  | Boolean | false | Display sample markers on the ECDF line |
| Marginal Plot |  | String | none | Optional marginal plot to display alongside the<br>ECDF |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
