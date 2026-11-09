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
title: "Histogram2D"
description: "Displays a bivariate histogram as a density heatmap"
category: "Statistical"
operator_type: "Histogram2D"
tags: [visualization, statistical]
---

[Home](../../../) > [Visualization](../../) > [Statistical](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| X Column | ✓ | String | - | Numeric column for the X axis bins |
| Y Column | ✓ | String | - | Numeric column for the Y axis bins |
| X Bins | ✓ | Integer | 10 | Number of bins along the X axis (Default: 10) |
| Y Bins | ✓ | Integer | 10 | Number of bins along the Y axis (Default: 10) |
| Normalization |  | density, probability, percent | density | Type of histogram normalization |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
