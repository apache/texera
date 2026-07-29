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
title: "Volcano Plot"
description: "Displays statistical significance versus effect size"
category: "Scientific"
operator_type: "VolcanoPlot"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Effect Size (log2 Fold Change) | ✓ | String | - | Select the column representing the effect size or<br>magnitude of change between two experimental<br>groups. This value is typically a log2 fold<br>change and is used for the x-axis of the volcano<br>plot |
| P-Value Column | ✓ | String | - | Select the column representing the p-value<br>associated with the statistical test for each<br>feature. This value is transformed using<br>-log10(p-value) and plotted on the y-axis to<br>indicate statistical significance |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
