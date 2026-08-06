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
title: "Filled Area Plot"
description: "Visualize data in a filled area plot"
category: "Basic"
operator_type: "FilledAreaPlot"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| X-axis Attribute | ✓ | String | - | The attribute for your x-axis |
| Y-axis Attribute | ✓ | String | - | The attribute for your y-axis |
| Line Group |  | String | - | The attribute for group of each line |
| Color |  | String | - | Choose an attribute to color the plot |
| Split Plot by Line Group | ✓ | Boolean | false | Do you want to split the graph |
| Pattern |  | String | - | Add texture to the chart based on an attribute |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
