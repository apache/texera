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
title: "Radar Plot"
description: "View the result in a radar plot."
category: "Scientific"
operator_type: "RadarPlot"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Axes | ✓ | List | - | Numeric columns to use as radar axes |
| Trace Name Column |  | String | No Selection | Optional - Select a column to use for naming each<br>radar trace |
| Trace Color Column |  | String | No Selection | Optional - Select a column to use for coloring<br>each radar trace (note: if there are too many<br>traces with distinct coloring values, colors may<br>repeat) |
| Line Pattern | ✓ | solid, dash, dot | solid | Pattern of the lines connecting points on the<br>radar plot |
| Max Normalize | ✓ | Boolean | true | Normalize radar plot values by scaling them<br>relative to the maximum value on their respective<br>axes |
| Fill Trace | ✓ | Boolean | true | Fill the area within each radar trace |
| Show Point Markers | ✓ | Boolean | true | Display point markers on the radar plot |
| Show Legend |  | Boolean | true | Display the legend (note: without the legend, you<br>are unable to selectively hide or show traces in<br>the plot) |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
