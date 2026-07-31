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
title: "Scatter Plot"
description: "View the result in a scatterplot"
category: "Basic"
operator_type: "Scatterplot"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| X-Column | ✓ | String (integer, double) | - | X Column |
| Y-Column | ✓ | String (integer, double) | - | Y Column |
| Alpha Value |  | Double | 1.0 | Alpha (opacity) value from 0.0 (transparent) to<br>1.0 (opaque) |
| Color-Column |  | String | - | Dots will be assigned different colors based on<br>their values of this column |
| log scale X |  | Boolean | false | Values in X-column is log-scaled |
| log scale Y |  | Boolean | false | Values in Y-column is log-scaled |
| Hover column |  | String | - | Column value to display when a dot is hovered over |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
