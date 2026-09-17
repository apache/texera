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
title: "Hierarchy Chart"
description: "Visualize data in hierarchy"
category: "Basic"
operator_type: "HierarchyChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Chart Type | ✓ | treemap, sunburst | - | Treemap or Sunburst |
| Hierarchy Path | ✓ | List<Hierarchy Section> | - | Hierarchy of attributes from a higher-level<br>category to lower-level category |
| ↳ Attribute Name | ✓ | String | - |  |
| Value Column | ✓ | String (integer, long, double) | - | The value associated with the size of each sector<br>in the chart |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
