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
title: "Figure Factory Table"
description: "Visualize data in a figure factory table"
category: "Basic"
operator_type: "FigureFactoryTable"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Font Size |  | Double | 12 | Font size of the Figure Factory Table |
| Font Color (Hex Code) |  | String | #000000 | Font color of the Figure Factory Table |
| Row Height |  | Double | 30 | Row height of the Figure Factory Table |
| Add Attribute | ✓ | List<Attribute> | [1 items] | List of columns to include in the figure factory<br>table |
| ↳ Attribute Name | ✓ | String | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
