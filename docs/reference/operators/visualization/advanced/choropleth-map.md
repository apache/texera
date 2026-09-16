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
title: "Choropleth Map"
description: "Visualize data using a Choropleth Map that uses shades of colors to show differences in properties or quantities between regions"
category: "Advanced"
operator_type: "ChoroplethMap"
tags: [visualization, advanced]
---

[Home](../../../) > [Visualization](../../) > [Advanced](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Locations Column | ✓ | String | - | Column used to describe location. Currently only<br>supports countries and needs to be three-letter<br>ISO country code |
| Color Column | ✓ | String (integer, long, double) | - | Column used to determine intensity of color of<br>the region |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
