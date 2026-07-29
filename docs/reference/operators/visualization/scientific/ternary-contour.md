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
title: "Ternary Contour"
description: "Shows how a measured value changes across all mixtures of three components that sum to a constant"
category: "Scientific"
operator_type: "TernaryContour"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Variable 1 | ✓ | String | - | First variable data field |
| Variable 2 | ✓ | String | - | Second variable data field |
| Variable 3 | ✓ | String | - | Third variable data field |
| Measured Value | ✓ | String | - | Measured value data field |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
