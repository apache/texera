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
title: "Split"
description: "Split data to two different ports"
category: "Utilities"
operator_type: "Split"
tags: [utilities]
---

[Home](../../) > [Utilities](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Split Percentage |  | Integer | 80 | Percentage of data going to the upper port |
| Auto-Generate Seed |  | Boolean | true | Shuffle the data based on a random seed |
| ↳ Seed |  | Integer | 1 | An int for reproducible output across multiple<br>runs |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
| 1 | [Set Snapshot](../../output-modes/#set-snapshot) |
