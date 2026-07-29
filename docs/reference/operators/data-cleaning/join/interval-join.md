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
title: "Interval Join"
description: "Join two inputs with left table join key in the range of [right table join key, right table join key + constant value]"
category: "Join"
operator_type: "IntervalJoin"
tags: [data-cleaning, join]
---

[Home](../../../) > [Data Cleaning](../../) > [Join](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Interval Constant | ✓ | Long | 10 | Left attri in (right, right + constant) |
| Include Left Bound | ✓ | Boolean | true | Include condition left attri = right attri |
| Include Right Bound | ✓ | Boolean | true | Include condition left attri = right attri |
| Time interval type |  | TimeIntervalType | day | Year, Month, Day, Hour, Minute or Second |
| Left Input attr | ✓ | String (integer, long, double, timestamp) | - | Choose one attribute in the left table |
| Right Input attr | ✓ | String | - | Choose one attribute in the right table |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
