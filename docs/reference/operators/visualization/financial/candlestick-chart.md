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
title: "Candlestick Chart"
description: "Visualize data in a Candlestick Chart"
category: "Financial"
operator_type: "CandlestickChart"
tags: [visualization, financial]
---

[Home](../../../) > [Visualization](../../) > [Financial](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Date Column | ✓ | String | - | The date of the candlestick |
| Opening Price Column | ✓ | String | - | The opening price of the candlestick |
| Highest Price Column | ✓ | String | - | The highest price of the candlestick |
| Lowest Price Column | ✓ | String | - | The lowest price of the candlestick |
| Closing Price Column | ✓ | String | - | The closing price of the candlestick |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
