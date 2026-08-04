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
title: "File Scan From Input"
description: "Scan data from file paths provided by input tuples"
category: "Data Input"
operator_type: "FileScanOp"
tags: [data-input]
---

[Home](../../) > [Data Input](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Encoding | ✓ | UTF_8, UTF_16, US_ASCII | UTF_8 |  |
| Extract |  | Boolean | false |  |
| Include Filename |  | Boolean | false |  |
| Attribute Type | ✓ | string, single string, integer, long,<br>double, boolean, timestamp, binary,<br>large binary | string |  |
| Attribute Name | ✓ | String | line |  |
| Limit |  | Integer | - |  |
| Offset |  | Integer | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
