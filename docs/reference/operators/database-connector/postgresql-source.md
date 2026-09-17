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
title: "PostgreSQL Source"
description: "Read data from a PostgreSQL instance"
category: "Database Connector"
operator_type: "PostgreSQLSource"
tags: [database-connector]
---

[Home](../../) > [Database Connector](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Host | ✓ | String | - |  |
| Port | ✓ | String | default | A port number or 'default' |
| Database | ✓ | String | - |  |
| Table Name | ✓ | String | - |  |
| Username | ✓ | String | - |  |
| Password | ✓ | String | - |  |
| Limit |  | Long | - | Max output count |
| Offset |  | Long | - | Starting point of output |
| Keyword Search? |  | Boolean | false |  |
| ↳ Keyword Search Column |  | String | - |  |
| ↳ Keywords to Search |  | String | - | E.g. 'sore & throat' for AND; 'sore', 'throat'<br>for OR. See official postgres documents for<br>details |
| Progressive? |  | Boolean | false |  |
| ↳ Batch by Column |  | String | - |  |
| ↳ Min |  | String | auto |  |
| ↳ Max |  | String | auto |  |
| ↳ Batch by Interval |  | Long | 1000000000 |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
