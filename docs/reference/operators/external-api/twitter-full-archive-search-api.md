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
title: "Twitter Full Archive Search API"
description: "Retrieve data from Twitter Full Archive Search API"
category: "External API"
operator_type: "TwitterFullArchiveSearch"
tags: [external-api]
---

[Home](../../) > [External Api](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| API Key | ✓ | String | - |  |
| API Secret Key | ✓ | String | - |  |
| Stop Upon Rate Limit | ✓ | Boolean | false | Stop when hitting rate limit? |
| Search Query | ✓ | String | - | Up to 1024 characters (Limited By Twitter) |
| From Datetime | ✓ | String | 2021-04-01T00:00:00Z | ISO 8601 format |
| To Datetime | ✓ | String | 2021-05-01T00:00:00Z | ISO 8601 format |
| Limit | ✓ | Integer | 100 | Maximum number of tweets to retrieve |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
