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
title: "Reddit Search"
description: "Search for recent posts with python-wrapped Reddit API, PRAW"
category: "External API"
operator_type: "RedditSearch"
tags: [external-api]
---

[Home](../../) > [External Api](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Client Id | ✓ | String | - | Client id that uses to access Reddit API |
| Client Secret | ✓ | String | - | Client secret that uses to access Reddit API |
| Query | ✓ | String | - | Search query |
| Limit | ✓ | Integer | 100 | Up to 1000 |
| Sorting | ✓ | none, controversial, gilded, hot, new,<br>rising, top | none | The sorting method, hot, new, etc |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
