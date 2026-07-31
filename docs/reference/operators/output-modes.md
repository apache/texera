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
title: "Output Port Modes"
description: "Reference for operator output port modes"
weight: 12
categories: [Reference]
tags: [reference]
---

[Home](../)

Texera operators emit data through output ports. Each port advertises a **mode** that describes how downstream operators should interpret the stream of tuples it produces.

### Set Snapshot

The port **re-emits the complete result set on each update**. Downstream operators always see the full materialized result.

### Delta Updates

The port emits an **incremental delta of the result set on each update**. Downstream operators apply the delta on top of prior state instead of receiving a re-materialized snapshot.

### Single Snapshot

The port emits **exactly one snapshot for the entire execution** (not per update). Used for visualization operators whose output may exceed the memory limit, making repeated full-snapshot emission impractical.

