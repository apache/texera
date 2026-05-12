---
title: "AsterixDB Source"
description: "Read data from a AsterixDB instance"
category: "Database Connector"
operator_type: "AsterixDBSource"
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
| Limit |  | Long | - | Max output count |
| Offset |  | Long | - | Starting point of output |
| Keyword Search? |  | Boolean | false |  |
| ↳ Keyword Search Column |  | String | - |  |
| ↳ Keywords to Search |  | String | - | "['hello', 'world'], {'mode':'any'}" OR<br>"['hello', 'world'], {'mode':'all'}" |
| Progressive? |  | Boolean | false |  |
| ↳ Batch by Column |  | String | - |  |
| ↳ Min |  | String | auto |  |
| ↳ Max |  | String | auto |  |
| ↳ Batch by Interval |  | Long | 1000000000 |  |
| Geo Search? |  | Boolean | false |  |
| ↳ Geo Search By Columns |  | List<String> | - | Column(s) to check if any of them is in the<br>bounding box below |
| ↳ Geo Search Bounding Box |  | List<String> | - | At least 2 entries should be provided to form a<br>bounding box. format of each entry: long, lat |
| Regex Search? |  | Boolean | false |  |
| ↳ Regex Search By Column |  | String | - |  |
| ↳ Regex to Search |  | String | - |  |
| Filter Condition? |  | Boolean | false |  |
| ↳ Predicates |  | List<Filter Predicate> | - | Multiple predicates in OR |
| &nbsp;&nbsp;↳ Attribute | ✓ | String | - |  |
| &nbsp;&nbsp;↳ Condition | ✓ | =, >, >=, <, <=, !=, is null,<br>is not null | - |  |
| &nbsp;&nbsp;↳ Value |  | String | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
