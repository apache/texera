---
title: "MySQL Source"
description: "Read data from a MySQL instance"
category: "Database Connector"
operator_type: "MySQLSource"
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
| ↳ Keywords to Search |  | String | - |  |
| Progressive? |  | Boolean | false |  |
| ↳ Batch by Column |  | String | - |  |
| ↳ Min |  | String | auto |  |
| ↳ Max |  | String | auto |  |
| ↳ Batch by Interval |  | Long | 1000000000 |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
