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
