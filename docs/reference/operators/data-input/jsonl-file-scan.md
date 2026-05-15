---
title: "JSONL File Scan"
description: "Scan data from a JSONL file"
category: "Data Input"
operator_type: "JSONLFileScan"
tags: [data-input]
---

[Home](../../) > [Data Input](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| File | ✓ | String | - |  |
| File Encoding | ✓ | UTF_8, UTF_16, US_ASCII | UTF_8 | Decoding charset to use on input |
| Limit |  | Integer | - | Max output count |
| Offset |  | Integer | - | Starting point of output |
| Flatten | ✓ | Boolean | false | Flatten nested objects and arrays |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
