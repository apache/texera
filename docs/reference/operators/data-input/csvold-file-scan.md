---
title: "CSVOld File Scan"
description: "Scan data from a CSVOld file"
category: "Data Input"
operator_type: "CSVOldFileScan"
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
| Delimiter |  | String | , | Delimiter to separate each line into fields |
| Header |  | Boolean | true | Whether the CSV file contains a header line |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
