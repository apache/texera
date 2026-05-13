---
title: "Arrow File Scan"
description: "Scan data from an Arrow file"
category: "Data Input"
operator_type: "ArrowSource"
tags: [data-input]
---

[Home](../../) > [Data Input](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| File | ✓ | String | - |  |
| Limit |  | Integer | - | Max output count |
| Offset |  | Integer | - | Starting point of output |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
