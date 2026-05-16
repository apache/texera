---
title: "Gauge Chart"
description: "Visualize a single value with a radial gauge chart, showing progress towards a goal with optional steps, threshold, and delta."
category: "Financial"
operator_type: "GaugeChart"
tags: [visualization, financial]
---

[Home](../../../) > [Visualization](../../) > [Financial](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Gauge Value | ✓ | String | - | The primary value displayed on the gauge chart |
| Delta |  | String | - | The baseline value used to calculate the delta<br>from the gauge value |
| Threshold Value |  | String | - | Defines a boundary or target value shown on the<br>gauge chart |
| Steps |  | List<Step> | - | List of step ranges for the gauge |
| ↳ Start |  | String | - |  |
| ↳ End |  | String | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
