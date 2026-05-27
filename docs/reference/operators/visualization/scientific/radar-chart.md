---
title: "Radar Chart"
description: "Visualize data in a Radar Chart"
category: "Scientific"
operator_type: "RadarChart"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Name Column | ✓ | String | - | Column containing entity names for each radar |
| Value Columns | ✓ | List | - | Columns containing numeric values for radar chart<br>axes |
| Fill Opacity | ✓ | Double | 0.5 | Opacity value for radar chart fill from 0.0<br>(transparent) to 1.0 (opaque) |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
