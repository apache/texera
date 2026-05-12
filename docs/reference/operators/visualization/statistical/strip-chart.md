---
title: "Strip Chart"
description: "Visualize distribution of data points as a strip plot"
category: "Statistical"
operator_type: "StripChart"
tags: [visualization, statistical]
---

[Home](../../../) > [Visualization](../../) > [Statistical](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| X-Axis Column | ✓ | String | - | Column containing numeric values for the x-axis |
| Y-Axis Column | ✓ | String | - | Column containing categorical values for the<br>y-axis |
| Color By |  | String | - | Optional - Color points by category |
| Facet Column |  | String | - | Optional - Create separate subplots for each<br>category |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
