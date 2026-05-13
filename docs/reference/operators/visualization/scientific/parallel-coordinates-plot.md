---
title: "Parallel Coordinates Plot"
description: "Visualize multivariate data using parallel coordinate axes"
category: "Scientific"
operator_type: "ParallelCoordinatesPlot"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Dimensions | ✓ | List | - | List of numeric columns to visualize as parallel<br>axes (min: 1, At least one dimension is required) |
| Color Column |  | String | - | Column used to color or group the lines |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
