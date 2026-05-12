---
title: "Line Chart"
description: "View the result in line chart"
category: "Basic"
operator_type: "LineChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Y Label |  | String | Y Axis | The label for y axis |
| X Label |  | String | X Axis | The label for x axis |
| Lines | ✓ | List<Line> | - |  |
| ↳ Y Value | ✓ | String | - | Value for y axis |
| ↳ X Value | ✓ | String | - | Value for x axis |
| ↳ Line Mode | ✓ | line, dots, line with dots | line with dots |  |
| ↳ Line Name |  | String | - |  |
| ↳ Line Color |  | String | - | Must be a valid CSS color or hex color string |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
