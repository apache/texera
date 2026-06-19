---
title: "Bar Chart"
description: "Visualize data in a Bar Chart"
category: "Basic"
operator_type: "BarChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Fields | ✓ | String | - | Visualize categorical data in a Bar Chart |
| Category Column |  | String | No Selection | Optional - Select a column to Color Code the<br>Categories |
| Horizontal Orientation |  | Boolean | false | Orientation Style |
| Pattern |  | String | - | Add texture to the chart based on an attribute |
| Value Column | ✓ | String (integer, long, double) | - | The value associated with each category |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
