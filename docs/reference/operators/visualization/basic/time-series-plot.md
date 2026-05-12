---
title: "Time Series Plot"
description: "Visualize trends and patterns over time."
category: "Basic"
operator_type: "TimeSeriesPlot"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Time Column | ✓ | String | - | The column containing time/date values (e.g.,<br>Date, Timestamp) |
| Value Column | ✓ | String | - | The numerical column to plot on the Y-axis (e.g.,<br>Sales, Temperature) |
| Category Column |  | String | No Selection | Optional - A categorical column to create<br>separate lines |
| Facet Column |  | String | No Selection | Optional - A column to create separate subplots |
| Plot Type | ✓ | String | line | Select the type of time series plot (line, area) |
| Show Range Slider |  | Boolean | false | Display a range slider at the bottom of the plot |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
