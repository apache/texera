---
title: "Radar Plot"
description: "View the result in a radar plot."
category: "Scientific"
operator_type: "RadarPlot"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Axes | ✓ | List | - | Numeric columns to use as radar axes |
| Trace Name Column |  | String | No Selection | Optional - Select a column to use for naming each<br>radar trace |
| Trace Color Column |  | String | No Selection | Optional - Select a column to use for coloring<br>each radar trace (note: if there are too many<br>traces with distinct coloring values, colors may<br>repeat) |
| Line Pattern | ✓ | solid, dash, dot | solid | Pattern of the lines connecting points on the<br>radar plot |
| Max Normalize | ✓ | Boolean | true | Normalize radar plot values by scaling them<br>relative to the maximum value on their respective<br>axes |
| Fill Trace | ✓ | Boolean | true | Fill the area within each radar trace |
| Show Point Markers | ✓ | Boolean | true | Display point markers on the radar plot |
| Show Legend |  | Boolean | true | Display the legend (note: without the legend, you<br>are unable to selectively hide or show traces in<br>the plot) |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
