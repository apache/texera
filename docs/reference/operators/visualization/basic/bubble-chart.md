---
title: "Bubble Chart"
description: "A 3D Scatter Plot; Bubbles are graphed using x and y labels, and their sizes determined by a z-value."
category: "Basic"
operator_type: "BubbleChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| X-Column | ✓ | String | - | Data column for the x-axis |
| Y-Column | ✓ | String | - | Data column for the y-axis |
| Z-Column | ✓ | String | - | Data column to determine bubble size |
| Enable Color |  | Boolean | false | Colors bubbles using a data column |
| Color-Column | ✓ | String | - | Picks data column to color bubbles with if color<br>is enabled |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
