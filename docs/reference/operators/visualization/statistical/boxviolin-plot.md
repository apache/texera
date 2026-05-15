---
title: "Box/Violin Plot"
description: "Visualize data using either a Box Plot or a Violin Plot. Box plots are drawn as a box with a vertical line down the middle which is mean value, and has horizontal lines attached to each side (known as “whiskers”). Violin plots provide more detail by showing a smoothed density curve on each side, and also include a box plot inside for comparison."
category: "Statistical"
operator_type: "BoxViolinPlot"
tags: [visualization, statistical]
---

[Home](../../../) > [Visualization](../../) > [Statistical](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Value Column | ✓ | String (integer, long, double) | - | Data column for box plot |
| Quartile Method | ✓ | linear, inclusive, exclusive | linear |  |
| Horizontal Orientation |  | Boolean | false | Orientation style |
| Violin Plot |  | Boolean | false | Check this box to overlay a violin plot on the<br>box plot; otherwise, show only the box plot |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
