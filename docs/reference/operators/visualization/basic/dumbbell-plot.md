---
title: "Dumbbell Plot"
description: "Visualize data in a Dumbbell Plot. A dumbbell plot (also known as a lollipop chart) is typically used to compare two distinct values or time points for the same entity."
category: "Basic"
operator_type: "DumbbellPlot"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Category Column Name | ✓ | String | - | The name of the category column |
| Dumbbell Start Value | ✓ | String | - | The start point value of each dumbbell |
| Dumbbell End Value | ✓ | String | - | The end value of each dumbbell |
| Measurement Column Name | ✓ | String (integer, long, double) | - | The name of the measurement column |
| Compared Column Name | ✓ | String | - | The column name that is being compared |
| Dots |  | List<Dumbbell Dot> | - |  |
| ↳ Dot Column Value | ✓ | String (integer, long, double) | - | Value for dot axis |
| Show Legends? |  | Boolean | false | Whether to show legends in the graph |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
