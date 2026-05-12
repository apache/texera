---
title: "Figure Factory Table"
description: "Visualize data in a figure factory table"
category: "Basic"
operator_type: "FigureFactoryTable"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Font Size |  | Double | 12 | Font size of the Figure Factory Table |
| Font Color (Hex Code) |  | String | #000000 | Font color of the Figure Factory Table |
| Row Height |  | Double | 30 | Row height of the Figure Factory Table |
| Add Attribute | ✓ | List<Attribute> | [1 items] | List of columns to include in the figure factory<br>table |
| ↳ Attribute Name | ✓ | String | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
