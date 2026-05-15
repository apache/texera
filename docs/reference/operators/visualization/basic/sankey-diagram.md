---
title: "Sankey Diagram"
description: "Visualize data using a Sankey diagram"
category: "Basic"
operator_type: "SankeyDiagram"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Source Attribute | ✓ | String | - | The source node of the Sankey diagram |
| Target Attribute | ✓ | String | - | The target node of the Sankey diagram |
| Value Attribute | ✓ | String | - | The value/volume of the flow between source and<br>target |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
