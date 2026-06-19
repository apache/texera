---
title: "Hierarchy Chart"
description: "Visualize data in hierarchy"
category: "Basic"
operator_type: "HierarchyChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Chart Type | ✓ | treemap, sunburst | - | Treemap or Sunburst |
| Hierarchy Path | ✓ | List<Hierarchy Section> | - | Hierarchy of attributes from a higher-level<br>category to lower-level category |
| ↳ Attribute Name | ✓ | String | - |  |
| Value Column | ✓ | String (integer, long, double) | - | The value associated with the size of each sector<br>in the chart |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
