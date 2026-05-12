---
title: "Icicle Chart"
description: "Visualize hierarchical data from root to leaves"
category: "Basic"
operator_type: "IcicleChart"
tags: [visualization, basic]
---

[Home](../../../) > [Visualization](../../) > [Basic](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Hierarchy Path | ✓ | List<Hierarchy Section> | - | Hierarchy of attributes from a root (higher-level<br>category) to leaves (lower-level category) |
| ↳ Attribute Name | ✓ | String | - |  |
| Value Column | ✓ | String (integer, long, double) | - | The value associated with the size of each sector<br>in the chart |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
