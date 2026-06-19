---
title: "Wind Rose Chart"
description: "Displays wind distribution using a polar bar chart"
category: "Scientific"
operator_type: "WindRoseChart"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Radial Values (r) | ✓ | String | - | Numeric values representing magnitude (e.g.,<br>frequency) |
| Angular Values (θ) | ✓ | String | - | Direction or angle categories (e.g., N, NE, E) |
| Color Group |  | String | - | Optional grouping column (e.g., wind strength) |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
