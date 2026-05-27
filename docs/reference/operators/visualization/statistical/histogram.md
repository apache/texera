---
title: "Histogram"
description: "Visualize data in a Histogram Chart"
category: "Statistical"
operator_type: "Histogram"
tags: [visualization, statistical]
---

[Home](../../../) > [Visualization](../../) > [Statistical](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Color Column |  | String | - | Column for differentiating data by its value |
| SeparateBy Column |  | String | - | Column for separating histogram chart by its value |
| Distribution Type |  | String | - | Distribution type (rug, box, violin) |
| Pattern |  | String | - | Add texture to the chart based on an attribute |
| Value Column | ✓ | String | - | Column for counting values |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
