---
title: "Bullet Chart"
description: "Visualize data using a Bullet Chart that shows a primary quantitative bar and delta indicator. Optional elements such as qualitative ranges (steps) and a performance threshold are displayed only when provided."
category: "Financial"
operator_type: "BulletChart"
tags: [visualization, financial]
---

[Home](../../../) > [Visualization](../../) > [Financial](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Value | ✓ | String | - | The actual value to display on the bullet chart |
| Delta Reference | ✓ | String | - | The reference value for the delta indicator.<br>e.g., 100 |
| Threshold Value |  | String | - | The performance threshold value. e.g., 100 |
| Steps |  | List<Step> | [] | Optional: Each step includes a start and end<br>value e.g., 0, 100 |
| ↳ Start |  | String | - |  |
| ↳ End |  | String | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
