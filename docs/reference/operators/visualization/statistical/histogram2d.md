---
title: "Histogram2D"
description: "Displays a bivariate histogram as a density heatmap"
category: "Statistical"
operator_type: "Histogram2D"
tags: [visualization, statistical]
---

[Home](../../../) > [Visualization](../../) > [Statistical](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| X Column | ✓ | String | - | Numeric column for the X axis bins |
| Y Column | ✓ | String | - | Numeric column for the Y axis bins |
| X Bins | ✓ | Integer | 10 | Number of bins along the X axis (Default: 10) |
| Y Bins | ✓ | Integer | 10 | Number of bins along the Y axis (Default: 10) |
| Normalization |  | density, probability, percent | density | Type of histogram normalization |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
