---
title: "Volcano Plot"
description: "Displays statistical significance versus effect size"
category: "Scientific"
operator_type: "VolcanoPlot"
tags: [visualization, scientific]
---

[Home](../../../) > [Visualization](../../) > [Scientific](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Effect Size (log2 Fold Change) | ✓ | String | - | Select the column representing the effect size or<br>magnitude of change between two experimental<br>groups. This value is typically a log2 fold<br>change and is used for the x-axis of the volcano<br>plot |
| P-Value Column | ✓ | String | - | Select the column representing the p-value<br>associated with the statistical test for each<br>feature. This value is transformed using<br>-log10(p-value) and plotted on the y-axis to<br>indicate statistical significance |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
