---
title: "Nested Table"
description: "Visualize Data in a Depth Two Nested Table"
category: "Visualization"
operator_type: "NestedTable"
tags: [visualization]
---

[Home](../../) > [Visualization](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Add Attribute | ✓ | List<Attribute> | - | List of columns to include in the nested table<br>chart and their subgroup |
| ↳ Attribute group | ✓ | String | - |  |
| ↳ Original attribute Name | ✓ | String | - |  |
| ↳ New Attribute Name |  | String | - |  |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../output-modes/#single-snapshot) |
