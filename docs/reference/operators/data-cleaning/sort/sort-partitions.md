---
title: "Sort Partitions"
description: "Sort Partitions"
category: "Sort"
operator_type: "SortPartitions"
tags: [data-cleaning, sort]
---

[Home](../../../) > [Data Cleaning](../../) > [Sort](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Attribute | ✓ | String (integer, long, double) | - | Attribute to sort (must be numerical) |
| Attribute Domain Min | ✓ | Long | 0 | Minimum value of the domain of the attribute |
| Attribute Domain Max | ✓ | Long | 0 | Maximum value of the domain of the attribute |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
