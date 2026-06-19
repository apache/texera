---
title: "Split"
description: "Split data to two different ports"
category: "Utilities"
operator_type: "Split"
tags: [utilities]
---

[Home](../../) > [Utilities](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Split Percentage |  | Integer | 80 | Percentage of data going to the upper port |
| Auto-Generate Seed |  | Boolean | true | Shuffle the data based on a random seed |
| ↳ Seed |  | Integer | 1 | An int for reproducible output across multiple<br>runs |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
| 1 | [Set Snapshot](../../output-modes/#set-snapshot) |
