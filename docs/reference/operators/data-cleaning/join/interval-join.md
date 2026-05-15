---
title: "Interval Join"
description: "Join two inputs with left table join key in the range of [right table join key, right table join key + constant value]"
category: "Join"
operator_type: "IntervalJoin"
tags: [data-cleaning, join]
---

[Home](../../../) > [Data Cleaning](../../) > [Join](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Interval Constant | ✓ | Long | 10 | Left attri in (right, right + constant) |
| Include Left Bound | ✓ | Boolean | true | Include condition left attri = right attri |
| Include Right Bound | ✓ | Boolean | true | Include condition left attri = right attri |
| Time interval type |  | TimeIntervalType | day | Year, Month, Day, Hour, Minute or Second |
| Left Input attr | ✓ | String (integer, long, double, timestamp) | - | Choose one attribute in the left table |
| Right Input attr | ✓ | String | - | Choose one attribute in the right table |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
