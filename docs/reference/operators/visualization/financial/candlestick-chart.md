---
title: "Candlestick Chart"
description: "Visualize data in a Candlestick Chart"
category: "Financial"
operator_type: "CandlestickChart"
tags: [visualization, financial]
---

[Home](../../../) > [Visualization](../../) > [Financial](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Date Column | ✓ | String | - | The date of the candlestick |
| Opening Price Column | ✓ | String | - | The opening price of the candlestick |
| Highest Price Column | ✓ | String | - | The highest price of the candlestick |
| Lowest Price Column | ✓ | String | - | The lowest price of the candlestick |
| Closing Price Column | ✓ | String | - | The closing price of the candlestick |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
