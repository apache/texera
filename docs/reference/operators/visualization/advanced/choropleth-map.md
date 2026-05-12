---
title: "Choropleth Map"
description: "Visualize data using a Choropleth Map that uses shades of colors to show differences in properties or quantities between regions"
category: "Advanced"
operator_type: "ChoroplethMap"
tags: [visualization, advanced]
---

[Home](../../../) > [Visualization](../../) > [Advanced](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Locations Column | ✓ | String | - | Column used to describe location. Currently only<br>supports countries and needs to be three-letter<br>ISO country code |
| Color Column | ✓ | String (integer, long, double) | - | Column used to determine intensity of color of<br>the region |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Single Snapshot](../../../output-modes/#single-snapshot) |
