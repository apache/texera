---
title: "Projection"
description: "Keeps or drops the column"
category: "Data Cleaning"
operator_type: "Projection"
tags: [data-cleaning]
---

[Home](../../) > [Data Cleaning](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Drop Option | ✓ | Boolean | false | Check to drop the selected attributes |
| Attributes | ✓ | List<Attribute Unit> | - |  |
| ↳ Attribute | ✓ | String | - | Attribute name in the schema |
| ↳ Alias |  | String | - | Renamed attribute name |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
