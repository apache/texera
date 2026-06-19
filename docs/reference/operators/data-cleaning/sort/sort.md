---
title: "Sort"
description: "Sort based on the columns and sorting methods"
category: "Sort"
operator_type: "Sort"
tags: [data-cleaning, sort]
---

[Home](../../../) > [Data Cleaning](../../) > [Sort](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Attributes | ✓ | List<Criteria Unit> | - | Column to perform sorting on |
| ↳ Attribute | ✓ | String | - | Attribute name to sort by |
| ↳ Sort Preference | ✓ | ASC, DESC | - | Sort preference (ASC or DESC) |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
