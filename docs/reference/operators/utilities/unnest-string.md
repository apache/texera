---
title: "Unnest String"
description: "Unnest the string values in the column separated by a delimiter to multiple values"
category: "Utilities"
operator_type: "UnnestString"
tags: [utilities]
---

[Home](../../) > [Utilities](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Delimiter | ✓ | String | , | String that separates the data |
| Attribute | ✓ | String | - | Column of the string to unnest |
| Result Attribute | ✓ | String | unnestResult | Column name of the unnest result |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
