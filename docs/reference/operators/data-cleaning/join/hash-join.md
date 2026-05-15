---
title: "Hash Join"
description: "Join two inputs"
category: "Join"
operator_type: "HashJoin"
tags: [data-cleaning, join]
---

[Home](../../../) > [Data Cleaning](../../) > [Join](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Left Input Attribute | ✓ | String | - | Attribute to be joined on the Left Input |
| Right Input Attribute | ✓ | String | - | Attribute to be joined on the Right Input |
| Join Type | ✓ | inner, left outer, right outer,<br>full outer | inner | Select the join type to execute |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
