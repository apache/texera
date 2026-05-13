---
title: "Substring Search"
description: "Search for Substring(s) in a string column"
category: "Search"
operator_type: "SubstringSearch"
tags: [search]
---

[Home](../../) > [Search](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| attribute | ✓ | String | - | Column to search substring on |
| Substring | ✓ | String | - | Substring |
| Case Sensitive | ✓ | Boolean | false | Whether the substring match is case sensitive |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
