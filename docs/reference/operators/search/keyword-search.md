---
title: "Keyword Search"
description: "Search for keyword(s) in a string column"
category: "Search"
operator_type: "KeywordSearch"
tags: [search]
---

[Home](../../) > [Search](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| attribute | ✓ | String | - | Column to search keyword on |
| keywords | ✓ | String | - | Keywords |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
