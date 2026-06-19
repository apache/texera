---
title: "Regular Expression"
description: "Search a regular expression in a string column"
category: "Search"
operator_type: "Regex"
tags: [search]
---

[Home](../../) > [Search](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Case Insensitive |  | Boolean | false | Regex match is case sensitive |
| Attribute | ✓ | String | - | Column to search regex on |
| Regex | ✓ | String | - | Regular expression |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
