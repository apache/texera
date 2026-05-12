---
title: "Reddit Search"
description: "Search for recent posts with python-wrapped Reddit API, PRAW"
category: "External API"
operator_type: "RedditSearch"
tags: [external-api]
---

[Home](../../) > [External Api](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Client Id | ✓ | String | - | Client id that uses to access Reddit API |
| Client Secret | ✓ | String | - | Client secret that uses to access Reddit API |
| Query | ✓ | String | - | Search query |
| Limit | ✓ | Integer | 100 | Up to 1000 |
| Sorting | ✓ | none, controversial, gilded, hot, new,<br>rising, top | none | The sorting method, hot, new, etc |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
