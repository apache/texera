---
title: "Twitter Full Archive Search API"
description: "Retrieve data from Twitter Full Archive Search API"
category: "External API"
operator_type: "TwitterFullArchiveSearch"
tags: [external-api]
---

[Home](../../) > [External Api](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| API Key | ✓ | String | - |  |
| API Secret Key | ✓ | String | - |  |
| Stop Upon Rate Limit | ✓ | Boolean | false | Stop when hitting rate limit? |
| Search Query | ✓ | String | - | Up to 1024 characters (Limited By Twitter) |
| From Datetime | ✓ | String | 2021-04-01T00:00:00Z | ISO 8601 format |
| To Datetime | ✓ | String | 2021-05-01T00:00:00Z | ISO 8601 format |
| Limit | ✓ | Integer | 100 | Maximum number of tweets to retrieve |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
