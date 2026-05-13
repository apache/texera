---
title: "Twitter Search API"
description: "Retrieve data from Twitter Search API"
category: "External API"
operator_type: "TwitterSearch"
tags: [external-api]
---

[Home](../../) > [External Api](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| API Key | ✓ | String | - |  |
| API Secret Key | ✓ | String | - |  |
| Stop Upon Rate Limit | ✓ | Boolean | false | Stop when hitting rate limit? |
| Search Query | ✓ | String | - | Up to 1024 characters (Limited by Twitter) |
| Limit | ✓ | Integer | 100 | Maximum number of tweets to retrieve |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
