---
title: "URL Fetcher"
description: "Fetch the content of a single URL"
category: "External API"
operator_type: "URLFetcher"
tags: [external-api]
---

[Home](../../) > [External Api](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| URL | ✓ | String | - | Only accepts standard URL format |
| Decoding | ✓ | UTF-8, RAW BYTES | - | The decoding method for the url content |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../output-modes/#set-snapshot) |
