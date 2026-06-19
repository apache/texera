---
title: "Sklearn Testing"
description: "It will generate scorers for Sklearn model"
category: "Sklearn"
operator_type: "SklearnTesting"
tags: [machine-learning, sklearn]
---

[Home](../../../) > [Machine Learning](../../) > [Sklearn](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Regression | ✓ | Boolean | false | Choose to solve a regression task |
| Model Attribute | ✓ | String | model | Attribute corresponding to ML model |
| Target Attribute | ✓ | String | - | Attribute in your dataset corresponding to target |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
