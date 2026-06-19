---
title: "Sklearn Prediction"
description: "Sklearn Prediction Operator"
category: "Sklearn"
operator_type: "SklearnPrediction"
tags: [machine-learning, sklearn]
---

[Home](../../../) > [Machine Learning](../../) > [Sklearn](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Model Attribute | ✓ | String | model | Attribute corresponding to ML model |
| Output Attribute Name | ✓ | String | prediction | Attribute name of the prediction result |
| Ground Truth Attribute Name To Ignore |  | String | - | Attribute name of the ground truth |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
