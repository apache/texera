---
title: "SVM Regressor"
description: "Sklearn SVM Regressor Operator"
category: "Advanced Sklearn"
operator_type: "SVRTrainer"
tags: [machine-learning, advanced-sklearn]
---

[Home](../../../) > [Machine Learning](../../) > [Advanced Sklearn](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Parameter Setting | ✓ | [SklearnAdvancedSVRParameters](../../../parameters/sklearn-advanced-svr-parameters/) | - |  |
| Ground Truth Attribute Column | ✓ | String | - | Ground truth attribute column |
| Selected Features | ✓ | List | - | Features used to train the model |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
