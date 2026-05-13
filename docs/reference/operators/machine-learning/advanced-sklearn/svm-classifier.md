---
title: "SVM Classifier"
description: "Sklearn SVM Classifier Operator"
category: "Advanced Sklearn"
operator_type: "SVCTrainer"
tags: [machine-learning, advanced-sklearn]
---

[Home](../../../) > [Machine Learning](../../) > [Advanced Sklearn](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Parameter Setting | ✓ | [SklearnAdvancedSVCParameters](../../../parameters/sklearn-advanced-svc-parameters/) | - |  |
| Ground Truth Attribute Column | ✓ | String | - | Ground truth attribute column |
| Selected Features | ✓ | List | - | Features used to train the model |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
