---
title: "KNN Classifier"
description: "Sklearn KNN Classifier Operator"
category: "Advanced Sklearn"
operator_type: "KNNClassifierTrainer"
tags: [machine-learning, advanced-sklearn]
---

[Home](../../../) > [Machine Learning](../../) > [Advanced Sklearn](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Parameter Setting | ✓ | [SklearnAdvancedKNNParameters](../../../parameters/sklearn-advanced-knn-parameters/) | - |  |
| Ground Truth Attribute Column | ✓ | String | - | Ground truth attribute column |
| Selected Features | ✓ | List | - | Features used to train the model |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
