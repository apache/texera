---
title: "Hugging Face Iris Logistic Regression"
description: "Predict whether an iris is an Iris-setosa using a pre-trained logistic regression model"
category: "Hugging Face"
operator_type: "HuggingFaceIrisLogisticRegression"
tags: [machine-learning, hugging-face]
---

[Home](../../../) > [Machine Learning](../../) > [Hugging Face](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Petal Length Cm Attribute | ✓ | String | - | Attribute in your dataset corresponding to<br>PetalLengthCm |
| Petal Width Cm Attribute | ✓ | String | - | Attribute in your dataset corresponding to<br>PetalWidthCm |
| Prediction Class Name | ✓ | String | Species_prediction | Output attribute name for the predicted class of<br>species |
| Prediction Probability Name | ✓ | String | Species_probability | Output attribute name for the prediction's<br>probability of being a Iris-setosa |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
