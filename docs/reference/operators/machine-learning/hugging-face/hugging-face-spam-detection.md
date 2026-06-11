---
title: "Hugging Face Spam Detection"
description: "Spam Detection by SMS Spam Detection Model from Hugging Face"
category: "Hugging Face"
operator_type: "HuggingFaceSpamSMSDetection"
tags: [machine-learning, hugging-face]
---

[Home](../../../) > [Machine Learning](../../) > [Hugging Face](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Attribute | ✓ | String | - | Column to perform spam detection on |
| Spam Result Attribute | ✓ | String | is_spam | Column name of whether spam or not |
| Score Result Attribute | ✓ | String | score | Column name of Probability for classification |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
