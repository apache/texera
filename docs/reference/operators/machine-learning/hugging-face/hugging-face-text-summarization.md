---
title: "Hugging Face Text Summarization"
description: "Summarize the given text content with a mini2bert pre-trained model from Hugging Face"
category: "Hugging Face"
operator_type: "HuggingFaceTextSummarization"
tags: [machine-learning, hugging-face]
---

[Home](../../../) > [Machine Learning](../../) > [Hugging Face](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Attribute | ✓ | String | - | Attribute to perform text summarization on |
| Result Attribute Name |  | String | summary | Attribute name of the text summary result |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
