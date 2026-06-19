---
title: "Hugging Face Sentiment Analysis"
description: "Analyzing Sentiments with a Twitter-Based Model from Hugging Face"
category: "Hugging Face"
operator_type: "HuggingFaceSentimentAnalysis"
tags: [machine-learning, hugging-face]
---

[Home](../../../) > [Machine Learning](../../) > [Hugging Face](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Attribute | ✓ | String | - | Column to perform sentiment analysis on |
| Positive Result Attribute | ✓ | String | huggingface_sentiment_positive | Column name of the sentiment analysis result<br>(positive) |
| Neutral Result Attribute | ✓ | String | huggingface_sentiment_neutral | Column name of the sentiment analysis result<br>(neutral) |
| Negative Result Attribute | ✓ | String | huggingface_sentiment_negative | Column name of the sentiment analysis result<br>(negative) |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
