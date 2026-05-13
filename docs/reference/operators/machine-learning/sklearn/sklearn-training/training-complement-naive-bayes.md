---
title: "Training: Complement Naive Bayes"
description: "Sklearn Training: Complement Naive Bayes Operator"
category: "Sklearn Training"
operator_type: "SklearnTrainingComplementNaiveBayes"
tags: [machine-learning, sklearn, sklearn-training]
---

[Home](../../../../) > [Machine Learning](../../../) > [Sklearn](../../) > [Sklearn Training](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Target Attribute | ✓ | String | - | Attribute in your dataset corresponding to target |
| Count Vectorizer |  | Boolean | false | Convert a collection of text documents to a<br>matrix of token counts |
| ↳ Text Attribute |  | String | - | Attribute in your dataset with text to vectorize |
| ↳ Tfidf Transformer |  | Boolean | false | Transform a count matrix to a normalized tf or<br>tf-idf representation |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../../output-modes/#set-snapshot) |
