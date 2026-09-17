<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

---
title: "Training: Gradient Boosting"
description: "Sklearn Training: Gradient Boosting Operator"
category: "Sklearn Training"
operator_type: "SklearnTrainingGradientBoosting"
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
