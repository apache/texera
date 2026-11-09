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
