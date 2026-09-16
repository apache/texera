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
title: "Sklearn Prediction"
description: "Sklearn Prediction Operator"
category: "Sklearn"
operator_type: "SklearnPrediction"
tags: [machine-learning, sklearn]
---

[Home](../../../) > [Machine Learning](../../) > [Sklearn](../)

### Input Properties

| Property | Requirement | Type | Default | Description |
|----------|-------------|------|---------|-------------|
| Model Attribute | ✓ | String | model | Attribute corresponding to ML model |
| Output Attribute Name | ✓ | String | prediction | Attribute name of the prediction result |
| Ground Truth Attribute Name To Ignore |  | String | - | Attribute name of the ground truth |

### Output Ports

| Port | Mode |
|------|------|
| 0 | [Set Snapshot](../../../output-modes/#set-snapshot) |
