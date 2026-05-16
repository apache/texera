/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export type VisualTraceStepKind = "source" | "match" | "compute" | "render";

export interface VisualTraceMetric {
  label: string;
  value: string;
}

export interface VisualTraceStep {
  title: string;
  detail?: string;
  operatorId?: string;
  operatorLabel?: string;
  image?: string;
  imageAlt?: string;
  kind?: VisualTraceStepKind;
  metrics?: VisualTraceMetric[];
}

export interface VisualTrace {
  title: string;
  subtitle?: string;
  summary?: string;
  heroImage?: string;
  heroImageAlt?: string;
  heroMetric?: VisualTraceMetric;
  steps: VisualTraceStep[];
}

export interface VisualTraceSelection {
  title?: string;
  image?: string;
  imageAlt?: string;
}
