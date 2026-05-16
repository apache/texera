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

export type QuickStepActionType =
  | "add_snippet"
  | "run_workflow"
  | "profile_data"
  | "generate_report"
  | "publish_hub"
  | "notify";

export interface QuickStepAction {
  order: number;
  action: QuickStepActionType;
  label: string;
  config?: {
    snippetName?: string;
    snippetCategory?: string;
    message?: string;
  };
  waitForCompletion?: boolean;
  simulatedDurationMs?: number;
}

export interface QuickStep {
  id: string;
  name: string;
  description: string;
  icon: string;
  steps: QuickStepAction[];
  author: string;
  isPublic: boolean;
  createdAt: string;
  updatedAt?: string;
  seeded?: boolean;
}

export const QUICK_STEP_ACTION_TEMPLATES: ReadonlyArray<{
  type: QuickStepActionType;
  label: string;
  defaultLabel: string;
  defaultDurationMs: number;
}> = [
  { type: "profile_data", label: "Profile data", defaultLabel: "Profile data source", defaultDurationMs: 900 },
  {
    type: "add_snippet",
    label: "Add a snippet",
    defaultLabel: "Add snippet to canvas",
    defaultDurationMs: 600,
  },
  { type: "run_workflow", label: "Run workflow", defaultLabel: "Run current workflow", defaultDurationMs: 1500 },
  {
    type: "generate_report",
    label: "Generate report",
    defaultLabel: "Generate Results Dashboard",
    defaultDurationMs: 900,
  },
  { type: "publish_hub", label: "Publish to Hub", defaultLabel: "Publish workflow to Hub", defaultDurationMs: 700 },
  { type: "notify", label: "Show notification", defaultLabel: "Show notification", defaultDurationMs: 300 },
];

export const QUICK_STEP_ICON_CHOICES: ReadonlyArray<string> = [
  "⚡",
  "🧹",
  "🚀",
  "📤",
  "🔬",
  "📊",
  "🛠️",
  "🎯",
];
