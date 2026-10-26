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

/** Fixed, named colors for workflow annotations. Never interpolate imported CSS. */
export const WORKFLOW_COLOR_PRESETS = [
  { id: "yellow", label: "Yellow", fill: "#fff1b8" },
  { id: "blue", label: "Blue", fill: "#d6e4ff" },
  { id: "green", label: "Green", fill: "#d9f7be" },
  { id: "pink", label: "Pink", fill: "#ffd6e7" },
  { id: "purple", label: "Purple", fill: "#efdbff" },
  { id: "orange", label: "Orange", fill: "#ffe7ba" },
] as const;

export type WorkflowColor = (typeof WORKFLOW_COLOR_PRESETS)[number]["id"];

export function isWorkflowColor(value: unknown): value is WorkflowColor {
  return WORKFLOW_COLOR_PRESETS.some(preset => preset.id === value);
}

/** Old workflows and unrecognized imported presets retain their original fill. */
export function getWorkflowColorFill(value: unknown, defaultFill: string): string {
  return WORKFLOW_COLOR_PRESETS.find(preset => preset.id === value)?.fill ?? defaultFill;
}
