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

import type { OperatorPropertySchema, PropertyIssue } from "./schema";

const imageOnly = ["image-classification", "object-detection", "image-segmentation", "image-to-text"];
const imageInput = [
  ...imageOnly,
  "visual-question-answering",
  "document-question-answering",
  "zero-shot-image-classification",
  "image-text-to-text",
  "image-to-image",
  "image-to-video",
];
const audioInput = ["automatic-speech-recognition", "audio-classification"];
const promptRequired = [
  "text-generation",
  "text-classification",
  "token-classification",
  "question-answering",
  "table-question-answering",
  "zero-shot-classification",
  "translation",
  "summarization",
  "feature-extraction",
  "fill-mask",
  "sentence-similarity",
  "text-ranking",
  "visual-question-answering",
  "document-question-answering",
  "zero-shot-image-classification",
];

function row(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}
function selectedTask(value: unknown): string | undefined {
  const task = row(value).task;
  return typeof task === "string" && task.trim() ? task : undefined;
}

export function huggingFaceFieldHidden(key: string, value: unknown): boolean {
  const task = selectedTask(value);
  if (key === "task") return true;
  if (["imageInput", "inputImageColumn"].includes(key)) return task === undefined || !imageInput.includes(task);
  if (["audioInput", "inputAudioColumn"].includes(key)) return task === undefined || !audioInput.includes(task);
  if (key === "promptColumn") return task !== undefined && (imageOnly.includes(task) || audioInput.includes(task));
  if (["systemPrompt", "maxNewTokens", "temperature"].includes(key)) return task !== "text-generation";
  if (key === "contextColumn") return task !== "question-answering";
  if (key === "candidateLabels")
    return task !== "zero-shot-classification" && task !== "zero-shot-image-classification";
  if (key === "sentencesColumn") return task !== "sentence-similarity" && task !== "text-ranking";
  return false;
}

export function huggingFaceIssues(schema: OperatorPropertySchema, value: unknown): PropertyIssue[] {
  const model = row(value);
  const task = selectedTask(value);
  const issues: PropertyIssue[] = [];
  const filled = (key: string) => typeof model[key] === "string" && String(model[key]).trim().length > 0;
  const check = (key: string, condition: boolean, message: string) => {
    if (schema.properties?.[key] && condition) issues.push({ path: [key], keyword: key, message });
  };
  if (!task) return [];
  check(
    "imageInput",
    imageInput.includes(task) && !filled("inputImageColumn") && !filled("imageInput"),
    "Upload an image or select an Input Image Column for this task."
  );
  check(
    "audioInput",
    audioInput.includes(task) && !filled("inputAudioColumn") && !filled("audioInput"),
    "Upload audio or select an Input Audio Column for this task."
  );
  check(
    "promptColumn",
    promptRequired.includes(task) && !filled("promptColumn"),
    "Select a prompt column for this task."
  );
  return issues;
}
