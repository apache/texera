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

import { AbstractControl } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { PropertyContext } from "../types/custom-json-schema.interface";
import { propertyModel } from "./custom-formly-type";

/** Only exposed siblings are writable; a Form View card also renames its own control. */
export function huggingFacePropertyKey(field: FormlyFieldConfig, key: string): string | undefined {
  const context = field.props?.["propertyContext"] as PropertyContext | undefined;
  if (!context) return key;
  return key === context.propertyKey ? String(field.key) : context.bindings[key];
}

const nativeModel = (field: FormlyFieldConfig) => propertyModel(field) ?? field.model;

const imageOnlyTasks = ["image-classification", "object-detection", "image-segmentation", "image-to-text"];
const imageInputTasks = [
  ...imageOnlyTasks,
  "visual-question-answering",
  "document-question-answering",
  "zero-shot-image-classification",
  "image-text-to-text",
  "image-to-image",
  "image-to-video",
];
const audioInputTasks = ["automatic-speech-recognition", "audio-classification"];
const promptRequiredTasks = [
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

const hasText = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;

const getSelectedTask = (field: FormlyFieldConfig): string | undefined => {
  const key = huggingFacePropertyKey(field, "task");
  const fromForm = key ? field.form?.get(key)?.value ?? field.formControl?.parent?.get(key)?.value : undefined;
  if (hasText(fromForm)) return fromForm;
  const fromModel = nativeModel(field)?.task;
  return hasText(fromModel) ? fromModel : undefined;
};

function hideUnlessTask(mappedField: FormlyFieldConfig, predicate: (task: string | undefined) => boolean): void {
  mappedField.expressions = {
    ...mappedField.expressions,
    hide: (field: FormlyFieldConfig) => !predicate(getSelectedTask(field)),
  };
}

function hideUnlessTaskIn(mappedField: FormlyFieldConfig, tasks: string[]): void {
  hideUnlessTask(mappedField, task => task !== undefined && tasks.includes(task));
}

function bindMediaInput(
  mappedField: FormlyFieldConfig,
  tasks: string[],
  validatorName: string,
  columnKey: string,
  inputKey: string,
  message: string
): void {
  hideUnlessTaskIn(mappedField, tasks);
  mappedField.validators = {
    ...mappedField.validators,
    [validatorName]: {
      expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        if (t === undefined || !tasks.includes(t)) {
          return true;
        }
        const inputCol = nativeModel(field)?.[columnKey];
        if (typeof inputCol === "string" && inputCol.trim().length > 0) {
          return true;
        }
        const value = field.formControl?.value ?? nativeModel(field)?.[inputKey];
        return typeof value === "string" && value.trim().length > 0;
      },
      message: () => message,
    },
  };
  mappedField.validation = {
    ...mappedField.validation,
    show: true,
  };
}

/**
 * Applies the HuggingFace operator's task-driven field behaviour to a single formly field: the
 * `task` field is hidden, and each input field (image/audio uploads and their column pickers, the
 * prompt column, text-generation controls, context, candidate labels, sentences) gets the hide
 * expression and required-validator that the currently selected task calls for. A no-op for any
 * field whose key the HuggingFace operator does not drive. The widget a field renders as is decided
 * separately in {@link customFormlyFieldType}.
 */
export function applyHuggingFaceFieldBindings(mappedField: FormlyFieldConfig): void {
  const context = mappedField.props?.["propertyContext"] as PropertyContext | undefined;
  const hfKey = context?.propertyKey ?? mappedField.key;
  if (typeof hfKey !== "string") return;

  if (hfKey === "task") mappedField.hide = true;
  if (hfKey === "imageInput") {
    // type ("huggingface-image-upload") is set by customFormlyFieldType
    bindMediaInput(
      mappedField,
      imageInputTasks,
      "requiredImageInput",
      "inputImageColumn",
      "imageInput",
      "Upload an image or select an Input Image Column for this task."
    );
  }
  if (hfKey === "audioInput") {
    // type ("huggingface-audio-upload") is set by customFormlyFieldType
    bindMediaInput(
      mappedField,
      audioInputTasks,
      "requiredAudioInput",
      "inputAudioColumn",
      "audioInput",
      "Upload audio or select an Input Audio Column for this task."
    );
  }
  if (hfKey === "inputImageColumn") {
    hideUnlessTaskIn(mappedField, imageInputTasks);
  }
  if (hfKey === "inputAudioColumn") {
    hideUnlessTaskIn(mappedField, audioInputTasks);
  }
  if (hfKey === "promptColumn") {
    hideUnlessTask(mappedField, t => t === undefined || (!imageOnlyTasks.includes(t) && !audioInputTasks.includes(t)));
    mappedField.validators = {
      ...mappedField.validators,
      requiredPromptColumn: {
        expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
          const t = getSelectedTask(field);
          if (t === undefined || !promptRequiredTasks.includes(t)) {
            return true;
          }
          const value = field.formControl?.value ?? nativeModel(field)?.promptColumn;
          return typeof value === "string" && value.trim().length > 0;
        },
        message: () => "Select a prompt column for this task.",
      },
    };
    mappedField.validation = {
      ...mappedField.validation,
      show: true,
    };
  }
  if (["systemPrompt", "maxNewTokens", "temperature"].includes(hfKey)) {
    hideUnlessTask(mappedField, t => t === "text-generation");
  }
  if (hfKey === "contextColumn") {
    hideUnlessTask(mappedField, t => t === "question-answering");
  }
  if (hfKey === "candidateLabels") {
    hideUnlessTask(mappedField, t => t === "zero-shot-classification" || t === "zero-shot-image-classification");
  }
  if (hfKey === "sentencesColumn") {
    hideUnlessTask(mappedField, t => t === "sentence-similarity" || t === "text-ranking");
  }
}
