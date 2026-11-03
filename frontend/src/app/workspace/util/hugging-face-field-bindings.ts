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

const getSelectedTask = (field: FormlyFieldConfig): string | undefined => {
  const fromForm = field.form?.get("task")?.value ?? field.formControl?.parent?.get("task")?.value;
  if (typeof fromForm === "string" && fromForm.trim().length > 0) {
    return fromForm;
  }
  const fromModel = field.model?.task;
  if (typeof fromModel === "string" && fromModel.trim().length > 0) {
    return fromModel;
  }
  return undefined;
};

/**
 * Applies the HuggingFace operator's task-driven field behaviour to a single formly field: the
 * `task` field is hidden, and each input field (image/audio uploads and their column pickers, the
 * prompt column, text-generation controls, context, candidate labels, sentences) gets the hide
 * expression and required-validator that the currently selected task calls for. A no-op for any
 * field whose key the HuggingFace operator does not drive. The widget a field renders as is decided
 * separately in {@link customFormlyFieldType}.
 */
export function applyHuggingFaceFieldBindings(mappedField: FormlyFieldConfig): void {
  if (mappedField.key === "task") {
    mappedField.hide = true;
  }

  if (typeof mappedField.key !== "string") {
    return;
  }
  const hfKey = mappedField.key;
  if (hfKey === "imageInput") {
    // type ("huggingface-image-upload") is set by customFormlyFieldType
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t === undefined || !imageInputTasks.includes(t);
      },
    };
    mappedField.validators = {
      ...mappedField.validators,
      requiredImageInput: {
        expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
          const t = getSelectedTask(field);
          if (t === undefined || !imageInputTasks.includes(t)) {
            return true;
          }
          const inputImageCol = field.model?.inputImageColumn;
          if (typeof inputImageCol === "string" && inputImageCol.trim().length > 0) {
            return true;
          }
          const value = field.formControl?.value ?? field.model?.imageInput;
          return typeof value === "string" && value.trim().length > 0;
        },
        message: () => "Upload an image or select an Input Image Column for this task.",
      },
    };
    mappedField.validation = {
      ...mappedField.validation,
      show: true,
    };
  }
  if (hfKey === "audioInput") {
    // type ("huggingface-audio-upload") is set by customFormlyFieldType
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t === undefined || !audioInputTasks.includes(t);
      },
    };
    mappedField.validators = {
      ...mappedField.validators,
      requiredAudioInput: {
        expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
          const t = getSelectedTask(field);
          if (t === undefined || !audioInputTasks.includes(t)) {
            return true;
          }
          const inputAudioCol = field.model?.inputAudioColumn;
          if (typeof inputAudioCol === "string" && inputAudioCol.trim().length > 0) {
            return true;
          }
          const value = field.formControl?.value ?? field.model?.audioInput;
          return typeof value === "string" && value.trim().length > 0;
        },
        message: () => "Upload audio or select an Input Audio Column for this task.",
      },
    };
    mappedField.validation = {
      ...mappedField.validation,
      show: true,
    };
  }
  if (hfKey === "inputImageColumn") {
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t === undefined || !imageInputTasks.includes(t);
      },
    };
  }
  if (hfKey === "inputAudioColumn") {
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t === undefined || !audioInputTasks.includes(t);
      },
    };
  }
  if (hfKey === "promptColumn") {
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t !== undefined && (imageOnlyTasks.includes(t) || audioInputTasks.includes(t));
      },
    };
    mappedField.validators = {
      ...mappedField.validators,
      requiredPromptColumn: {
        expression: (_control: AbstractControl, field: FormlyFieldConfig) => {
          const t = getSelectedTask(field);
          if (t === undefined || !promptRequiredTasks.includes(t)) {
            return true;
          }
          const value = field.formControl?.value ?? field.model?.promptColumn;
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
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t !== "text-generation";
      },
    };
  }
  if (hfKey === "contextColumn") {
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => getSelectedTask(field) !== "question-answering",
    };
  }
  if (hfKey === "candidateLabels") {
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t !== "zero-shot-classification" && t !== "zero-shot-image-classification";
      },
    };
  }
  if (hfKey === "sentencesColumn") {
    mappedField.expressions = {
      ...mappedField.expressions,
      hide: (field: FormlyFieldConfig) => {
        const t = getSelectedTask(field);
        return t !== "sentence-similarity" && t !== "text-ranking";
      },
    };
  }
}
