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
import { applyHuggingFaceFieldBindings } from "./hugging-face-field-bindings";

// Builds a field with the given key, runs the bindings on it, and returns it ready to probe.
function bind(key: unknown): FormlyFieldConfig {
  const field: FormlyFieldConfig = { key: key as FormlyFieldConfig["key"] };
  applyHuggingFaceFieldBindings(field);
  return field;
}

// Invokes the field's compiled hide expression against a model carrying the selected task.
function hiddenForTask(field: FormlyFieldConfig, task?: string): boolean {
  const hide = field.expressions?.hide as (f: FormlyFieldConfig) => boolean;
  return hide({ model: task === undefined ? {} : { task } } as FormlyFieldConfig);
}

// Invokes a named validator's expression (true = valid) against a given field state.
function validatorPasses(
  field: FormlyFieldConfig,
  name: string,
  state: { task?: string; value?: string; model?: Record<string, unknown> }
): boolean {
  const validator = (field.validators as Record<string, { expression: Function }>)[name];
  const probe = {
    model: { task: state.task, ...(state.model ?? {}) },
    formControl: { value: state.value },
  } as unknown as FormlyFieldConfig;
  return validator.expression({} as AbstractControl, probe);
}

describe("applyHuggingFaceFieldBindings", () => {
  it("keeps task visibility and validators on projected macro field aliases", () => {
    const model = { "public-task": "image-classification", "public-image": "", "public-column": "" };
    const field: FormlyFieldConfig = {
      key: "public-image",
      model,
      parent: { model },
      props: {
        propertyContext: {
          operatorType: "HuggingFace",
          propertyKey: "imageInput",
          properties: {},
          bindings: { task: "public-task", imageInput: "public-image", inputImageColumn: "public-column" },
        },
      },
      formControl: { value: "" } as AbstractControl,
    };
    applyHuggingFaceFieldBindings(field);
    const hide = field.expressions?.hide as (f: FormlyFieldConfig) => boolean;
    const valid = (field.validators as any)?.requiredImageInput?.expression;
    expect(hide?.(field)).toBe(false);
    expect(valid?.({} as AbstractControl, field)).toBe(false);
    model["public-column"] = "image";
    expect(valid({} as AbstractControl, field)).toBe(true);
    model["public-task"] = "text-generation";
    expect(hide(field)).toBe(true);
  });
  it("hides the task field itself", () => {
    expect(bind("task").hide).toBe(true);
  });

  it.each([undefined, "modelId"])("leaves an unmanaged field %s alone", key => {
    const field = bind(key);
    expect(field.expressions).toBeUndefined();
    expect(field.validators).toBeUndefined();
  });

  it.each([
    {
      key: "imageInput",
      visible: ["image-classification", "visual-question-answering"],
      hidden: ["text-generation", undefined],
    },
    { key: "audioInput", visible: ["automatic-speech-recognition"], hidden: ["image-classification"] },
    { key: "inputImageColumn", visible: ["object-detection"], hidden: ["audio-classification"] },
    { key: "inputAudioColumn", visible: ["audio-classification"], hidden: ["object-detection"] },
    {
      key: "promptColumn",
      visible: ["text-generation", "visual-question-answering", undefined],
      hidden: ["image-classification", "audio-classification"],
    },
    ...["systemPrompt", "maxNewTokens", "temperature"].map(key => ({
      key,
      visible: ["text-generation"],
      hidden: ["summarization"],
    })),
    { key: "contextColumn", visible: ["question-answering"], hidden: ["text-generation"] },
    {
      key: "candidateLabels",
      visible: ["zero-shot-classification", "zero-shot-image-classification"],
      hidden: ["text-classification"],
    },
    { key: "sentencesColumn", visible: ["sentence-similarity", "text-ranking"], hidden: ["translation"] },
  ])("shows $key for its supported task families", ({ key, visible, hidden }) => {
    const field = bind(key);
    for (const task of visible) expect(hiddenForTask(field, task), String(task)).toBe(false);
    for (const task of hidden) expect(hiddenForTask(field, task), String(task)).toBe(true);
  });

  it.each([
    {
      key: "imageInput",
      validator: "requiredImageInput",
      task: "image-classification",
      column: "inputImageColumn",
      upload: "pic.png",
    },
    {
      key: "audioInput",
      validator: "requiredAudioInput",
      task: "audio-classification",
      column: "inputAudioColumn",
      upload: "a.wav",
    },
  ])("requires an upload or a column for $key when applicable", ({ key, validator, task, column, upload }) => {
    const field = bind(key);
    expect(validatorPasses(field, validator, { task: "text-generation" })).toBe(true);
    expect(validatorPasses(field, validator, { task })).toBe(false);
    expect(validatorPasses(field, validator, { task, value: upload })).toBe(true);
    expect(validatorPasses(field, validator, { task, model: { [column]: "col" } })).toBe(true);
    expect(validatorPasses(field, validator, { task, value: " ", model: { [column]: " " } })).toBe(false);
  });

  it("requires a prompt column only for a prompt-required task", () => {
    const field = bind("promptColumn");
    expect(validatorPasses(field, "requiredPromptColumn", { task: "image-classification" })).toBe(true);
    expect(validatorPasses(field, "requiredPromptColumn", { task: "summarization" })).toBe(false);
    expect(validatorPasses(field, "requiredPromptColumn", { task: "summarization", value: "col" })).toBe(true);
  });

  it("preserves expressions already present on the field", () => {
    const existing = () => false;
    const field: FormlyFieldConfig = { key: "contextColumn", expressions: { "props.required": existing } };
    applyHuggingFaceFieldBindings(field);
    expect(field.expressions?.["props.required"]).toBe(existing);
    expect(typeof field.expressions?.hide).toBe("function");
  });
});
