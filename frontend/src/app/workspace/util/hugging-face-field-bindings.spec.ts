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
  it("hides the task field itself", () => {
    expect(bind("task").hide).toBe(true);
  });

  it("does nothing for a non-string key", () => {
    const field = bind(undefined);
    expect(field.expressions).toBeUndefined();
    expect(field.validators).toBeUndefined();
  });

  it("does nothing for a key the operator does not drive", () => {
    const field = bind("modelId");
    expect(field.expressions).toBeUndefined();
    expect(field.validators).toBeUndefined();
  });

  describe("imageInput", () => {
    it("shows for an image task and hides otherwise", () => {
      const field = bind("imageInput");
      expect(hiddenForTask(field, "image-classification")).toBe(false);
      expect(hiddenForTask(field, "visual-question-answering")).toBe(false);
      expect(hiddenForTask(field, "text-generation")).toBe(true);
      expect(hiddenForTask(field, undefined)).toBe(true);
    });

    it("requires an upload or an Input Image Column only for an image task", () => {
      const field = bind("imageInput");
      // non-image task: always valid
      expect(validatorPasses(field, "requiredImageInput", { task: "text-generation" })).toBe(true);
      // image task, nothing provided: invalid
      expect(validatorPasses(field, "requiredImageInput", { task: "image-classification" })).toBe(false);
      // satisfied by an uploaded value
      expect(validatorPasses(field, "requiredImageInput", { task: "image-classification", value: "pic.png" })).toBe(
        true
      );
      // satisfied by an Input Image Column instead
      expect(
        validatorPasses(field, "requiredImageInput", {
          task: "image-classification",
          model: { inputImageColumn: "col" },
        })
      ).toBe(true);
    });
  });

  describe("audioInput", () => {
    it("shows for an audio task and hides otherwise", () => {
      const field = bind("audioInput");
      expect(hiddenForTask(field, "automatic-speech-recognition")).toBe(false);
      expect(hiddenForTask(field, "image-classification")).toBe(true);
    });

    it("requires an upload or an Input Audio Column only for an audio task", () => {
      const field = bind("audioInput");
      expect(validatorPasses(field, "requiredAudioInput", { task: "text-generation" })).toBe(true);
      expect(validatorPasses(field, "requiredAudioInput", { task: "audio-classification" })).toBe(false);
      expect(validatorPasses(field, "requiredAudioInput", { task: "audio-classification", value: "a.wav" })).toBe(true);
      expect(
        validatorPasses(field, "requiredAudioInput", {
          task: "audio-classification",
          model: { inputAudioColumn: "col" },
        })
      ).toBe(true);
    });
  });

  it("hides inputImageColumn / inputAudioColumn by their task families", () => {
    expect(hiddenForTask(bind("inputImageColumn"), "object-detection")).toBe(false);
    expect(hiddenForTask(bind("inputImageColumn"), "audio-classification")).toBe(true);
    expect(hiddenForTask(bind("inputAudioColumn"), "audio-classification")).toBe(false);
    expect(hiddenForTask(bind("inputAudioColumn"), "object-detection")).toBe(true);
  });

  describe("promptColumn", () => {
    it("hides only for image-only and audio tasks", () => {
      const field = bind("promptColumn");
      expect(hiddenForTask(field, "image-classification")).toBe(true);
      expect(hiddenForTask(field, "audio-classification")).toBe(true);
      expect(hiddenForTask(field, "text-generation")).toBe(false);
      expect(hiddenForTask(field, "visual-question-answering")).toBe(false);
      expect(hiddenForTask(field, undefined)).toBe(false);
    });

    it("requires a value only for a prompt-required task", () => {
      const field = bind("promptColumn");
      expect(validatorPasses(field, "requiredPromptColumn", { task: "image-classification" })).toBe(true);
      expect(validatorPasses(field, "requiredPromptColumn", { task: "summarization" })).toBe(false);
      expect(validatorPasses(field, "requiredPromptColumn", { task: "summarization", value: "col" })).toBe(true);
    });
  });

  it("shows text-generation-only controls solely for text-generation", () => {
    for (const key of ["systemPrompt", "maxNewTokens", "temperature"]) {
      const field = bind(key);
      expect(hiddenForTask(field, "text-generation")).toBe(false);
      expect(hiddenForTask(field, "summarization")).toBe(true);
    }
  });

  it("shows contextColumn only for question-answering", () => {
    const field = bind("contextColumn");
    expect(hiddenForTask(field, "question-answering")).toBe(false);
    expect(hiddenForTask(field, "text-generation")).toBe(true);
  });

  it("shows candidateLabels only for the zero-shot tasks", () => {
    const field = bind("candidateLabels");
    expect(hiddenForTask(field, "zero-shot-classification")).toBe(false);
    expect(hiddenForTask(field, "zero-shot-image-classification")).toBe(false);
    expect(hiddenForTask(field, "text-classification")).toBe(true);
  });

  it("shows sentencesColumn only for similarity and ranking tasks", () => {
    const field = bind("sentencesColumn");
    expect(hiddenForTask(field, "sentence-similarity")).toBe(false);
    expect(hiddenForTask(field, "text-ranking")).toBe(false);
    expect(hiddenForTask(field, "translation")).toBe(true);
  });

  it("preserves expressions already present on the field", () => {
    const existing = () => false;
    const field: FormlyFieldConfig = { key: "contextColumn", expressions: { "props.required": existing } };
    applyHuggingFaceFieldBindings(field);
    expect(field.expressions?.["props.required"]).toBe(existing);
    expect(typeof field.expressions?.hide).toBe("function");
  });
});
