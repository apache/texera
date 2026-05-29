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

import {
  HuggingFaceComponent,
  HuggingFaceModelOption,
  STATIC_TASK_OPTIONS,
  invalidateHuggingFaceModelCache,
} from "./hugging-face.component";

describe("HuggingFaceComponent (unit)", () => {
  beforeEach(() => {
    invalidateHuggingFaceModelCache();
  });

  it("should export a non-empty static task list", () => {
    expect(STATIC_TASK_OPTIONS.length).toBeGreaterThan(0);
  });

  it("should include text-generation in static task options", () => {
    const textGen = STATIC_TASK_OPTIONS.find(t => t.tag === "text-generation");
    expect(textGen).toBeTruthy();
    expect(textGen!.label).toBe("Text Generation");
  });

  it("should include image tasks in static task options", () => {
    const imageTasks = STATIC_TASK_OPTIONS.filter(t =>
      ["image-classification", "object-detection", "image-segmentation", "image-to-text"].includes(t.tag)
    );
    expect(imageTasks.length).toBe(4);
  });

  it("should include audio tasks in static task options", () => {
    const audioTasks = STATIC_TASK_OPTIONS.filter(t =>
      ["automatic-speech-recognition", "audio-classification", "text-to-speech"].includes(t.tag)
    );
    expect(audioTasks.length).toBe(3);
  });

  it("should include QA/ranking tasks in static task options", () => {
    const qaTasks = STATIC_TASK_OPTIONS.filter(t =>
      ["question-answering", "zero-shot-classification", "sentence-similarity", "text-ranking"].includes(t.tag)
    );
    expect(qaTasks.length).toBe(4);
  });

  it("should clear caches on invalidateHuggingFaceModelCache", () => {
    // Just verify it doesn't throw — the function clears module-level Maps
    expect(() => invalidateHuggingFaceModelCache()).not.toThrow();
  });

  it("should have unique tags in static task options", () => {
    const tags = STATIC_TASK_OPTIONS.map(t => t.tag);
    const uniqueTags = new Set(tags);
    expect(uniqueTags.size).toBe(tags.length);
  });
});
