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

/**
 * Widget types that cannot be a form field at all, so the property is not offered for exposure on
 * the Form View. Only the code editor: editing code is not "filling in a value", and a form reader
 * should not be writing code. (A drag-reorder property such as Projection's columns stays exposable
 * -- it just renders without the drag in the form -- so it is deliberately NOT in this set.)
 */
export const NON_FORM_FIELD_TYPES: ReadonlySet<string> = new Set(["codearea"]);

/**
 * Widgets that only work on the operator canvas, so the Form View does not render them: it falls
 * back to formly's default control instead. The code editor (also blocked from exposure by
 * {@link NON_FORM_FIELD_TYPES}) and the drag-reorder list, whose drag has nowhere to attach on a
 * form -- a workflow may still carry an exposed drag-reorder property from before, and it degrades
 * to a plain editable list rather than a control that cannot function here.
 */
export const CANVAS_ONLY_FORMLY_TYPES: ReadonlySet<string> = new Set(["codearea", "repeat-section-dnd"]);

/**
 * The custom formly widget an operator-schema property renders as, decided from the property key
 * and its operator. A single source of truth extracted from the operator property panel so that a
 * later view (the Form View) can render the same control instead of letting a selectable/uploadable
 * property silently degrade to a plain text box.
 *
 * Returns undefined to keep formly's default control (string/number/textarea/...). Only the widget
 * TYPE lives here; each caller keeps its own field behaviour (the panel's task-driven hide rules,
 * validators, and the Projection reorder callback).
 */
export function customFormlyFieldType(input: {
  key: unknown;
  operatorType: string | undefined;
  description?: string;
  /** formly's already-resolved type; the code box only replaces an editable control. */
  currentType?: unknown;
}): string | undefined {
  const { key, operatorType, description, currentType } = input;

  if (key === "fileName") {
    return "inputautocomplete";
  }
  if (key === "huggingFaceModel") {
    return "huggingface";
  }
  if (key === "modelId" && operatorType === "HuggingFace") {
    return "huggingface";
  }
  if (key === "imageInput" && operatorType === "HuggingFace") {
    return "huggingface-image-upload";
  }
  if (key === "audioInput" && operatorType === "HuggingFace") {
    return "huggingface-audio-upload";
  }
  if (key === "uiParameters") {
    return "ui-udf-parameters";
  }
  if (key === "datasetVersionPath") {
    return "datasetversionselector";
  }
  // Python UDF script box: only when the schema already resolved to an editable control.
  if (description?.toLowerCase() === "input your code here" && currentType) {
    return "codearea";
  }
  if (operatorType === "Projection" && key === "attributes") {
    return "repeat-section-dnd";
  }
  return undefined;
}
