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
 * The operator an exposed field belongs to, which the Form View sets in `props` on every field it
 * renders. A shared widget that needs its operator reads it from here on the form, where nothing is
 * highlighted: the ui-udf-parameters renderer adds a declared parameter to that operator's code. On
 * the operator property panel, whose fields exist because of the highlight, the highlighted operator
 * is the one, and no field carries this. So it also tells a widget which host it is on, where the two
 * must differ: the same renderer keeps its fixed column headers on the panel and derives them from the
 * author's renames and hides on the form (#8763).
 */
export const FIELD_OPERATOR_ID_PROP = "operatorID";

/** The operator the Form View bound a field to, if it rendered the field; see {@link FIELD_OPERATOR_ID_PROP}. */
export function fieldOperatorID(field: { props?: Record<string, unknown> }): string | undefined {
  const id = field.props?.[FIELD_OPERATOR_ID_PROP];
  return typeof id === "string" && id !== "" ? id : undefined;
}

/** Whether a field is being rendered by the Form View (see {@link FIELD_OPERATOR_ID_PROP}). */
export function renderedInFormView(field: { props?: Record<string, unknown> }): boolean {
  return fieldOperatorID(field) !== undefined;
}

/**
 * Whether the Form View rendered a field for a reader, who may not change the workflow: it marks the
 * field `props.disabled`, the one lock it puts on a card, and formly disables the controls it builds
 * under such a field. A widget with a button of its own (ui-udf-parameters' Add parameter) reads the
 * mark here rather than its control: formly disables controls, not arrays, so an array with no rows
 * reads as enabled.
 */
export function renderedReadOnly(field: { props?: Record<string, unknown> }): boolean {
  return field.props?.["disabled"] === true;
}

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
