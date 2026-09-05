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

import { customFormlyFieldType, NON_FORM_FIELD_TYPES } from "./custom-formly-type";

describe("NON_FORM_FIELD_TYPES", () => {
  it("blocks only the code editor from being a form field, not the drag-reorder list", () => {
    expect(NON_FORM_FIELD_TYPES.has("codearea")).toBe(true);
    // a drag-reorder property is still a valid form field (it just renders without the drag)
    expect(NON_FORM_FIELD_TYPES.has("repeat-section-dnd")).toBe(false);
  });
});

describe("customFormlyFieldType", () => {
  it("maps a fileName property to the autocomplete input", () => {
    expect(customFormlyFieldType({ key: "fileName", operatorType: "CSVFileScan" })).toBe("inputautocomplete");
  });

  it("maps huggingFaceModel to the model picker regardless of operator", () => {
    expect(customFormlyFieldType({ key: "huggingFaceModel", operatorType: "Anything" })).toBe("huggingface");
  });

  it("maps modelId to the model picker only on a HuggingFace operator", () => {
    expect(customFormlyFieldType({ key: "modelId", operatorType: "HuggingFace" })).toBe("huggingface");
    expect(customFormlyFieldType({ key: "modelId", operatorType: "PythonUDF" })).toBeUndefined();
  });

  it("maps HuggingFace image/audio inputs to their uploaders", () => {
    expect(customFormlyFieldType({ key: "imageInput", operatorType: "HuggingFace" })).toBe("huggingface-image-upload");
    expect(customFormlyFieldType({ key: "audioInput", operatorType: "HuggingFace" })).toBe("huggingface-audio-upload");
    // Off a HuggingFace operator they are plain fields.
    expect(customFormlyFieldType({ key: "imageInput", operatorType: "PythonUDF" })).toBeUndefined();
    expect(customFormlyFieldType({ key: "audioInput", operatorType: "PythonUDF" })).toBeUndefined();
  });

  it("maps uiParameters and datasetVersionPath to their custom controls", () => {
    expect(customFormlyFieldType({ key: "uiParameters", operatorType: "PythonUDF" })).toBe("ui-udf-parameters");
    expect(customFormlyFieldType({ key: "datasetVersionPath", operatorType: "CSVFileScan" })).toBe(
      "datasetversionselector"
    );
  });

  it("maps the code-editor property to the code box only when it already has an editable control", () => {
    expect(
      customFormlyFieldType({
        key: "code",
        operatorType: "PythonUDF",
        description: "Input your code here",
        currentType: "textarea",
      })
    ).toBe("codearea");
    // The description matches but the schema left no editable control -> keep the default.
    expect(
      customFormlyFieldType({ key: "code", operatorType: "PythonUDF", description: "input your code here" })
    ).toBeUndefined();
  });

  it("maps Projection's attributes to the drag-reorder list, only on Projection", () => {
    expect(customFormlyFieldType({ key: "attributes", operatorType: "Projection" })).toBe("repeat-section-dnd");
    expect(customFormlyFieldType({ key: "attributes", operatorType: "Filter" })).toBeUndefined();
  });

  it("returns undefined for an ordinary property, keeping formly's default control", () => {
    expect(customFormlyFieldType({ key: "limit", operatorType: "Limit" })).toBeUndefined();
  });
});
