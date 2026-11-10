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

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { OperatorSupplements, typeCastingAttributes, huggingFaceTaskPreview } from "./operator-supplements";
afterEach(cleanup);
describe("operator-specific property supplements", () => {
  it("retains all input attributes in port order, replaces cast types and preserves the input response", () => {
    const attributes = {
      "0_false": [
        { attributeName: "x", attributeType: "integer" },
        { attributeName: "β", attributeType: "string" },
      ],
      "1_false": undefined,
    };
    expect(typeCastingAttributes({ typeCastingUnits: [{ attribute: "x", resultType: "double" }] }, attributes)).toEqual(
      [
        { attributeName: "x", attributeType: "double" },
        { attributeName: "β", attributeType: "string" },
      ]
    );
    expect(attributes["0_false"][0].attributeType).toBe("integer");
    expect(typeCastingAttributes({}, undefined)).toEqual([]);
    expect(typeCastingAttributes({ typeCastingUnits: [null, 42, { attribute: "x" }] }, attributes)).toEqual(
      attributes["0_false"]
    );
  });
  it("renders the type casting result table for the exact legacy operator only", () => {
    const { rerender } = render(
      <OperatorSupplements
        operatorType="TypeCasting"
        value={{}}
        inputAttributes={{ "0_false": [{ attributeName: "x", attributeType: "integer" }] }}
      />
    );
    expect(screen.getByRole("table").textContent).toContain("Attribute Name");
    expect(screen.getByRole("table").textContent).toContain("integer");
    rerender(
      <OperatorSupplements
        operatorType="Other"
        value={{}}
      />
    );
    expect(screen.queryByRole("table")).toBeNull();
  });
  it("retains task-specific media and safe text fallback previews without an upload", () => {
    expect(huggingFaceTaskPreview("")).toBeNull();
    expect(huggingFaceTaskPreview(null)).toBeNull();
    expect(huggingFaceTaskPreview("text-to-image")).toMatchObject({
      kind: "image",
      assetSrc: "assets/sample-image.png",
    });
    expect(huggingFaceTaskPreview("future-task")).toMatchObject({ kind: "text", title: "Future Task" });
    const { rerender } = render(
      <OperatorSupplements
        operatorType="HuggingFace"
        value={{ task: "text-to-image" }}
      />
    );
    expect(screen.getByRole("img", { name: "Hugging Face task preview" }).getAttribute("src")).toBe(
      "assets/sample-image.png"
    );
    rerender(
      <OperatorSupplements
        operatorType="HuggingFace"
        value={{ task: "text-to-video" }}
      />
    );
    expect(document.querySelector("video")?.getAttribute("src")).toBe("assets/sample-video.mp4");
    rerender(
      <OperatorSupplements
        operatorType="HuggingFace"
        value={{ task: "<script>" }}
      />
    );
    expect(screen.getByText("<script>")).toBeTruthy();
    expect(document.querySelector("script")).toBeNull();
  });
});
