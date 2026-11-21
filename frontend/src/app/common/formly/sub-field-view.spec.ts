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
import { describe, expect, it } from "vitest";
import { SUB_FIELD_VIEW_PROP, subFieldLabel, subFieldsInView, subFieldViews } from "./sub-field-view";

describe("sub-field view", () => {
  const parts = [
    { path: "value", label: "Value" },
    { path: "attribute.attributeName", label: "Name" },
    { path: "attribute.attributeType", label: "Type" },
  ] as const;

  const host = (view?: Record<string, { label: string; shown: boolean }>) => ({
    props: view === undefined ? {} : { [SUB_FIELD_VIEW_PROP]: view },
  });

  describe("a host that publishes nothing", () => {
    // The operator property panel: no author, no overrides, so a widget's own headers are the answer.
    it("leaves every part in view, under the widget's own name", () => {
      const field = host();
      expect(subFieldViews(field)).toBeUndefined();
      expect(subFieldsInView(field, parts)).toBe(parts);
      expect(subFieldLabel(field, "attribute.attributeName", "Name")).toBe("Name");
    });
  });

  describe("a host that publishes", () => {
    it("drops what the author hid and keeps the rest in the widget's order", () => {
      const field = host({
        value: { label: "Value", shown: true },
        "attribute.attributeName": { label: "Parameter", shown: true },
        "attribute.attributeType": { label: "Type", shown: false },
      });

      expect(subFieldsInView(field, parts).map(p => p.path)).toEqual(["value", "attribute.attributeName"]);
    });

    it("gives the author's name, and the widget's own where the author kept it", () => {
      const field = host({
        "attribute.attributeName": { label: "Parameter", shown: true },
        "attribute.attributeType": { label: "Type", shown: true },
      });

      expect(subFieldLabel(field, "attribute.attributeName", "Name")).toBe("Parameter");
      expect(subFieldLabel(field, "attribute.attributeType", "Type")).toBe("Type");
    });

    // A part the host said nothing about is one the author was never offered, not one they hid.
    it("keeps a part the host did not mention", () => {
      const field = host({ "attribute.attributeType": { label: "Type", shown: false } });

      expect(subFieldsInView(field, parts).map(p => p.path)).toEqual(["value", "attribute.attributeName"]);
    });

    it("falls back to the widget's name for a part the host did not mention", () => {
      expect(subFieldLabel(host({}), "value", "Value")).toBe("Value");
    });
  });

  // A widget reads this from its template, so it runs on every check; a new array each time would
  // have an ngFor tear its header down and rebuild it, losing the focus in a cell being used.
  describe("identity", () => {
    it("returns the same array while the answer has not changed", () => {
      const field = host({ "attribute.attributeType": { label: "Type", shown: false } });

      const first = subFieldsInView(field, parts);
      expect(subFieldsInView(field, parts)).toBe(first);
    });

    it("returns a new array once the answer changes", () => {
      const field = host({ "attribute.attributeType": { label: "Type", shown: false } });
      const first = subFieldsInView(field, parts);

      field.props = {
        [SUB_FIELD_VIEW_PROP]: {
          "attribute.attributeType": { label: "Type", shown: true },
        },
      };

      const second = subFieldsInView(field, parts);
      expect(second).not.toBe(first);
      expect(second.map(p => p.path)).toEqual(parts.map(p => p.path));
    });

    it("keeps each host's answer apart", () => {
      const hidden = host({ "attribute.attributeType": { label: "Type", shown: false } });
      const whole = host({});

      expect(subFieldsInView(hidden, parts)).toHaveLength(2);
      expect(subFieldsInView(whole, parts)).toHaveLength(3);
      expect(subFieldsInView(hidden, parts)).toHaveLength(2);
    });
  });

  it("ignores a published value that is not a map", () => {
    expect(subFieldViews({ props: { [SUB_FIELD_VIEW_PROP]: "nonsense" } })).toBeUndefined();
    expect(subFieldViews({ props: { [SUB_FIELD_VIEW_PROP]: [] } })).toBeUndefined();
    expect(subFieldViews({ props: {} })).toBeUndefined();
    expect(subFieldViews({})).toBeUndefined();
  });
});
