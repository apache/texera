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

import { WorkflowFormComponent } from "./workflow-form.component";

// The two statics that back per-sub-field rename/hide: locating a sub-field means walking formly's
// output, which is a plain box, an object with a fieldGroup, or a repeated section whose row
// template is either a value or a builder function.
describe("sub-fields of an input", () => {
  describe("override paths", () => {
    it("joins nested keys", () => {
      expect(WorkflowFormComponent.childPath("pairs", "key")).toBe("pairs.key");
      expect(WorkflowFormComponent.childPath("", "value")).toBe("value");
    });

    it("drops array indices so every row shares one override", () => {
      expect(WorkflowFormComponent.childPath("pairs", "0")).toBe("pairs");
      expect(WorkflowFormComponent.childPath("pairs", "12")).toBe("pairs");
    });

    it("ignores keys that are not names", () => {
      expect(WorkflowFormComponent.childPath("pairs", undefined)).toBe("pairs");
      expect(WorkflowFormComponent.childPath("", "key")).toBe("key");
    });
  });

  describe("finding the row template of a repeated section", () => {
    it("takes it directly when it is a value", () => {
      const item = { key: "row" };
      expect(WorkflowFormComponent.arrayItemOf({ fieldArray: item })).toBe(item);
    });

    it("calls it when formly supplies a builder", () => {
      const item = { key: "row" };
      expect(WorkflowFormComponent.arrayItemOf({ fieldArray: () => item })).toBe(item);
    });

    it("gives up quietly on a builder it cannot call", () => {
      const throwing = () => {
        throw new Error("needs a real field");
      };
      expect(WorkflowFormComponent.arrayItemOf({ fieldArray: throwing as never })).toBeUndefined();
    });

    it("has nothing to offer when there is no array", () => {
      expect(WorkflowFormComponent.arrayItemOf({ key: "plain" })).toBeUndefined();
    });
  });
});
