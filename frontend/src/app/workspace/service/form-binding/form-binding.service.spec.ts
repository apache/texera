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

import { TestBed } from "@angular/core/testing";
import { FormBindingService } from "./form-binding.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { mockPoint, mockScanPredicate, mockSentimentPredicate } from "../workflow-graph/model/mock-workflow-data";

describe("FormBindingService", () => {
  let service: FormBindingService;
  let workflowActionService: WorkflowActionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        FormBindingService,
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(FormBindingService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
  });

  const scanId = mockScanPredicate.operatorID;

  it("starts with nothing exposed", () => {
    expect(service.getConfig().fields).toEqual([]);
    expect(service.getConfig().resultOperatorIds).toEqual([]);
  });

  describe("exposing a property", () => {
    it("seeds the input from the operator's schema and current value", () => {
      workflowActionService.setOperatorProperty(scanId, { tableName: "twitter" });

      service.addBinding(scanId, "tableName");

      const [binding] = service.getConfig().fields;
      expect(binding.operatorID).toBe(scanId);
      expect(binding.propertyKey).toBe("tableName");
      // taken from the schema, so the author starts from a readable label
      expect(binding.displayName).toBe("table name");
      // Not seeded from the schema: that description is written for whoever wired the
      // operator up, and shown to a reader it reads as advice the author never gave.
      expect(binding.helpText).toBeUndefined();
    });

    it("does not expose the same property twice", () => {
      service.addBinding(scanId, "tableName");
      service.addBinding(scanId, "tableName");

      expect(service.getConfig().fields.length).toBe(1);
    });
  });

  // Filling in an input is the same edit as changing the property on the regular
  // canvas. That equivalence is what lets both views run the very same workflow.
  describe("filling in a value", () => {
    it("writes through to the operator's property", () => {
      service.addBinding(scanId, "tableName");
      const [binding] = service.getConfig().fields;

      service.writeValue(binding, "reddit");

      expect(workflowActionService.getTexeraGraph().getOperator(scanId).operatorProperties["tableName"]).toBe("reddit");
    });

    it("leaves the operator's other properties alone", () => {
      workflowActionService.setOperatorProperty(scanId, { tableName: "twitter", keep: "me" });
      service.addBinding(scanId, "tableName");
      const [binding] = service.getConfig().fields;

      service.writeValue(binding, "reddit");

      expect(workflowActionService.getTexeraGraph().getOperator(scanId).operatorProperties["keep"]).toBe("me");
    });
  });

  describe("resolving against the live graph", () => {
    it("pairs a healthy binding with its value and schema", () => {
      workflowActionService.setOperatorProperty(scanId, { tableName: "twitter" });
      service.addBinding(scanId, "tableName");

      const [resolved] = service.resolveFields();

      expect(resolved.value).toBe("twitter");
      expect(resolved.schema?.title).toBe("table name");
      expect(resolved.operatorLabel).toBe("Source: Scan");
      expect(resolved.brokenReason).toBeUndefined();
    });

    // Deleting the operator on the regular canvas must not leave the form offering an
    // input that can no longer do anything.
    it("flags a binding whose operator is gone", () => {
      service.addBinding(scanId, "tableName");
      workflowActionService.deleteOperator(scanId);

      const [resolved] = service.resolveFields();

      expect(resolved.brokenReason).toContain("removed from the workflow");
    });

    it("flags a raw key that is not a property of its operator", () => {
      service.addBinding(scanId, "tableName");
      const [binding] = service.getConfig().fields;

      service.updateBinding(binding.id, { propertyKey: "nonsense" });

      expect(service.resolveFields()[0].brokenReason).toContain("not a setting of");
    });
  });

  describe("arranging the form", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
      service.addBinding(scanId, "tableName");
      service.addBinding(mockSentimentPredicate.operatorID, "attribute");
    });

    it("reorders by moving an input to a new position", () => {
      service.reorder(1, 0);

      expect(service.getConfig().fields.map(p => p.propertyKey)).toEqual(["attribute", "tableName"]);
    });

    it("ignores a move that goes nowhere or out of range", () => {
      service.reorder(0, 0);
      service.reorder(0, 5);
      service.reorder(-1, 0);

      expect(service.getConfig().fields.map(p => p.propertyKey)).toEqual(["tableName", "attribute"]);
    });

    // The id is what reordering and removal key on, so renaming must not disturb them.
    it("keeps identity when the display name or raw key changes", () => {
      const [first] = service.getConfig().fields;

      service.updateBinding(first.id, { displayName: "Input table", propertyKey: "tableName" });

      expect(service.getConfig().fields[0].id).toBe(first.id);
      expect(service.getConfig().fields[0].displayName).toBe("Input table");
    });

    it("removes an input", () => {
      const [first] = service.getConfig().fields;

      service.removeBinding(first.id);

      expect(service.getConfig().fields.map(p => p.propertyKey)).toEqual(["attribute"]);
    });
  });

  // The tick box in the property editor drives these two, so they have to be exact
  // inverses of each other for a double-click to be a no-op.
  describe("the property editor's expose tick box", () => {
    it("reports whether a property is already on the form", () => {
      expect(service.isExposed(scanId, "tableName")).toBe(false);

      service.addBinding(scanId, "tableName");

      expect(service.isExposed(scanId, "tableName")).toBe(true);
      expect(service.isExposed(scanId, "somethingElse")).toBe(false);
    });

    it("adds the property when ticked and removes it when unticked", () => {
      service.setExposed(scanId, "tableName", true);
      expect(service.getConfig().fields.map(p => p.propertyKey)).toEqual(["tableName"]);

      service.setExposed(scanId, "tableName", false);
      expect(service.getConfig().fields).toEqual([]);
    });

    it("is idempotent in both directions", () => {
      service.setExposed(scanId, "tableName", true);
      service.setExposed(scanId, "tableName", true);
      expect(service.getConfig().fields.length).toBe(1);

      service.setExposed(scanId, "tableName", false);
      service.setExposed(scanId, "tableName", false);
      expect(service.getConfig().fields.length).toBe(0);
    });
  });

  // Choosing what a form offers means visiting several operators, so the mode has to
  // survive each tick rather than closing itself.
  describe("the choose-fields mode", () => {
    it("starts off", () => {
      expect(service.isChoosing()).toBe(false);
    });

    it("stays on across ticking properties", () => {
      service.setChoosing(true);

      service.setExposed(scanId, "tableName", true);
      service.setExposed(scanId, "tableName", false);

      expect(service.isChoosing()).toBe(true);
    });

    it("only ends when it is switched off", () => {
      service.setChoosing(true);
      service.setChoosing(false);

      expect(service.isChoosing()).toBe(false);
    });

    it("announces changes, and only real ones", () => {
      const seen: boolean[] = [];
      const sub = service.choosing$.subscribe(v => seen.push(v));

      service.setChoosing(true);
      service.setChoosing(true);
      service.setChoosing(false);

      expect(seen).toEqual([false, true, false]);
      sub.unsubscribe();
    });
  });

  // A property is rarely one box. The author names what the reader sees and hides what
  // is none of their business; the schema's own labels are only the default.
  describe("naming and hiding the fields inside an input", () => {
    let bindingId: string;

    beforeEach(() => {
      service.addBinding(scanId, "tableName");
      bindingId = service.getConfig().fields[0].id;
    });

    it("says nothing until the author decides something", () => {
      expect(service.getConfig().fields[0].overrides).toBeUndefined();
      expect(service.getConfig().fields[0].overrides?.["alias"]).toBeUndefined();
    });

    it("renames one field without touching the others", () => {
      service.setFieldOverride(bindingId, "alias", { displayName: "Rename to" });

      const b = service.getConfig().fields[0];
      expect(b.overrides?.["alias"]?.displayName).toBe("Rename to");
      expect(b.overrides?.["attribute"]).toBeUndefined();
    });

    it("hides a field and can bring it back", () => {
      service.setFieldOverride(bindingId, "alias", { hidden: true });
      expect(service.getConfig().fields[0].overrides?.["alias"]?.hidden).toBe(true);

      service.setFieldOverride(bindingId, "alias", { hidden: false });
      expect(service.getConfig().fields[0].overrides?.["alias"]?.hidden).toBeUndefined();
    });

    // Otherwise the definition slowly fills up with every field the author looked at.
    it("keeps no entry for a field back at its defaults", () => {
      service.setFieldOverride(bindingId, "alias", { displayName: "x" });
      service.setFieldOverride(bindingId, "alias", { displayName: "" });

      expect(service.getConfig().fields[0].overrides).toBeUndefined();
    });

    // An explicit `undefined` in the patch clears the value, exactly like an empty one; it
    // must not leave a hollow override that would serialize to `{}`.
    it("keeps no entry when a patch value is explicitly undefined", () => {
      service.setFieldOverride(bindingId, "alias", { displayName: undefined });
      expect(service.getConfig().fields[0].overrides).toBeUndefined();

      service.setFieldOverride(bindingId, "alias", { displayName: "Renamed" });
      service.setFieldOverride(bindingId, "alias", { displayName: undefined });
      expect(service.getConfig().fields[0].overrides).toBeUndefined();
    });

    it("leaves a field alone when the binding does not exist", () => {
      service.setFieldOverride("no-such-binding", "alias", { hidden: true });

      expect(service.getConfig().fields[0].overrides).toBeUndefined();
    });
  });

  describe("choosing which results to show", () => {
    // The form records only its own selection. It never writes the canvas's view-result flags:
    // the picker offers exactly the operators the workflow already views, so those results are
    // already materialised, and a canvas user's own view-result choices are left untouched.
    it("toggles an operator in and out of the shown set, without touching the canvas", () => {
      const setView = vi.spyOn(workflowActionService, "setViewOperatorResults");

      service.toggleResultOperator(scanId);
      expect(service.getConfig().resultOperatorIds).toEqual([scanId]);

      service.toggleResultOperator(scanId);
      expect(service.getConfig().resultOperatorIds).toEqual([]);

      expect(setView).not.toHaveBeenCalled();
    });
  });

  describe("reading, labeling and schema lookup", () => {
    it("reads a value off the operator, undefined once the operator is gone", () => {
      workflowActionService.setOperatorProperty(scanId, { tableName: "twitter" });
      expect(service.readValue(scanId, "tableName")).toBe("twitter");
      expect(service.readValue("missing", "tableName")).toBeUndefined();
    });

    it("writing to a missing operator is a no-op", () => {
      expect(() =>
        service.writeValue({ id: "b", operatorID: "gone", propertyKey: "x", displayName: "" }, 1)
      ).not.toThrow();
    });

    it("labels by custom name (trimmed), else falls back to the operator type", () => {
      expect(service.operatorLabel({ customDisplayName: "  My Step  ", operatorType: "X" } as any)).toBe("My Step");
      expect(service.operatorLabel({ operatorType: "UnknownType" } as any)).toBe("UnknownType");
    });

    it("returns empty properties for a missing operator", () => {
      expect((service as any).operatorSchemaProperties("gone")).toEqual({});
    });

    it("falls back to the static schema when there is no dynamic one", () => {
      vi.spyOn((service as any).dynamicSchemaService, "getDynamicSchema").mockImplementation(() => {
        throw new Error("none");
      });
      expect((service as any).operatorSchemaProperties(scanId)).toBeDefined();
    });

    it("returns empty properties when neither dynamic nor static resolves", () => {
      vi.spyOn((service as any).dynamicSchemaService, "getDynamicSchema").mockImplementation(() => {
        throw new Error("none");
      });
      vi.spyOn((service as any).operatorMetadataService, "getOperatorSchema").mockImplementation(() => {
        throw new Error("none");
      });
      expect((service as any).operatorSchemaProperties(scanId)).toEqual({});
    });

    it("treats a dynamic schema that carries no properties as empty", () => {
      vi.spyOn((service as any).dynamicSchemaService, "getDynamicSchema").mockReturnValue({ jsonSchema: {} } as any);
      expect((service as any).operatorSchemaProperties(scanId)).toEqual({});
    });

    it("treats a static schema that carries no properties as empty", () => {
      vi.spyOn((service as any).dynamicSchemaService, "getDynamicSchema").mockImplementation(() => {
        throw new Error("none");
      });
      vi.spyOn((service as any).operatorMetadataService, "getOperatorSchema").mockReturnValue({
        jsonSchema: {},
      } as any);
      expect((service as any).operatorSchemaProperties(scanId)).toEqual({});
    });

    it("labels an exposed property by its raw key when the schema has no title", () => {
      service.addBinding(scanId, "no-such-setting");
      const [binding] = service.getConfig().fields;
      expect(binding.displayName).toBe("no-such-setting");
    });
  });

  // The definition is presentation only; a run reads operator properties and the
  // graph. This is what guarantees a workflow still runs when its form is broken.
  it("keeps the definition out of the executable graph", () => {
    service.addBinding(scanId, "tableName");

    const content = workflowActionService.getWorkflowContent();

    expect(content.formBinding?.fields.length).toBe(1);
    expect(Object.keys(content.operators[0])).not.toContain("formBinding");
    expect(Object.keys(content.operators[0].operatorProperties)).not.toContain("displayName");
  });
});
