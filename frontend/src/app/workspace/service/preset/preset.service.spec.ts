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
import { NzMessageService } from "ng-zorro-antd/message";
import { config, of } from "rxjs";
import { UserConfigService } from "src/app/common/service/user/config/user-config.service";
import { CustomJSONSchema7 } from "../../types/custom-json-schema.interface";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { mockPresetEnabledSchema } from "../operator-metadata/mock-operator-metadata.data";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { mockPoint, mockPresetEnabledPredicate } from "../workflow-graph/model/mock-workflow-data";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { Preset, PresetService } from "./preset.service";

// Ajv 8 defaults to strict mode and rejects unknown keywords at compile time, so
// `isValidOperatorPreset` (which compiles operator schemas containing the
// 'enable-presets' marker) throws before it can validate. Register the keyword
// once as a no-op so the validation paths are exercisable in tests.
const ajvInstance = (PresetService as any).ajv;
if (!ajvInstance.getKeyword("enable-presets")) {
  ajvInstance.addKeyword({ keyword: "enable-presets", schemaType: "boolean" });
}

describe("PresetService", () => {
  let userConfigStub: {
    fetchKey: ReturnType<typeof vi.fn>;
    set: ReturnType<typeof vi.fn>;
    delete: ReturnType<typeof vi.fn>;
  };
  let messageStub: {
    success: ReturnType<typeof vi.fn>;
    error: ReturnType<typeof vi.fn>;
    info: ReturnType<typeof vi.fn>;
    warning: ReturnType<typeof vi.fn>;
  };
  let presetService: PresetService;
  let workflowActionService: WorkflowActionService;

  // RxJS 7 reports errors thrown from a subscribe `next` handler via
  // `config.onUnhandledError` on a macrotask, not synchronously, so a
  // try/catch around the call would not see them. Capture them explicitly.
  const captureRxjsUnhandled = async (run: () => void) => {
    const captured: unknown[] = [];
    const previous = config.onUnhandledError;
    config.onUnhandledError = err => captured.push(err);
    try {
      run();
      await new Promise(resolve => setTimeout(resolve, 0));
    } finally {
      config.onUnhandledError = previous;
    }
    return captured;
  };

  const presetType = "operator";
  const presetTarget = mockPresetEnabledPredicate.operatorType;
  const presetDictKey = `${presetType}-${presetTarget}`;

  beforeEach(() => {
    userConfigStub = {
      fetchKey: vi.fn().mockReturnValue(of(null)),
      set: vi.fn().mockReturnValue(of(void 0)),
      delete: vi.fn().mockReturnValue(of(void 0)),
    };
    messageStub = {
      success: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      warning: vi.fn(),
    };

    TestBed.configureTestingModule({
      providers: [
        PresetService,
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserConfigService, useValue: userConfigStub },
        { provide: NzMessageService, useValue: messageStub },
        ...commonTestProviders,
      ],
    });

    presetService = TestBed.inject(PresetService);
    workflowActionService = TestBed.inject(WorkflowActionService);
  });

  it("should be created", () => {
    expect(presetService).toBeTruthy();
  });

  describe("preset I/O", () => {
    it("emits an event on applyPresetStream when a preset is applied", () => {
      const seen: { type: string; target: string; preset: Preset }[] = [];
      const sub = presetService.applyPresetStream.subscribe(value => seen.push(value));

      const preset: Preset = { presetProperty: "applied" };
      presetService.applyPreset("nonOperatorType", "anyTarget", preset);

      expect(seen).toEqual([{ type: "nonOperatorType", target: "anyTarget", preset }]);
      sub.unsubscribe();
    });

    it("emits an event on savePresetsStream when presets are saved", () => {
      const seen: { type: string; target: string; presets: Preset[] }[] = [];
      const sub = presetService.savePresetsStream.subscribe(value => seen.push(value));

      const presets: Preset[] = [{ presetProperty: "v1" }];
      presetService.savePresets(presetType, presetTarget, presets);

      expect(seen).toEqual([{ type: presetType, target: presetTarget, presets }]);
      sub.unsubscribe();
    });

    it("writes through UserConfigService.set when saving a non-empty preset list", () => {
      const presets: Preset[] = [{ presetProperty: "v1" }];
      presetService.savePresets(presetType, presetTarget, presets);

      expect(userConfigStub.set).toHaveBeenCalledTimes(1);
      expect(userConfigStub.set).toHaveBeenCalledWith(presetDictKey, JSON.stringify(presets));
      expect(userConfigStub.delete).not.toHaveBeenCalled();
    });

    it("calls UserConfigService.delete instead of set when saving an empty preset list", () => {
      presetService.savePresets(presetType, presetTarget, []);

      expect(userConfigStub.delete).toHaveBeenCalledTimes(1);
      expect(userConfigStub.delete).toHaveBeenCalledWith(presetDictKey);
      expect(userConfigStub.set).not.toHaveBeenCalled();
    });

    it("displays the success toast by default when saving presets", () => {
      presetService.savePresets(presetType, presetTarget, [{ presetProperty: "v1" }]);
      expect(messageStub.success).toHaveBeenCalledWith("Preset saved");
    });

    it("suppresses the toast when displayMessage is explicitly null", () => {
      presetService.savePresets(presetType, presetTarget, [{ presetProperty: "v1" }], null);
      expect(messageStub.success).not.toHaveBeenCalled();
      expect(messageStub.error).not.toHaveBeenCalled();
    });

    it("createPreset appends to existing presets and writes back", () => {
      const existing: Preset[] = [{ presetProperty: "v1" }];
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify(existing)));

      presetService.createPreset(presetType, presetTarget, { presetProperty: "v2" });

      expect(userConfigStub.set).toHaveBeenCalledWith(
        presetDictKey,
        JSON.stringify([{ presetProperty: "v1" }, { presetProperty: "v2" }])
      );
    });

    it("createPreset does not write the preset back when it already exists", async () => {
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify([{ presetProperty: "v1" }])));

      const errors = await captureRxjsUnhandled(() =>
        presetService.createPreset(presetType, presetTarget, { presetProperty: "v1" })
      );

      expect(userConfigStub.set).not.toHaveBeenCalled();
      expect(userConfigStub.delete).not.toHaveBeenCalled();
      expect(errors).toHaveLength(1);
      expect((errors[0] as Error).message).toMatch(/already exists/);
    });

    it("updatePreset does not write the preset back when the original preset is missing", async () => {
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify([{ presetProperty: "v1" }])));

      const errors = await captureRxjsUnhandled(() =>
        presetService.updatePreset(presetType, presetTarget, { presetProperty: "missing" }, { presetProperty: "v3" })
      );

      expect(userConfigStub.set).not.toHaveBeenCalled();
      expect(errors).toHaveLength(1);
      expect((errors[0] as Error).message).toMatch(/doesn't exist/);
    });

    it("deletePreset removes the matching preset via savePresets", () => {
      const a: Preset = { presetProperty: "v1" };
      const b: Preset = { presetProperty: "v2" };
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify([a, b])));

      presetService.deletePreset(presetType, presetTarget, b);

      expect(userConfigStub.set).toHaveBeenCalledWith(presetDictKey, JSON.stringify([a]));
    });

    it("deletePreset clears the dictionary entry when the last preset is removed", () => {
      const only: Preset = { presetProperty: "v1" };
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify([only])));

      presetService.deletePreset(presetType, presetTarget, only);

      // savePresets routes empty arrays to delete(), not set().
      expect(userConfigStub.delete).toHaveBeenCalledWith(presetDictKey);
      expect(userConfigStub.set).not.toHaveBeenCalled();
    });

    it("getPresets returns the parsed preset array stored in user config", () => {
      const stored: Preset[] = [{ presetProperty: "v1" }];
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify(stored)));

      let result: readonly Preset[] | undefined;
      presetService.getPresets(presetType, presetTarget).subscribe(v => (result = v));
      expect(result).toEqual(stored);
    });

    it("getPresets yields an empty array when no entry exists", () => {
      userConfigStub.fetchKey.mockReturnValue(of(null));

      let result: readonly Preset[] | undefined;
      presetService.getPresets(presetType, presetTarget).subscribe(v => (result = v));
      expect(result).toEqual([]);
    });

    it("getPresets emits an error when the stored value is not a valid preset array", () => {
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify([{ presetProperty: 42 }, "not-an-object"])));

      let err: unknown;
      // throws inside an rxjs map() — surface via the error subscriber, not toThrow.
      presetService.getPresets(presetType, presetTarget).subscribe({
        next: () => {},
        error: (e: unknown) => (err = e),
      });
      expect(err).toBeInstanceOf(Error);
      expect((err as Error).message).toMatch(/formatted incorrectly/);
    });
  });

  describe("operator preset application", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockPresetEnabledPredicate, mockPoint);
      workflowActionService.setOperatorProperty(mockPresetEnabledPredicate.operatorID, {
        presetProperty: "before",
        normalProperty: "untouched",
      });
    });

    it("does not set operator properties when applyPreset uses a non-operator type", () => {
      presetService.applyPreset("notAnOperator", mockPresetEnabledPredicate.operatorID, { presetProperty: "applied" });

      expect(
        workflowActionService.getTexeraGraph().getOperator(mockPresetEnabledPredicate.operatorID).operatorProperties
      ).toEqual({ presetProperty: "before", normalProperty: "untouched" });
    });

    it("merges preset values into operator properties when a valid preset is applied", () => {
      presetService.applyPreset("operator", mockPresetEnabledPredicate.operatorID, { presetProperty: "applied" });

      // normalProperty is preserved because applyPreset merges, rather than replaces.
      expect(
        workflowActionService.getTexeraGraph().getOperator(mockPresetEnabledPredicate.operatorID).operatorProperties
      ).toEqual({ presetProperty: "applied", normalProperty: "untouched" });
    });

    it("does not change operator properties when an invalid preset is applied", async () => {
      const errors = await captureRxjsUnhandled(() =>
        presetService.applyPreset("operator", mockPresetEnabledPredicate.operatorID, {
          notAPresetProperty: "applied",
        })
      );

      expect(
        workflowActionService.getTexeraGraph().getOperator(mockPresetEnabledPredicate.operatorID).operatorProperties
      ).toEqual({ presetProperty: "before", normalProperty: "untouched" });
      expect(errors).toHaveLength(1);
      expect((errors[0] as Error).message).toMatch(/Error applying preset/);
    });

    it("ignores apply events targeting an operator that does not exist on the graph", () => {
      // unknown operator IDs are silently skipped so cross-workflow events don't raise.
      expect(() => presetService.applyPreset("operator", "missing-op-id", { presetProperty: "applied" })).not.toThrow();
    });
  });

  describe("operator preset validation", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockPresetEnabledPredicate, mockPoint);
    });

    it("rejects an empty preset", () => {
      expect(presetService.isValidOperatorPreset({}, mockPresetEnabledPredicate.operatorID)).toBe(false);
    });

    it("rejects presets containing only properties that are not preset-enabled", () => {
      expect(presetService.isValidOperatorPreset({ wrongProperty: "x" }, mockPresetEnabledPredicate.operatorID)).toBe(
        false
      );
    });

    it("rejects presets with empty string values", () => {
      expect(presetService.isValidOperatorPreset({ presetProperty: "" }, mockPresetEnabledPredicate.operatorID)).toBe(
        false
      );
    });

    it("accepts presets that match the preset schema with non-empty values", () => {
      expect(
        presetService.isValidOperatorPreset({ presetProperty: "applied" }, mockPresetEnabledPredicate.operatorID)
      ).toBe(true);
    });

    it("isValidNewOperatorPreset returns false when the preset already exists", () => {
      const existing: Preset = { presetProperty: "applied" };
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify([existing])));

      let result: boolean | undefined;
      presetService
        .isValidNewOperatorPreset(existing, mockPresetEnabledPredicate.operatorID)
        .subscribe(v => (result = v));
      expect(result).toBe(false);
    });

    it("isValidNewOperatorPreset returns true when the preset is novel", () => {
      userConfigStub.fetchKey.mockReturnValue(of(JSON.stringify([{ presetProperty: "applied" }])));

      let result: boolean | undefined;
      presetService
        .isValidNewOperatorPreset({ presetProperty: "novel" }, mockPresetEnabledPredicate.operatorID)
        .subscribe(v => (result = v));
      expect(result).toBe(true);
    });

    it("isValidNewOperatorPreset short-circuits to false when the preset itself is invalid", () => {
      let result: boolean | undefined;
      presetService.isValidNewOperatorPreset({}, mockPresetEnabledPredicate.operatorID).subscribe(v => (result = v));
      expect(result).toBe(false);
    });
  });

  describe("static schema helpers", () => {
    it("getOperatorPresetSchema keeps only enable-preset properties and marks them required", () => {
      const operatorSchema = <CustomJSONSchema7>{
        type: "object",
        properties: {
          presetProperty: {
            type: "string",
            description: "property that can be saved in presets",
            title: "presetProperty",
            "enable-presets": true,
          },
          normalProperty: {
            type: "string",
            description: "property that is excluded in presets",
            title: "normalProperty",
          },
        },
        required: ["normalProperty"],
      };

      expect(PresetService.getOperatorPresetSchema(operatorSchema)).toEqual({
        type: "object",
        properties: {
          presetProperty: {
            type: "string",
            description: "property that can be saved in presets",
            title: "presetProperty",
            "enable-presets": true,
          },
        },
        required: ["presetProperty"],
        additionalProperties: false,
      });
    });

    it("getOperatorPresetSchema throws when the operator schema has no properties", () => {
      expect(() =>
        PresetService.getOperatorPresetSchema(<CustomJSONSchema7>{ type: "object", properties: {} })
      ).toThrow();
    });

    it("getOperatorPresetSchema throws when no property is preset-enabled", () => {
      expect(() =>
        PresetService.getOperatorPresetSchema(<CustomJSONSchema7>{
          type: "object",
          properties: {
            normalProperty: { type: "string", title: "normalProperty" },
          },
        })
      ).toThrow();
    });

    describe("getOperatorPreset", () => {
      it("throws when operator properties are empty", () => {
        expect(() => PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, {})).toThrow();
      });

      it("throws when operator properties miss a required preset property", () => {
        expect(() =>
          PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, { wrongProperty: "x" })
        ).toThrow();
      });

      it("returns the preset when properties cover all preset fields", () => {
        expect(PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, { presetProperty: "v" })).toEqual({
          presetProperty: "v",
        });
      });

      it("strips non-preset properties when returning the preset", () => {
        expect(
          PresetService.getOperatorPreset(mockPresetEnabledSchema.jsonSchema, {
            presetProperty: "v",
            otherProperty: "extra",
          })
        ).toEqual({ presetProperty: "v" });
      });
    });

    describe("filterOperatorPresetProperties", () => {
      it("returns empty when input is empty (never adds keys)", () => {
        expect(PresetService.filterOperatorPresetProperties(mockPresetEnabledSchema.jsonSchema, {})).toEqual({});
      });

      it("filters out non-preset properties only when at least one preset property is present", () => {
        // Ajv 8's removeAdditional traversal short-circuits when `required` fails,
        // so an input that contains *only* non-preset keys is left untouched.
        // The "+ extras" case below covers the normal stripping path.
        expect(
          PresetService.filterOperatorPresetProperties(mockPresetEnabledSchema.jsonSchema, { wrongProperty: "x" })
        ).toEqual({ wrongProperty: "x" });
      });

      it("keeps preset properties and strips extras", () => {
        expect(
          PresetService.filterOperatorPresetProperties(mockPresetEnabledSchema.jsonSchema, {
            presetProperty: "v",
            otherProperty: "extra",
          })
        ).toEqual({ presetProperty: "v" });
      });
    });
  });
});
