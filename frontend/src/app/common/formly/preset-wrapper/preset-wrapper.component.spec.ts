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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormControl } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { NzMessageService } from "ng-zorro-antd/message";
import { Subject, of } from "rxjs";
import { Preset, PresetService } from "src/app/workspace/service/preset/preset.service";
import { PresetKey, PresetWrapperComponent } from "./preset-wrapper.component";

const fieldKey = "testkey";
const presetKey: PresetKey = {
  presetType: "testPresetType",
  saveTarget: "testPresetSaveTarget",
  applyTarget: "testPresetApplyTarget",
};
const testPreset: Preset = { testkey: "testPresetValue", otherkey: "otherPresetValue" };
const otherPreset: Preset = { testkey: "otherPresetValue2", otherkey: "otherPresetValue3" };

describe("PresetWrapperComponent", () => {
  let component: PresetWrapperComponent;
  let fixture: ComponentFixture<PresetWrapperComponent>;
  let formControl: FormControl;
  let presetServiceStub: {
    applyPreset: ReturnType<typeof vi.fn>;
    deletePreset: ReturnType<typeof vi.fn>;
    createPreset: ReturnType<typeof vi.fn>;
    getPresets: ReturnType<typeof vi.fn>;
    isValidPreset: ReturnType<typeof vi.fn>;
    savePresetsStream: Subject<{ type: string; target: string; presets: Preset[] }>;
    applyPresetStream: Subject<{ type: string; target: string; preset: Preset }>;
  };
  let messageStub: {
    error: ReturnType<typeof vi.fn>;
    success: ReturnType<typeof vi.fn>;
    info: ReturnType<typeof vi.fn>;
    warning: ReturnType<typeof vi.fn>;
  };

  // Builds a minimal FormlyFieldConfig sufficient for ngOnInit to run.
  // ngOnInit also calls filterPresetFromForm(), which iterates
  // field.parent.fieldGroup looking for sibling preset-wrapper fields, so
  // we expose a single sibling pointing at an empty model by default.
  const buildField = (overrides: Partial<FormlyFieldConfig> = {}): FormlyFieldConfig => {
    const self = {
      key: fieldKey,
      wrappers: ["preset-wrapper"],
      model: { [fieldKey]: "" },
    } as FormlyFieldConfig;
    return {
      key: fieldKey,
      formControl,
      templateOptions: { presetKey },
      parent: { fieldGroup: [self] },
      ...overrides,
    } as FormlyFieldConfig;
  };

  beforeEach(async () => {
    formControl = new FormControl("");

    presetServiceStub = {
      applyPreset: vi.fn(),
      deletePreset: vi.fn(),
      createPreset: vi.fn(),
      getPresets: vi.fn().mockReturnValue(of([])),
      isValidPreset: vi.fn().mockReturnValue(true),
      savePresetsStream: new Subject(),
      applyPresetStream: new Subject(),
    };
    messageStub = {
      error: vi.fn(),
      success: vi.fn(),
      info: vi.fn(),
      warning: vi.fn(),
    };

    // Override the template so the spec doesn't depend on the ng-zorro
    // dropdown machinery — we exercise the public component API directly.
    TestBed.overrideComponent(PresetWrapperComponent, { set: { template: "" } });

    await TestBed.configureTestingModule({
      imports: [PresetWrapperComponent],
      providers: [
        { provide: PresetService, useValue: presetServiceStub },
        { provide: NzMessageService, useValue: messageStub },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(PresetWrapperComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    component.field = buildField();
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe("ngOnInit", () => {
    it("throws when field.key is missing", () => {
      component.field = buildField({ key: undefined });
      expect(() => component.ngOnInit()).toThrow();
    });

    it("throws when templateOptions is missing", () => {
      component.field = buildField({ templateOptions: undefined });
      expect(() => component.ngOnInit()).toThrow();
    });

    it("throws when templateOptions.presetKey is missing", () => {
      component.field = buildField({ templateOptions: {} });
      expect(() => component.ngOnInit()).toThrow();
    });

    it("populates searchResults from presetService.getPresets on init", () => {
      presetServiceStub.getPresets.mockReturnValue(of([testPreset, otherPreset]));
      component.field = buildField();

      component.ngOnInit();

      expect(presetServiceStub.getPresets).toHaveBeenCalledWith(presetKey.presetType, presetKey.saveTarget);
      expect(component.searchResults).toEqual([testPreset, otherPreset]);
    });
  });

  describe("functional api", () => {
    beforeEach(() => {
      component.field = buildField();
      component.ngOnInit();
    });

    it("applyPreset forwards to PresetService with the configured presetType + applyTarget", () => {
      component.applyPreset(testPreset);
      expect(presetServiceStub.applyPreset).toHaveBeenCalledTimes(1);
      expect(presetServiceStub.applyPreset).toHaveBeenCalledWith(
        presetKey.presetType,
        presetKey.applyTarget,
        testPreset
      );
    });

    it("deletePreset forwards to PresetService with the configured presetType + saveTarget", () => {
      component.deletePreset(testPreset);
      expect(presetServiceStub.deletePreset).toHaveBeenCalledTimes(1);
      const args = presetServiceStub.deletePreset.mock.calls[0];
      expect(args.slice(0, 3)).toEqual([presetKey.presetType, presetKey.saveTarget, testPreset]);
    });

    it("getEntryTitle returns the value at field.key", () => {
      expect(component.getEntryTitle(testPreset)).toBe("testPresetValue");
    });

    it("getEntryDescription joins all non-key values with commas", () => {
      expect(component.getEntryDescription(testPreset)).toBe("otherPresetValue");
      expect(
        component.getEntryDescription({
          testkey: "title",
          a: "first",
          b: "second",
        })
      ).toBe("first, second");
    });

    describe("getSearchResults", () => {
      it("returns a copy of all presets when showAllResults is true", () => {
        const presets: Preset[] = [testPreset, otherPreset];
        const results = component.getSearchResults(presets, "anything", true);
        expect(results).toEqual(presets);
        expect(results).not.toBe(presets);
      });

      it("returns all presets when showAllResults is true even if the search term doesn't match", () => {
        expect(component.getSearchResults([testPreset], "no-match", true)).toEqual([testPreset]);
      });

      it("filters by case-insensitive prefix match on the entry title when showAllResults is false", () => {
        const presets: Preset[] = [testPreset, otherPreset];
        // testPreset title 'testPresetValue' starts with 'TEST'
        expect(component.getSearchResults(presets, "TEST", false)).toEqual([testPreset]);
        // otherPreset title 'otherPresetValue2' starts with 'other'
        expect(component.getSearchResults(presets, "other", false)).toEqual([otherPreset]);
      });

      it("returns the full list when search term is empty and showAllResults is false", () => {
        expect(component.getSearchResults([testPreset], "", false)).toEqual([testPreset]);
      });

      it("returns an empty list when the search term matches nothing", () => {
        expect(component.getSearchResults([testPreset], "zzzz", false)).toEqual([]);
      });
    });
  });

  describe("dropdown visibility", () => {
    beforeEach(() => {
      component.field = buildField();
      component.ngOnInit();
    });

    it("re-fetches presets and updates searchResults when the dropdown opens", () => {
      presetServiceStub.getPresets.mockReturnValue(of([testPreset]));
      // ngOnInit has already called getPresets once.
      const baseline = presetServiceStub.getPresets.mock.calls.length;

      component.onDropdownVisibilityEvent(true);

      expect(presetServiceStub.getPresets.mock.calls.length).toBe(baseline + 1);
      expect(component.searchResults).toEqual([testPreset]);
    });

    it("does not refetch when the dropdown closes", () => {
      const baseline = presetServiceStub.getPresets.mock.calls.length;
      component.onDropdownVisibilityEvent(false);
      expect(presetServiceStub.getPresets.mock.calls.length).toBe(baseline);
    });
  });

  describe("PresetService stream subscriptions", () => {
    beforeEach(() => {
      component.field = buildField();
      component.ngOnInit();
    });

    it("updates searchResults when savePresetsStream emits a matching event", () => {
      component.searchResults = [];
      const presets: Preset[] = [testPreset, otherPreset];

      presetServiceStub.savePresetsStream.next({
        type: presetKey.presetType,
        target: presetKey.saveTarget,
        presets,
      });

      expect(component.searchResults).toEqual(presets);
    });

    it("ignores savePresetsStream events for a different presetType", () => {
      component.searchResults = [];
      presetServiceStub.savePresetsStream.next({
        type: "differentType",
        target: presetKey.saveTarget,
        presets: [testPreset],
      });
      expect(component.searchResults).toEqual([]);
    });

    it("ignores savePresetsStream events for a different saveTarget", () => {
      component.searchResults = [];
      presetServiceStub.savePresetsStream.next({
        type: presetKey.presetType,
        target: "differentTarget",
        presets: [testPreset],
      });
      expect(component.searchResults).toEqual([]);
    });

    it("does not refresh searchResults from form value changes while the dropdown is closed", () => {
      const baselineCalls = presetServiceStub.getPresets.mock.calls.length;
      component.presetMenuVisible = false;

      formControl.setValue("typing");

      // No additional getPresets call because the menu is closed.
      expect(presetServiceStub.getPresets.mock.calls.length).toBe(baselineCalls);
    });

    it("refreshes searchResults from form value changes while the dropdown is open", async () => {
      component.presetMenuVisible = true;
      presetServiceStub.getPresets.mockReturnValue(of([testPreset]));
      const baselineCalls = presetServiceStub.getPresets.mock.calls.length;

      formControl.setValue("typing");
      // The valueChanges handler is debounced(0) — wait one microtask tick.
      await new Promise(resolve => setTimeout(resolve, 0));

      expect(presetServiceStub.getPresets.mock.calls.length).toBe(baselineCalls + 1);
    });

    it("stops responding to stream events after ngOnDestroy", () => {
      component.searchResults = [];
      component.ngOnDestroy();

      presetServiceStub.savePresetsStream.next({
        type: presetKey.presetType,
        target: presetKey.saveTarget,
        presets: [testPreset],
      });

      expect(component.searchResults).toEqual([]);
    });
  });

  describe("savePreset", () => {
    // savePreset() reads sibling preset-wrapper fields off field.parent.fieldGroup
    // to construct the preset payload.
    const buildFieldWithSiblings = (model: Record<string, unknown>): FormlyFieldConfig => {
      const fieldGroup: FormlyFieldConfig[] = [
        { key: fieldKey, wrappers: ["preset-wrapper"], model } as FormlyFieldConfig,
        { key: "otherkey", wrappers: ["preset-wrapper"], model } as FormlyFieldConfig,
        // Non-preset sibling — must be ignored.
        { key: "ignored", wrappers: ["form-field"], model } as FormlyFieldConfig,
      ];
      return {
        key: fieldKey,
        formControl,
        templateOptions: { presetKey },
        parent: { fieldGroup },
      } as FormlyFieldConfig;
    };

    it("creates a preset built from sibling preset-wrapper fields when the preset is valid", () => {
      component.field = buildFieldWithSiblings({ testkey: "v1", otherkey: "v2", ignored: "x" });
      component.ngOnInit();
      presetServiceStub.isValidPreset.mockReturnValue(true);

      component.savePreset();

      expect(presetServiceStub.isValidPreset).toHaveBeenCalledWith({ testkey: "v1", otherkey: "v2" });
      expect(presetServiceStub.createPreset).toHaveBeenCalledWith(presetKey.presetType, presetKey.saveTarget, {
        testkey: "v1",
        otherkey: "v2",
      });
      expect(messageStub.error).not.toHaveBeenCalled();
    });

    it("shows an error toast and does not create a preset when the preset is invalid", () => {
      component.field = buildFieldWithSiblings({ testkey: "", otherkey: "v2" });
      component.ngOnInit();
      presetServiceStub.isValidPreset.mockReturnValue(false);

      component.savePreset();

      expect(presetServiceStub.createPreset).not.toHaveBeenCalled();
      expect(messageStub.error).toHaveBeenCalledTimes(1);
    });
  });
});
