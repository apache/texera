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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { FieldWrapper, FormlyFieldConfig } from "@ngx-formly/core";
import { merge } from "lodash";
import { ReplaySubject } from "rxjs";
import { filter, first, takeUntil } from "rxjs/operators";
import { Preset, PresetService } from "src/app/workspace/service/preset/preset.service";
import { asType } from "../../util/assert";
import { NzMessageService } from "ng-zorro-antd/message";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NgFor } from "@angular/common";

/**
 * PresetWrapperComponent is a custom formly form field wrapper: https://formly.dev/guide/custom-formly-wrapper
 * It uses PresetService to create a dropdown menu for a form field that includes preset entries that when clicked are
 * applied through the PresetService (generating an event in PresetService.applyPresetStream).
 * Currently the PresetService only truly handles operator presets. (i.e. causing the for data to change immediately)
 * For non-operator presets, an application event is generated, but no action is taken (please implement listeners to apply the presets properly)
 * USAGE:
 * Formly field key should match attributes of preset
 * FormlyFieldConfig.wrappers should include 'preset-wrapper'
 * FormlyFieldConfig.templateOptions.presetKey should be a PresetKey
 * @author Albert Liu
 */

/**
 * A PresetKey must be passed to PresetWrapperComponent via templateOptions.presetKey (add a template option to the formly field config)
 * This can be done easily by using PresetWrapperComponent.setupFieldConfig()
 */
export interface PresetKey {
  presetType: string;
  saveTarget: string;
  applyTarget: string;
}

@Component({
  templateUrl: "./preset-wrapper.component.html",
  styleUrls: ["./preset-wrapper.component.scss"],
  imports: [
    NzDropdownDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NgFor,
    NzMenuItemComponent,
  ],
})
export class PresetWrapperComponent extends FieldWrapper implements OnInit, OnDestroy {
  public searchResults: readonly Preset[] = []; // the list of presets shown in the dropdown
  private presetType: string = ""; // corresponds to type used in presetService.getPresets(type, target). Usually "operator"
  private saveTarget: string = ""; // corresponds to target used in presetService.getPresets(type, target). Usually operator type, i.e. "MySQLSource"
  private applyTarget: string = ""; // corresponds to target used in presetService.applyPreset(type, target). Usually operatorID, i.e "MySQLSource-operator-8fb88f81-1bb1-4b00-bbd1-3d2f23c5e1d7"
  private teardownObservable: ReplaySubject<boolean> = new ReplaySubject(1); // observable used OnDestroy to tear down subscriptions that takeUntil(teardownObservable)

  constructor(
    private presetService: PresetService,
    private messageService: NzMessageService
  ) {
    super();
  }

  ngOnInit(): void {
    if (
      this.field.key === undefined ||
      this.field.templateOptions === undefined ||
      this.field.templateOptions.presetKey === undefined ||
      this.field.parent?.fieldGroup === undefined
    ) {
      throw Error(
        `form preset-wrapper field ${this.field} doesn't contain necessary .key, .templateOptions.presetKey and .parent.fieldGroup attributes`
      );
    }
    const presetKey = <PresetKey>this.field.templateOptions.presetKey;
    this.presetType = presetKey.presetType;
    this.saveTarget = presetKey.saveTarget;
    this.applyTarget = presetKey.applyTarget;
    this.updateSearchResults();

    this.handleSavePresets(); // handles when presets for this saveTarget change
  }

  /**
   * applies preset using PresetService.savePresetsStream event/observable system
   * @param preset
   */
  public applyPreset(preset: Preset) {
    this.presetService.applyPreset(this.presetType, this.applyTarget, preset);
  }

  public deletePreset(preset: Preset) {
    this.presetService.deletePreset(
      this.presetType,
      this.saveTarget,
      preset,
      `Deleted preset: ${this.getEntryTitle(preset)}`,
      "error"
    );
  }

  /**
   * Generates title for dropdown menu entries
   * @param preset to generate title for
   * @returns title
   */
  public getEntryTitle(preset: Preset): string {
    return preset[asType(this.field.key, "string")].toString();
  }

  /**
   * Generates description of dropdown menu entries
   * @param preset to generate description of
   * @returns description
   */
  public getEntryDescription(preset: Preset): string {
    return Object.keys(preset)
      .filter(key => key !== asType(this.field.key, "string"))
      .map(key => preset[key])
      .join(", ");
  }

  /**
   * updates search results when dropdown is activated (clicking form field opens dropdown)
   * @param visible Event value, whether or not dropdown is visible
   */
  public onDropdownVisibilityEvent(visible: boolean) {
    if (visible) {
      this.updateSearchResults();
    }
  }

  /**
   * called when service is destroyed by angular.
   * tears down subscriptions that takeUntil(teardownObservable)
   */
  public ngOnDestroy() {
    this.teardownObservable.next(true);
    this.teardownObservable.complete();
  }

  public savePreset() {
    const preset = this.filterPresetFromForm();
    if (this.presetService.isValidPreset(preset)) {
      this.presetService.createPreset(this.presetType, this.saveTarget, preset);
    } else {
      this.messageService.error("Preset not saved: Fill out all preset fields.");
    }
  }

  /**
   * handles when presets for the current presetType are changed due to saving new presets
   * updates search results to account for new presets
   */
  private handleSavePresets() {
    this.presetService.savePresetsStream
      .pipe(
        filter(presets => presets.type === this.presetType && presets.target === this.saveTarget),
        takeUntil(this.teardownObservable)
      )
      .subscribe({
        next: saveEvent => {
          this.searchResults = saveEvent.presets;
        },
      });
  }

  /**
   * Filters formData to only include members that are in the preset schema of the given operatorType
   * @returns partially finished Preset. use PresetService.isValidOperatorPreset to verify all preset attributes exist
   */
  filterPresetFromForm(): Preset {
    let preset: Preset = {};
    let arr = this.field.parent?.fieldGroup?.filter(formfield => formfield.wrappers?.includes("preset-wrapper"));
    (arr as FormlyFieldConfig[]).forEach(field => {
      const key = asType(field.key, "string");
      preset[key] = field.model[key];
    });

    return preset;
  }

  /**
   * updates search results
   */
  private updateSearchResults() {
    this.presetService
      .getPresets(this.presetType, this.saveTarget)
      .pipe(first(), takeUntil(this.teardownObservable))
      .subscribe(presets => {
        this.searchResults = presets;
      });
  }

  /**
   * setup FormlyFieldConfig to use PresetWrapperComponent:
   * adds preset-wrapper and form-field (default wrapper) as wrappers
   * @param config FormlyFieldConfig to setup
   * @param presetType corresponds to type used in presetService.getPresets(type, target). Usually "operator"
   * @param saveTarget corresponds to target used in presetService.getPresets(type, target). Usually operator type, i.e. "MySQLSource"
   * @param applyTarget corresponds to target used in presetService.applyPreset(type, target). Usually operatorID, i.e "MySQLSource-operator-8fb88f81-1bb1-4b00-bbd1-3d2f23c5e1d7"
   */
  public static setupFieldConfig(
    config: FormlyFieldConfig,
    presetType: string,
    saveTarget: string,
    applyTarget: string
  ) {
    const fieldConfig: FormlyFieldConfig = {
      wrappers: ["form-field", "preset-wrapper"], // wrap form field in default theme and then preset wrapper
      templateOptions: {
        presetKey: <PresetKey>{
          presetType: presetType,
          saveTarget: saveTarget,
          applyTarget: applyTarget,
        },
        // disable browser's default autocomplete to not block our preset autocomplete
        attributes: {
          autocomplete: "off",
        },
      },
    };
    merge(config, fieldConfig);
  }
}
