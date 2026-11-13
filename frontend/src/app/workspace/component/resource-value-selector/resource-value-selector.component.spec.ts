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
import { FormControl } from "@angular/forms";
import { of } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import type { Mock } from "vitest";
import { vi as vitest } from "vitest";
import { ResourceValueSelectorComponent } from "./resource-value-selector.component";
import { ModelSelectionModalComponent } from "../model-selection-modal/model-selection-modal.component";
import { DatasetSelectionModalComponent } from "../dataset-selection-modal/dataset-selection-modal.component";

describe("ResourceValueSelectorComponent", () => {
  let modalService: { create: Mock };

  function selector(resource: string, value = "", disabled = false): ResourceValueSelectorComponent {
    const component = TestBed.runInInjectionContext(
      () => new ResourceValueSelectorComponent(modalService as unknown as NzModalService)
    );
    (component as any).field = {
      props: { resource },
      formControl: new FormControl({ value, disabled }),
    };
    return component;
  }

  beforeEach(() => {
    modalService = { create: vitest.fn() };
  });

  it("labels a chosen version by name and version, and an empty one by what to pick", () => {
    expect(selector("model", "/model/owner@x.com/resnet/v2").label).toBe("resnet · v2");
    expect(selector("model").label).toBe("Select model");
    expect(selector("dataset").label).toBe("Select dataset");
  });

  it("opens the model browser and stores the version it returns", () => {
    const component = selector("model", "/model/owner@x.com/resnet/v1");
    modalService.create.mockReturnValue({ afterClose: of("/model/owner@x.com/resnet/v2") });

    component.openPicker();

    const options = modalService.create.mock.calls[0][0];
    expect(options.nzContent).toBe(ModelSelectionModalComponent);
    expect(options.nzData.selectedPath).toBe("/model/owner@x.com/resnet/v1");
    expect(component.formControl.value).toBe("/model/owner@x.com/resnet/v2");
    expect(component.formControl.dirty).toBe(true);
  });

  it("opens the dataset browser on whole versions rather than files", () => {
    const component = selector("dataset");
    modalService.create.mockReturnValue({ afterClose: of(undefined) });

    component.openPicker();

    const options = modalService.create.mock.calls[0][0];
    expect(options.nzContent).toBe(DatasetSelectionModalComponent);
    expect(options.nzData).toEqual({ fileMode: false, selectedPath: null });
    expect(component.formControl.value).toBe("");
  });

  it("does nothing while the field is disabled", () => {
    selector("model", "", true).openPicker();
    expect(modalService.create).not.toHaveBeenCalled();
  });
});
