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
import { FieldTypeConfig } from "@ngx-formly/core";
import { of } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { DatasetFileSelectorComponent } from "./dataset-file-selector.component";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";

describe("DatasetFileSelectorComponent", () => {
  let component: DatasetFileSelectorComponent;
  let fixture: ComponentFixture<DatasetFileSelectorComponent>;
  // Minimal NzModalService stub: only `create` is used, and the component only
  // reads `afterClose` off the returned modal ref. Each test overrides what
  // `afterClose` emits via `mockReturnValue`.
  let modalServiceSpy: { create: ReturnType<typeof vi.fn> };

  // Attach a fresh FormControl with a known starting value, since the component
  // extends FieldType and reads/writes through `this.formControl`.
  function setFormControl(initialValue: string): FormControl {
    const formControl = new FormControl(initialValue);
    component.field = { formControl } as FieldTypeConfig;
    return formControl;
  }

  beforeEach(async () => {
    modalServiceSpy = { create: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [DatasetFileSelectorComponent],
      providers: [
        { provide: NzModalService, useValue: modalServiceSpy },
        { provide: WorkflowActionService, useValue: {} },
        { provide: GuiConfigService, useValue: { env: { selectingFilesFromDatasetsEnabled: true } } },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(DatasetFileSelectorComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("opens the dataset selection modal once when clicked", () => {
    modalServiceSpy.create.mockReturnValue({ afterClose: of(undefined) });
    setFormControl("");

    component.onClickOpenFileSelectionModal();

    expect(modalServiceSpy.create).toHaveBeenCalledTimes(1);
  });

  it("writes the chosen path back into the form control when a path is selected", () => {
    const formControl = setFormControl("");
    modalServiceSpy.create.mockReturnValue({ afterClose: of("/dataset/data.csv") });

    component.onClickOpenFileSelectionModal();

    expect(formControl.value).toBe("/dataset/data.csv");
  });

  it("leaves the form control unchanged when the modal is dismissed without a path", () => {
    const formControl = setFormControl("/existing/path.csv");
    modalServiceSpy.create.mockReturnValue({ afterClose: of("") });

    component.onClickOpenFileSelectionModal();

    expect(formControl.value).toBe("/existing/path.csv");
  });

  it("exposes isFileSelectionEnabled from the GUI config", () => {
    expect(component.isFileSelectionEnabled).toBe(true);
  });
});
