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
import { of } from "rxjs";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import type { Mock } from "vitest";
import { vi as vitest } from "vitest";
import { ModelSelectionModalComponent } from "./model-selection-modal.component";
import { ModelService } from "../../../dashboard/service/user/model/model.service";
import { DashboardModel } from "../../../dashboard/type/dashboard-model.interface";
import { ModelVersion } from "../../../common/type/model";
import { DatasetFileNode } from "../../../common/type/datasetVersionFileTree";

const OWNER = "owner@x.com";

const model: DashboardModel = {
  isOwner: true,
  ownerEmail: OWNER,
  accessPrivilege: "WRITE",
  size: 0,
  model: {
    mid: 7,
    ownerUid: 1,
    name: "resnet",
    repositoryName: "model-7",
    isPublic: false,
    isDownloadable: true,
    description: "",
    creationTime: undefined,
    coverImage: undefined,
    framework: "pytorch",
    format: "torchscript",
  },
};

const version: ModelVersion = {
  mvid: 70,
  mid: 7,
  creatorUid: 1,
  name: "v1",
  versionHash: undefined,
  creationTime: undefined,
  fileNodes: undefined,
};

const fileNode: DatasetFileNode = { name: "model.pt", type: "file", parentDir: `/${OWNER}/resnet/v1` };

describe("ModelSelectionModalComponent", () => {
  let component: ModelSelectionModalComponent;
  let fixture: ComponentFixture<ModelSelectionModalComponent>;
  let modalData: { selectedPath?: string | null };
  let modalRef: { close: Mock };
  let modelService: {
    retrieveAccessibleModels: Mock;
    retrieveModelVersionList: Mock;
    retrieveModelVersionFileTree: Mock;
  };

  function build(): void {
    fixture = TestBed.createComponent(ModelSelectionModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }

  beforeEach(async () => {
    modalData = {};
    modalRef = { close: vitest.fn() };
    modelService = {
      retrieveAccessibleModels: vitest.fn().mockReturnValue(of([model])),
      retrieveModelVersionList: vitest.fn().mockReturnValue(of([version])),
      retrieveModelVersionFileTree: vitest.fn().mockReturnValue(of({ fileNodes: [fileNode], size: 0 })),
    };
    await TestBed.configureTestingModule({
      imports: [ModelSelectionModalComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: modalData },
        { provide: NzModalRef, useValue: modalRef },
        { provide: ModelService, useValue: modelService },
      ],
    }).compileComponents();
  });

  it("lists the models the caller can see, with nothing selected yet", () => {
    build();
    expect(component.models).toEqual([model]);
    expect(component.selectedPath).toBeUndefined();
    const button: HTMLButtonElement = fixture.nativeElement.querySelector("button[nz-button]");
    expect(button.disabled).toBe(true);
  });

  it("reopens on the version it was given", () => {
    modalData.selectedPath = `/model/${OWNER}/resnet/v1`;
    build();

    expect(component.selectedModel).toBe(model);
    expect(component.selectedVersion).toBe(version);
    expect(component.fileTree).toEqual([fileNode]);
    expect(modelService.retrieveModelVersionFileTree).toHaveBeenCalledWith(7, 70);
  });

  it("closes with the chosen version's model path", () => {
    build();
    component.selectedModel = model;
    component.onModelChange("v1");

    expect(component.selectedPath).toBe(`/model/${OWNER}/resnet/v1`);
    component.onConfirmSelection();
    expect(modalRef.close).toHaveBeenCalledWith(`/model/${OWNER}/resnet/v1`);
  });

  it("clears the choice when the model is cleared", () => {
    build();
    component.selectedModel = undefined;
    component.onModelChange();
    expect(component.modelVersions).toEqual([]);
    expect(component.selectedPath).toBeUndefined();
  });

  it("filters models by name or by #id", () => {
    build();
    const option = { nzValue: model } as any;
    expect(component.modelFilterOption("res", option)).toBe(true);
    expect(component.modelFilterOption("#7", option)).toBe(true);
    expect(component.modelFilterOption("bert", option)).toBe(false);
  });
});
