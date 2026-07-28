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
import { DatasetSelectionModalComponent } from "./dataset-selection-modal.component";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetVersion } from "../../../common/type/dataset";
import { DatasetFileNode } from "../../../common/type/datasetVersionFileTree";

const OWNER = "owner@x.com";

const dataset: DashboardDataset = {
  isOwner: true,
  ownerEmail: OWNER,
  dataset: {
    did: 10,
    name: "myds",
    ownerUid: 1,
    isPublic: false,
    isDownloadable: false,
    storagePath: undefined,
    description: "",
    creationTime: undefined,
    coverImage: undefined,
  },
  accessPrivilege: "WRITE",
  size: 0,
};

const version: DatasetVersion = {
  dvid: 100,
  did: 10,
  creatorUid: 1,
  name: "v1",
  versionHash: undefined,
  creationTime: undefined,
  fileNodes: undefined,
};

const fileNode: DatasetFileNode = { name: "a.csv", type: "file", parentDir: `/${OWNER}/myds/v1` };

describe("DatasetSelectionModalComponent", () => {
  let component: DatasetSelectionModalComponent;
  let fixture: ComponentFixture<DatasetSelectionModalComponent>;
  let modalData: { fileMode: boolean; selectedPath?: string | null };
  let modalRef: { close: ReturnType<typeof vi.fn> };
  let datasetService: {
    retrieveAccessibleDatasets: ReturnType<typeof vi.fn>;
    retrieveDatasetVersionList: ReturnType<typeof vi.fn>;
    retrieveDatasetVersionFileTree: ReturnType<typeof vi.fn>;
  };

  // Build the component after the test has configured `modalData`, so that
  // ngOnInit (run by detectChanges) sees the intended fileMode / selectedPath.
  function build(): void {
    fixture = TestBed.createComponent(DatasetSelectionModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }

  beforeEach(async () => {
    modalData = { fileMode: false };
    modalRef = { close: vi.fn() };
    datasetService = {
      retrieveAccessibleDatasets: vi.fn().mockReturnValue(of([dataset])),
      retrieveDatasetVersionList: vi.fn().mockReturnValue(of([version])),
      retrieveDatasetVersionFileTree: vi.fn().mockReturnValue(of({ fileNodes: [fileNode], size: 0 })),
    };

    await TestBed.configureTestingModule({
      imports: [DatasetSelectionModalComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: modalData },
        { provide: NzModalRef, useValue: modalRef },
        { provide: DatasetService, useValue: datasetService },
      ],
    }).compileComponents();
  });

  it("creates and renders, loading the accessible datasets", () => {
    build();
    expect(component).toBeTruthy();
    expect(datasetService.retrieveAccessibleDatasets).toHaveBeenCalled();
    expect(component.datasets).toEqual([dataset]);
    // template rendered: the confirm button is disabled until a path is selected
    const button: HTMLButtonElement = fixture.nativeElement.querySelector("button[nz-button]");
    expect(button.disabled).toBe(true);
  });

  it("ngOnInit initializes selectedDataset and selectedVersion from data.selectedPath", () => {
    modalData.fileMode = true;
    modalData.selectedPath = `/${OWNER}/myds/v1`;

    build();

    expect(component.selectedDataset).toBe(dataset);
    expect(component.datasetVersions).toEqual([version]);
    expect(component.selectedVersion).toBe(version);
    expect(component.fileTree).toEqual([fileNode]);
    expect(datasetService.retrieveDatasetVersionList).toHaveBeenCalledWith(10);
    expect(datasetService.retrieveDatasetVersionFileTree).toHaveBeenCalledWith(10, 100);
  });

  it("onDatasetChange loads the version list and auto-selects a version in file mode", () => {
    modalData.fileMode = true;
    build();

    component.selectedDataset = dataset;
    component.onDatasetChange();

    expect(datasetService.retrieveDatasetVersionList).toHaveBeenCalledWith(10);
    expect(component.datasetVersions).toEqual([version]);
    expect(component.selectedVersion).toBe(version);
  });

  it("onVersionChange composes selectedPath from the dataset/version in non-file mode", () => {
    build(); // fileMode false

    component.selectedDataset = dataset;
    component.selectedVersion = version;
    component.onVersionChange();

    expect(datasetService.retrieveDatasetVersionFileTree).toHaveBeenCalledWith(10, 100);
    expect(component.fileTree).toEqual([fileNode]);
    expect(component.selectedPath).toBe(`/${OWNER}/myds/v1`);
  });

  it("onFileSelected sets selectedPath to the node's full path in file mode", () => {
    modalData.fileMode = true;
    build();

    component.onFileSelected(fileNode);

    expect(component.selectedPath).toBe(`/${OWNER}/myds/v1/a.csv`);
  });

  it("onConfirmSelection closes the modal with the selected path", () => {
    build();
    component.selectedPath = "/some/path";

    component.onConfirmSelection();

    expect(modalRef.close).toHaveBeenCalledWith("/some/path");
  });
});
