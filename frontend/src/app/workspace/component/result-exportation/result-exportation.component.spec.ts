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
import { ResultExportationComponent } from "./result-exportation.component";
import {
  WorkflowResultDownloadability,
  WorkflowResultExportService,
} from "../../service/workflow-result-export/workflow-result-export.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { UserDatasetVersionCreatorComponent } from "../../../dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-version-creator/user-dataset-version-creator.component";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { NZ_MODAL_DATA, NzModalRef, NzModalService } from "ng-zorro-antd/modal";

const writeDataset = {
  dataset: { did: 1, name: "writable" },
  accessPrivilege: "WRITE",
} as unknown as DashboardDataset;

const readDataset = {
  dataset: { did: 2, name: "readonly" },
  accessPrivilege: "READ",
} as unknown as DashboardDataset;

const MODAL_DATA = {
  sourceTriggered: "menu",
  workflowName: "my-workflow",
  defaultFileName: "out.csv",
  rowIndex: -1,
  columnIndex: -1,
  exportType: "csv",
};

describe("ResultExportationComponent", () => {
  let component: ResultExportationComponent;
  let fixture: ComponentFixture<ResultExportationComponent>;

  let exportWorkflowExecutionResult: ReturnType<typeof vi.fn>;
  let modalClose: ReturnType<typeof vi.fn>;
  let modalCreate: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    exportWorkflowExecutionResult = vi.fn();
    modalClose = vi.fn();
    modalCreate = vi.fn().mockReturnValue({ afterClose: of(null) });

    await TestBed.configureTestingModule({
      imports: [ResultExportationComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: MODAL_DATA },
        { provide: NzModalRef, useValue: { close: modalClose, getConfig: () => ({}) } },
        { provide: NzModalService, useValue: { create: modalCreate } },
        {
          provide: WorkflowResultExportService,
          useValue: {
            computeRestrictionAnalysis: vi.fn().mockReturnValue(of(new WorkflowResultDownloadability(new Map()))),
            exportWorkflowExecutionResult,
          },
        },
        {
          provide: DatasetService,
          useValue: { retrieveAccessibleDatasets: vi.fn().mockReturnValue(of([writeDataset, readDataset])) },
        },
        {
          provide: WorkflowActionService,
          useValue: {
            getTexeraGraph: vi.fn().mockReturnValue({ getAllOperators: vi.fn().mockReturnValue([]) }),
            getJointGraphWrapper: vi
              .fn()
              .mockReturnValue({ getCurrentHighlightedOperatorIDs: vi.fn().mockReturnValue([]) }),
          },
        },
        {
          provide: WorkflowResultService,
          useValue: {
            determineOutputTypes: vi.fn().mockReturnValue({
              hasAnyResult: false,
              isTableOutput: false,
              isVisualizationOutput: false,
              containsBinaryData: false,
            }),
          },
        },
        {
          provide: ComputingUnitStatusService,
          useValue: { getSelectedComputingUnit: vi.fn().mockReturnValue(of(null)) },
        },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ResultExportationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create and render", () => {
    expect(component).toBeTruthy();
  });

  it("ngOnInit keeps only WRITE-accessible datasets and stores the downloadability", () => {
    // detectChanges() in beforeEach already ran ngOnInit with its synchronous mock streams.
    expect(component.userAccessibleDatasets).toEqual([writeDataset]);
    expect(component.filteredUserAccessibleDatasets).toEqual([writeDataset]);
    expect(component.downloadability).toBeDefined();
    expect(component.selectedComputingUnit).toBeNull();
  });

  it("onClickExportResult('dataset', ds) exports to the dataset destination and closes the modal", () => {
    component.onClickExportResult("dataset", writeDataset);

    expect(exportWorkflowExecutionResult).toHaveBeenCalledTimes(1);
    const args = exportWorkflowExecutionResult.mock.calls[0];
    expect(args[0]).toBe("csv"); // exportType, from modal data
    expect(args[1]).toBe("my-workflow"); // workflowName
    expect(args[2]).toEqual([1]); // datasetIds resolved from ds.dataset.did
    expect(args[6]).toBe(true); // exportAll, because sourceTriggered === "menu"
    expect(args[7]).toBe("dataset"); // destination
    expect(modalClose).toHaveBeenCalledTimes(1);
  });

  it("onClickExportResult('local') exports to local download with no dataset ids", () => {
    component.onClickExportResult("local");

    expect(exportWorkflowExecutionResult).toHaveBeenCalledTimes(1);
    const args = exportWorkflowExecutionResult.mock.calls[0];
    expect(args[2]).toEqual([]); // local download carries no dataset ids
    expect(args[7]).toBe("local");
    expect(modalClose).toHaveBeenCalledTimes(1);
  });

  it("onClickCreateNewDataset opens the dataset-creator modal and adopts the created dataset", () => {
    const created = {
      dataset: { did: 9, name: "brand-new" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;
    modalCreate.mockReturnValue({ afterClose: of(created) });

    component.onClickCreateNewDataset();

    expect(modalCreate).toHaveBeenCalledTimes(1);
    const config = modalCreate.mock.calls[0][0];
    expect(config.nzTitle).toBe("Create New Dataset");
    expect(config.nzContent).toBe(UserDatasetVersionCreatorComponent);
    // afterClose emitted a dataset, so the component adopts it into its lists.
    expect(component.userAccessibleDatasets[0]).toBe(created);
    expect(component.inputDatasetName).toBe("brand-new");
  });
});
