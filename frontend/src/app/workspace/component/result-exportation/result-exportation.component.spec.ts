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

  afterEach(() => {
    fixture?.destroy();
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

  describe("operator-id resolution and downloadability getters (menu source)", () => {
    // Reconfigure the graph mock so the "menu" branch of getOperatorIdsToCheck
    // returns concrete operator IDs (drives the `.getAllOperators().map(op => op.operatorID)` branch).
    function setAllOperators(ids: string[]): void {
      const graph = TestBed.inject(WorkflowActionService) as unknown as {
        getTexeraGraph: ReturnType<typeof vi.fn>;
      };
      graph.getTexeraGraph.mockReturnValue({
        getAllOperators: () => ids.map(id => ({ operatorID: id })),
      });
    }

    function restrict(entries: Record<string, string[]>): void {
      const map = new Map<string, Set<string>>();
      Object.entries(entries).forEach(([op, labels]) => map.set(op, new Set(labels)));
      component.downloadability = new WorkflowResultDownloadability(map);
    }

    it("exportableOperatorIds maps all operators and filters out restricted ones", () => {
      setAllOperators(["op-a", "op-b", "op-c"]);
      restrict({ "op-b": ["ds (owner@x.com)"] });

      expect(component.exportableOperatorIds).toEqual(["op-a", "op-c"]);
      expect(component.blockedOperatorIds).toEqual(["op-b"]);
    });

    it("getters return [] when downloadability has not been resolved yet", () => {
      component.downloadability = undefined;

      expect(component.exportableOperatorIds).toEqual([]);
      expect(component.blockedOperatorIds).toEqual([]);
      expect(component.blockingDatasetLabels).toEqual([]);
    });

    it("blockingDatasetLabels and blockingDatasetSummary surface the blocking datasets", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-a": ["Sales (a@x.com)"], "op-b": ["Sales (a@x.com)", "HR (b@x.com)"] });

      expect(component.blockingDatasetLabels).toEqual(["Sales (a@x.com)", "HR (b@x.com)"]);
      expect(component.blockingDatasetSummary).toBe("Sales (a@x.com), HR (b@x.com)");
    });

    it("isExportRestricted is true only when every operator is blocked", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-a": ["ds1"], "op-b": ["ds2"] });

      expect(component.isExportRestricted).toBe(true);
      expect(component.hasPartialNonDownloadable).toBe(false);
    });

    it("hasPartialNonDownloadable is true when some but not all operators are blocked", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-b": ["ds1"] });

      expect(component.isExportRestricted).toBe(false);
      expect(component.hasPartialNonDownloadable).toBe(true);
    });
  });

  describe("updateOutputType", () => {
    function setAllOperators(ids: string[]): void {
      const graph = TestBed.inject(WorkflowActionService) as unknown as {
        getTexeraGraph: ReturnType<typeof vi.fn>;
      };
      graph.getTexeraGraph.mockReturnValue({
        getAllOperators: () => ids.map(id => ({ operatorID: id })),
      });
    }

    function outputTypes(byOperator: Record<string, unknown>): void {
      const results = TestBed.inject(WorkflowResultService) as unknown as {
        determineOutputTypes: ReturnType<typeof vi.fn>;
      };
      results.determineOutputTypes.mockImplementation((operatorId: string) => byOperator[operatorId]);
    }

    it("leaves the output flags untouched when downloadability is not resolved", () => {
      component.downloadability = undefined;
      component.isTableOutput = true;
      component.isVisualizationOutput = true;
      component.containsBinaryData = true;

      component.updateOutputType();

      // Early return: flags are preserved rather than recomputed/reset.
      expect(component.isTableOutput).toBe(true);
      expect(component.isVisualizationOutput).toBe(true);
      expect(component.containsBinaryData).toBe(true);
    });

    it("clears every output flag when all selected operators are export-restricted", () => {
      setAllOperators(["op-a"]);
      component.downloadability = new WorkflowResultDownloadability(new Map([["op-a", new Set(["ds"])]]));
      component.isTableOutput = true;
      component.isVisualizationOutput = true;
      component.containsBinaryData = true;

      component.updateOutputType();

      expect(component.isTableOutput).toBe(false);
      expect(component.isVisualizationOutput).toBe(false);
      expect(component.containsBinaryData).toBe(false);
    });

    it("aggregates mixed output types across exportable operators", () => {
      setAllOperators(["skip", "table-only", "viz-binary"]);
      component.downloadability = new WorkflowResultDownloadability(new Map());
      outputTypes({
        // No result at all -> skipped via `continue`.
        skip: { hasAnyResult: false, isTableOutput: true, isVisualizationOutput: true, containsBinaryData: true },
        // Pure table output.
        "table-only": {
          hasAnyResult: true,
          isTableOutput: true,
          isVisualizationOutput: false,
          containsBinaryData: false,
        },
        // Visualization output carrying binary data.
        "viz-binary": {
          hasAnyResult: true,
          isTableOutput: false,
          isVisualizationOutput: true,
          containsBinaryData: true,
        },
      });

      component.updateOutputType();

      // Not all are tables and not all are visualizations -> both false; one carries binary data.
      expect(component.isTableOutput).toBe(false);
      expect(component.isVisualizationOutput).toBe(false);
      expect(component.containsBinaryData).toBe(true);
    });

    it("reports table output when every operator with a result is a table", () => {
      setAllOperators(["t1", "t2"]);
      component.downloadability = new WorkflowResultDownloadability(new Map());
      outputTypes({
        t1: { hasAnyResult: true, isTableOutput: true, isVisualizationOutput: false, containsBinaryData: false },
        t2: { hasAnyResult: true, isTableOutput: true, isVisualizationOutput: false, containsBinaryData: false },
      });

      component.updateOutputType();

      expect(component.isTableOutput).toBe(true);
      expect(component.isVisualizationOutput).toBe(false);
      expect(component.containsBinaryData).toBe(false);
    });
  });

  describe("onUserInputDatasetName", () => {
    const alpha = {
      dataset: { did: 1, name: "Alpha" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;
    const beta = {
      dataset: { did: 2, name: "Beta" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;
    const noId = {
      dataset: { did: undefined, name: "AlphaLike" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;

    it("filters datasets by case-insensitive name match and requires a dataset id", () => {
      component.userAccessibleDatasets = [alpha, beta, noId];
      component.inputDatasetName = "alph";

      component.onUserInputDatasetName(new Event("input"));

      // noId matches the name but has no did, so it is excluded; Beta does not match.
      expect(component.filteredUserAccessibleDatasets).toEqual([alpha]);
    });

    it("resets to the full list when the input is cleared", () => {
      component.userAccessibleDatasets = [alpha, beta];
      component.inputDatasetName = "";

      component.onUserInputDatasetName(new Event("input"));

      expect(component.filteredUserAccessibleDatasets).toEqual([alpha, beta]);
      // A fresh copy, not the same array reference.
      expect(component.filteredUserAccessibleDatasets).not.toBe(component.userAccessibleDatasets);
    });
  });

  it("onClickCreateNewDataset leaves state unchanged when the creator modal is dismissed", () => {
    // beforeEach wires modalCreate to emit null on afterClose.
    const before = component.userAccessibleDatasets;
    const nameBefore = component.inputDatasetName;

    component.onClickCreateNewDataset();

    expect(modalCreate).toHaveBeenCalledTimes(1);
    expect(component.userAccessibleDatasets).toBe(before);
    expect(component.inputDatasetName).toBe(nameBefore);
  });
});

describe("ResultExportationComponent (context-menu source with default modal data)", () => {
  let component: ResultExportationComponent;
  let fixture: ComponentFixture<ResultExportationComponent>;

  // Modal data intentionally omits defaultFileName / rowIndex / columnIndex / exportType
  // so the component's `?? default` initializers are exercised, and uses a non-"menu"
  // trigger so getOperatorIdsToCheck reads the highlighted-operator branch.
  const CONTEXT_MENU_DATA = {
    sourceTriggered: "context-menu",
    workflowName: "ctx-workflow",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ResultExportationComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: CONTEXT_MENU_DATA },
        { provide: NzModalRef, useValue: { close: vi.fn(), getConfig: () => ({}) } },
        { provide: NzModalService, useValue: { create: vi.fn().mockReturnValue({ afterClose: of(null) }) } },
        {
          provide: WorkflowResultExportService,
          useValue: {
            computeRestrictionAnalysis: vi.fn().mockReturnValue(of(new WorkflowResultDownloadability(new Map()))),
            exportWorkflowExecutionResult: vi.fn(),
          },
        },
        {
          provide: DatasetService,
          useValue: { retrieveAccessibleDatasets: vi.fn().mockReturnValue(of([])) },
        },
        {
          provide: WorkflowActionService,
          useValue: {
            getTexeraGraph: vi.fn().mockReturnValue({ getAllOperators: vi.fn().mockReturnValue([]) }),
            getJointGraphWrapper: vi.fn().mockReturnValue({
              getCurrentHighlightedOperatorIDs: vi.fn().mockReturnValue(["hl-1", "hl-2"]),
            }),
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

  afterEach(() => {
    fixture?.destroy();
  });

  it("applies fallback defaults for absent modal-data fields", () => {
    expect(component.inputFileName).toBe("");
    expect(component.rowIndex).toBe(-1);
    expect(component.columnIndex).toBe(-1);
    expect(component.exportType).toBe("");
  });

  it("resolves operator ids from the highlighted-operator selection", () => {
    // Empty restriction map => every highlighted operator is exportable.
    expect(component.exportableOperatorIds).toEqual(["hl-1", "hl-2"]);
    expect(component.blockedOperatorIds).toEqual([]);
  });

  it("exports highlighted operators only (exportAll === false) for a context-menu trigger", () => {
    const exportService = TestBed.inject(WorkflowResultExportService)
      .exportWorkflowExecutionResult as unknown as ReturnType<typeof vi.fn>;

    component.onClickExportResult("local");

    expect(exportService).toHaveBeenCalledTimes(1);
    const args = exportService.mock.calls[0];
    expect(args[6]).toBe(false); // exportAll is false because sourceTriggered !== "menu"
    expect(args[7]).toBe("local");
  });
});
