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
import { WorkflowResultDownloadability, WorkflowResultExportService } from "./workflow-result-export.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { Observable, of, throwError } from "rxjs";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { DownloadService, ExportWorkflowJsonResponse } from "src/app/dashboard/service/user/download/download.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { JointGraphWrapper } from "../workflow-graph/model/joint-graph-wrapper";
import { WorkflowGraph } from "../workflow-graph/model/workflow-graph";
import { HttpResponse } from "@angular/common/http";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../common/service/gui-config.service.mock";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";
describe("WorkflowResultExportService", () => {
  let service: WorkflowResultExportService;
  let workflowWebsocketServiceSpy: Mocked<WorkflowWebsocketService>;
  let workflowActionServiceSpy: Mocked<WorkflowActionService>;
  let notificationServiceSpy: Mocked<NotificationService>;
  let executeWorkflowServiceSpy: Mocked<ExecuteWorkflowService>;
  let workflowResultServiceSpy: Mocked<WorkflowResultService>;
  let downloadServiceSpy: Mocked<DownloadService>;
  let datasetServiceSpy: Mocked<DatasetService>;

  let jointGraphWrapperSpy: Mocked<JointGraphWrapper>;
  let texeraGraphSpy: Mocked<WorkflowGraph>;

  beforeEach(() => {
    // Create spies for the required services
    jointGraphWrapperSpy = {
      getCurrentHighlightedOperatorIDs: vi.fn(),
      getJointOperatorHighlightStream: vi.fn(),
      getJointOperatorUnhighlightStream: vi.fn(),
    } as unknown as Mocked<JointGraphWrapper>;
    jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);
    jointGraphWrapperSpy.getJointOperatorHighlightStream.mockReturnValue(of());
    jointGraphWrapperSpy.getJointOperatorUnhighlightStream.mockReturnValue(of());

    texeraGraphSpy = {
      getAllOperators: vi.fn(),
      getOperatorAddStream: vi.fn(),
      getOperatorDeleteStream: vi.fn(),
      getOperatorPropertyChangeStream: vi.fn(),
      getLinkAddStream: vi.fn(),
      getLinkDeleteStream: vi.fn(),
      getDisabledOperatorsChangedStream: vi.fn(),
      getAllLinks: vi.fn(),
    } as unknown as Mocked<WorkflowGraph>;
    texeraGraphSpy.getAllOperators.mockReturnValue([]);
    texeraGraphSpy.getOperatorAddStream.mockReturnValue(of());
    texeraGraphSpy.getOperatorDeleteStream.mockReturnValue(of());
    texeraGraphSpy.getOperatorPropertyChangeStream.mockReturnValue(of());
    texeraGraphSpy.getLinkAddStream.mockReturnValue(of());
    texeraGraphSpy.getLinkDeleteStream.mockReturnValue(of());
    texeraGraphSpy.getDisabledOperatorsChangedStream.mockReturnValue(of());
    texeraGraphSpy.getAllLinks.mockReturnValue([]);

    const wsSpy = { subscribeToEvent: vi.fn(), send: vi.fn() };
    wsSpy.subscribeToEvent.mockReturnValue(of()); // Return an empty observable
    const waSpy = { getJointGraphWrapper: vi.fn(), getTexeraGraph: vi.fn(), getWorkflow: vi.fn() };
    waSpy.getJointGraphWrapper.mockReturnValue(jointGraphWrapperSpy);
    waSpy.getTexeraGraph.mockReturnValue(texeraGraphSpy);
    waSpy.getWorkflow.mockReturnValue({ wid: "workflow1", name: "Test Workflow" });

    const ntSpy = { success: vi.fn(), error: vi.fn(), loading: vi.fn() };
    const ewSpy = { getExecutionStateStream: vi.fn(), getExecutionState: vi.fn() };
    ewSpy.getExecutionStateStream.mockReturnValue(of({ previous: {}, current: { state: ExecutionState.Completed } }));
    ewSpy.getExecutionState.mockReturnValue({ state: ExecutionState.Completed });

    const wrSpy = { hasAnyResult: vi.fn(), getResultService: vi.fn(), getPaginatedResultService: vi.fn() };
    const downloadSpy = {
      downloadOperatorsResult: vi.fn(),
      getWorkflowResultDownloadability: vi.fn(),
      exportWorkflowResultToDataset: vi.fn(),
      exportWorkflowResultToLocal: vi.fn(),
    };
    downloadSpy.downloadOperatorsResult.mockReturnValue(of(new Blob()));

    const datasetSpy = { retrieveAccessibleDatasets: vi.fn() };
    datasetSpy.retrieveAccessibleDatasets.mockReturnValue(of([]));

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        WorkflowResultExportService,
        { provide: WorkflowWebsocketService, useValue: wsSpy },
        { provide: WorkflowActionService, useValue: waSpy },
        { provide: NotificationService, useValue: ntSpy },
        { provide: ExecuteWorkflowService, useValue: ewSpy },
        { provide: WorkflowResultService, useValue: wrSpy },
        { provide: DownloadService, useValue: downloadSpy },
        { provide: DatasetService, useValue: datasetSpy },
        ...commonTestProviders,
      ],
    });

    // Inject the service and spies
    service = TestBed.inject(WorkflowResultExportService);
    workflowWebsocketServiceSpy = TestBed.inject(
      WorkflowWebsocketService
    ) as unknown as Mocked<WorkflowWebsocketService>;
    workflowActionServiceSpy = TestBed.inject(WorkflowActionService) as unknown as Mocked<WorkflowActionService>;
    notificationServiceSpy = TestBed.inject(NotificationService) as unknown as Mocked<NotificationService>;
    executeWorkflowServiceSpy = TestBed.inject(ExecuteWorkflowService) as unknown as Mocked<ExecuteWorkflowService>;
    workflowResultServiceSpy = TestBed.inject(WorkflowResultService) as unknown as Mocked<WorkflowResultService>;
    downloadServiceSpy = TestBed.inject(DownloadService) as unknown as Mocked<DownloadService>;
    datasetServiceSpy = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;

    // Reset the GuiConfig to a known baseline (export disabled) so tests that
    // enable it via enableExport() do not leak state and become order-dependent.
    (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
      exportExecutionResultEnabled: false,
    });
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("resetFlags clears the highlighted flag and resets the all-operators subject to false", () => {
    service.hasResultToExportOnHighlightedOperators = true;
    service.hasResultToExportOnAllOperators.next(true);

    service.resetFlags();

    expect(service.hasResultToExportOnHighlightedOperators).toBe(false);
    expect(service.hasResultToExportOnAllOperators.value).toBe(false);
  });

  // ---- Test helpers ----------------------------------------------------------

  function enableExport(): void {
    const config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
    config.setConfig({ exportExecutionResultEnabled: true });
  }

  function makeUnit(): DashboardWorkflowComputingUnit {
    return { computingUnit: { cuid: 1, type: "local" } } as unknown as DashboardWorkflowComputingUnit;
  }

  /** Stubs the export-flow methods the real DownloadService exposes but the base spy omits. */
  function stubDownloadService(overrides?: {
    downloadability?: Record<string, string[]>;
    datasetResponse?: HttpResponse<unknown>;
    datasetError?: unknown;
  }): Pick<
    Mocked<DownloadService>,
    "getWorkflowResultDownloadability" | "exportWorkflowResultToDataset" | "exportWorkflowResultToLocal"
  > {
    downloadServiceSpy.getWorkflowResultDownloadability.mockReturnValue(of(overrides?.downloadability ?? {}));
    downloadServiceSpy.exportWorkflowResultToDataset.mockReturnValue(
      (overrides?.datasetError !== undefined
        ? throwError(() => overrides.datasetError)
        : of(
            overrides?.datasetResponse ?? new HttpResponse({ body: { status: "success", message: "ok" } })
          )) as Observable<HttpResponse<ExportWorkflowJsonResponse>>
    );
    // determineOutputExtension echoes the requested export type so the stubbed
    // outputType stays coherent with the format passed to each export call.
    (workflowResultServiceSpy as any).determineOutputExtension = vi
      .fn()
      .mockImplementation((_operatorId: string, exportType: string) => exportType);
    return {
      getWorkflowResultDownloadability: downloadServiceSpy.getWorkflowResultDownloadability,
      exportWorkflowResultToDataset: downloadServiceSpy.exportWorkflowResultToDataset,
      exportWorkflowResultToLocal: downloadServiceSpy.exportWorkflowResultToLocal,
    };
  }

  describe("computeRestrictionAnalysis", () => {
    it("returns an empty restriction map without hitting the backend when the workflow has no id", () => {
      workflowActionServiceSpy.getWorkflow.mockReturnValue({ wid: undefined } as any);
      const download = stubDownloadService({ downloadability: { op1: ["ds1"] } });

      let result: WorkflowResultDownloadability | undefined;
      service.computeRestrictionAnalysis().subscribe(r => (result = r));

      expect(result?.restrictedOperatorMap.size).toBe(0);
      expect(download.getWorkflowResultDownloadability).not.toHaveBeenCalled();
    });

    it("maps the backend response into a Map of operatorId -> Set<datasetLabel>", () => {
      const download = stubDownloadService({
        downloadability: { op1: ["ds1 (a@x.com)"], op2: ["ds2 (b@y.com)", "ds3 (c@z.com)"] },
      });

      let result: WorkflowResultDownloadability | undefined;
      service.computeRestrictionAnalysis().subscribe(r => (result = r));

      expect(download.getWorkflowResultDownloadability).toHaveBeenCalledWith("workflow1");
      expect(result?.restrictedOperatorMap.get("op1")).toEqual(new Set(["ds1 (a@x.com)"]));
      expect(result?.restrictedOperatorMap.get("op2")).toEqual(new Set(["ds2 (b@y.com)", "ds3 (c@z.com)"]));
    });

    it("falls back to an empty restriction map when the backend call errors", () => {
      const getWorkflowResultDownloadability = vi.fn().mockReturnValue(throwError(() => new Error("boom")));
      (downloadServiceSpy as any).getWorkflowResultDownloadability = getWorkflowResultDownloadability;

      let result: WorkflowResultDownloadability | undefined;
      service.computeRestrictionAnalysis().subscribe(r => (result = r));

      expect(result).toBeInstanceOf(WorkflowResultDownloadability);
      expect(result?.restrictedOperatorMap.size).toBe(0);
    });
  });

  describe("export flow (exportWorkflowExecutionResult / performExport)", () => {
    it("does nothing when export is disabled in the gui config", () => {
      const download = stubDownloadService();
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.loading).not.toHaveBeenCalled();
      expect(notificationServiceSpy.error).not.toHaveBeenCalled();
      expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
    });

    it("errors when the computing unit is null", () => {
      enableExport();
      const download = stubDownloadService();
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", true, "dataset", null);

      expect(notificationServiceSpy.error).toHaveBeenCalledWith(
        "Cannot export result: computing unit is not available"
      );
      expect(notificationServiceSpy.loading).not.toHaveBeenCalled();
      expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
    });

    it("errors when the workflow id is not available", () => {
      enableExport();
      workflowActionServiceSpy.getWorkflow.mockReturnValue({ wid: undefined } as any);
      const download = stubDownloadService();
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.error).toHaveBeenCalledWith("Cannot export result: workflow ID is not available");
      expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
    });

    it("does nothing when no operators are selected (highlighted export with empty selection)", () => {
      enableExport();
      const download = stubDownloadService();
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue([]);

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", false, "dataset", makeUnit());

      expect(notificationServiceSpy.loading).not.toHaveBeenCalled();
      expect(notificationServiceSpy.error).not.toHaveBeenCalled();
      expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
    });

    it("errors (no export) when every selected operator is blocked by a non-downloadable dataset", () => {
      enableExport();
      const download = stubDownloadService({ downloadability: { op1: ["ds1 (a@x.com)"] } });
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.error).toHaveBeenCalledWith(
        "Cannot export result: selection depends on dataset(s) that are not downloadable: ds1 (a@x.com)"
      );
      expect(notificationServiceSpy.loading).not.toHaveBeenCalled();
      expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
    });

    it("warns and still exports the unblocked operators when only some are blocked", () => {
      enableExport();
      (notificationServiceSpy as any).warning = vi.fn();
      const download = stubDownloadService({ downloadability: { op2: ["ds2 (b@y.com)"] } });
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }, { operatorID: "op2" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [7], 1, 2, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.warning).toHaveBeenCalledWith(
        "Some operators were skipped because their results depend on dataset(s) that are not downloadable" +
          " (ds2 (b@y.com))"
      );
      expect(notificationServiceSpy.loading).toHaveBeenCalledWith("Exporting...");
      expect(download.exportWorkflowResultToDataset).toHaveBeenCalledWith(
        "csv",
        "workflow1",
        "wf",
        [{ id: "op1", outputType: "csv" }],
        [7],
        1,
        2,
        "file",
        expect.anything()
      );
      expect(notificationServiceSpy.success).toHaveBeenCalledWith("Result exported successfully");
    });

    it("exports to dataset and reports success on a success response", () => {
      enableExport();
      (notificationServiceSpy as any).warning = vi.fn();
      const download = stubDownloadService();
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [5], 0, 0, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.warning).not.toHaveBeenCalled();
      expect(notificationServiceSpy.loading).toHaveBeenCalledWith("Exporting...");
      expect(download.exportWorkflowResultToDataset).toHaveBeenCalledTimes(1);
      expect(notificationServiceSpy.success).toHaveBeenCalledWith("Result exported successfully");
    });

    it("reports the server message when the dataset export response is not successful", () => {
      enableExport();
      stubDownloadService({
        datasetResponse: new HttpResponse({ body: { status: "failure", message: "quota exceeded" } }),
      });
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [5], 0, 0, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.error).toHaveBeenCalledWith("quota exceeded");
      expect(notificationServiceSpy.success).not.toHaveBeenCalled();
    });

    it("reports a default error message when the dataset export response has no body", () => {
      enableExport();
      stubDownloadService({ datasetResponse: new HttpResponse({}) });
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [5], 0, 0, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.error).toHaveBeenCalledWith("An error occurred during export");
    });

    it("reports the extracted error message when the dataset export call fails", () => {
      enableExport();
      stubDownloadService({ datasetError: { error: { message: "server exploded" } } });
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("csv", "wf", [5], 0, 0, "file", true, "dataset", makeUnit());

      expect(notificationServiceSpy.error).toHaveBeenCalledWith(
        "An error happened in exporting operator results: server exploded"
      );
    });

    it("routes to the local-filesystem export when destination is 'local'", () => {
      enableExport();
      const download = stubDownloadService();
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "op1" }] as any);

      service.exportWorkflowExecutionResult("json", "wf", [], 3, 4, "local-file", true, "local", makeUnit());

      expect(notificationServiceSpy.loading).toHaveBeenCalledWith("Exporting...");
      expect(download.exportWorkflowResultToLocal).toHaveBeenCalledWith(
        "json",
        "workflow1",
        "wf",
        [{ id: "op1", outputType: "json" }],
        3,
        4,
        "local-file",
        expect.anything()
      );
      expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
    });
  });

  describe("updateExportAvailabilityFlags", () => {
    it("marks results exportable when execution is idle and operators have a result snapshot", () => {
      executeWorkflowServiceSpy.getExecutionState.mockReturnValue({ state: ExecutionState.Completed } as any);
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["opH"]);
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "opA" }] as any);
      // hasAnyResult is false, so the snapshot side of the OR must decide exportability.
      workflowResultServiceSpy.hasAnyResult.mockReturnValue(false);
      workflowResultServiceSpy.getResultService.mockReturnValue({
        getCurrentResultSnapshot: () => ({}),
      } as any);

      (service as any).updateExportAvailabilityFlags();

      expect(service.hasResultToExportOnHighlightedOperators).toBe(true);
      expect(service.hasResultToExportOnAllOperators.value).toBe(true);
    });

    it("keeps results non-exportable while a workflow is still executing", () => {
      executeWorkflowServiceSpy.getExecutionState.mockReturnValue({ state: ExecutionState.Running } as any);
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(["opH"]);
      texeraGraphSpy.getAllOperators.mockReturnValue([{ operatorID: "opA" }] as any);
      workflowResultServiceSpy.hasAnyResult.mockReturnValue(true);
      workflowResultServiceSpy.getResultService.mockReturnValue({
        getCurrentResultSnapshot: () => ({}),
      } as any);

      (service as any).updateExportAvailabilityFlags();

      expect(service.hasResultToExportOnHighlightedOperators).toBe(false);
      expect(service.hasResultToExportOnAllOperators.value).toBe(false);
    });
  });

  it("getExportOnAllOperatorsStatusStream emits the current all-operators availability value", () => {
    service.hasResultToExportOnAllOperators.next(true);

    const emitted: boolean[] = [];
    service
      .getExportOnAllOperatorsStatusStream()
      .subscribe(v => emitted.push(v))
      .unsubscribe();

    expect(emitted).toEqual([true]);
  });
});

describe("WorkflowResultDownloadability", () => {
  const restrictedMap = new Map<string, Set<string>>([
    ["opB", new Set(["ds1 (a@x.com)"])],
    ["opD", new Set(["ds2 (b@y.com)", "ds1 (a@x.com)"])],
  ]);
  const ids = ["opA", "opB", "opC", "opD"];

  it("getExportableOperatorIds keeps only operators absent from the restricted map", () => {
    const downloadability = new WorkflowResultDownloadability(restrictedMap);
    expect(downloadability.getExportableOperatorIds(ids)).toEqual(["opA", "opC"]);
  });

  it("getBlockedOperatorIds keeps only operators present in the restricted map", () => {
    const downloadability = new WorkflowResultDownloadability(restrictedMap);
    expect(downloadability.getBlockedOperatorIds(ids)).toEqual(["opB", "opD"]);
  });

  it("getBlockingDatasets returns the deduped union of blocking dataset labels", () => {
    const downloadability = new WorkflowResultDownloadability(restrictedMap);
    const result = downloadability.getBlockingDatasets(["opB", "opD"]);
    // ds1 is shared by opB and opD, so the union must be deduped (length 2, not 3).
    // The return builds a Set, so assert membership order-independently.
    expect(result).toHaveLength(2);
    expect([...result].sort()).toEqual(["ds1 (a@x.com)", "ds2 (b@y.com)"]);
  });

  it("getBlockingDatasets returns an empty array for unrestricted operators", () => {
    const downloadability = new WorkflowResultDownloadability(restrictedMap);
    expect(downloadability.getBlockingDatasets(["opA", "opC"])).toEqual([]);
  });

  it("treats every operator as exportable when the restricted map is empty", () => {
    const downloadability = new WorkflowResultDownloadability(new Map());
    expect(downloadability.getExportableOperatorIds(ids)).toEqual(ids);
    expect(downloadability.getBlockedOperatorIds(ids)).toEqual([]);
  });
});
