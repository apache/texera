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
import { Observable, of, Subject, throwError } from "rxjs";
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
  let workflowActionServiceSpy: Mocked<WorkflowActionService>;
  let notificationServiceSpy: Mocked<NotificationService>;
  let executeWorkflowServiceSpy: Mocked<ExecuteWorkflowService>;
  let workflowResultServiceSpy: Mocked<WorkflowResultService>;
  let downloadServiceSpy: Mocked<DownloadService>;

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
    TestBed.inject(WorkflowWebsocketService) as unknown as Mocked<WorkflowWebsocketService>;
    workflowActionServiceSpy = TestBed.inject(WorkflowActionService) as unknown as Mocked<WorkflowActionService>;
    notificationServiceSpy = TestBed.inject(NotificationService) as unknown as Mocked<NotificationService>;
    executeWorkflowServiceSpy = TestBed.inject(ExecuteWorkflowService) as unknown as Mocked<ExecuteWorkflowService>;
    workflowResultServiceSpy = TestBed.inject(WorkflowResultService) as unknown as Mocked<WorkflowResultService>;
    downloadServiceSpy = TestBed.inject(DownloadService) as unknown as Mocked<DownloadService>;
    TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;

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

  // It used to replace the subject with a fresh one, which silently orphaned whoever was subscribed.
  it("resetFlags tells the existing subscribers, rather than replacing the subject under them", () => {
    const seen: boolean[] = [];
    service.getExportOnAllOperatorsStatusStream().subscribe(v => seen.push(v));
    service.hasResultToExportOnAllOperators.next(true);

    service.resetFlags();

    expect(seen).toEqual([false, true, false]);
  });

  // A menu arriving on results kept across a hand-over between a workflow's two views asks for
  // the flags to be recomputed, since the departing menu reset them on its way out.
  it("refreshExportAvailability recomputes the flags from what is in hand", () => {
    const recompute = vi.spyOn(service as any, "updateExportAvailabilityFlags");

    service.refreshExportAvailability();

    expect(recompute).toHaveBeenCalledTimes(1);
  });

  // ---- Test helpers ----------------------------------------------------------

  function enableExport(): void {
    const config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
    config.setConfig({ exportExecutionResultEnabled: true });
  }

  function makeUnit(): DashboardWorkflowComputingUnit {
    return { computingUnit: { cuid: 1, type: "local" } } as unknown as DashboardWorkflowComputingUnit;
  }

  function setCanvasOperators(ids: string[], highlighted: string[] = []): void {
    texeraGraphSpy.getAllOperators.mockReturnValue(ids.map(operatorID => ({ operatorID })) as any);
    if (highlighted.length > 0) {
      jointGraphWrapperSpy.getCurrentHighlightedOperatorIDs.mockReturnValue(highlighted);
    }
  }

  function setupExportScenario(
    options: {
      enable?: boolean;
      operators?: string[];
      highlighted?: string[];
      wid?: string | undefined;
      hasWidOverride?: boolean;
      downloadability?: Record<string, string[]>;
      datasetResponse?: HttpResponse<unknown>;
      datasetError?: unknown;
      withWarningSpy?: boolean;
    } = {}
  ) {
    if (options.enable !== false) {
      enableExport();
    }
    if (options.hasWidOverride) {
      workflowActionServiceSpy.getWorkflow.mockReturnValue({ wid: options.wid } as any);
    }
    if (options.withWarningSpy) {
      (notificationServiceSpy as any).warning = vi.fn();
    }
    setCanvasOperators(options.operators ?? ["op1"], options.highlighted ?? []);
    return stubDownloadService({
      downloadability: options.downloadability,
      datasetResponse: options.datasetResponse,
      datasetError: options.datasetError,
    });
  }

  function expectExportAborted(download: ReturnType<typeof stubDownloadService>, expectedError?: string): void {
    if (expectedError !== undefined) {
      expect(notificationServiceSpy.error).toHaveBeenCalledWith(expectedError);
    } else {
      expect(notificationServiceSpy.error).not.toHaveBeenCalled();
    }
    expect(notificationServiceSpy.loading).not.toHaveBeenCalled();
    expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
  }

  function expectDatasetExported(
    download: ReturnType<typeof stubDownloadService>,
    operatorIds: string[],
    datasetIds = [7]
  ): void {
    expect(download.exportWorkflowResultToDataset).toHaveBeenCalledWith(
      "csv",
      "workflow1",
      "wf",
      operatorIds.map(id => ({ id, outputType: "csv" })),
      datasetIds,
      1,
      2,
      "file",
      expect.anything()
    );
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
      const download = setupExportScenario({ enable: false });

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", "dataset", makeUnit(), ["op1"]);

      expectExportAborted(download);
    });

    it("errors when the computing unit is null", () => {
      const download = setupExportScenario();

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", "dataset", null, ["op1"]);

      expectExportAborted(download, "Cannot export result: computing unit is not available");
    });

    it("errors when the workflow id is not available", () => {
      const download = setupExportScenario({ hasWidOverride: true, wid: undefined });

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", "dataset", makeUnit(), ["op1"]);

      expect(notificationServiceSpy.error).toHaveBeenCalledWith("Cannot export result: workflow ID is not available");
      expect(download.exportWorkflowResultToDataset).not.toHaveBeenCalled();
    });

    it("does nothing when the caller's scope is empty", () => {
      const download = setupExportScenario({ operators: [] });

      service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", "dataset", makeUnit(), []);

      expectExportAborted(download);
    });

    // The scope is the caller's to decide and the service does not second-guess it: the dialog
    // resolves what the export covers in order to report on it, and the same list is what gets
    // exported. Which list that is, for each way of opening the dialog, is the dialog's own spec.
    it("exports exactly the operators it was given, whatever the canvas has selected", () => {
      const download = setupExportScenario({ operators: ["opA", "opB"], highlighted: ["opHighlighted"] });

      service.exportWorkflowExecutionResult("csv", "wf", [7], 1, 2, "file", "dataset", makeUnit(), ["opNamed"]);

      expectDatasetExported(download, ["opNamed"]);
    });

    // The restriction analysis is asynchronous, and a caller may hand over the canvas selection,
    // which is the live array rather than a copy of it. Without a snapshot the export would go
    // out with whatever is selected when the analysis answers, not what was asked for.
    it("exports the scope it was given, not what that array became while the analysis ran", () => {
      const download = setupExportScenario({ operators: [] });
      const pending = new Subject<Record<string, string[]>>();
      downloadServiceSpy.getWorkflowResultDownloadability.mockReturnValue(pending as any);
      const liveSelection = ["opA"];

      service.exportWorkflowExecutionResult("csv", "wf", [7], 1, 2, "file", "dataset", makeUnit(), liveSelection);
      liveSelection.push("opB");
      pending.next({});
      pending.complete();

      expectDatasetExported(download, ["opA"]);
    });

    it("exports every operator it was given, in the order it was given them", () => {
      const download = setupExportScenario({ operators: [] });

      service.exportWorkflowExecutionResult("csv", "wf", [7], 1, 2, "file", "dataset", makeUnit(), ["opA", "opB"]);

      expectDatasetExported(download, ["opA", "opB"]);
    });

    [
      {
        title: "errors (no export) when every selected operator is blocked by a non-downloadable dataset",
        blockedLabels: ["ds1 (a@x.com)"],
        expectedMessage:
          "Cannot export result: selection depends on dataset(s) that are not downloadable: ds1 (a@x.com)",
      },
      {
        // The backend can mark an operator restricted while naming no dataset labels for it,
        // which leaves the message with nothing to append.
        title: "errors without a dataset list when every operator is blocked but no dataset was named",
        blockedLabels: [] as string[],
        expectedMessage: "Cannot export result: selection depends on dataset(s) that are not downloadable",
      },
    ].forEach(({ title, blockedLabels, expectedMessage }) => {
      it(title, () => {
        const download = setupExportScenario({ downloadability: { op1: blockedLabels } });

        service.exportWorkflowExecutionResult("csv", "wf", [1], 0, 0, "file", "dataset", makeUnit(), ["op1"]);

        expectExportAborted(download, expectedMessage);
      });
    });

    [
      {
        title: "warns and still exports the unblocked operators when only some are blocked",
        blockedLabels: ["ds2 (b@y.com)"],
        expectedWarning:
          "Some operators were skipped because their results depend on dataset(s) that are not downloadable" +
          " (ds2 (b@y.com))",
        expectSuccessToast: true,
      },
      {
        title: "warns without a dataset list when only some operators are blocked and no dataset was named",
        blockedLabels: [] as string[],
        expectedWarning:
          "Some operators were skipped because their results depend on dataset(s) that are not downloadable",
        expectSuccessToast: false,
      },
    ].forEach(({ title, blockedLabels, expectedWarning, expectSuccessToast }) => {
      it(title, () => {
        const download = setupExportScenario({
          operators: ["op1", "op2"],
          downloadability: { op2: blockedLabels },
          withWarningSpy: true,
        });

        // Both are in scope; only op2 is blocked, which is what makes this the partial case.
        service.exportWorkflowExecutionResult("csv", "wf", [7], 1, 2, "file", "dataset", makeUnit(), ["op1", "op2"]);

        expect(notificationServiceSpy.warning).toHaveBeenCalledWith(expectedWarning);
        expect(notificationServiceSpy.loading).toHaveBeenCalledWith("Exporting...");
        expectDatasetExported(download, ["op1"]);
        if (expectSuccessToast) {
          expect(notificationServiceSpy.success).toHaveBeenCalledWith("Result exported successfully");
        }
      });
    });

    it("exports to dataset and reports success on a success response", () => {
      const download = setupExportScenario({ withWarningSpy: true });

      service.exportWorkflowExecutionResult("csv", "wf", [5], 0, 0, "file", "dataset", makeUnit(), ["op1"]);

      expect(notificationServiceSpy.warning).not.toHaveBeenCalled();
      expect(notificationServiceSpy.loading).toHaveBeenCalledWith("Exporting...");
      expect(download.exportWorkflowResultToDataset).toHaveBeenCalledTimes(1);
      expect(notificationServiceSpy.success).toHaveBeenCalledWith("Result exported successfully");
    });

    [
      {
        title: "reports the server message when the dataset export response is not successful",
        overrides: {
          datasetResponse: new HttpResponse({ body: { status: "failure", message: "quota exceeded" } }),
        },
        expectedError: "quota exceeded",
        checkNoSuccess: true,
      },
      {
        title: "reports a default error message when the dataset export response has no body",
        overrides: { datasetResponse: new HttpResponse({}) },
        expectedError: "An error occurred during export",
      },
      {
        title: "reports the extracted error message when the dataset export call fails",
        overrides: { datasetError: { error: { message: "server exploded" } } },
        expectedError: "An error happened in exporting operator results: server exploded",
      },
      {
        // .error is present but is a bare string, so there is no .error.message to extract.
        title: "reports the error payload itself when the failure carries no nested message",
        overrides: { datasetError: { error: "dataset quota exceeded" } },
        expectedError: "An error happened in exporting operator results: dataset quota exceeded",
      },
      {
        // e.g. a transport-level failure surfacing as a bare value rather than an HttpErrorResponse.
        title: "reports the raw failure when it carries no error field at all",
        overrides: { datasetError: "connection reset by peer" },
        expectedError: "An error happened in exporting operator results: connection reset by peer",
      },
    ].forEach(({ title, overrides, expectedError, checkNoSuccess }) => {
      it(title, () => {
        setupExportScenario(overrides);

        service.exportWorkflowExecutionResult("csv", "wf", [5], 0, 0, "file", "dataset", makeUnit(), ["op1"]);

        expect(notificationServiceSpy.error).toHaveBeenCalledWith(expectedError);
        if (checkNoSuccess) {
          expect(notificationServiceSpy.success).not.toHaveBeenCalled();
        }
      });
    });

    it("routes to the local-filesystem export when destination is 'local'", () => {
      const download = setupExportScenario();

      service.exportWorkflowExecutionResult("json", "wf", [], 3, 4, "local-file", "local", makeUnit(), ["op1"]);

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

  function setupAvailabilityCheck(state: ExecutionState, hasAnyResult: boolean, operatorWithSnapshot?: string): void {
    executeWorkflowServiceSpy.getExecutionState.mockReturnValue({ state } as any);
    setCanvasOperators(["opA"], ["opH"]);
    workflowResultServiceSpy.hasAnyResult.mockReturnValue(hasAnyResult);
    workflowResultServiceSpy.getResultService.mockImplementation((operatorId: string) =>
      operatorWithSnapshot === undefined || operatorId === operatorWithSnapshot
        ? ({ getCurrentResultSnapshot: () => ({}) } as any)
        : undefined
    );
    (service as any).updateExportAvailabilityFlags();
  }

  describe("updateExportAvailabilityFlags", () => {
    it("marks results exportable when execution is idle and operators have a result snapshot", () => {
      // hasAnyResult is false, so the snapshot side of the OR must decide exportability.
      setupAvailabilityCheck(ExecutionState.Completed, false);

      expect(service.hasResultToExportOnHighlightedOperators).toBe(true);
      expect(service.hasResultToExportOnAllOperators.value).toBe(true);
    });

    // The two flags read two DIFFERENT operator scopes -- the highlighted selection for the
    // context-menu button, every operator on the canvas for the top-menu button. Giving only one
    // scope a result proves each flag reads its own scope and not the other's.
    [
      {
        scope: "the highlighted selection",
        operatorWithSnapshot: "opH",
        expectedHighlightedFlag: true,
        expectedAllOperatorsFlag: false,
      },
      {
        scope: "an unselected operator elsewhere on the canvas",
        operatorWithSnapshot: "opA",
        expectedHighlightedFlag: false,
        expectedAllOperatorsFlag: true,
      },
    ].forEach(({ scope, operatorWithSnapshot, expectedHighlightedFlag, expectedAllOperatorsFlag }) => {
      it(`marks only the matching flag exportable when just ${scope} has a result`, () => {
        setupAvailabilityCheck(ExecutionState.Completed, false, operatorWithSnapshot);

        expect(service.hasResultToExportOnHighlightedOperators).toBe(expectedHighlightedFlag);
        expect(service.hasResultToExportOnAllOperators.value).toBe(expectedAllOperatorsFlag);
      });
    });

    it("keeps results non-exportable while a workflow is still executing", () => {
      setupAvailabilityCheck(ExecutionState.Running, true);

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
