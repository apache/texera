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
import { SimpleChange } from "@angular/core";

import { ResultTableFrameComponent } from "./result-table-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { NzTableModule, NzTableQueryParams } from "ng-zorro-antd/table";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { By, DomSanitizer } from "@angular/platform-browser";
import { of, Subject } from "rxjs";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { isAudioUrl, isImageUrl, isVideoUrl } from "../../../../common/util/media-type.util";
import {
  OperatorPaginationResultService,
  WorkflowResultService,
} from "../../../service/workflow-result/workflow-result.service";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { PanelResizeService } from "../../../service/workflow-result/panel-resize/panel-resize.service";
import { OperatorState, OperatorStatistics, WebResultUpdate } from "../../../types/execute-workflow.interface";
import { PaginatedResultEvent } from "../../../types/workflow-websocket.interface";
import { IndexableObject } from "../../../types/result-table.interface";
import { RowModalComponent } from "../result-panel-modal.component";
import { ResultExportationComponent } from "../../result-exportation/result-exportation.component";

type OperatorStatsMap = Record<string, Record<string, Record<string, number>>>;

describe("ResultTableFrameComponent", () => {
  let component: ResultTableFrameComponent;
  let fixture: ComponentFixture<ResultTableFrameComponent>;
  let workflowResultService: WorkflowResultService;
  let workflowStatusService: WorkflowStatusService;
  let resizeService: PanelResizeService;
  let modalService: NzModalService;

  const GUI_CONFIG_LIMIT = 15;

  const SAMPLE_ROW: IndexableObject = { _id: 0, name: "alice", score: 90 };

  const makePageEvent = (
    pageIndex: number,
    table: ReadonlyArray<IndexableObject> = [SAMPLE_ROW]
  ): PaginatedResultEvent => ({
    requestID: "",
    operatorID: "op1",
    pageIndex,
    table,
    schema: [],
  });

  // Installs a paginated-result-service double behind WorkflowResultService.getPaginatedResultService.
  const makePaginatedResultService = (overrides: Record<string, unknown> = {}) => {
    const paginatedResultService = {
      getCurrentTotalNumTuples: vi.fn().mockReturnValue(42),
      getCurrentPageIndex: vi.fn().mockReturnValue(1),
      getStats: vi.fn().mockReturnValue({ name: { min: 1 } }),
      selectPage: vi.fn().mockReturnValue(of(makePageEvent(1))),
      ...overrides,
    };
    vi.spyOn(workflowResultService, "getPaginatedResultService").mockReturnValue(
      paginatedResultService as unknown as OperatorPaginationResultService
    );
    return paginatedResultService;
  };

  const makeStatistics = (state: OperatorState): OperatorStatistics => ({
    operatorState: state,
    aggregatedInputRowCount: 0,
    inputPortMetrics: {},
    aggregatedOutputRowCount: 0,
    outputPortMetrics: {},
  });

  const paginationUpdate = (totalNumTuples: number, dirtyPageIndices: number[]): WebResultUpdate => ({
    mode: { type: "PaginationMode" },
    totalNumTuples,
    dirtyPageIndices,
  });

  const queryParams = (pageIndex: number): NzTableQueryParams => ({ pageIndex, pageSize: 5, sort: [], filter: [] });

  // Re-creates the component so spies installed on service streams are picked up by ngOnInit.
  // Destroys the fixture created in beforeEach (or a prior recreate) first so its
  // untilDestroyed subscriptions are torn down and cannot leak across the test.
  const recreateComponent = (operatorId?: string): void => {
    fixture?.destroy();
    fixture = TestBed.createComponent(ResultTableFrameComponent);
    component = fixture.componentInstance;
    component.operatorId = operatorId;
    fixture.detectChanges();
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ResultTableFrameComponent, HttpClientTestingModule, NzModalModule, NzTableModule, NoopAnimationsModule],
      providers: [
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        {
          provide: GuiConfigService,
          useValue: {
            env: {
              limitColumns: GUI_CONFIG_LIMIT,
            },
          },
        },
        ...commonTestProviders,
      ],
    }).compileComponents();
    workflowResultService = TestBed.inject(WorkflowResultService);
    workflowStatusService = TestBed.inject(WorkflowStatusService);
    resizeService = TestBed.inject(PanelResizeService);
    modalService = TestBed.inject(NzModalService);
    fixture = TestBed.createComponent(ResultTableFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(container => (container.innerHTML = ""));
    vi.restoreAllMocks();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("currentResult should not be modified if setupResultTable is called with empty (zero-length) execution result", () => {
    component.currentResult = [{ test: "property" }];
    (component as any).setupResultTable([], 0);

    expect(component.currentResult).toEqual([{ test: "property" }]);
  });

  it("should set columnLimit from gui-config", () => {
    expect(component.columnLimit).toEqual(GUI_CONFIG_LIMIT);
  });

  describe("ngOnChanges", () => {
    it("ignores a change that carries no operator id", () => {
      const getServiceSpy = vi.spyOn(workflowResultService, "getPaginatedResultService");

      component.ngOnChanges({});

      expect(component.isFrontPagination).toBe(true);
      expect(getServiceSpy).not.toHaveBeenCalled();
    });

    it("keeps front-end pagination when the operator has no paginated result service", () => {
      vi.spyOn(workflowResultService, "getPaginatedResultService").mockReturnValue(undefined);

      component.ngOnChanges({ operatorId: new SimpleChange(undefined, "op1", true) });

      expect(component.operatorId).toBe("op1");
      expect(component.isFrontPagination).toBe(true);
      expect(component.totalNumTuples).toBe(0);
    });

    it("switches to server-side pagination and loads the operator's current page", () => {
      const paginated = makePaginatedResultService();

      component.ngOnChanges({ operatorId: new SimpleChange(undefined, "op1", true) });

      expect(component.isFrontPagination).toBe(false);
      expect(component.currentPageIndex).toBe(1);
      expect(paginated.selectPage).toHaveBeenCalledWith(1, component.pageSize, 0, GUI_CONFIG_LIMIT, "");
      expect(component.currentResult).toEqual([SAMPLE_ROW]);
      expect(component.currentColumns?.map(c => c.columnDef)).toEqual(["name", "score"]);
      expect(component.totalNumTuples).toBe(42);
      expect(component.isLoadingResult).toBe(false);
      expect(component.tableStats).toEqual({ name: { min: 1 } });
      expect(component.prevTableStats).toBe(component.tableStats);
    });
  });

  describe("changePaginatedResultData", () => {
    it("is a no-op without an operator id or a paginated result service", () => {
      const getServiceSpy = vi.spyOn(workflowResultService, "getPaginatedResultService").mockReturnValue(undefined);

      component.operatorId = undefined;
      component.changePaginatedResultData();
      expect(getServiceSpy).not.toHaveBeenCalled();

      component.operatorId = "op1";
      component.changePaginatedResultData();
      expect(getServiceSpy).toHaveBeenCalledTimes(1);
      expect(component.isLoadingResult).toBe(false);
    });

    it("ignores a page response when the user has already switched to another page", () => {
      const paginated = makePaginatedResultService({
        selectPage: vi.fn().mockReturnValue(of(makePageEvent(9))),
      });
      component.operatorId = "op1";
      component.currentResult = [];

      component.changePaginatedResultData();

      expect(paginated.selectPage).toHaveBeenCalledTimes(1);
      expect(component.currentResult).toEqual([]);
      expect(component.isLoadingResult).toBe(true);
    });
  });

  describe("setupResultTable", () => {
    it("returns early when no operator is selected", () => {
      component.operatorId = undefined;

      component.setupResultTable([SAMPLE_ROW], 1);

      expect(component.currentColumns).toBeUndefined();
      expect(component.totalNumTuples).toBe(0);
    });

    it("builds columns from the first row and drops the internal _id column", () => {
      component.operatorId = "op1";
      component.isLoadingResult = true;

      component.setupResultTable([SAMPLE_ROW, { _id: 1, name: "bob", score: 75 }], 12);

      expect(component.isLoadingResult).toBe(false);
      expect(component.currentResult).toEqual([SAMPLE_ROW, { _id: 1, name: "bob", score: 75 }]);
      expect(component.currentColumns?.map(c => c.header)).toEqual(["name", "score"]);
      expect(component.currentColumns?.[0].getCell(SAMPLE_ROW)).toBe("alice");
      expect(component.currentColumns?.[1].getCell(SAMPLE_ROW)).toBe("90");
      expect(component.totalNumTuples).toBe(12);
    });
  });

  describe("workflow status stream", () => {
    it("marks the operator finished only while its reported state is Completed", () => {
      const statusStream = new Subject<Record<string, OperatorStatistics>>();
      vi.spyOn(workflowStatusService, "getStatusUpdateStream").mockReturnValue(statusStream.asObservable());
      recreateComponent("op1");

      statusStream.next({ op1: makeStatistics(OperatorState.Completed) });
      expect(component.isOperatorFinished).toBe(true);

      statusStream.next({ op1: makeStatistics(OperatorState.Running) });
      expect(component.isOperatorFinished).toBe(false);

      statusStream.next({ otherOp: makeStatistics(OperatorState.Completed) });
      expect(component.isOperatorFinished).toBe(false);
    });
  });

  describe("result update stream", () => {
    let resultUpdates: Subject<Record<string, WebResultUpdate | undefined>>;

    beforeEach(() => {
      resultUpdates = new Subject<Record<string, WebResultUpdate | undefined>>();
      vi.spyOn(workflowResultService, "getResultUpdateStream").mockReturnValue(resultUpdates.asObservable());
    });

    it("refreshes the table when the currently shown page becomes dirty", () => {
      const paginated = makePaginatedResultService({
        getCurrentTotalNumTuples: vi.fn().mockReturnValue(77),
      });
      recreateComponent("op1");
      component.currentColumns = component.generateColumns([
        { columnKey: "name", columnText: "name" },
        { columnKey: "score", columnText: "score" },
      ]);

      resultUpdates.next({ op1: paginationUpdate(77, [1]) });

      expect(component.isFrontPagination).toBe(false);
      expect(component.widthPercent).toBe("50%");
      expect(paginated.selectPage).toHaveBeenCalledWith(1, component.pageSize, 0, GUI_CONFIG_LIMIT, "");
      expect(component.currentResult).toEqual([SAMPLE_ROW]);
      expect(component.totalNumTuples).toBe(77);
    });

    it("updates the tuple count without refetching when the current page is clean", () => {
      const paginated = makePaginatedResultService();
      recreateComponent("op1");

      resultUpdates.next({ op1: paginationUpdate(63, [4]) });

      expect(component.totalNumTuples).toBe(63);
      expect(component.isFrontPagination).toBe(false);
      expect(component.widthPercent).toBe("");
      expect(paginated.selectPage).not.toHaveBeenCalled();
    });

    it("ignores snapshot updates, cleared entries, and updates for other operators", () => {
      recreateComponent("op1");

      resultUpdates.next({ op1: { mode: { type: "SetSnapshotMode" }, table: [] } });
      resultUpdates.next({ op1: undefined });
      resultUpdates.next({ otherOp: paginationUpdate(9, [1]) });

      expect(component.isFrontPagination).toBe(true);
      expect(component.totalNumTuples).toBe(0);
    });

    it("ignores updates when the frame has no operator", () => {
      recreateComponent(undefined);

      resultUpdates.next({ op1: paginationUpdate(9, [1]) });

      expect(component.isFrontPagination).toBe(true);
      expect(component.totalNumTuples).toBe(0);
    });
  });

  describe("result table stats stream", () => {
    let statsStream: Subject<[OperatorStatsMap, OperatorStatsMap]>;

    beforeEach(() => {
      statsStream = new Subject<[OperatorStatsMap, OperatorStatsMap]>();
      vi.spyOn(workflowResultService, "getResultTableStats").mockReturnValue(statsStream.asObservable());
    });

    it("keeps the previous stats when both snapshots describe the same columns", () => {
      recreateComponent("op1");

      statsStream.next([{ op1: { colA: { min: 1 } } }, { op1: { colA: { min: 2 } } }]);

      expect(component.tableStats).toEqual({ colA: { min: 2 } });
      expect(component.prevTableStats).toEqual({ colA: { min: 1 } });
    });

    it("falls back to the current stats when columns changed or no previous stats exist", () => {
      recreateComponent("op1");

      statsStream.next([{ op1: { colB: { min: 1 } } }, { op1: { colA: { min: 2 } } }]);
      expect(component.tableStats).toEqual({ colA: { min: 2 } });
      expect(component.prevTableStats).toBe(component.tableStats);

      statsStream.next([{}, { op1: { colC: { max: 3 } } }]);
      expect(component.tableStats).toEqual({ colC: { max: 3 } });
      expect(component.prevTableStats).toBe(component.tableStats);
    });

    it("ignores stats without an operator id or without an entry for this operator", () => {
      recreateComponent(undefined);
      statsStream.next([{ op1: { colA: { min: 1 } } }, { op1: { colA: { min: 2 } } }]);
      expect(component.tableStats).toEqual({});

      recreateComponent("op1");
      statsStream.next([{}, {}]);
      expect(component.tableStats).toEqual({});
    });
  });

  describe("checkKeys", () => {
    it("compares the column sets of two stats snapshots", () => {
      expect(component.checkKeys({ a: {}, b: {} }, { a: {}, b: {} })).toBe(true);
      expect(component.checkKeys({ a: {} }, { a: {}, b: {} })).toBe(false);
      expect(component.checkKeys({ a: {}, c: {} }, { a: {}, b: {} })).toBe(false);
    });
  });

  describe("compare", () => {
    const black = (char: string) => `<span style="color: black">${char}</span>`;
    const blue = (char: string) => `<span style="color: blue">${char}</span>`;

    it("highlights digits that changed since the previous stats in blue", () => {
      const bypassSpy = vi.spyOn(TestBed.inject(DomSanitizer), "bypassSecurityTrustHtml");
      component.isOperatorFinished = false;
      component.tableStats = { col: { count: 12 } };
      component.prevTableStats = { col: { count: 15 } };

      component.compare("col", "count");

      expect(bypassSpy).toHaveBeenCalledWith(black("1") + blue("2") + black(".") + black("0") + black("0"));
    });

    it("renders every digit in black once the operator has finished", () => {
      const bypassSpy = vi.spyOn(TestBed.inject(DomSanitizer), "bypassSecurityTrustHtml");
      component.isOperatorFinished = true;
      component.tableStats = { col: { count: 3 } };
      component.prevTableStats = { col: { count: 9 } };

      component.compare("col", "count");

      expect(bypassSpy).toHaveBeenCalledWith(black("3") + black(".") + black("0") + black("0"));
    });

    it("falls back to plain formatting when the previous stat is missing", () => {
      const bypassSpy = vi.spyOn(TestBed.inject(DomSanitizer), "bypassSecurityTrustHtml");
      component.isOperatorFinished = false;
      component.tableStats = { col: { min: 7 } };
      component.prevTableStats = { col: {} };

      component.compare("col", "min");

      expect(bypassSpy).toHaveBeenCalledWith(black("7"));
    });
  });

  describe("panel resizing", () => {
    it("recomputes the page size from the available height", () => {
      component.totalNumTuples = 3;

      resizeService.changePanelSize(800, 700);

      expect(component.panelHeight).toBe(700);
      expect(component.pageSize).toBe(11);
      expect(resizeService.pageSize).toBe(11);
      expect(component.currentPageIndex).toBe(1);
    });

    it("clamps the page index to the last available page when the panel shrinks", () => {
      component.totalNumTuples = 3;
      component.currentPageIndex = 5;
      component.pageSize = 5;

      resizeService.changePanelSize(800, 300);

      expect(component.pageSize).toBe(1);
      expect(component.currentPageIndex).toBe(3);
    });
  });

  describe("onTableQueryParamsChange", () => {
    it("ignores page changes under front-end pagination or without an operator", () => {
      component.isFrontPagination = true;
      component.operatorId = "op1";
      component.onTableQueryParamsChange(queryParams(3));
      expect(component.currentPageIndex).toBe(1);

      component.isFrontPagination = false;
      component.operatorId = undefined;
      component.onTableQueryParamsChange(queryParams(3));
      expect(component.currentPageIndex).toBe(1);
    });

    it("fetches the newly selected page from the paginated result service", () => {
      const paginated = makePaginatedResultService({
        selectPage: vi.fn().mockReturnValue(of(makePageEvent(3))),
      });
      component.operatorId = "op1";
      component.isFrontPagination = false;

      component.onTableQueryParamsChange(queryParams(3));

      expect(component.currentPageIndex).toBe(3);
      expect(paginated.selectPage).toHaveBeenCalledWith(3, component.pageSize, 0, GUI_CONFIG_LIMIT, "");
      expect(component.currentResult).toEqual([SAMPLE_ROW]);
    });
  });

  describe("row detail modal", () => {
    it("opens the modal for the absolute row index with working footer navigation", () => {
      const modalComponent = { rowIndex: 0, ngOnChanges: vi.fn() };
      const modalRef: any = { componentInstance: modalComponent, destroy: vi.fn() };
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue(modalRef);
      component.operatorId = "op1";
      component.currentPageIndex = 2;
      component.pageSize = 5;
      component.totalNumTuples = 10;

      component.open(2, SAMPLE_ROW);

      const config: any = createSpy.mock.calls[0][0];
      expect(config.nzTitle).toBe("Row Details");
      expect(config.nzContent).toBe(RowModalComponent);
      expect(config.nzData).toEqual({ operatorId: "op1", rowIndex: 7 });
      expect(config.nzAutofocus).toBeNull();
      const [prev, next, ok] = config.nzFooter;
      expect([prev.label, next.label, ok.label]).toEqual(["<", ">", "OK"]);

      // "<" steps one row back and re-syncs the table's page index
      modalComponent.rowIndex = 7;
      prev.onClick();
      expect(modalComponent.rowIndex).toBe(6);
      expect(component.currentPageIndex).toBe(2);
      expect(modalComponent.ngOnChanges).toHaveBeenCalledTimes(1);
      expect(prev.disabled()).toBe(false);
      modalComponent.rowIndex = 0;
      expect(prev.disabled()).toBe(true);

      // ">" steps one row forward
      next.onClick();
      expect(modalComponent.rowIndex).toBe(1);
      expect(component.currentPageIndex).toBe(1);
      expect(modalComponent.ngOnChanges).toHaveBeenCalledTimes(2);
      expect(next.disabled()).toBe(false);
      modalComponent.rowIndex = 9;
      expect(next.disabled()).toBe(true);

      expect(ok.type).toBe("primary");
      ok.onClick();
      expect(modalRef.destroy).toHaveBeenCalledTimes(1);

      // navigation becomes a no-op once the modal content is destroyed
      modalRef.componentInstance = null;
      component.currentPageIndex = 4;
      prev.onClick();
      next.onClick();
      expect(component.currentPageIndex).toBe(4);
      expect(prev.disabled()).toBe(false);
      expect(next.disabled()).toBe(false);
    });
  });

  describe("downloadData", () => {
    it("opens the export modal for the clicked cell's absolute row index", () => {
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as any);
      component.currentPageIndex = 2;
      component.pageSize = 5;

      component.downloadData("alice", 1, 3, "name");

      const config: any = createSpy.mock.calls[0][0];
      expect(config.nzTitle).toBe("Export Data and Save to a Dataset");
      expect(config.nzContent).toBe(ResultExportationComponent);
      expect(config.nzFooter).toBeNull();
      expect(config.nzData).toEqual(
        expect.objectContaining({ exportType: "data", defaultFileName: "name_6", rowIndex: 6, columnIndex: 3 })
      );
    });
  });

  describe("column navigation and search", () => {
    it("shifts columns left only when a positive offset exists", () => {
      const refreshSpy = vi.spyOn(component, "changePaginatedResultData").mockImplementation(() => {});

      component.currentColumnOffset = 0;
      component.onColumnShiftLeft();
      expect(component.currentColumnOffset).toBe(0);
      expect(refreshSpy).not.toHaveBeenCalled();

      component.currentColumnOffset = GUI_CONFIG_LIMIT * 2;
      component.onColumnShiftLeft();
      expect(component.currentColumnOffset).toBe(GUI_CONFIG_LIMIT);

      component.currentColumnOffset = 7;
      component.onColumnShiftLeft();
      expect(component.currentColumnOffset).toBe(0);
      expect(refreshSpy).toHaveBeenCalledTimes(2);
    });

    it("shifts columns right only when a full page of columns is shown", () => {
      const refreshSpy = vi.spyOn(component, "changePaginatedResultData").mockImplementation(() => {});

      component.currentColumns = undefined;
      component.onColumnShiftRight();
      expect(component.currentColumnOffset).toBe(0);
      expect(refreshSpy).not.toHaveBeenCalled();

      component.currentColumns = component.generateColumns(
        Array.from({ length: GUI_CONFIG_LIMIT }, (_, i) => ({ columnKey: `c${i}`, columnText: `c${i}` }))
      );
      component.onColumnShiftRight();
      expect(component.currentColumnOffset).toBe(GUI_CONFIG_LIMIT);
      expect(refreshSpy).toHaveBeenCalledTimes(1);

      component.currentColumns = component.generateColumns([{ columnKey: "only", columnText: "only" }]);
      component.onColumnShiftRight();
      expect(component.currentColumnOffset).toBe(GUI_CONFIG_LIMIT);
      expect(refreshSpy).toHaveBeenCalledTimes(1);
    });

    it("typing in the column search resets the offset and refetches", () => {
      const refreshSpy = vi.spyOn(component, "changePaginatedResultData").mockImplementation(() => {});
      component.currentColumnOffset = 30;

      const input: HTMLInputElement = fixture.nativeElement.querySelector("input[nz-input]");
      input.value = "age";
      input.dispatchEvent(new Event("input"));

      expect(component.columnSearch).toBe("age");
      expect(component.currentColumnOffset).toBe(0);
      expect(refreshSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("template rendering", () => {
    it("shows the empty-result hint and hides the table until results arrive", () => {
      expect(fixture.nativeElement.textContent).toContain("Empty result set");
      expect(fixture.nativeElement.querySelector(".result-table").hidden).toBe(true);
    });

    it("renders headers, per-column stats, and clickable row cells once results arrive", () => {
      component.operatorId = "op1";
      component.setupResultTable([SAMPLE_ROW], 1);
      component.isFrontPagination = false;
      component.tableStats = {
        name: {
          min: 1,
          max: 10,
          not_null_count: 5,
          firstPercent: 60,
          secondPercent: 30,
          other: 10,
          reachedLimit: 1,
          firstCat: 1,
          secondCat: 2,
        },
      };
      component.prevTableStats = component.tableStats;
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector(".result-table").hidden).toBe(false);
      expect(fixture.nativeElement.textContent).not.toContain("Empty result set");

      const headers = fixture.debugElement
        .queryAll(By.css("th.header-size"))
        .map(th => th.nativeElement.textContent.trim());
      expect(headers).toEqual(["name", "score"]);

      // stats are rendered only for the column that has them
      const statsText = fixture.nativeElement.querySelector(".custom-stats-row").textContent;
      expect(statsText).toContain("Min");
      expect(statsText).toContain("Max");
      expect(statsText).toContain("Non-Null Count");
      expect(statsText).toContain("Other");
      expect(statsText).toContain("(approximate)");
      expect(fixture.debugElement.queryAll(By.css(".statsLine")).length).toBe(6);

      const openSpy = vi.spyOn(component, "open").mockImplementation(() => {});
      const downloadSpy = vi.spyOn(component, "downloadData").mockImplementation(() => {});

      const cell = fixture.debugElement.query(By.css("td.table-cell"));
      expect(cell.nativeElement.textContent).toContain("alice");
      cell.triggerEventHandler("click", null);
      expect(openSpy).toHaveBeenCalledWith(0, SAMPLE_ROW);

      const download = fixture.debugElement.query(By.css("button.download-button"));
      download.triggerEventHandler("click", { stopPropagation: vi.fn() });
      expect(downloadSpy).toHaveBeenCalledWith("alice", 0, 0, "name");
    });
  });

  it("should detect media URLs for result cells", () => {
    expect(component.isVideoCell("https://example.com/clip.mp4")).toBe(true);
    expect(component.isAudioCell("https://example.com/sound.wav")).toBe(true);
    expect(component.isImageCell("data:image/png;base64,AAAA")).toBe(true);
  });

  it("should reject non-media values for result cells", () => {
    expect(component.isVideoCell("plain text")).toBe(false);
    expect(component.isAudioCell(123 as unknown)).toBe(false);
    expect(component.isImageCell(null as unknown)).toBe(false);
  });

  it("media-type util helpers should classify URLs consistently", () => {
    expect(isVideoUrl("https://example.com/clip.webm")).toBe(true);
    expect(isAudioUrl("https://example.com/track.flac")).toBe(true);
    expect(isImageUrl("https://example.com/image.webp")).toBe(true);
    expect(isVideoUrl("text")).toBe(false);
    expect(isAudioUrl("text")).toBe(false);
    expect(isImageUrl("text")).toBe(false);
  });

  describe("media cell rendering in table", () => {
    beforeEach(() => {
      component.operatorId = "test-op";
    });

    it("should render Play Video indicator for video URL cells", () => {
      (component as any).setupResultTable([{ media: "https://example.com/clip.mp4" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("Play Video");
    });

    it("should render Play Audio indicator for audio URL cells", () => {
      (component as any).setupResultTable([{ media: "https://example.com/clip.mp3" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("Play Audio");
    });

    it("should render View Image indicator for image URL cells", () => {
      (component as any).setupResultTable([{ media: "https://example.com/photo.jpg" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("View Image");
    });

    it("should render plain text for non-media cell values", () => {
      (component as any).setupResultTable([{ label: "just text" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("just text");
    });

    it("should render column headers matching the row keys", () => {
      (component as any).setupResultTable([{ score: "0.95", url: "https://example.com/a.png" }], 1);
      fixture.detectChanges();

      const el = fixture.nativeElement as HTMLElement;
      expect(el.textContent).toContain("score");
      expect(el.textContent).toContain("url");
    });
  });
});
