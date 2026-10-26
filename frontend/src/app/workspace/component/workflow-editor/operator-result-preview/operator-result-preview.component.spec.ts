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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { By } from "@angular/platform-browser";
import { NzModalModule } from "ng-zorro-antd/modal";

import { NEVER, of } from "rxjs";
import {
  formatPreviewCell,
  OperatorResultPreviewComponent,
  PREVIEW_CELL_LIMIT,
  PREVIEW_COLUMN_LIMIT,
  PREVIEW_ROW_LIMIT,
} from "./operator-result-preview.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../../service/workflow-result/workflow-result.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { mockPoint, mockScanPredicate } from "../../../service/workflow-graph/model/mock-workflow-data";
import { ExecutionState } from "../../../types/execute-workflow.interface";
import { ConsoleMessage } from "../../../types/workflow-common.interface";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { AttributeType } from "../../../types/workflow-compiling.interface";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";
import { OperatorPaginationResultService } from "../../../service/workflow-result/workflow-result.service";

describe("OperatorResultPreviewComponent", () => {
  let component: OperatorResultPreviewComponent;
  let fixture: ComponentFixture<OperatorResultPreviewComponent>;
  let resultService: WorkflowResultService;
  let compilingService: WorkflowCompilingService;
  let executeService: ExecuteWorkflowService;

  const id = mockScanPredicate.operatorID;

  const fatalError = (operatorId: string): WorkflowFatalError => ({
    message: "KeyError: 'price'",
    details: "",
    operatorId,
    workerId: "",
    type: { name: "ExecutionError" },
    timestamp: { nanos: 0, seconds: 0 },
  });

  const withColumns = (count: number) =>
    vi.spyOn(compilingService, "getOperatorOutputSchemaMap").mockReturnValue({
      "output-0": Array.from({ length: count }, (_, i) => ({
        attributeName: `col${i}`,
        attributeType: "string" as AttributeType,
      })),
    });

  // A paginated result of `rows` tuples whose first page is `page`; `page` undefined never answers.
  const withRows = (rows: number, page?: Record<string, unknown>[]) => {
    const peekFirstRows = vi.fn(() => (page ? of({ table: page }) : NEVER));
    vi.spyOn(resultService, "getPaginatedResultService").mockReturnValue({
      getCurrentTotalNumTuples: () => rows,
      peekFirstRows,
    } as unknown as OperatorPaginationResultService);
    return peekFirstRows;
  };

  // Set through the input, as the canvas does, so ngOnChanges loads the rows.
  const hover = (operatorId: string) => {
    fixture.componentRef.setInput("operatorId", operatorId);
    fixture.detectChanges();
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [OperatorResultPreviewComponent, HttpClientTestingModule, NzModalModule],
      providers: [
        WorkflowActionService,
        ExecuteWorkflowService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(OperatorResultPreviewComponent);
    component = fixture.componentInstance;
    resultService = TestBed.inject(WorkflowResultService);
    compilingService = TestBed.inject(WorkflowCompilingService);
    executeService = TestBed.inject(ExecuteWorkflowService);
    TestBed.inject(WorkflowActionService).addOperator({ ...mockScanPredicate, customDisplayName: "Orders" }, mockPoint);
    component.operatorId = id;
  });

  afterEach(() => {
    vi.restoreAllMocks();
    fixture.destroy();
  });

  it("shows a table result's size and its columns", () => {
    withRows(1240);
    withColumns(3);

    expect(component.preview).toEqual({
      title: "Orders",
      kind: "table",
      summary: "1,240 rows · 3 columns",
      columns: ["col0", "col1", "col2"],
      moreColumns: 0,
    });
  });

  it("lists only the first columns of a wide result and counts the rest", () => {
    withRows(1);
    withColumns(PREVIEW_COLUMN_LIMIT + 4);

    const preview = component.preview;
    expect(preview.summary).toBe(`1 row · ${PREVIEW_COLUMN_LIMIT + 4} columns`);
    expect(preview.columns.length).toBe(PREVIEW_COLUMN_LIMIT);
    expect(preview.moreColumns).toBe(4);
  });

  it("says the rows are the first output's when the operator has several", () => {
    const split = "split-op";
    TestBed.inject(WorkflowActionService).addOperator(
      {
        ...mockScanPredicate,
        operatorID: split,
        outputPorts: [{ portID: "output-0" }, { portID: "output-1" }],
      },
      mockPoint
    );
    component.operatorId = split;
    withRows(10);
    withColumns(2);

    expect(component.preview.summary).toBe("10 rows · 2 columns · first of 2 outputs");
  });

  it("says a chart was drawn for a result that is not a table", () => {
    vi.spyOn(resultService, "hasResult").mockReturnValue(true);

    expect(component.preview.kind).toBe("chart");
  });

  it("shows the operator's error ahead of any result", () => {
    withRows(5);
    vi.spyOn(executeService, "getErrorMessages").mockReturnValue([fatalError(id)]);

    expect(component.preview).toMatchObject({ kind: "error", summary: "failed", detail: "KeyError: 'price'" });
  });

  // A bad tuple pauses the worker and writes an ERROR to the console rather than failing the run.
  it("shows the error an operator wrote to its console", () => {
    withRows(5);
    vi.spyOn(TestBed.inject(WorkflowConsoleService), "getConsoleMessages").mockReturnValue([
      { msgType: { name: "PRINT" }, title: "hello" },
      { msgType: { name: "ERROR" }, title: 'NumberFormatException: For input string: "a"' },
    ] as unknown as ConsoleMessage[]);

    expect(component.preview).toMatchObject({
      kind: "error",
      summary: "failed",
      detail: 'NumberFormatException: For input string: "a"',
    });
  });

  it("says to run the workflow when nothing has run yet", () => {
    expect(component.preview).toMatchObject({ kind: "none", detail: "Run the workflow to see its result." });
  });

  it("says how to keep the result when the run did not keep one", () => {
    vi.spyOn(executeService, "getExecutionState").mockReturnValue({ state: ExecutionState.Completed });

    expect(component.preview.kind).toBe("none");
    expect(component.preview.detail).toContain("Turn on its eye icon");
  });

  it("renders the first rows under the first columns, and counts the columns left out", () => {
    withRows(2, [
      { col0: "a", col1: 1, col6: "hidden" },
      { col0: null, col1: { nested: true }, col6: "hidden" },
    ]);
    withColumns(PREVIEW_COLUMN_LIMIT + 1);
    hover(id);

    const text = (css: string) => fixture.debugElement.query(By.css(css)).nativeElement.textContent.trim();
    const cells = (css: string) =>
      fixture.debugElement.queryAll(By.css(css)).map(e => e.nativeElement.textContent.trim());
    expect(text(".result-preview__title")).toBe("Orders");
    expect(text(".result-preview__summary")).toBe(`2 rows · ${PREVIEW_COLUMN_LIMIT + 1} columns`);
    expect(cells("th").length).toBe(PREVIEW_COLUMN_LIMIT);
    expect(fixture.debugElement.queryAll(By.css("tbody tr")).length).toBe(2);
    expect(cells("tbody tr:first-child td").slice(0, 2)).toEqual(["a", "1"]);
    expect(cells("tbody tr:nth-child(2) td").slice(0, 2)).toEqual(["", '{"nested":true}']);
    expect(text(".result-preview__note")).toBe("+1 more column");
    expect(fixture.debugElement.query(By.css(".result-preview__detail"))).toBeNull();
  });

  it("asks for the first rows only, and shows that they are loading until they come", () => {
    const peek = withRows(10);
    withColumns(2);
    hover(id);

    expect(peek).toHaveBeenCalledWith(PREVIEW_ROW_LIMIT);
    expect(component.rows).toBeUndefined();
    expect(fixture.debugElement.query(By.css(".result-preview__note")).nativeElement.textContent.trim()).toBe(
      "Loading rows…"
    );
  });

  it("asks for no rows when the result has none", () => {
    const peek = withRows(0);
    withColumns(2);
    hover(id);

    expect(peek).not.toHaveBeenCalled();
    expect(component.rows).toEqual([]);
  });

  it("prints a cell short, blank for null, and as JSON for an object", () => {
    expect(formatPreviewCell(null)).toBe("");
    expect(formatPreviewCell(undefined)).toBe("");
    expect(formatPreviewCell(3.5)).toBe("3.5");
    expect(formatPreviewCell({ a: 1 })).toBe('{"a":1}');
    const long = "x".repeat(PREVIEW_CELL_LIMIT + 10);
    expect(formatPreviewCell(long)).toBe("x".repeat(PREVIEW_CELL_LIMIT - 1) + "…");
  });
});
