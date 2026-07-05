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

import { SimpleChange, SimpleChanges } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { DomSanitizer } from "@angular/platform-browser";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { of } from "rxjs";
import { AgentInteractionComponent } from "./agent-interaction.component";
import { AgentService, OperatorResultMode, SampleRow } from "../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("AgentInteractionComponent", () => {
  let fixture: ComponentFixture<AgentInteractionComponent>;
  let component: AgentInteractionComponent;
  let sanitizer: DomSanitizer;

  const agentServiceStub = {
    getAllAgents: () => of([]),
    agentChange$: of(null),
    getActivelyConnectedAgentIds: () => [],
    isAgentActivelyConnected: () => false,
    sendMessage: () => {},
  } as unknown as AgentService;

  function row(rowIndex: number, tuple: Record<string, any>): SampleRow {
    return { rowIndex, tuple };
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [AgentInteractionComponent, HttpClientTestingModule],
      providers: [
        { provide: AgentService, useValue: agentServiceStub },
        { provide: WorkflowActionService, useValue: {} },
        { provide: NotificationService, useValue: { error: vi.fn(), success: vi.fn() } },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AgentInteractionComponent);
    component = fixture.componentInstance;
    sanitizer = TestBed.inject(DomSanitizer);
    fixture.componentRef.setInput("operatorId", "op-1");
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("template rendering", () => {
    it("renders the sample table with a leading Row column when sample rows are present", () => {
      component.sampleTuples = [row(0, { a: 1, b: "x" }), row(1, { a: 2, b: "y" })];
      component.resultMode = OperatorResultMode.TABLE;
      fixture.detectChanges();

      const headers: string[] = Array.from(
        fixture.nativeElement.querySelectorAll(".sample-records-table thead th") as NodeListOf<HTMLElement>
      ).map(th => th.textContent?.trim() ?? "");
      expect(headers).toEqual(["Row", "a", "b"]);

      const firstDataRowCells: string[] = Array.from(
        fixture.nativeElement.querySelectorAll(
          ".sample-records-table tbody tr:first-child td"
        ) as NodeListOf<HTMLElement>
      ).map(td => td.textContent?.trim() ?? "");
      expect(firstDataRowCells).toEqual(["0", "1", "x"]);
    });

    it("renders an ellipsis row spanning all columns plus the Row column when indices have a gap", () => {
      component.sampleTuples = [row(0, { a: 1, b: "x" }), row(5, { a: 2, b: "y" })];
      component.resultMode = OperatorResultMode.TABLE;
      fixture.detectChanges();

      const ellipsisCell = fixture.nativeElement.querySelector(".ellipsis-row td") as HTMLElement;
      expect(ellipsisCell).toBeTruthy();
      expect(ellipsisCell.textContent?.trim()).toEqual("...");
      // 2 tuple columns + 1 leading "Row" column
      expect(ellipsisCell.getAttribute("colspan")).toEqual("3");
    });

    it("renders the visualization iframe instead of the table in visualization mode", () => {
      fixture.componentRef.setInput("sampleTuples", [row(0, { "html-content": "<h1>chart</h1>" })]);
      fixture.componentRef.setInput("resultMode", OperatorResultMode.VISUALIZATION);
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector(".visualization-iframe")).toBeTruthy();
      expect(fixture.nativeElement.querySelector(".sample-records-table")).toBeNull();
    });

    it("renders neither the table nor the iframe when there are no sample rows", () => {
      expect(fixture.nativeElement.querySelector(".sample-records-table")).toBeNull();
      expect(fixture.nativeElement.querySelector(".visualization-iframe")).toBeNull();
    });
  });

  describe("ngOnChanges - visualization html caching", () => {
    it("caches sanitized html when a visualization tuple carries html-content", () => {
      component.sampleTuples = [row(0, { "html-content": "<h1>chart</h1>" })];
      component.ngOnChanges({ sampleTuples: new SimpleChange(undefined, component.sampleTuples, true) });

      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml("<h1>chart</h1>"));
    });

    it("keeps the cached html when the content is unchanged across calls", () => {
      component.sampleTuples = [row(0, { "html-content": "<p>same</p>" })];
      const changes: SimpleChanges = { sampleTuples: new SimpleChange(undefined, component.sampleTuples, true) };

      component.ngOnChanges(changes);
      component.ngOnChanges(changes); // identical html -> unchanged branch, cache reused

      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml("<p>same</p>"));
    });

    it("clears the cached html when no html-content is present", () => {
      component.sampleTuples = [row(0, { "html-content": "<p>x</p>" })];
      component.ngOnChanges({ resultMode: new SimpleChange(undefined, OperatorResultMode.VISUALIZATION, true) });
      // Now switch to a tuple with no html-content.
      component.sampleTuples = [row(0, { value: 1 })];
      component.ngOnChanges({ sampleTuples: new SimpleChange(undefined, component.sampleTuples, false) });

      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml(""));
    });

    it("ignores changes unrelated to sampleTuples/resultMode", () => {
      component.ngOnChanges({ operatorId: new SimpleChange(undefined, "op-1", true) });
      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml(""));
    });
  });

  describe("isVisualization", () => {
    it("is true only in visualization mode", () => {
      component.resultMode = OperatorResultMode.VISUALIZATION;
      expect(component.isVisualization()).toBe(true);

      component.resultMode = OperatorResultMode.TABLE;
      expect(component.isVisualization()).toBe(false);
    });
  });

  describe("getSampleColumns", () => {
    it("returns the keys of the first tuple", () => {
      component.sampleTuples = [row(0, { a: 1, b: 2 })];
      expect(component.getSampleColumns()).toEqual(["a", "b"]);
    });

    it("returns an empty array when there are no sample tuples", () => {
      component.sampleTuples = [];
      expect(component.getSampleColumns()).toEqual([]);

      component.sampleTuples = undefined;
      expect(component.getSampleColumns()).toEqual([]);
    });

    it("maps a column header to its own name", () => {
      expect(component.getColumnDisplayName("colX")).toBe("colX");
    });
  });

  describe("getDisplayRows", () => {
    it("returns an empty array when there are no sample tuples", () => {
      component.sampleTuples = [];
      expect(component.getDisplayRows()).toEqual([]);
    });

    it("returns rows without ellipsis when indices are contiguous", () => {
      component.sampleTuples = [row(0, { a: 1 }), row(1, { a: 2 })];
      const rows = component.getDisplayRows();
      expect(rows.length).toBe(2);
      expect(rows.every(r => !r.isEllipsis)).toBe(true);
    });

    it("inserts an ellipsis marker when there is a gap between row indices", () => {
      component.sampleTuples = [row(0, { a: 1 }), row(5, { a: 2 })];
      const rows = component.getDisplayRows();

      expect(rows.length).toBe(3);
      expect(rows[1].isEllipsis).toBe(true);
      expect(rows[1].row).toBeUndefined();
      expect(rows[2].row?.rowIndex).toBe(5);
    });
  });
});
