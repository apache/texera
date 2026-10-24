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
import { Observable, Subject, of } from "rxjs";
import { AgentInteractionComponent } from "./agent-interaction.component";
import { AgentInfo, AgentService, IndexedTuple, Tuple } from "../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

function makeAgent(id: string, name: string = id): AgentInfo {
  return { id, name, modelType: "gpt-test", isBaselineMode: false, createdAt: new Date("2026-01-01T00:00:00Z") };
}

describe("AgentInteractionComponent", () => {
  let fixture: ComponentFixture<AgentInteractionComponent>;
  let component: AgentInteractionComponent;
  let sanitizer: DomSanitizer;
  let agentChangeSubject: Subject<void>;
  let agentService: {
    agentChange$: Observable<void>;
    getAllAgents: ReturnType<typeof vi.fn>;
    getActivelyConnectedAgentIds: ReturnType<typeof vi.fn>;
    isAgentActivelyConnected: ReturnType<typeof vi.fn>;
    sendMessage: ReturnType<typeof vi.fn>;
  };
  let workflowActionService: { getTexeraGraph: ReturnType<typeof vi.fn> };
  let notification: Record<"success" | "error" | "info" | "warning", ReturnType<typeof vi.fn>>;

  function recordToTuple(rec: Record<string, any>): Tuple {
    return {
      schema: { attributes: Object.keys(rec).map(name => ({ attributeName: name, attributeType: "string" })) },
      fields: Object.values(rec),
    };
  }

  function indexedTuple(rowIndex: number, record: Record<string, any>): IndexedTuple {
    return [rowIndex, recordToTuple(record)];
  }

  beforeEach(async () => {
    agentChangeSubject = new Subject<void>();
    agentService = {
      agentChange$: agentChangeSubject.asObservable(),
      getAllAgents: vi.fn().mockReturnValue(of([])),
      getActivelyConnectedAgentIds: vi.fn().mockReturnValue([]),
      isAgentActivelyConnected: vi.fn().mockReturnValue(false),
      sendMessage: vi.fn(),
    };
    workflowActionService = {
      getTexeraGraph: vi.fn().mockReturnValue({ getOperator: vi.fn().mockReturnValue(undefined) }),
    };
    notification = { success: vi.fn(), error: vi.fn(), info: vi.fn(), warning: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [AgentInteractionComponent, HttpClientTestingModule],
      providers: [
        { provide: AgentService, useValue: agentService },
        { provide: WorkflowActionService, useValue: workflowActionService },
        { provide: NotificationService, useValue: notification },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  /**
   * Create the fixture and run the first change detection (which triggers ngOnInit).
   * Mocks such as getAllAgents must be configured before calling this.
   */
  function createComponent(
    inputs: Partial<
      Pick<AgentInteractionComponent, "operatorId" | "operatorDisplayName" | "sampleTuples" | "resultMode">
    > = {}
  ): void {
    fixture = TestBed.createComponent(AgentInteractionComponent);
    component = fixture.componentInstance;
    sanitizer = TestBed.inject(DomSanitizer);
    fixture.componentRef.setInput("operatorId", inputs.operatorId ?? "op-1");
    if (inputs.operatorDisplayName !== undefined) {
      fixture.componentRef.setInput("operatorDisplayName", inputs.operatorDisplayName);
    }
    if (inputs.sampleTuples !== undefined) fixture.componentRef.setInput("sampleTuples", inputs.sampleTuples);
    if (inputs.resultMode !== undefined) fixture.componentRef.setInput("resultMode", inputs.resultMode);
    fixture.detectChanges();
  }

  it("should create", () => {
    createComponent();
    expect(component).toBeTruthy();
  });

  it("binds the @Input properties", () => {
    const tuples = [indexedTuple(0, { a: 1 })];
    const mode = { type: "PaginationMode" } as const;
    createComponent({
      operatorId: "op-7",
      operatorDisplayName: "Filter",
      sampleTuples: tuples,
      resultMode: mode,
    });
    expect(component.operatorId).toBe("op-7");
    expect(component.operatorDisplayName).toBe("Filter");
    expect(component.sampleTuples).toBe(tuples);
    expect(component.resultMode).toBe(mode);
  });

  describe("loadAvailableAgents (ngOnInit)", () => {
    it("maps agents and flags the actively connected ones", () => {
      agentService.getAllAgents.mockReturnValue(of([makeAgent("a1"), makeAgent("a2")]));
      agentService.getActivelyConnectedAgentIds.mockReturnValue(["a2"]);

      createComponent();

      expect(component.availableAgents).toEqual([
        { id: "a1", name: "a1", isConnected: false },
        { id: "a2", name: "a2", isConnected: true },
      ]);
    });

    it("auto-selects the connected agent", () => {
      agentService.getAllAgents.mockReturnValue(of([makeAgent("a1"), makeAgent("a2")]));
      agentService.getActivelyConnectedAgentIds.mockReturnValue(["a2"]);
      createComponent();
      expect(component.selectedAgentId).toBe("a2");
    });

    it("auto-selects the only agent when none is connected", () => {
      agentService.getAllAgents.mockReturnValue(of([makeAgent("solo")]));
      agentService.getActivelyConnectedAgentIds.mockReturnValue([]);
      createComponent();
      expect(component.selectedAgentId).toBe("solo");
    });

    it("leaves the selection empty with multiple disconnected agents", () => {
      agentService.getAllAgents.mockReturnValue(of([makeAgent("a1"), makeAgent("a2")]));
      agentService.getActivelyConnectedAgentIds.mockReturnValue([]);
      createComponent();
      expect(component.selectedAgentId).toBeNull();
    });

    it("reloads the agent list when agentChange$ emits", () => {
      agentService.getAllAgents.mockReturnValue(of([makeAgent("a1")]));
      createComponent();
      expect(component.availableAgents).toHaveLength(1);

      agentService.getAllAgents.mockReturnValue(of([makeAgent("a1"), makeAgent("a2")]));
      agentChangeSubject.next();

      expect(component.availableAgents).toHaveLength(2);
    });
  });

  describe("isSelectedAgentConnected", () => {
    it("returns false when no agent is selected", () => {
      createComponent();
      component.selectedAgentId = null;
      expect(component.isSelectedAgentConnected()).toBe(false);
      expect(agentService.isAgentActivelyConnected).not.toHaveBeenCalled();
    });

    it("delegates to the service for the selected agent", () => {
      createComponent();
      component.selectedAgentId = "a1";
      agentService.isAgentActivelyConnected.mockReturnValue(true);
      expect(component.isSelectedAgentConnected()).toBe(true);
      expect(agentService.isAgentActivelyConnected).toHaveBeenCalledWith("a1");
    });
  });

  describe("canSend", () => {
    it("requires both a selected agent and a non-blank message", () => {
      createComponent();
      component.selectedAgentId = null;
      component.feedbackMessage = "hi";
      expect(component.canSend()).toBe(false);

      component.selectedAgentId = "a1";
      component.feedbackMessage = "   ";
      expect(component.canSend()).toBe(false);

      component.feedbackMessage = "hi";
      expect(component.canSend()).toBe(true);
    });
  });

  describe("sendFeedbackToAgent", () => {
    beforeEach(() => {
      createComponent({ operatorId: "op-1", operatorDisplayName: "MyOp" });
      component.selectedAgentId = "a1";
      component.feedbackMessage = "hello";
      agentService.isAgentActivelyConnected.mockReturnValue(true);
    });

    it("is a no-op when no agent is selected", () => {
      component.selectedAgentId = null;
      component.sendFeedbackToAgent();
      expect(agentService.sendMessage).not.toHaveBeenCalled();
    });

    it("is a no-op when the message is blank", () => {
      component.feedbackMessage = "   ";
      component.sendFeedbackToAgent();
      expect(agentService.sendMessage).not.toHaveBeenCalled();
    });

    it("is a no-op when there is no operatorId", () => {
      component.operatorId = "";
      component.sendFeedbackToAgent();
      expect(agentService.sendMessage).not.toHaveBeenCalled();
    });

    it("errors and does not send when the selected agent is not connected", () => {
      agentService.isAgentActivelyConnected.mockReturnValue(false);
      component.sendFeedbackToAgent();
      expect(notification.error).toHaveBeenCalledWith(
        "Agent is not connected. Please open the agent chat panel first."
      );
      expect(agentService.sendMessage).not.toHaveBeenCalled();
    });

    it("sends the feedback with operator context, notifies, and clears the input", () => {
      component.sendFeedbackToAgent();
      expect(agentService.sendMessage).toHaveBeenCalledWith(
        "a1",
        'Regarding operator "MyOp" (ID: op-1): hello',
        "feedback"
      );
      expect(notification.success).toHaveBeenCalledWith("Message sent to agent successfully");
      expect(component.feedbackMessage).toBe("");
    });

    it("falls back to the graph's custom display name when no display name is provided", () => {
      component.operatorDisplayName = undefined;
      workflowActionService.getTexeraGraph.mockReturnValue({
        getOperator: vi.fn().mockReturnValue({ customDisplayName: "GraphOp" }),
      });
      component.sendFeedbackToAgent();
      expect(agentService.sendMessage).toHaveBeenCalledWith(
        "a1",
        'Regarding operator "GraphOp" (ID: op-1): hello',
        "feedback"
      );
    });

    it("falls back to 'this operator' when no name can be resolved", () => {
      component.operatorDisplayName = undefined;
      workflowActionService.getTexeraGraph.mockImplementation(() => {
        throw new Error("no graph");
      });
      component.sendFeedbackToAgent();
      expect(agentService.sendMessage).toHaveBeenCalledWith(
        "a1",
        'Regarding operator "this operator" (ID: op-1): hello',
        "feedback"
      );
    });
  });

  describe("template rendering", () => {
    beforeEach(() => createComponent());

    it("renders the sample table with a leading Row column when sample tuples are present", () => {
      component.sampleTuples = [indexedTuple(0, { a: 1, b: "x" }), indexedTuple(1, { a: 2, b: "y" })];
      component.resultMode = { type: "PaginationMode" };
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
      component.sampleTuples = [indexedTuple(0, { a: 1, b: "x" }), indexedTuple(5, { a: 2, b: "y" })];
      component.resultMode = { type: "PaginationMode" };
      fixture.detectChanges();

      const ellipsisCell = fixture.nativeElement.querySelector(".ellipsis-row td") as HTMLElement;
      expect(ellipsisCell).toBeTruthy();
      expect(ellipsisCell.textContent?.trim()).toEqual("...");
      // 2 tuple columns + 1 leading "Row" column
      expect(ellipsisCell.getAttribute("colspan")).toEqual("3");
    });

    it("renders the visualization iframe instead of the table in visualization mode", () => {
      fixture.componentRef.setInput("sampleTuples", [indexedTuple(0, { "html-content": "<h1>chart</h1>" })]);
      fixture.componentRef.setInput("resultMode", { type: "SetSnapshotMode" });
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector(".visualization-iframe")).toBeTruthy();
      expect(fixture.nativeElement.querySelector(".sample-records-table")).toBeNull();
    });

    it("renders neither the table nor the iframe when there are no sample tuples", () => {
      expect(fixture.nativeElement.querySelector(".sample-records-table")).toBeNull();
      expect(fixture.nativeElement.querySelector(".visualization-iframe")).toBeNull();
    });
  });

  describe("ngOnChanges - visualization html caching", () => {
    beforeEach(() => createComponent());

    it("caches sanitized html when a visualization tuple carries html-content", () => {
      const spy = vi.spyOn(sanitizer, "bypassSecurityTrustHtml");
      component.sampleTuples = [indexedTuple(0, { "html-content": "<h1>chart</h1>" })];
      component.ngOnChanges({ sampleTuples: new SimpleChange(undefined, component.sampleTuples, true) });

      expect(spy).toHaveBeenCalledWith("<h1>chart</h1>");
      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml("<h1>chart</h1>"));
    });

    it("keeps the cached html when the content is unchanged across calls", () => {
      component.sampleTuples = [indexedTuple(0, { "html-content": "<p>same</p>" })];
      const changes: SimpleChanges = { sampleTuples: new SimpleChange(undefined, component.sampleTuples, true) };

      component.ngOnChanges(changes);
      component.ngOnChanges(changes); // identical html -> unchanged branch, cache reused

      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml("<p>same</p>"));
    });

    it("clears the cached html when no html-content is present", () => {
      component.sampleTuples = [indexedTuple(0, { "html-content": "<p>x</p>" })];
      component.ngOnChanges({ resultMode: new SimpleChange(undefined, { type: "SetSnapshotMode" }, true) });
      // Now switch to a tuple with no html-content.
      component.sampleTuples = [indexedTuple(0, { value: 1 })];
      component.ngOnChanges({ sampleTuples: new SimpleChange(undefined, component.sampleTuples, false) });

      // A cleared cache makes getVisualizationHtml fall through to the empty sanitized
      // fallback; if the cache had survived it would return the cached value instead.
      const spy = vi.spyOn(sanitizer, "bypassSecurityTrustHtml");
      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml(""));
      expect(spy).toHaveBeenCalledWith("");
    });

    it("ignores changes unrelated to sampleTuples/resultMode", () => {
      component.ngOnChanges({ operatorId: new SimpleChange(undefined, "op-1", true) });
      expect(component.getVisualizationHtml()).toEqual(sanitizer.bypassSecurityTrustHtml(""));
    });
  });

  describe("isVisualization", () => {
    beforeEach(() => createComponent());

    it("is true only in snapshot mode", () => {
      component.resultMode = { type: "SetSnapshotMode" };
      expect(component.isVisualization()).toBe(true);

      component.resultMode = { type: "PaginationMode" };
      expect(component.isVisualization()).toBe(false);

      component.resultMode = { type: "SetDeltaMode" };
      expect(component.isVisualization()).toBe(false);
    });

    it("is false when no result mode is set, regardless of sample tuples", () => {
      component.resultMode = undefined;
      component.sampleTuples = [indexedTuple(0, { "html-content": "<p>x</p>" })];
      expect(component.isVisualization()).toBe(false);

      component.sampleTuples = [];
      expect(component.isVisualization()).toBe(false);
    });
  });

  describe("getSampleColumns", () => {
    beforeEach(() => createComponent());

    it("returns the keys of the first tuple", () => {
      component.sampleTuples = [indexedTuple(0, { a: 1, b: 2 })];
      expect(component.getSampleColumns()).toEqual(["a", "b"]);
    });

    it("returns an empty array when there are no sample tuples", () => {
      component.sampleTuples = [];
      expect(component.getSampleColumns()).toEqual([]);

      component.sampleTuples = undefined;
      expect(component.getSampleColumns()).toEqual([]);
    });
  });

  describe("getDisplayTuples", () => {
    beforeEach(() => createComponent());

    it("returns an empty array when there are no sample tuples", () => {
      component.sampleTuples = [];
      expect(component.getDisplayTuples()).toEqual([]);
    });

    it("returns the original tuples without ellipsis when indices are contiguous", () => {
      const firstTuple = recordToTuple({ a: 1 });
      const secondTuple = recordToTuple({ a: 2 });
      component.sampleTuples = [
        [0, firstTuple],
        [1, secondTuple],
      ];

      const entries = component.getDisplayTuples();

      expect(entries).toHaveLength(2);
      expect(entries.every(entry => !entry.isEllipsis)).toBe(true);
      expect(entries[0]).toEqual({ rowIndex: 0, tuple: firstTuple, isEllipsis: false });
      expect(entries[1]).toEqual({ rowIndex: 1, tuple: secondTuple, isEllipsis: false });
    });

    it("inserts an ellipsis marker when there is a gap between row indices", () => {
      component.sampleTuples = [indexedTuple(0, { a: 1 }), indexedTuple(5, { a: 2 })];
      const entries = component.getDisplayTuples();

      expect(entries).toHaveLength(3);
      expect(entries[1].isEllipsis).toBe(true);
      expect(entries[1].tuple).toBeUndefined();
      expect(entries[2].rowIndex).toBe(5);
    });
  });
});
