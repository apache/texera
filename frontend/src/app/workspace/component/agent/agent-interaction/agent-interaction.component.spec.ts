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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { DomSanitizer } from "@angular/platform-browser";
import { Subject, of } from "rxjs";
import { AgentInteractionComponent } from "./agent-interaction.component";
import { AgentInfo, AgentService } from "../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

function makeAgent(id: string, name: string = id): AgentInfo {
  return { id, name, modelType: "gpt-test", isBaselineMode: false, createdAt: new Date("2026-01-01T00:00:00Z") };
}

describe("AgentInteractionComponent", () => {
  let fixture: ComponentFixture<AgentInteractionComponent>;
  let component: AgentInteractionComponent;
  let agentChangeSubject: Subject<void>;
  let agentService: {
    agentChange$: ReturnType<Subject<void>["asObservable"]>;
    getAllAgents: ReturnType<typeof vi.fn>;
    getActivelyConnectedAgentIds: ReturnType<typeof vi.fn>;
    isAgentActivelyConnected: ReturnType<typeof vi.fn>;
    sendMessage: ReturnType<typeof vi.fn>;
  };
  let workflowActionService: { getTexeraGraph: ReturnType<typeof vi.fn> };
  let notification: Record<"success" | "error" | "info" | "warning", ReturnType<typeof vi.fn>>;

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
      imports: [AgentInteractionComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        { provide: AgentService, useValue: agentService },
        { provide: WorkflowActionService, useValue: workflowActionService },
        { provide: NotificationService, useValue: notification },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  function createComponent(
    inputs: Partial<
      Pick<AgentInteractionComponent, "operatorId" | "operatorDisplayName" | "sampleRecords" | "resultStatistics">
    > = {}
  ): void {
    fixture = TestBed.createComponent(AgentInteractionComponent);
    component = fixture.componentInstance;
    component.operatorId = inputs.operatorId ?? "op-1";
    if (inputs.operatorDisplayName !== undefined) component.operatorDisplayName = inputs.operatorDisplayName;
    if (inputs.sampleRecords !== undefined) component.sampleRecords = inputs.sampleRecords;
    if (inputs.resultStatistics !== undefined) component.resultStatistics = inputs.resultStatistics;
    fixture.detectChanges();
  }

  it("should create", () => {
    createComponent();
    expect(component).toBeTruthy();
  });

  it("binds the @Input properties", () => {
    const records = [{ a: 1 }];
    const stats = { a: "{}" };
    createComponent({
      operatorId: "op-7",
      operatorDisplayName: "Filter",
      sampleRecords: records,
      resultStatistics: stats,
    });
    expect(component.operatorId).toBe("op-7");
    expect(component.operatorDisplayName).toBe("Filter");
    expect(component.sampleRecords).toBe(records);
    expect(component.resultStatistics).toBe(stats);
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

  describe("sample-record helpers", () => {
    it("detects a visualization record", () => {
      createComponent({ sampleRecords: [{ __is_visualization__: true, "html-content": "<p>x</p>" }] });
      expect(component.isVisualization()).toBe(true);
    });

    it("reports non-visualization and empty records as not a visualization", () => {
      createComponent({ sampleRecords: [{ a: 1 }] });
      expect(component.isVisualization()).toBe(false);
      component.sampleRecords = [];
      expect(component.isVisualization()).toBe(false);
    });

    it("lists columns with the row-index column first and maps its display name to 'Row'", () => {
      createComponent({ sampleRecords: [{ __row_index__: 0, a: 1, b: 2 }] });
      expect(component.getSampleColumns()).toEqual(["__row_index__", "a", "b"]);
      expect(component.getColumnDisplayName("__row_index__")).toBe("Row");
      expect(component.getColumnDisplayName("a")).toBe("a");
    });

    it("inserts an ellipsis row where the row index skips ahead", () => {
      createComponent({
        sampleRecords: [
          { __row_index__: 0, a: 1 },
          { __row_index__: 5, a: 2 },
        ],
      });
      const rows = component.getDisplayRows();
      expect(rows.map(r => r.isEllipsis)).toEqual([false, true, false]);
      expect(rows[0].record).toEqual({ __row_index__: 0, a: 1 });
      expect(rows[2].record).toEqual({ __row_index__: 5, a: 2 });
    });
  });

  describe("column statistics", () => {
    const stats = {
      score: JSON.stringify({
        data_type: "integer",
        statistics: { min: 1, max: 10, mean: 5.5, count: 100 },
      }),
    };

    it("parses resultStatistics and excludes noisy keys", () => {
      createComponent({ resultStatistics: stats });
      const parsed = component.getParsedColumnStats();
      expect(parsed).toHaveLength(1);
      expect(parsed[0].column).toBe("score");
      expect(parsed[0].dataType).toBe("integer");
      const keys = parsed[0].stats.map(s => s.key);
      expect(keys).toContain("min");
      expect(keys).toContain("max");
      expect(keys).not.toContain("count");
    });

    it("reports whether any column stats are present", () => {
      createComponent({ resultStatistics: stats });
      expect(component.hasColumnStats()).toBe(true);

      createComponent();
      expect(component.hasColumnStats()).toBe(false);
    });
  });

  describe("ngOnChanges visualization caching", () => {
    function sampleRecordsChange(currentValue: Record<string, any>[] | undefined): SimpleChanges {
      return { sampleRecords: new SimpleChange(undefined, currentValue, true) };
    }

    it("sanitizes and caches the html-content when sampleRecords change", () => {
      createComponent();
      const sanitizer = TestBed.inject(DomSanitizer);
      const spy = vi.spyOn(sanitizer, "bypassSecurityTrustHtml");

      component.ngOnChanges(sampleRecordsChange([{ "html-content": "<p>hi</p>" }]));

      expect(spy).toHaveBeenCalledWith("<p>hi</p>");
      expect(component.getVisualizationHtml()).toBeTruthy();
    });

    it("clears the cached html when the new records carry no html-content", () => {
      createComponent();
      component.ngOnChanges(sampleRecordsChange([{ "html-content": "<p>hi</p>" }]));
      component.ngOnChanges(sampleRecordsChange([{ a: 1 }]));

      // A cleared cache makes getVisualizationHtml fall through to the empty sanitized
      // fallback; if the cache had survived it would return the cached value instead.
      const sanitizer = TestBed.inject(DomSanitizer);
      const spy = vi.spyOn(sanitizer, "bypassSecurityTrustHtml");
      component.getVisualizationHtml();
      expect(spy).toHaveBeenCalledWith("");
    });
  });
});
