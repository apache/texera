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

import { ApplicationRef } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NzModalService } from "ng-zorro-antd/modal";
import { MarkdownModule } from "ngx-markdown";
import { BehaviorSubject, Observable, Subject, of, throwError } from "rxjs";
import { AgentChatComponent } from "./agent-chat.component";
import { AgentInfo, AgentService, AgentSettingsApi } from "../../../../service/agent/agent.service";
import { AgentState, ReActStep, ToolOperatorAccess } from "../../../../service/agent/agent-types";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../../../common/service/workflow-persist/workflow-persist.service";
import { Workflow } from "../../../../../common/type/workflow";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

const AGENT_ID = "agent-1";

/**
 * Subject-backed double of AgentService. The component's ngOnInit subscribes to
 * the state / steps / HEAD / workflow streams, so each test drives behavior by
 * calling `next(...)` on the corresponding subject — mirroring how the real
 * service pushes WebSocket updates through BehaviorSubjects.
 */
class MockAgentService {
  public stateSubject = new BehaviorSubject<AgentState>(AgentState.AVAILABLE);
  public stepsSubject = new BehaviorSubject<ReActStep[]>([]);
  public headIdSubject = new BehaviorSubject<string | null>(null);
  public workflowSubject = new BehaviorSubject<Workflow | null>(null);
  public scrollToStepSubject = new Subject<{ agentId: string; messageId: string; stepId: number }>();
  public scrollToStep$ = this.scrollToStepSubject.asObservable();

  public ensureWorkflowPolling = vi.fn();
  public getAgentState = vi.fn((): Observable<AgentState> => of(this.stateSubject.getValue()));
  public getAgentStateObservable = vi.fn((): Observable<AgentState> => this.stateSubject.asObservable());
  public getReActStepsObservable = vi.fn((): Observable<ReActStep[]> => this.stepsSubject.asObservable());
  public getHeadIdObservable = vi.fn((): Observable<string | null> => this.headIdSubject.asObservable());
  public getWorkflowObservable = vi.fn((): Observable<Workflow | null> => this.workflowSubject.asObservable());
  public setHoveredMessage = vi.fn();
  public sendMessage = vi.fn();
  public stopGeneration = vi.fn();
  public clearMessages = vi.fn();
  public getReActSteps = vi.fn((): Observable<ReActStep[]> => of([]));
  public getSystemInfo = vi.fn(
    (): Observable<{
      systemPrompt: string;
      tools: Array<{ name: string; description: string; inputSchema: any; enabled: boolean }>;
    }> => of({ systemPrompt: "", tools: [] })
  );
  public getAgentSettings = vi.fn((): Observable<AgentSettingsApi> => of({}));
  public getAvailableOperatorTypes = vi.fn((): Observable<Array<{ type: string; description: string }>> => of([]));
  public updateAgentSettings = vi.fn(
    (_agentId: string, settings: Partial<AgentSettingsApi>): Observable<AgentSettingsApi> =>
      of(settings as AgentSettingsApi)
  );
}

function makeAgentInfo(overrides: Partial<AgentInfo> = {}): AgentInfo {
  return {
    id: AGENT_ID,
    name: "Test Agent",
    modelType: "gpt-test",
    isBaselineMode: false,
    createdAt: new Date("2026-01-01T00:00:00Z"),
    ...overrides,
  };
}

function makeStep(overrides: Partial<ReActStep> = {}): ReActStep {
  const messageId = overrides.messageId ?? "m1";
  const stepId = overrides.stepId ?? 0;
  return {
    messageId,
    stepId,
    timestamp: new Date("2026-01-01T00:00:00Z"),
    role: "agent",
    content: "step content",
    isBegin: false,
    isEnd: false,
    id: `${messageId}-${stepId}`,
    ...overrides,
  };
}

describe("AgentChatComponent", () => {
  let fixture: ComponentFixture<AgentChatComponent>;
  let component: AgentChatComponent;
  let agentService: MockAgentService;
  let reloadWorkflow: ReturnType<typeof vi.fn>;
  let notification: Record<"success" | "error" | "warning" | "info", ReturnType<typeof vi.fn>>;
  let persist: { setWorkflowPersistFlag: ReturnType<typeof vi.fn> };
  let createObjectURL: ReturnType<typeof vi.fn>;
  let revokeObjectURL: ReturnType<typeof vi.fn>;
  let scrollIntoViewMock: ReturnType<typeof vi.fn>;
  // Originals of the globals we overwrite below, captured so afterEach can restore them
  // (direct assignment is not undone by vi.restoreAllMocks, so they would leak across spec files).
  let origScrollIntoView: typeof Element.prototype.scrollIntoView;
  let origCreateObjectURL: typeof URL.createObjectURL;
  let origRevokeObjectURL: typeof URL.revokeObjectURL;

  beforeEach(async () => {
    // jsdom gaps: Element#scrollIntoView and URL.createObjectURL/revokeObjectURL
    // are not implemented; the component calls them from scrollToMessage and
    // exportReActSteps.
    origScrollIntoView = Element.prototype.scrollIntoView;
    origCreateObjectURL = URL.createObjectURL;
    origRevokeObjectURL = URL.revokeObjectURL;
    scrollIntoViewMock = vi.fn();
    (Element.prototype as any).scrollIntoView = scrollIntoViewMock;
    createObjectURL = vi.fn(() => "blob:mock-url");
    revokeObjectURL = vi.fn();
    (URL as any).createObjectURL = createObjectURL;
    (URL as any).revokeObjectURL = revokeObjectURL;

    agentService = new MockAgentService();
    reloadWorkflow = vi.fn();
    notification = { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() };
    persist = { setWorkflowPersistFlag: vi.fn() };

    await TestBed.configureTestingModule({
      // MarkdownModule.forRoot() backs the <markdown> elements in the chat
      // bubbles and the system-prompt tab (same wiring as AppModule).
      imports: [AgentChatComponent, HttpClientTestingModule, NoopAnimationsModule, MarkdownModule.forRoot()],
      providers: [
        // The declarative <nz-modal> delegates opening to NzModalService.create(),
        // so the real service is required to render the system-info modal.
        NzModalService,
        { provide: AgentService, useValue: agentService },
        { provide: WorkflowActionService, useValue: { reloadWorkflow } },
        { provide: NotificationService, useValue: notification },
        { provide: WorkflowPersistService, useValue: persist },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  afterEach(() => {
    // Some tests destroy the fixture themselves; guard against a double-destroy.
    try {
      fixture?.destroy();
    } catch {
      // already destroyed
    }
    fixture = undefined as unknown as ComponentFixture<AgentChatComponent>;
    // The system-info modal renders into the CDK overlay attached to document.body.
    // Clear its contents (rather than removing the container) so document-level queries
    // stay test-local without detaching the element CDK's OverlayContainer caches.
    document.querySelectorAll(".cdk-overlay-container").forEach(el => (el.innerHTML = ""));
    // Restore the globals overwritten by direct assignment in beforeEach.
    Element.prototype.scrollIntoView = origScrollIntoView;
    URL.createObjectURL = origCreateObjectURL;
    URL.revokeObjectURL = origRevokeObjectURL;
    vi.restoreAllMocks();
  });

  function createComponent(agentInfo: AgentInfo = makeAgentInfo(), isActive = false): AgentChatComponent {
    fixture = TestBed.createComponent(AgentChatComponent);
    component = fixture.componentInstance;
    fixture.componentRef.setInput("agentInfo", agentInfo);
    fixture.componentRef.setInput("isActive", isActive);
    fixture.detectChanges();
    return component;
  }

  describe("ngOnInit workflow polling", () => {
    it("ensures workflow polling when the agent delegate carries a workflowId", () => {
      createComponent(
        makeAgentInfo({
          delegate: {
            userInfo: { uid: 1, name: "user", email: "user@example.com", role: "REGULAR" },
            workflowId: 42,
          },
        })
      );
      expect(agentService.ensureWorkflowPolling).toHaveBeenCalledWith(AGENT_ID, 42);
    });

    it("does not start polling when there is no delegate", () => {
      createComponent();
      expect(agentService.ensureWorkflowPolling).not.toHaveBeenCalled();
    });
  });

  describe("auto-persist toggling on state transitions", () => {
    it("disables persistence when generation starts and re-enables when it finishes", () => {
      createComponent();
      // Initial AVAILABLE (paired with the startWith(UNAVAILABLE) seed) must not touch the flag.
      expect(persist.setWorkflowPersistFlag).not.toHaveBeenCalled();

      agentService.stateSubject.next(AgentState.GENERATING);
      expect(persist.setWorkflowPersistFlag).toHaveBeenNthCalledWith(1, false);

      // A repeated GENERATING emission must not re-disable.
      agentService.stateSubject.next(AgentState.GENERATING);
      expect(persist.setWorkflowPersistFlag).toHaveBeenCalledTimes(1);

      agentService.stateSubject.next(AgentState.AVAILABLE);
      expect(persist.setWorkflowPersistFlag).toHaveBeenNthCalledWith(2, true);
      expect(persist.setWorkflowPersistFlag).toHaveBeenCalledTimes(2);
    });

    it("re-enables persistence when STOPPING resolves to AVAILABLE", () => {
      createComponent();
      agentService.stateSubject.next(AgentState.GENERATING);
      agentService.stateSubject.next(AgentState.STOPPING);
      expect(persist.setWorkflowPersistFlag).toHaveBeenCalledTimes(1); // only the disable so far

      agentService.stateSubject.next(AgentState.AVAILABLE);
      expect(persist.setWorkflowPersistFlag).toHaveBeenLastCalledWith(true);
    });

    it("re-enables persistence on destroy if it was left disabled", () => {
      createComponent();
      agentService.stateSubject.next(AgentState.GENERATING);
      expect(persist.setWorkflowPersistFlag).toHaveBeenCalledWith(false);

      fixture.destroy();
      expect(persist.setWorkflowPersistFlag).toHaveBeenLastCalledWith(true);
      expect(persist.setWorkflowPersistFlag).toHaveBeenCalledTimes(2);
    });

    it("does not touch the flag on destroy when it was never disabled", () => {
      createComponent();
      fixture.destroy();
      expect(persist.setWorkflowPersistFlag).not.toHaveBeenCalled();
    });
  });

  describe("step subscription and hovered-message tracking", () => {
    it("auto-advances the hover highlight to the latest step on new emissions", () => {
      createComponent();
      const steps = [makeStep({ stepId: 0 }), makeStep({ stepId: 1 }), makeStep({ stepId: 2 })];
      agentService.stepsSubject.next(steps);

      expect(component.agentResponses).toEqual(steps);
      expect(component.hoveredMessageIndex).toBe(2);
      expect(agentService.setHoveredMessage).toHaveBeenLastCalledWith(AGENT_ID, steps[2]);
    });

    it("keeps a manually hovered non-latest step across the next emission", () => {
      createComponent();
      const steps = [makeStep({ stepId: 0 }), makeStep({ stepId: 1 }), makeStep({ stepId: 2 })];
      agentService.stepsSubject.next(steps);

      component.setHoveredMessage(0);
      expect(agentService.setHoveredMessage).toHaveBeenLastCalledWith(AGENT_ID, steps[0]);

      agentService.stepsSubject.next([...steps, makeStep({ stepId: 3 })]);
      expect(component.hoveredMessageIndex).toBe(0);
    });

    it("advances a hover that was sitting on the previous latest step", () => {
      createComponent();
      const steps = [makeStep({ stepId: 0 }), makeStep({ stepId: 1 })];
      agentService.stepsSubject.next(steps);
      expect(component.hoveredMessageIndex).toBe(1);

      const more = [...steps, makeStep({ stepId: 2 })];
      agentService.stepsSubject.next(more);
      expect(component.hoveredMessageIndex).toBe(2);
      expect(agentService.setHoveredMessage).toHaveBeenLastCalledWith(AGENT_ID, more[2]);
    });

    it("reverts to the latest step when unhovered (null)", () => {
      createComponent();
      const steps = [makeStep({ stepId: 0 }), makeStep({ stepId: 1 }), makeStep({ stepId: 2 })];
      agentService.stepsSubject.next(steps);
      component.setHoveredMessage(0);

      component.setHoveredMessage(null);
      expect(component.hoveredMessageIndex).toBe(2);
      expect(agentService.setHoveredMessage).toHaveBeenLastCalledWith(AGENT_ID, steps[2]);
    });
  });

  describe("HEAD-path filtering of visible steps", () => {
    it("shows only the root-to-HEAD ancestor chain when a HEAD is set", () => {
      createComponent();
      const a = makeStep({ messageId: "m1", stepId: 0, id: "a" });
      const b = makeStep({ messageId: "m1", stepId: 1, id: "b", parentId: "a" });
      const c = makeStep({ messageId: "m1", stepId: 2, id: "c", parentId: "b" });
      const d = makeStep({ messageId: "m2", stepId: 0, id: "d", parentId: "a" }); // branch off "a"
      agentService.stepsSubject.next([a, b, c, d]);

      // No HEAD yet: everything is visible.
      expect(component.visibleSteps.map(s => s.id)).toEqual(["a", "b", "c", "d"]);

      // HEAD at mid-chain "b": only the a -> b path remains.
      agentService.headIdSubject.next("b");
      expect(component.visibleSteps.map(s => s.id)).toEqual(["a", "b"]);

      // Clearing HEAD restores all steps.
      agentService.headIdSubject.next(null);
      expect(component.visibleSteps.map(s => s.id)).toEqual(["a", "b", "c", "d"]);
    });
  });

  describe("workflow reload while the tab is active", () => {
    const wfA = { wid: 42, name: "wf", content: { operators: [{ operatorID: "op-1" }] } } as unknown as Workflow;

    it("reloads on activation, deduplicates identical content, and stops when deactivated", () => {
      createComponent(makeAgentInfo(), false);

      // Inactive: workflow emissions are ignored.
      agentService.workflowSubject.next(wfA);
      expect(reloadWorkflow).not.toHaveBeenCalled();

      // Activating subscribes and replays the latest workflow.
      fixture.componentRef.setInput("isActive", true);
      fixture.detectChanges();
      expect(reloadWorkflow).toHaveBeenCalledTimes(1);
      expect(reloadWorkflow).toHaveBeenCalledWith(wfA, false, false);

      // A different object with JSON-identical content is suppressed.
      agentService.workflowSubject.next(JSON.parse(JSON.stringify(wfA)) as Workflow);
      expect(reloadWorkflow).toHaveBeenCalledTimes(1);

      // Content change goes through.
      const wfB = { ...wfA, content: { operators: [] } } as unknown as Workflow;
      agentService.workflowSubject.next(wfB);
      expect(reloadWorkflow).toHaveBeenCalledTimes(2);

      // Deactivating stops the subscription; later emissions do not reload.
      fixture.componentRef.setInput("isActive", false);
      fixture.detectChanges();
      agentService.workflowSubject.next({
        ...wfA,
        content: { operators: [{ operatorID: "op-2" }] },
      } as unknown as Workflow);
      expect(reloadWorkflow).toHaveBeenCalledTimes(2);
    });
  });

  describe("sendMessage / onEnterPress", () => {
    it("ignores whitespace-only input", () => {
      createComponent();
      component.currentMessage = "   ";
      component.sendMessage();
      expect(agentService.sendMessage).not.toHaveBeenCalled();
      expect(component.currentMessage).toBe("   ");
    });

    it("does not send while the agent is not AVAILABLE", () => {
      createComponent();
      agentService.stateSubject.next(AgentState.GENERATING);
      component.currentMessage = "hello";
      component.sendMessage();
      expect(agentService.sendMessage).not.toHaveBeenCalled();
      expect(component.currentMessage).toBe("hello");
    });

    it("sends the trimmed text and clears the input", () => {
      createComponent();
      component.currentMessage = "  hello world  ";
      component.sendMessage();
      expect(agentService.sendMessage).toHaveBeenCalledWith(AGENT_ID, "hello world");
      expect(component.currentMessage).toBe("");
    });

    it("Enter sends the message and prevents the default newline", () => {
      createComponent();
      component.currentMessage = "hi";
      const event = new KeyboardEvent("keydown", { key: "Enter" });
      const preventDefault = vi.spyOn(event, "preventDefault");
      component.onEnterPress(event);
      expect(preventDefault).toHaveBeenCalled();
      expect(agentService.sendMessage).toHaveBeenCalledWith(AGENT_ID, "hi");
    });

    it("Shift+Enter inserts a newline instead of sending", () => {
      createComponent();
      component.currentMessage = "hi";
      const event = new KeyboardEvent("keydown", { key: "Enter", shiftKey: true });
      const preventDefault = vi.spyOn(event, "preventDefault");
      component.onEnterPress(event);
      expect(preventDefault).not.toHaveBeenCalled();
      expect(agentService.sendMessage).not.toHaveBeenCalled();
    });
  });

  describe("state display helpers", () => {
    const cases: ReadonlyArray<[AgentState, string, string, string]> = [
      [AgentState.AVAILABLE, "check-circle", "#52c41a", "Agent is ready"],
      [AgentState.GENERATING, "sync", "#1890ff", "Agent is generating response..."],
      [AgentState.STOPPING, "sync", "#1890ff", "Agent is stopping..."],
      [AgentState.UNAVAILABLE, "close-circle", "#ff4d4f", "Agent is unavailable"],
    ];

    cases.forEach(([state, icon, color, tooltip]) => {
      it(`maps ${state} to icon "${icon}", color "${color}" and its tooltip`, () => {
        createComponent();
        agentService.stateSubject.next(state);
        expect(component.getStateIcon()).toBe(icon);
        expect(component.getStateIconColor()).toBe(color);
        expect(component.getStateTooltip()).toBe(tooltip);
      });
    });

    it("reports the per-state boolean flags", () => {
      createComponent();

      agentService.stateSubject.next(AgentState.AVAILABLE);
      expect(component.canSendMessage()).toBe(true);
      expect(component.isAvailable()).toBe(true);
      expect(component.isConnected()).toBe(true);
      expect(component.isGenerating()).toBe(false);
      expect(component.isStopping()).toBe(false);

      agentService.stateSubject.next(AgentState.GENERATING);
      expect(component.canSendMessage()).toBe(false);
      expect(component.isGenerating()).toBe(true);
      expect(component.isConnected()).toBe(true);

      agentService.stateSubject.next(AgentState.STOPPING);
      expect(component.isStopping()).toBe(true);
      expect(component.isConnected()).toBe(true);

      agentService.stateSubject.next(AgentState.UNAVAILABLE);
      expect(component.isConnected()).toBe(false);
      expect(component.canSendMessage()).toBe(false);
    });

    it("falls back to the unknown-status tooltip for an unrecognized state", () => {
      createComponent();
      (component as any).agentState = "Bogus";
      expect(component.getStateTooltip()).toBe("Agent status unknown");
      expect(component.getStateIcon()).toBe("close-circle");
      expect(component.getStateIconColor()).toBe("#ff4d4f");
    });
  });

  describe("tool result and operator-access helpers", () => {
    it("prefers output, then result, then the raw entry", () => {
      createComponent();
      const step = makeStep({ toolResults: [{ output: "out" }, { result: "res" }, { other: 1 }] });
      expect(component.getToolResult(step, 0)).toBe("out");
      expect(component.getToolResult(step, 1)).toBe("res");
      expect(component.getToolResult(step, 2)).toEqual({ other: 1 });
    });

    it("returns null for missing tool results or an out-of-range index", () => {
      createComponent();
      expect(component.getToolResult(makeStep(), 0)).toBeNull();
      expect(component.getToolResult(makeStep({ toolResults: [{ output: "x" }] }), 5)).toBeNull();
    });

    it("resolves per-tool-call operator access from the map", () => {
      createComponent();
      const access: ToolOperatorAccess = {
        viewedOperatorIds: ["op-v"],
        addedOperatorIds: [],
        modifiedOperatorIds: ["op-m"],
      };
      const step = makeStep({ operatorAccess: new Map([[0, access]]) });
      expect(component.getToolOperatorAccess(step, 0)).toBe(access);
      expect(component.getToolOperatorAccess(step, 1)).toBeNull();
      expect(component.hasOperatorAccess(step)).toBe(true);
    });

    it("reports no operator access for a missing or empty map", () => {
      createComponent();
      expect(component.getToolOperatorAccess(makeStep(), 0)).toBeNull();
      expect(component.hasOperatorAccess(makeStep())).toBe(false);
      expect(component.hasOperatorAccess(makeStep({ operatorAccess: new Map() }))).toBe(false);
    });
  });

  describe("showSystemInfo / refreshSystemInfo", () => {
    it("populates prompt and tools, applies default settings, and sorts operator types", () => {
      createComponent();
      const tool = { name: "createOperator", description: "Creates an operator", inputSchema: {}, enabled: true };
      agentService.getSystemInfo.mockReturnValue(of({ systemPrompt: "SYSTEM PROMPT TEXT", tools: [tool] }));
      agentService.getAgentSettings.mockReturnValue(of({}));
      agentService.getAvailableOperatorTypes.mockReturnValue(
        of([
          { type: "PythonUDF", description: "Runs Python code" },
          { type: "CSVScan", description: "Reads a CSV file" },
        ])
      );

      component.showSystemInfo();

      expect(component.isSystemInfoModalVisible).toBe(true);
      expect(component.systemPrompt).toBe("SYSTEM PROMPT TEXT");
      expect(component.availableTools).toEqual([tool]);
      // ?? defaults for a settings payload with no fields set:
      expect(component.settingsMaxCharLimit).toBe(20000);
      expect(component.settingsMaxCellCharLimit).toBe(4000);
      expect(component.settingsToolTimeoutSeconds).toBe(120);
      expect(component.settingsExecutionTimeoutMinutes).toBe(10);
      expect(component.settingsMaxSteps).toBe(10);
      expect(component.settingsAllowedOperatorTypes).toEqual([]);
      // Operator types come back sorted alphabetically:
      expect(component.allAvailableOperatorTypes.map(t => t.type)).toEqual(["CSVScan", "PythonUDF"]);
    });

    it("applies explicit settings values from the server", () => {
      createComponent();
      agentService.getAgentSettings.mockReturnValue(
        of({
          maxOperatorResultCharLimit: 111,
          maxOperatorResultCellCharLimit: 222,
          toolTimeoutSeconds: 33,
          executionTimeoutMinutes: 4,
          maxSteps: 5,
          allowedOperatorTypes: ["CSVScan"],
        })
      );

      component.showSystemInfo();

      expect(component.settingsMaxCharLimit).toBe(111);
      expect(component.settingsMaxCellCharLimit).toBe(222);
      expect(component.settingsToolTimeoutSeconds).toBe(33);
      expect(component.settingsExecutionTimeoutMinutes).toBe(4);
      expect(component.settingsMaxSteps).toBe(5);
      expect(component.settingsAllowedOperatorTypes).toEqual(["CSVScan"]);
    });

    it("closeSystemInfoModal hides the modal", () => {
      createComponent();
      component.showSystemInfo();
      component.closeSystemInfoModal();
      expect(component.isSystemInfoModalVisible).toBe(false);
    });
  });

  describe("settings save methods", () => {
    const saveCases = [
      {
        method: "saveMaxCharLimit",
        prop: "settingsMaxCharLimit",
        key: "maxOperatorResultCharLimit",
        value: 31000,
        toast: "Max character limit saved",
      },
      {
        method: "saveMaxCellCharLimit",
        prop: "settingsMaxCellCharLimit",
        key: "maxOperatorResultCellCharLimit",
        value: 512,
        toast: "Max cell character limit saved",
      },
      {
        method: "saveToolTimeout",
        prop: "settingsToolTimeoutSeconds",
        key: "toolTimeoutSeconds",
        value: 90,
        toast: "Tool timeout saved",
      },
      {
        method: "saveExecutionTimeout",
        prop: "settingsExecutionTimeoutMinutes",
        key: "executionTimeoutMinutes",
        value: 5,
        toast: "Execution timeout saved",
      },
      {
        method: "saveMaxSteps",
        prop: "settingsMaxSteps",
        key: "maxSteps",
        value: 25,
        toast: "Max steps saved",
      },
    ] as const;

    saveCases.forEach(({ method, prop, key, value, toast }) => {
      it(`${method} sends only { ${key} } and toasts "${toast}"`, () => {
        createComponent();
        (component as any)[prop] = value;
        (component as any)[method]();
        expect(agentService.updateAgentSettings).toHaveBeenCalledTimes(1);
        expect(agentService.updateAgentSettings).toHaveBeenCalledWith(AGENT_ID, { [key]: value });
        expect(notification.success).toHaveBeenCalledWith(toast);
      });
    });

    it("stays silent when the settings update errors (service already notified)", () => {
      createComponent();
      agentService.updateAgentSettings.mockReturnValue(throwError(() => new Error("nope")));
      component.saveMaxCharLimit();
      expect(notification.success).not.toHaveBeenCalled();
    });
  });

  describe("operator type selection", () => {
    const csv = { type: "CSVScan", description: "Reads a CSV file" };
    const python = { type: "PythonUDF", description: "Runs Python code" };

    it("toggling on adds without duplicates and saves; toggling off removes", () => {
      createComponent();
      component.allAvailableOperatorTypes = [csv, python];
      component.settingsAllowedOperatorTypes = ["CSVScan"];

      component.toggleOperatorType("PythonUDF", true);
      expect(component.settingsAllowedOperatorTypes).toEqual(["CSVScan", "PythonUDF"]);
      expect(agentService.updateAgentSettings).toHaveBeenLastCalledWith(AGENT_ID, {
        allowedOperatorTypes: ["CSVScan", "PythonUDF"],
      });
      expect(notification.success).toHaveBeenLastCalledWith("2 operators enabled");

      // Enabling an already-enabled type must not duplicate it.
      component.toggleOperatorType("PythonUDF", true);
      expect(component.settingsAllowedOperatorTypes).toEqual(["CSVScan", "PythonUDF"]);

      component.toggleOperatorType("CSVScan", false);
      expect(component.settingsAllowedOperatorTypes).toEqual(["PythonUDF"]);
      expect(component.isOperatorTypeEnabled("PythonUDF")).toBe(true);
      expect(component.isOperatorTypeEnabled("CSVScan")).toBe(false);
    });

    it("enableAllOperatorTypes selects every known type; deselectAllOperatorTypes clears", () => {
      createComponent();
      component.allAvailableOperatorTypes = [csv, python];

      component.enableAllOperatorTypes();
      expect(component.settingsAllowedOperatorTypes).toEqual(["CSVScan", "PythonUDF"]);
      expect(notification.success).toHaveBeenLastCalledWith("2 operators enabled");

      component.deselectAllOperatorTypes();
      expect(component.settingsAllowedOperatorTypes).toEqual([]);
      // An empty allow-list means "all operators enabled".
      expect(notification.success).toHaveBeenLastCalledWith("All operators enabled");
      expect(agentService.updateAgentSettings).toHaveBeenLastCalledWith(AGENT_ID, { allowedOperatorTypes: [] });
    });

    it("filters operator types by type or description, case-insensitively", () => {
      createComponent();
      component.allAvailableOperatorTypes = [csv, python];

      component.operatorTypeSearchQuery = "";
      expect(component.getFilteredOperatorTypes()).toEqual([csv, python]);

      component.operatorTypeSearchQuery = "PYTHON";
      expect(component.getFilteredOperatorTypes()).toEqual([python]);

      component.operatorTypeSearchQuery = "csv file";
      expect(component.getFilteredOperatorTypes()).toEqual([csv]);

      component.operatorTypeSearchQuery = "zzz";
      expect(component.getFilteredOperatorTypes()).toEqual([]);
    });
  });

  describe("exportReActSteps", () => {
    it("warns and skips the fetch when there is nothing to export", () => {
      createComponent();
      // Exercise the template binding too: the export button is the second toolbar button.
      const exportButton = fixture.nativeElement.querySelectorAll(".chat-toolbar button")[1] as HTMLButtonElement;
      exportButton.click();
      expect(notification.warning).toHaveBeenCalledWith("No ReAct steps to export");
      expect(agentService.getReActSteps).not.toHaveBeenCalled();
    });

    it("downloads fetched steps as JSON with operatorAccess flattened to a plain object", async () => {
      createComponent();
      const step = makeStep({
        operatorAccess: new Map([
          [0, { viewedOperatorIds: ["op-1"], addedOperatorIds: [], modifiedOperatorIds: ["op-2"] }],
        ]),
      });
      agentService.stepsSubject.next([step]);
      agentService.getReActSteps.mockReturnValue(of([step]));
      const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});

      component.exportReActSteps();

      expect(agentService.getReActSteps).toHaveBeenCalledWith(AGENT_ID);
      expect(createObjectURL).toHaveBeenCalledTimes(1);
      const blob = createObjectURL.mock.calls[0][0] as Blob;
      // jsdom's Blob has no .text(); read the contents with a FileReader instead.
      const text = await new Promise<string>((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result));
        reader.onerror = () => reject(reader.error);
        reader.readAsText(blob);
      });
      const exported = JSON.parse(text);
      expect(exported.agentId).toBe(AGENT_ID);
      expect(exported.agentName).toBe("Test Agent");
      expect(exported.modelType).toBe("gpt-test");
      expect(exported.stepCount).toBe(1);
      expect(exported.steps[0].operatorAccess).toEqual({
        "0": { viewedOperatorIds: ["op-1"], addedOperatorIds: [], modifiedOperatorIds: ["op-2"] },
      });
      expect(clickSpy).toHaveBeenCalledTimes(1);
      expect(revokeObjectURL).toHaveBeenCalledWith("blob:mock-url");
      expect(notification.success).toHaveBeenCalledWith("Exported 1 ReAct steps");
    });

    it("shows an error toast when the fetch fails", () => {
      createComponent();
      agentService.stepsSubject.next([makeStep()]);
      agentService.getReActSteps.mockReturnValue(throwError(() => new Error("boom")));
      vi.spyOn(console, "error").mockImplementation(() => {});

      component.exportReActSteps();

      expect(notification.error).toHaveBeenCalledWith("Failed to export ReAct steps");
      expect(createObjectURL).not.toHaveBeenCalled();
    });
  });

  describe("scroll-to-step requests", () => {
    it("scrolls to and highlights the requested step of this agent", () => {
      createComponent();
      const s0 = makeStep({ messageId: "m1", stepId: 0 });
      const s1 = makeStep({ messageId: "m1", stepId: 1 });
      agentService.stepsSubject.next([s0, s1]);
      fixture.detectChanges();
      agentService.setHoveredMessage.mockClear();

      agentService.scrollToStepSubject.next({ agentId: AGENT_ID, messageId: "m1", stepId: 0 });

      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: "smooth", block: "center" });
      expect(component.hoveredMessageIndex).toBe(0);
      expect(agentService.setHoveredMessage).toHaveBeenCalledWith(AGENT_ID, s0);
    });

    it("ignores scroll requests addressed to other agents", () => {
      createComponent();
      agentService.stepsSubject.next([makeStep()]);
      fixture.detectChanges();

      agentService.scrollToStepSubject.next({ agentId: "someone-else", messageId: "m1", stepId: 0 });

      expect(scrollIntoViewMock).not.toHaveBeenCalled();
    });
  });

  describe("template rendering", () => {
    it("renders user and agent message bubbles with roles, content and tool summary", async () => {
      createComponent();
      const userStep = makeStep({ messageId: "m1", stepId: 0, role: "user", content: "hello agent" });
      const agentStep = makeStep({
        messageId: "m1",
        stepId: 1,
        role: "agent",
        content: "hi human",
        isBegin: true,
        toolCalls: [{ toolName: "addOperator" }],
        usage: { totalTokens: 10 },
      });
      agentService.stepsSubject.next([userStep, agentStep]);
      fixture.detectChanges();
      // <markdown> writes its parsed HTML into the element only after awaiting an
      // async parse(), so flush a macrotask before asserting on the rendered text.
      // (fixture.whenStable() never settles here — the markdown pipeline keeps the
      // zone perpetually unstable.)
      await new Promise(resolve => setTimeout(resolve, 0));
      fixture.detectChanges();

      const messages = fixture.nativeElement.querySelectorAll(".messages-container .message");
      expect(messages.length).toBe(2);
      expect(messages[0].classList.contains("user-message")).toBe(true);
      expect(messages[0].textContent).toContain("You");
      expect(messages[0].textContent).toContain("hello agent");
      expect(messages[1].classList.contains("ai-message")).toBe(true);
      expect(messages[1].textContent).toContain("Test Agent");
      expect(messages[1].textContent).toContain("hi human");
      expect(messages[1].textContent).toContain("Execute 1 tool");
      // The latest step is auto-hovered, so its details button is visible.
      expect(messages[1].querySelector("button")).toBeTruthy();
    });

    it("shows the thinking spinner and stop button while generating", async () => {
      createComponent();
      agentService.stateSubject.next(AgentState.GENERATING);
      fixture.detectChanges();
      // The textarea's [disabled] binding rides on ngModel, whose disabled state is
      // pushed to the control value accessor on a microtask; flush it before asserting.
      await new Promise(resolve => setTimeout(resolve, 0));
      fixture.detectChanges();

      const loading = fixture.nativeElement.querySelector(".loading-message");
      expect(loading).toBeTruthy();
      expect(loading.textContent).toContain("Thinking...");
      expect(fixture.nativeElement.querySelector("textarea").disabled).toBe(true);

      const inputButtons = fixture.nativeElement.querySelectorAll(".input-area button");
      expect(inputButtons.length).toBe(2); // send + stop
      (inputButtons[1] as HTMLButtonElement).click();
      expect(agentService.stopGeneration).toHaveBeenCalledWith(AGENT_ID);
    });

    it("shows the stopping indicator while the agent stops", () => {
      createComponent();
      agentService.stateSubject.next(AgentState.STOPPING);
      fixture.detectChanges();
      expect(fixture.nativeElement.querySelector(".loading-message").textContent).toContain("Stopping...");
    });

    it("shows the disconnected banner and hides the clear button when unavailable", () => {
      createComponent();
      agentService.stateSubject.next(AgentState.UNAVAILABLE);
      fixture.detectChanges();

      const warning = fixture.nativeElement.querySelector(".connection-warning");
      expect(warning).toBeTruthy();
      expect(warning.textContent).toContain("Agent is disconnected. Please check your connection.");
      // Clear button (*ngIf isAvailable) is gone: only info + export remain.
      expect(fixture.nativeElement.querySelectorAll(".chat-toolbar button").length).toBe(2);

      agentService.stateSubject.next(AgentState.AVAILABLE);
      fixture.detectChanges();
      expect(fixture.nativeElement.querySelector(".connection-warning")).toBeNull();
      expect(fixture.nativeElement.querySelectorAll(".chat-toolbar button").length).toBe(3);
    });

    it("disables the send button until there is input and the agent is available", () => {
      createComponent();
      const sendButton = fixture.nativeElement.querySelector(".input-area button") as HTMLButtonElement;
      expect(sendButton.disabled).toBe(true); // empty input

      component.currentMessage = "run it";
      fixture.detectChanges();
      expect(sendButton.disabled).toBe(false);
      sendButton.click();
      expect(agentService.sendMessage).toHaveBeenCalledWith(AGENT_ID, "run it");

      component.currentMessage = "again";
      agentService.stateSubject.next(AgentState.GENERATING);
      fixture.detectChanges();
      expect(sendButton.disabled).toBe(true);
    });

    it("clear button delegates to the service", () => {
      createComponent();
      const clearButton = fixture.nativeElement.querySelectorAll(".chat-toolbar button")[2] as HTMLButtonElement;
      clearButton.click();
      expect(agentService.clearMessages).toHaveBeenCalledWith(AGENT_ID);
    });

    describe("system-info modal", () => {
      // The modal body renders inside the CDK overlay, which is attached to
      // ApplicationRef rather than to this fixture — tick() re-renders it.
      const flushOverlay = (): void => {
        fixture.detectChanges();
        TestBed.inject(ApplicationRef).tick();
      };

      // Lets the <markdown> component's awaited parse() result land in the DOM
      // without depending on full zone stability while the overlay is open.
      const flushAsyncRendering = (): Promise<void> => new Promise(resolve => setTimeout(resolve, 0));

      const clickTab = (label: string): void => {
        // Prefer the innermost [role="tab"] node: HTMLElement.click() bubbles,
        // so this reaches the tab's click listener wherever nz-tabs attached it.
        let candidates = Array.from(document.querySelectorAll<HTMLElement>("[role='tab']"));
        if (candidates.length === 0) {
          candidates = Array.from(document.querySelectorAll<HTMLElement>(".ant-tabs-tab"));
        }
        const tab = candidates.find(el => (el.textContent ?? "").includes(label));
        expect(tab, `expected a tab header containing "${label}"`).toBeTruthy();
        tab!.click();
        flushOverlay();
      };

      beforeEach(() => {
        createComponent();
        agentService.getSystemInfo.mockReturnValue(
          of({
            systemPrompt: "SYSTEM PROMPT TEXT",
            tools: [{ name: "createOperator", description: "Creates an operator", inputSchema: {}, enabled: true }],
          })
        );
        agentService.getAvailableOperatorTypes.mockReturnValue(
          of([
            { type: "PythonUDF", description: "Runs Python code" },
            { type: "CSVScan", description: "Reads a CSV file" },
          ])
        );
      });

      it("opens from the toolbar info button and renders all four tab headers", async () => {
        const infoButton = fixture.nativeElement.querySelector(".chat-toolbar button") as HTMLButtonElement;
        infoButton.click();
        fixture.detectChanges();
        await flushAsyncRendering();
        flushOverlay();

        const bodyText = document.body.textContent ?? "";
        expect(bodyText).toContain("Agent System Information");
        expect(bodyText).toContain("System Prompt");
        expect(bodyText).toContain("Tools (1)");
        expect(bodyText).toContain("Parameters");
        expect(bodyText).toContain("Operators");
        // The first tab is active by default and shows the fetched prompt.
        expect(bodyText).toContain("SYSTEM PROMPT TEXT");
      });

      it("renders the tools list, parameter inputs, and sorted operator toggles per tab", async () => {
        component.showSystemInfo();
        fixture.detectChanges();
        await flushAsyncRendering();
        flushOverlay();

        clickTab("Tools (1)");
        expect(document.body.textContent).toContain("createOperator");
        expect(document.body.textContent).toContain("Creates an operator");

        clickTab("Parameters");
        expect(document.querySelectorAll("nz-input-number").length).toBe(5);
        const paramsText = document.body.textContent ?? "";
        expect(paramsText).toContain("Max Operator Result Character Limit");
        expect(paramsText).toContain("Max Cell Character Limit");
        expect(paramsText).toContain("Tool Execution Timeout (seconds)");
        expect(paramsText).toContain("Workflow Execution Timeout (minutes)");
        expect(paramsText).toContain("Max Steps per Message");

        clickTab("Operators");
        expect(document.querySelectorAll("nz-switch").length).toBe(2);
        const operatorsText = document.body.textContent ?? "";
        // Sorted alphabetically: CSVScan renders before PythonUDF.
        expect(operatorsText.indexOf("CSVScan")).toBeGreaterThan(-1);
        expect(operatorsText.indexOf("CSVScan")).toBeLessThan(operatorsText.indexOf("PythonUDF"));

        // A query with no matches renders the empty-state hint instead of rows.
        component.operatorTypeSearchQuery = "nomatch-xyz";
        flushOverlay();
        expect(document.querySelectorAll("nz-switch").length).toBe(0);
        expect(document.body.textContent).toContain('No operators match "nomatch-xyz"');
      });
    });
  });
});
