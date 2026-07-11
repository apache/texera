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
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { AgentService, AgentInfo, OperatorResultSummary } from "./agent.service";
import { AgentState, ReActStep } from "./agent-types";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("AgentService", () => {
  let service: AgentService;
  let httpMock: HttpTestingController;
  let selectedUnit: DashboardWorkflowComputingUnit | null;
  let notifications: { error: any; success: any; info: any; warning: any };

  const apiAgent = {
    id: "agent-1",
    name: "Bob",
    modelType: "gpt-5-mini",
    state: "AVAILABLE",
    createdAt: "2026-06-11T00:00:00.000Z",
  };

  /** Seed the local cache with a backend agent by replaying a getAllAgents sync. */
  function seedAgent(overrides: Partial<typeof apiAgent> & Record<string, any> = {}): void {
    service.getAllAgents().subscribe();
    httpMock
      .expectOne(r => r.method === "GET" && r.url === "/api/agents")
      .flush({ agents: [{ ...apiAgent, ...overrides }] });
  }

  beforeEach(() => {
    selectedUnit = null;
    notifications = { error: vi.fn(), success: vi.fn(), info: vi.fn(), warning: vi.fn() };
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        AgentService,
        { provide: NotificationService, useValue: notifications },
        { provide: WorkflowPersistService, useValue: {} },
        {
          provide: ComputingUnitStatusService,
          useValue: { getSelectedComputingUnitValue: () => selectedUnit },
        },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(AgentService);
    httpMock = TestBed.inject(HttpTestingController);
    // The constructor syncs the local agent cache with the backend.
    httpMock.expectOne(req => req.method === "GET" && req.url === "/api/agents").flush({ agents: [] });
  });

  afterEach(() => {
    httpMock.verify();
  });

  describe("createAgent", () => {
    it("creates an agent without putting the user token in the payload", () => {
      let created: AgentInfo | undefined;
      service.createAgent("gpt-5-mini", "Bob").subscribe(agent => (created = agent));

      const req = httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents");
      expect(req.request.body.userToken).toBeUndefined();
      expect(req.request.body.modelType).toEqual("gpt-5-mini");
      expect(req.request.body.name).toEqual("Bob");
      expect(req.request.body.workflowId).toBeUndefined();
      expect(req.request.body.computingUnitId).toBeUndefined();
      req.flush(apiAgent);

      expect(created?.id).toEqual("agent-1");
      expect(created?.modelType).toEqual("gpt-5-mini");
    });

    it("includes workflowId and the selected computing unit id in the payload", () => {
      selectedUnit = { computingUnit: { cuid: 7 } } as unknown as DashboardWorkflowComputingUnit;
      service.createAgent("gpt-5-mini", "Bob", 42).subscribe();

      const req = httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents");
      expect(req.request.body.workflowId).toEqual(42);
      expect(req.request.body.computingUnitId).toEqual(7);
      expect(req.request.body.userToken).toBeUndefined();
      req.flush(apiAgent);
    });

    it("caches the created agent, seeds its state, and notifies subscribers", () => {
      const changes = vi.fn();
      service.agentChange$.subscribe(changes);

      service.createAgent("gpt-5-mini", "Bob").subscribe();
      httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents").flush(apiAgent);

      // Cached: getAgent returns it without a second HTTP call.
      let fetched: AgentInfo | undefined;
      service.getAgent("agent-1").subscribe(a => (fetched = a));
      expect(fetched?.id).toEqual("agent-1");

      let count = 0;
      service.getAgentCount().subscribe(c => (count = c));
      expect(count).toEqual(1);

      let state: AgentState | undefined;
      service.getAgentState("agent-1").subscribe(s => (state = s));
      expect(state).toEqual(AgentState.AVAILABLE);

      expect(changes).toHaveBeenCalled();
    });

    it("surfaces the backend error message and rethrows on failure", () => {
      let errored: Error | undefined;
      service.createAgent("gpt-5-mini", "Bob").subscribe({ error: (e: unknown) => (errored = e as Error) });

      httpMock
        .expectOne(r => r.method === "POST" && r.url === "/api/agents")
        .flush({ error: "model unavailable" }, { status: 400, statusText: "Bad Request" });

      expect(notifications.error).toHaveBeenCalledWith("model unavailable");
      expect(errored?.message).toEqual("model unavailable");
    });
  });

  describe("mapApiAgentInfo (via getAgent)", () => {
    it("maps all API fields, including the delegate and parsed createdAt", () => {
      let fetched: AgentInfo | undefined;
      service.getAgent("agent-9").subscribe(a => (fetched = a));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-9")
        .flush({
          id: "agent-9",
          name: "Alice",
          modelType: "gpt-5",
          state: "GENERATING",
          createdAt: "2026-06-11T00:00:00.000Z",
          delegate: {
            userToken: "secret-should-not-leak",
            userInfo: { uid: 5, name: "Alice", email: "a@x.io", role: "ADMIN" },
            workflowId: 11,
            workflowName: "wf",
          },
          settings: { maxSteps: 3 },
        });

      expect(fetched?.id).toEqual("agent-9");
      expect(fetched?.name).toEqual("Alice");
      expect(fetched?.modelType).toEqual("gpt-5");
      expect(fetched?.isBaselineMode).toBe(false);
      expect(fetched?.state).toEqual(AgentState.GENERATING);
      expect(fetched?.createdAt instanceof Date).toBe(true);
      expect(fetched?.createdAt.toISOString()).toEqual("2026-06-11T00:00:00.000Z");
      expect(fetched?.settings).toEqual({ maxSteps: 3 });
      // delegate keeps userInfo/workflow fields but drops the userToken
      expect(fetched?.delegate?.userInfo.uid).toEqual(5);
      expect(fetched?.delegate?.workflowId).toEqual(11);
      expect(fetched?.delegate?.workflowName).toEqual("wf");
      expect((fetched?.delegate as any)?.userToken).toBeUndefined();
    });

    it("leaves delegate undefined when the API omits it", () => {
      let fetched: AgentInfo | undefined;
      service.getAgent("agent-2").subscribe(a => (fetched = a));
      httpMock.expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-2").flush(apiAgent);
      expect(fetched?.delegate).toBeUndefined();
    });

    it.each([
      ["AVAILABLE", AgentState.AVAILABLE],
      ["GENERATING", AgentState.GENERATING],
      ["STOPPING", AgentState.STOPPING],
      ["UNAVAILABLE", AgentState.UNAVAILABLE],
      ["something-unknown", AgentState.UNAVAILABLE],
    ])("maps API state %s to %s", (apiState, expected) => {
      let fetched: AgentInfo | undefined;
      service.getAgent("agent-x").subscribe(a => (fetched = a));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-x")
        .flush({ ...apiAgent, id: "agent-x", state: apiState });
      expect(fetched?.state).toEqual(expected);
    });
  });

  describe("getAgent", () => {
    it("returns the cached agent without issuing an HTTP request", () => {
      seedAgent();
      let fetched: AgentInfo | undefined;
      service.getAgent("agent-1").subscribe(a => (fetched = a));
      expect(fetched?.id).toEqual("agent-1");
      // afterEach's httpMock.verify() asserts no request was made.
    });

    it("errors when the agent is not found on the backend", () => {
      let errored: Error | undefined;
      service.getAgent("missing").subscribe({ error: (e: unknown) => (errored = e as Error) });
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/missing")
        .flush("nope", { status: 404, statusText: "Not Found" });
      expect(errored?.message).toEqual("Agent with ID missing not found");
    });
  });

  describe("getAllAgents", () => {
    it("maps the list and removes local agents no longer on the backend", () => {
      seedAgent();

      let agents: AgentInfo[] = [];
      service.getAllAgents().subscribe(a => (agents = a));
      // Backend no longer returns agent-1.
      httpMock.expectOne(r => r.method === "GET" && r.url === "/api/agents").flush({ agents: [] });

      expect(agents).toEqual([]);
      let count = -1;
      service.getAgentCount().subscribe(c => (count = c));
      expect(count).toEqual(0);
    });

    it("falls back to the locally cached agents when the request fails", () => {
      seedAgent();

      let agents: AgentInfo[] = [];
      service.getAllAgents().subscribe(a => (agents = a));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(agents.map(a => a.id)).toEqual(["agent-1"]);
    });
  });

  describe("deleteAgent", () => {
    it("removes the agent from the cache when the backend confirms deletion", () => {
      seedAgent();
      let result: boolean | undefined;
      service.deleteAgent("agent-1").subscribe(r => (result = r));
      httpMock.expectOne(r => r.method === "DELETE" && r.url === "/api/agents/agent-1").flush({ deleted: true });

      expect(result).toBe(true);
      let count = -1;
      service.getAgentCount().subscribe(c => (count = c));
      expect(count).toEqual(0);
    });

    it("still cleans up locally when the delete request fails", () => {
      seedAgent();
      let result: boolean | undefined;
      service.deleteAgent("agent-1").subscribe(r => (result = r));
      httpMock
        .expectOne(r => r.method === "DELETE" && r.url === "/api/agents/agent-1")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(result).toBe(true);
      let count = -1;
      service.getAgentCount().subscribe(c => (count = c));
      expect(count).toEqual(0);
    });
  });

  describe("convertApiReActStep (via getReActSteps)", () => {
    it("converts operatorAccess objects to a Map and normalizes tool results", () => {
      let steps: ReActStep[] = [];
      service.getReActSteps("agent-1").subscribe(s => (steps = s));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps")
        .flush({
          state: "AVAILABLE",
          steps: [
            {
              messageId: "m1",
              stepId: 2,
              timestamp: "2026-06-11T00:00:00.000Z",
              role: "agent",
              content: "hi",
              toolResults: [{ output: "out-only" }, { result: "result-only" }],
              operatorAccess: { "0": { viewedOperatorIds: ["op-a"] } },
            },
          ],
        });

      expect(steps.length).toEqual(1);
      const step = steps[0];
      expect(step.messageId).toEqual("m1");
      expect(step.stepId).toEqual(2);
      expect(step.timestamp instanceof Date).toBe(true);
      // operatorAccess: string keys become numeric Map keys
      expect(step.operatorAccess instanceof Map).toBe(true);
      expect(step.operatorAccess?.get(0)?.viewedOperatorIds).toEqual(["op-a"]);
      // tool results: output and result are kept in sync both ways
      expect(step.toolResults?.[0].result).toEqual("out-only");
      expect(step.toolResults?.[0].output).toEqual("out-only");
      expect(step.toolResults?.[1].result).toEqual("result-only");
      expect(step.toolResults?.[1].output).toEqual("result-only");
    });

    it("synthesizes an id from messageId/stepId and defaults optional fields", () => {
      let steps: ReActStep[] = [];
      service.getReActSteps("agent-1").subscribe(s => (steps = s));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps")
        .flush({ state: "AVAILABLE", steps: [{ messageId: "m7" }] });

      const step = steps[0];
      expect(step.id).toEqual("m7-0");
      expect(step.stepId).toEqual(0);
      expect(step.role).toEqual("agent");
      expect(step.content).toEqual("");
      expect(step.isBegin).toBe(false);
      expect(step.isEnd).toBe(false);
      expect(step.operatorAccess).toBeUndefined();
    });

    it("returns an empty array when the request fails", () => {
      let steps: ReActStep[] = [];
      service.getReActSteps("agent-1").subscribe(s => (steps = s));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(steps).toEqual([]);
    });
  });

  describe("getStepsByOperatorIds", () => {
    it("posts the operator ids and converts the returned steps", () => {
      let result: { steps: ReActStep[] } | undefined;
      service.getStepsByOperatorIds("agent-1", ["op-a", "op-b"]).subscribe(r => (result = r));

      const req = httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/steps-by-operators");
      expect(req.request.body).toEqual({ operatorIds: ["op-a", "op-b"] });
      req.flush({ steps: [{ messageId: "m1", stepId: 1 }] });

      expect(result?.steps.length).toEqual(1);
      expect(result?.steps[0].id).toEqual("m1-1");
    });

    it("returns empty steps when the request fails", () => {
      let result: { steps: ReActStep[] } | undefined;
      service.getStepsByOperatorIds("agent-1", ["op-a"]).subscribe(r => (result = r));
      httpMock
        .expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/steps-by-operators")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(result?.steps).toEqual([]);
    });
  });

  describe("getReActStepsByOperatorAccess", () => {
    it("partitions steps into those that viewed vs modified an operator", () => {
      let result: { viewedBy: ReActStep[]; modifiedBy: ReActStep[] } | undefined;
      service.getReActStepsByOperatorAccess("agent-1", "op-a").subscribe(r => (result = r));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps")
        .flush({
          state: "AVAILABLE",
          steps: [
            {
              messageId: "viewed",
              stepId: 1,
              operatorAccess: { "0": { viewedOperatorIds: ["op-a"], modifiedOperatorIds: [] } },
            },
            {
              messageId: "modified",
              stepId: 1,
              operatorAccess: { "0": { viewedOperatorIds: [], modifiedOperatorIds: ["op-a"] } },
            },
            {
              messageId: "unrelated",
              stepId: 1,
              operatorAccess: { "0": { viewedOperatorIds: ["op-z"], modifiedOperatorIds: [] } },
            },
          ],
        });

      expect(result?.viewedBy.map(s => s.messageId)).toEqual(["viewed"]);
      expect(result?.modifiedBy.map(s => s.messageId)).toEqual(["modified"]);
    });
  });

  describe("agentHeaders", () => {
    it("attaches X-Agent-Workflow-Id once a workflow id is known for the agent", () => {
      service.createAgent("gpt-5-mini", "Bob", 42).subscribe();
      httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents").flush(apiAgent);

      service.getReActSteps("agent-1").subscribe();
      const req = httpMock.expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps");
      expect(req.request.headers.get("X-Agent-Workflow-Id")).toEqual("42");
      req.flush({ state: "AVAILABLE", steps: [] });
    });

    it("omits the header when no workflow id is tracked", () => {
      service.getReActSteps("agent-1").subscribe();
      const req = httpMock.expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps");
      expect(req.request.headers.has("X-Agent-Workflow-Id")).toBe(false);
      req.flush({ state: "AVAILABLE", steps: [] });
    });
  });

  describe("getAgentWorkflowId", () => {
    it("returns the delegate workflow id of a cached agent", () => {
      seedAgent({ delegate: { userInfo: { uid: 1, name: "x", email: "x", role: "r" }, workflowId: 99 } });
      expect(service.getAgentWorkflowId("agent-1")).toEqual(99);
    });

    it("returns undefined for an unknown agent", () => {
      expect(service.getAgentWorkflowId("ghost")).toBeUndefined();
    });
  });

  describe("fetchModelTypes", () => {
    it("formats model ids and caches the result (shareReplay)", () => {
      let models: any[] = [];
      service.fetchModelTypes().subscribe(m => (models = m));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "api/models")
        .flush({
          object: "list",
          data: [{ id: "gpt-5-mini", object: "model", created: 0, owned_by: "openai" }],
        });

      expect(models[0]).toEqual({
        id: "gpt-5-mini",
        name: "Gpt 5 Mini",
        description: "Model: gpt-5-mini",
        icon: "robot",
      });

      // Second subscription is served from cache, no new HTTP request.
      let cached: any[] = [];
      service.fetchModelTypes().subscribe(m => (cached = cached.concat(m)));
      expect(cached.length).toEqual(1);
    });

    it("falls back to an empty list when the request fails", () => {
      let models: any[] | undefined;
      service.fetchModelTypes().subscribe(m => (models = m));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "api/models")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(models).toEqual([]);
    });
  });

  describe("getAgentSettings / updateAgentSettings", () => {
    it("returns settings from the backend", () => {
      let settings: any;
      service.getAgentSettings("agent-1").subscribe(s => (settings = s));
      httpMock.expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/settings").flush({ maxSteps: 5 });
      expect(settings.maxSteps).toEqual(5);
    });

    it("falls back to defaults when fetching settings fails", () => {
      let settings: any;
      service.getAgentSettings("agent-1").subscribe(s => (settings = s));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/settings")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(settings.maxSteps).toEqual(10);
      expect(settings.disabledTools).toEqual([]);
    });

    it("patches settings and updates the cached agent", () => {
      seedAgent();
      let updated: any;
      service.updateAgentSettings("agent-1", { maxSteps: 8 }).subscribe(s => (updated = s));
      const req = httpMock.expectOne(r => r.method === "PATCH" && r.url === "/api/agents/agent-1/settings");
      expect(req.request.body).toEqual({ maxSteps: 8 });
      req.flush({ maxSteps: 8 });

      expect(updated.maxSteps).toEqual(8);
      let fetched: AgentInfo | undefined;
      service.getAgent("agent-1").subscribe(a => (fetched = a));
      expect(fetched?.settings?.maxSteps).toEqual(8);
    });

    it("notifies and rethrows when patching settings fails", () => {
      let errored: Error | undefined;
      service
        .updateAgentSettings("agent-1", { maxSteps: 8 })
        .subscribe({ error: (e: unknown) => (errored = e as Error) });
      httpMock
        .expectOne(r => r.method === "PATCH" && r.url === "/api/agents/agent-1/settings")
        .flush({ error: "nope" }, { status: 400, statusText: "Bad Request" });
      expect(notifications.error).toHaveBeenCalledWith("nope");
      expect(errored?.message).toEqual("nope");
    });
  });

  describe("getAvailableOperatorTypes", () => {
    it("returns the backend list", () => {
      let types: any[] = [];
      service.getAvailableOperatorTypes("agent-1").subscribe(t => (types = t));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/operator-types")
        .flush([{ type: "Filter", description: "d" }]);
      expect(types).toEqual([{ type: "Filter", description: "d" }]);
    });

    it("falls back to an empty list on error", () => {
      let types: any[] | undefined;
      service.getAvailableOperatorTypes("agent-1").subscribe(t => (types = t));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/operator-types")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(types).toEqual([]);
    });
  });

  describe("getSystemInfo", () => {
    it("returns the backend system info", () => {
      let info: any;
      service.getSystemInfo("agent-1").subscribe(i => (info = i));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/system-info")
        .flush({ systemPrompt: "be helpful", tools: [] });
      expect(info.systemPrompt).toEqual("be helpful");
    });

    it("falls back to a placeholder when the request fails", () => {
      let info: any;
      service.getSystemInfo("agent-1").subscribe(i => (info = i));
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/system-info")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(info.systemPrompt).toEqual("Unable to retrieve system prompt");
      expect(info.tools).toEqual([]);
    });
  });

  describe("clearMessages", () => {
    it("clears the local ReAct steps once the backend confirms", () => {
      const tracking = (service as any).getOrCreateStateTracking("agent-1");
      tracking.reActStepsSubject.next([{ messageId: "m1" } as ReActStep]);

      let steps: ReActStep[] = [];
      service.getReActStepsObservable("agent-1").subscribe(s => (steps = s));

      service.clearMessages("agent-1");
      httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/clear").flush({});

      expect(steps).toEqual([]);
    });
  });

  describe("fetchOperatorResults", () => {
    it("pulls operator results over REST and pushes them to operatorResultSummaries$", () => {
      let latest: Map<string, OperatorResultSummary> | undefined;
      service.operatorResultSummaries$.subscribe(m => (latest = m));

      service.fetchOperatorResults("agent-1");

      const req = httpMock.expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/operator-results");
      req.flush({
        results: {
          "op-1": { sampleRecords: [{ a: 1 }], resultStatistics: { a: "{}" } },
        },
      });

      expect(latest?.has("op-1")).toBe(true);
      expect(latest?.get("op-1")?.sampleRecords).toEqual([{ a: 1 }]);
    });

    it("falls back to empty results when the request fails", () => {
      let latest: Map<string, OperatorResultSummary> | undefined;
      service.operatorResultSummaries$.subscribe(m => (latest = m));

      service.fetchOperatorResults("agent-1");

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/operator-results")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(latest?.size).toBe(0);
    });
  });

  describe("stopGeneration", () => {
    it("sends a stop command over the websocket when one is open", () => {
      const send = vi.fn();
      (service as any).agentStateTracking.set("agent-1", {
        websocket: { readyState: WebSocket.OPEN, send },
      });

      service.stopGeneration("agent-1");

      expect(send).toHaveBeenCalledWith(JSON.stringify({ type: "WsClientStopCommand" }));
    });

    it("falls back to the REST stop endpoint when no websocket is open", () => {
      (service as any).agentStateTracking.set("agent-1", {
        websocket: { readyState: WebSocket.CLOSED, send: vi.fn() },
      });

      service.stopGeneration("agent-1");

      httpMock
        .expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/stop")
        .flush({ status: "stopping" });
    });
  });

  describe("sendMessage", () => {
    it("errors when the agent is unknown", () => {
      service.sendMessage("ghost", "hi");
      expect(notifications.error).toHaveBeenCalledWith("Agent with ID ghost not found");
    });

    it("errors when no websocket connection is available", () => {
      seedAgent();
      service.sendMessage("agent-1", "hi");
      expect(notifications.error).toHaveBeenCalledWith("WebSocket connection not available");
    });

    it("sends a prompt command over an open websocket", () => {
      seedAgent();
      const send = vi.fn();
      (service as any).agentStateTracking.set("agent-1", { websocket: { readyState: WebSocket.OPEN, send } });

      service.sendMessage("agent-1", "hello", "feedback");

      expect(send).toHaveBeenCalledWith(
        JSON.stringify({ type: "WsClientPromptCommand", content: "hello", messageSource: "feedback" })
      );
    });
  });

  describe("state observables", () => {
    it("getAgentState returns UNAVAILABLE for an untracked agent", () => {
      let state: AgentState | undefined;
      service.getAgentState("ghost").subscribe(s => (state = s));
      expect(state).toEqual(AgentState.UNAVAILABLE);
    });

    it("isAgentConnected reflects the tracked state", () => {
      const tracking = (service as any).getOrCreateStateTracking("agent-1");
      tracking.stateSubject.next(AgentState.AVAILABLE);
      let connected: boolean | undefined;
      service.isAgentConnected("agent-1").subscribe(c => (connected = c));
      expect(connected).toBe(true);
    });

    it("getAgentStateObservable emits subsequent state changes", () => {
      const emitted: AgentState[] = [];
      service.getAgentStateObservable("agent-1").subscribe(s => emitted.push(s));
      const tracking = (service as any).agentStateTracking.get("agent-1");
      tracking.stateSubject.next(AgentState.GENERATING);
      expect(emitted).toEqual([AgentState.UNAVAILABLE, AgentState.GENERATING]);
    });
  });

  describe("HEAD pointer and visible steps", () => {
    it("getHeadId returns null without tracking and the tracked value otherwise", () => {
      expect(service.getHeadId("ghost")).toBeNull();
      const tracking = (service as any).getOrCreateStateTracking("agent-1");
      tracking.headIdSubject.next("head-1");
      expect(service.getHeadId("agent-1")).toEqual("head-1");
    });

    it("getVisibleSteps returns the current snapshot, or [] without tracking", () => {
      expect(service.getVisibleSteps("ghost")).toEqual([]);
      const tracking = (service as any).getOrCreateStateTracking("agent-1");
      const step = { messageId: "m1" } as ReActStep;
      tracking.reActStepsSubject.next([step]);
      expect(service.getVisibleSteps("agent-1")).toEqual([step]);
    });
  });

  describe("hovered message operators", () => {
    it("dedupes operator ids gathered across operatorAccess entries", () => {
      let hovered:
        | { viewedOperatorIds: string[]; addedOperatorIds: string[]; modifiedOperatorIds: string[] }
        | undefined;
      service.getHoveredMessageOperatorsObservable("agent-1").subscribe(h => (hovered = h));

      const step = {
        operatorAccess: new Map<number, any>([
          [0, { viewedOperatorIds: ["a", "b"], addedOperatorIds: ["c"], modifiedOperatorIds: [] }],
          [1, { viewedOperatorIds: ["b"], addedOperatorIds: [], modifiedOperatorIds: ["d"] }],
        ]),
      } as unknown as ReActStep;

      service.setHoveredMessage("agent-1", step);
      expect(hovered?.viewedOperatorIds.sort()).toEqual(["a", "b"]);
      expect(hovered?.addedOperatorIds).toEqual(["c"]);
      expect(hovered?.modifiedOperatorIds).toEqual(["d"]);
    });

    it("clears the hovered operators when given a null step", () => {
      let hovered: any;
      service.getHoveredMessageOperatorsObservable("agent-1").subscribe(h => (hovered = h));
      service.setHoveredMessage("agent-1", null);
      expect(hovered).toEqual({ viewedOperatorIds: [], addedOperatorIds: [], modifiedOperatorIds: [] });
    });
  });

  describe("canvas annotation toggles", () => {
    it("togglePortShapes updates the value and stream", () => {
      const emitted: boolean[] = [];
      service.showPortShapes$.subscribe(v => emitted.push(v));
      service.togglePortShapes(false);
      expect(service.getShowPortShapes()).toBe(false);
      expect(emitted).toEqual([true, false]);
    });

    it("requestScrollToStep emits a scroll request", () => {
      let req: { agentId: string; messageId: string; stepId: number } | undefined;
      service.scrollToStep$.subscribe(r => (req = r));
      service.requestScrollToStep("agent-1", "m1", 3);
      expect(req).toEqual({ agentId: "agent-1", messageId: "m1", stepId: 3 });
    });
  });

  describe("getWorkflowObservable", () => {
    it("returns of(null) for an untracked agent", () => {
      let value: unknown = "unset";
      service.getWorkflowObservable("ghost").subscribe(v => (value = v));
      expect(value).toBeNull();
    });
  });

  // --------------------------------------------------------------------------
  // WebSocket lifecycle + message handling. Swap the global WebSocket for a
  // controllable fake so activate/deactivate and handleWebSocketMessage can be
  // exercised without a real connection.
  // --------------------------------------------------------------------------
  describe("WebSocket lifecycle", () => {
    class FakeWebSocket {
      static CONNECTING = 0;
      static OPEN = 1;
      static CLOSING = 2;
      static CLOSED = 3;
      static instances: FakeWebSocket[] = [];
      readyState = FakeWebSocket.OPEN;
      onmessage: ((e: { data: string }) => void) | null = null;
      onerror: ((e: unknown) => void) | null = null;
      onclose: ((e: { code: number }) => void) | null = null;
      sent: string[] = [];
      closed = false;
      constructor(public url: string) {
        FakeWebSocket.instances.push(this);
      }
      send(data: string): void {
        this.sent.push(data);
      }
      close(): void {
        this.closed = true;
        this.readyState = FakeWebSocket.CLOSED;
      }
    }

    let originalWebSocket: typeof WebSocket;

    beforeEach(() => {
      FakeWebSocket.instances = [];
      originalWebSocket = globalThis.WebSocket;
      (globalThis as any).WebSocket = FakeWebSocket;
    });

    afterEach(() => {
      (globalThis as any).WebSocket = originalWebSocket;
    });

    it("activateAgent opens a websocket and reports the agent as connected", () => {
      seedAgent();
      expect(service.activateAgent("agent-1")).toBe(true);
      expect(FakeWebSocket.instances.length).toEqual(1);
      expect(service.isAgentActivelyConnected("agent-1")).toBe(true);
      expect(service.getActivelyConnectedAgentIds()).toEqual(["agent-1"]);
    });

    it("activateAgent returns false for an unknown agent", () => {
      expect(service.activateAgent("ghost")).toBe(false);
      expect(FakeWebSocket.instances.length).toEqual(0);
    });

    it("deactivateAgent closes the websocket and drops the connection", () => {
      seedAgent();
      service.activateAgent("agent-1");
      const ws = FakeWebSocket.instances[0];

      service.deactivateAgent("agent-1");

      expect(ws.closed).toBe(true);
      expect(service.isAgentActivelyConnected("agent-1")).toBe(false);
    });

    it("handles a snapshot message by pushing state and converted steps", () => {
      seedAgent();
      service.activateAgent("agent-1");
      const ws = FakeWebSocket.instances[0];

      let state: AgentState | undefined;
      service.getAgentStateObservable("agent-1").subscribe(s => (state = s));
      let steps: ReActStep[] = [];
      service.getReActStepsObservable("agent-1").subscribe(s => (steps = s));

      ws.onmessage!({
        data: JSON.stringify({
          type: "WsServerSnapshotEvent",
          state: "GENERATING",
          steps: [{ messageId: "m1", stepId: 1 }],
          headId: "m1-1",
        }),
      });

      expect(state).toEqual(AgentState.GENERATING);
      expect(steps.length).toEqual(1);
      expect(steps[0].id).toEqual("m1-1");
      expect(service.getHeadId("agent-1")).toEqual("m1-1");
    });

    it("appends new step events and advances HEAD", () => {
      seedAgent();
      service.activateAgent("agent-1");
      const ws = FakeWebSocket.instances[0];

      let steps: ReActStep[] = [];
      service.getReActStepsObservable("agent-1").subscribe(s => (steps = s));

      ws.onmessage!({ data: JSON.stringify({ type: "WsServerStepEvent", step: { messageId: "m1", stepId: 1 } }) });
      expect(steps.length).toEqual(1);

      // Same messageId/stepId updates in place rather than appending.
      ws.onmessage!({
        data: JSON.stringify({ type: "WsServerStepEvent", step: { messageId: "m1", stepId: 1, isEnd: true } }),
      });
      expect(steps.length).toEqual(1);
      expect(steps[0].isEnd).toBe(true);

      // A different stepId appends.
      ws.onmessage!({ data: JSON.stringify({ type: "WsServerStepEvent", step: { messageId: "m1", stepId: 2 } }) });
      expect(steps.length).toEqual(2);
      expect(service.getHeadId("agent-1")).toEqual("m1-2");
    });

    it("cleans up local state and warns when the backend reports the agent is gone", () => {
      seedAgent();
      service.activateAgent("agent-1");
      const ws = FakeWebSocket.instances[0];

      ws.onmessage!({ data: JSON.stringify({ type: "WsServerErrorEvent", error: "Agent not found" }) });

      expect(notifications.warning).toHaveBeenCalledWith("Agent was removed (backend may have restarted)");
      let count = -1;
      service.getAgentCount().subscribe(c => (count = c));
      expect(count).toEqual(0);
    });

    it("surfaces other websocket errors via the notification service", () => {
      seedAgent();
      service.activateAgent("agent-1");
      const ws = FakeWebSocket.instances[0];

      ws.onmessage!({ data: JSON.stringify({ type: "WsServerErrorEvent", error: "boom" }) });

      expect(notifications.error).toHaveBeenCalledWith("boom");
    });
  });
});
