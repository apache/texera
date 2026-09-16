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
import { of, throwError } from "rxjs";
import { AgentService, AgentInfo, AgentSettingsApi, ModelType, OperatorResultSummary } from "./agent.service";
import { AgentState, ReActStep } from "./agent-types";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";
import { Workflow } from "../../../common/type/workflow";
import { commonTestProviders } from "../../../common/testing/test-utils";

/**
 * Minimal WebSocket double. Installed with vi.stubGlobal so the service's
 * `new WebSocket(...)` and `WebSocket.OPEN` references resolve to it; tests
 * drive the connection by invoking onmessage/onclose directly.
 */
class FakeWebSocket {
  public static readonly CONNECTING = 0;
  public static readonly OPEN = 1;
  public static readonly CLOSING = 2;
  public static readonly CLOSED = 3;
  public static instances: FakeWebSocket[] = [];

  public readyState: number = FakeWebSocket.CONNECTING;
  public onmessage: ((event: { data: string }) => void) | null = null;
  public onerror: ((event: unknown) => void) | null = null;
  public onclose: ((event: { code: number }) => void) | null = null;
  public send = vi.fn();
  public close = vi.fn(() => {
    this.readyState = FakeWebSocket.CLOSED;
  });

  constructor(public url: string) {
    FakeWebSocket.instances.push(this);
  }

  public static latest(): FakeWebSocket {
    return FakeWebSocket.instances[FakeWebSocket.instances.length - 1];
  }
}

describe("AgentService", () => {
  let service: AgentService;
  let httpMock: HttpTestingController;
  let selectedUnit: DashboardWorkflowComputingUnit | null;
  let notification: Record<"error" | "success" | "info" | "warning", ReturnType<typeof vi.fn>>;
  let workflowPersist: { retrieveWorkflow: ReturnType<typeof vi.fn> };

  const apiAgent = {
    id: "agent-1",
    name: "Bob",
    modelType: "gpt-5-mini",
    state: "AVAILABLE",
    createdAt: "2026-06-11T00:00:00.000Z",
  };

  const stubWorkflow = {
    wid: 42,
    name: "wf",
    content: { operators: [], operatorPositions: {}, links: [] },
  } as unknown as Workflow;

  /** Populate the local cache (and state tracking) through the public createAgent API. */
  function seedAgent(id = "agent-1", workflowId?: number): void {
    service.createAgent("gpt-5-mini", "Bob", workflowId).subscribe();
    httpMock
      .expectOne(r => r.method === "POST" && r.url === "/api/agents")
      .flush({
        ...apiAgent,
        id,
        delegate:
          workflowId === undefined
            ? undefined
            : {
                userToken: "secret",
                userInfo: { uid: 1, name: "u", email: "u@example.com", role: "REGULAR" },
                workflowId,
                workflowName: "wf",
              },
      });
  }

  beforeEach(() => {
    selectedUnit = null;
    notification = { error: vi.fn(), success: vi.fn(), info: vi.fn(), warning: vi.fn() };
    workflowPersist = { retrieveWorkflow: vi.fn().mockReturnValue(of(stubWorkflow)) };
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        AgentService,
        { provide: NotificationService, useValue: notification },
        { provide: WorkflowPersistService, useValue: workflowPersist },
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
    // Belt and braces: TestBed rebuilds the injector per test, so the spies installed
    // on the service's own collaborators are already discarded with it. This keeps the
    // spec safe if any of them ever moves to a shared provider.
    vi.restoreAllMocks();
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

  describe("syncAgentsWithBackend", () => {
    // The constructor's initial sync request is flushed empty in beforeEach,
    // so these tests drive the same code path again with a seeded cache.

    it("evicts locally-cached agents missing from the backend and notifies subscribers", () => {
      seedAgent("agent-1");
      let changes = 0;
      service.agentChange$.subscribe(() => changes++);

      (service as any).syncAgentsWithBackend();
      httpMock.expectOne(r => r.method === "GET" && r.url === "/api/agents").flush({ agents: [] });

      expect((service as any).agents.size).toBe(0);
      expect((service as any).agentStateTracking.has("agent-1")).toBe(false);
      expect(changes).toBe(1);
    });

    it("pushes backend state updates into existing agents' state streams without a change event", () => {
      seedAgent("agent-1");
      const states: AgentState[] = [];
      service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));
      let changes = 0;
      service.agentChange$.subscribe(() => changes++);

      (service as any).syncAgentsWithBackend();
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush({ agents: [{ ...apiAgent, state: "GENERATING" }] });

      expect(states[states.length - 1]).toBe(AgentState.GENERATING);
      expect(changes).toBe(0);
    });
  });

  describe("getAgent", () => {
    it("serves cache hits without issuing a request", () => {
      seedAgent("agent-1");
      let got: AgentInfo | undefined;

      service.getAgent("agent-1").subscribe(a => (got = a));

      httpMock.expectNone(r => r.url === "/api/agents/agent-1");
      expect(got?.id).toBe("agent-1");
      expect(got?.name).toBe("Bob");
    });

    it("fetches, maps and caches an unknown agent", () => {
      let got: AgentInfo | undefined;
      service.getAgent("agent-9").subscribe(a => (got = a));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-9")
        .flush({
          id: "agent-9",
          name: "Eve",
          modelType: "o3",
          state: "GENERATING",
          createdAt: "2026-06-11T00:00:00.000Z",
          delegate: {
            userToken: "secret",
            userInfo: { uid: 1, name: "u", email: "u@example.com", role: "REGULAR" },
            workflowId: 42,
            workflowName: "wf",
          },
        });

      expect(got?.createdAt).toBeInstanceOf(Date);
      expect(got?.createdAt.getTime()).toBe(new Date("2026-06-11T00:00:00.000Z").getTime());
      expect(got?.state).toBe(AgentState.GENERATING);
      expect(got?.delegate?.workflowId).toBe(42);
      expect((got?.delegate as any).userToken).toBeUndefined();

      // Second lookup is a cache hit.
      let again: AgentInfo | undefined;
      service.getAgent("agent-9").subscribe(a => (again = a));
      httpMock.expectNone(r => r.url === "/api/agents/agent-9");
      expect(again).toBe(got);
    });

    it("maps a 404 to a descriptive error", () => {
      let message: string | undefined;
      service.getAgent("ghost").subscribe({ error: (e: unknown) => (message = (e as Error).message) });

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/ghost")
        .flush("not found", { status: 404, statusText: "Not Found" });

      expect(message).toBe("Agent with ID ghost not found");
    });
  });

  describe("getAllAgents", () => {
    it("maps backend agents and evicts stale local ones", () => {
      seedAgent("agent-1");
      let result: AgentInfo[] | undefined;
      service.getAllAgents().subscribe(r => (result = r));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush({
          agents: [
            {
              id: "agent-2",
              name: "Eve",
              modelType: "o3",
              state: "GENERATING",
              createdAt: "2026-06-11T00:00:00.000Z",
              delegate: {
                userToken: "secret",
                userInfo: { uid: 1, name: "u", email: "u@example.com", role: "REGULAR" },
                workflowId: 7,
                workflowName: "wf",
              },
            },
          ],
        });

      expect(result?.map(a => a.id)).toEqual(["agent-2"]);
      expect(result?.[0].state).toBe(AgentState.GENERATING);
      expect(result?.[0].createdAt).toBeInstanceOf(Date);
      expect(result?.[0].delegate?.workflowId).toBe(7);
      // agent-1 was not on the backend: cache and tracking are gone.
      expect((service as any).agents.has("agent-1")).toBe(false);
      expect((service as any).agentStateTracking.has("agent-1")).toBe(false);
      expect((service as any).agents.has("agent-2")).toBe(true);
    });

    it("falls back to the local cache on HTTP failure", () => {
      seedAgent("agent-1");
      let result: AgentInfo[] | undefined;
      service.getAllAgents().subscribe(r => (result = r));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(result?.map(a => a.id)).toEqual(["agent-1"]);
    });
  });

  describe("deleteAgent", () => {
    it("evicts the agent and notifies on {deleted: true}", () => {
      seedAgent("agent-1");
      let changes = 0;
      service.agentChange$.subscribe(() => changes++);
      let deleted: boolean | undefined;

      service.deleteAgent("agent-1").subscribe(d => (deleted = d));
      httpMock.expectOne(r => r.method === "DELETE" && r.url === "/api/agents/agent-1").flush({ deleted: true });

      expect(deleted).toBe(true);
      expect((service as any).agents.has("agent-1")).toBe(false);
      expect((service as any).agentStateTracking.has("agent-1")).toBe(false);
      expect(changes).toBe(1);
    });

    it("keeps the agent and emits false when the backend refuses the delete", () => {
      seedAgent("agent-1");
      let deleted: boolean | undefined;

      service.deleteAgent("agent-1").subscribe(d => (deleted = d));
      httpMock.expectOne(r => r.method === "DELETE" && r.url === "/api/agents/agent-1").flush({ deleted: false });

      expect(deleted).toBe(false);
      expect((service as any).agents.has("agent-1")).toBe(true);
    });

    it("still evicts locally and emits true when the DELETE fails", () => {
      seedAgent("agent-1");
      let changes = 0;
      service.agentChange$.subscribe(() => changes++);
      let deleted: boolean | undefined;

      service.deleteAgent("agent-1").subscribe(d => (deleted = d));
      httpMock
        .expectOne(r => r.method === "DELETE" && r.url === "/api/agents/agent-1")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(deleted).toBe(true);
      expect((service as any).agents.has("agent-1")).toBe(false);
      expect(changes).toBe(1);
    });
  });

  describe("fetchModelTypes", () => {
    it("maps LiteLLM models and replays the cached result without a second request", () => {
      let first: ModelType[] | undefined;
      service.fetchModelTypes().subscribe(m => (first = m));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "api/models")
        .flush({
          data: [{ id: "gpt-5-mini", object: "model", created: 0, owned_by: "openai" }],
          object: "list",
        });

      expect(first).toEqual([
        { id: "gpt-5-mini", name: "Gpt 5 Mini", description: "Model: gpt-5-mini", icon: "robot" },
      ]);

      let second: ModelType[] | undefined;
      service.fetchModelTypes().subscribe(m => (second = m));
      httpMock.expectNone(r => r.url === "api/models");
      expect(second).toEqual(first);
    });

    it("emits an empty list when the model endpoint fails", () => {
      let models: ModelType[] | undefined;
      service.fetchModelTypes().subscribe(m => (models = m));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "api/models")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(models).toEqual([]);
    });
  });

  describe("websocket lifecycle and events", () => {
    function emit(ws: FakeWebSocket, message: object): void {
      ws.onmessage!({ data: JSON.stringify(message) });
    }

    beforeEach(() => {
      FakeWebSocket.instances = [];
      vi.stubGlobal("WebSocket", FakeWebSocket);
    });

    afterEach(() => {
      vi.unstubAllGlobals();
      vi.useRealTimers();
      vi.restoreAllMocks();
    });

    describe("activateAgent / deactivateAgent", () => {
      it("returns false for unknown agents and opens no socket", () => {
        expect(service.activateAgent("nope")).toBe(false);
        expect(FakeWebSocket.instances.length).toBe(0);
      });

      it("opens one websocket to the agent's react endpoint and reuses it on re-activation", () => {
        seedAgent("agent-1");

        expect(service.activateAgent("agent-1")).toBe(true);
        expect(FakeWebSocket.instances.length).toBe(1);
        expect(FakeWebSocket.latest().url).toMatch(/^ws:\/\/[^/]+\/api\/agents\/agent-1\/react$/);

        expect(service.activateAgent("agent-1")).toBe(true);
        expect(FakeWebSocket.instances.length).toBe(1);
      });

      it("closes the socket and recreates stopPolling$ on deactivate, then reconnects on activate", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();
        const tracking = (service as any).agentStateTracking.get("agent-1");
        const oldStopPolling = tracking.stopPolling$;

        service.deactivateAgent("agent-1");

        expect(ws.close).toHaveBeenCalled();
        expect(tracking.websocket).toBeUndefined();
        expect(tracking.isActive).toBe(false);
        expect(tracking.stopPolling$).not.toBe(oldStopPolling);

        service.activateAgent("agent-1");
        expect(FakeWebSocket.instances.length).toBe(2);
      });

      it("reflects the socket readyState in the connection queries", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");

        // Still CONNECTING: not considered actively connected.
        expect(service.isAgentActivelyConnected("agent-1")).toBe(false);
        expect(service.getActivelyConnectedAgentIds()).toEqual([]);

        FakeWebSocket.latest().readyState = FakeWebSocket.OPEN;
        expect(service.isAgentActivelyConnected("agent-1")).toBe(true);
        expect(service.getActivelyConnectedAgentIds()).toEqual(["agent-1"]);
      });

      it("keeps an already-open socket on re-activation and replaces a non-open one", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const tracking = (service as any).agentStateTracking.get("agent-1");
        const ws = FakeWebSocket.latest();

        // deactivateAgent always drops the socket, so the "inactive but still
        // holding a socket" shape that activateAgent's readyState guard defends
        // against is set up directly.
        ws.readyState = FakeWebSocket.OPEN;
        tracking.isActive = false;
        expect(service.activateAgent("agent-1")).toBe(true);
        expect(FakeWebSocket.instances.length).toBe(1);
        expect(tracking.websocket).toBe(ws);

        // The same shape with a socket that is no longer OPEN reconnects instead.
        ws.readyState = FakeWebSocket.CLOSED;
        tracking.isActive = false;
        expect(service.activateAgent("agent-1")).toBe(true);
        expect(FakeWebSocket.instances.length).toBe(2);
        expect(tracking.websocket).toBe(FakeWebSocket.latest());
      });

      it("ignores deactivation of an unknown agent and of an already-inactive one", () => {
        service.deactivateAgent("nope");
        expect((service as any).agentStateTracking.has("nope")).toBe(false);

        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();
        service.deactivateAgent("agent-1");
        expect(ws.close).toHaveBeenCalledTimes(1);

        const tracking = (service as any).agentStateTracking.get("agent-1");
        const stopPolling = tracking.stopPolling$;
        service.deactivateAgent("agent-1");

        // The second call returns at the isActive guard: nothing is torn down twice.
        expect(ws.close).toHaveBeenCalledTimes(1);
        expect(tracking.stopPolling$).toBe(stopPolling);
      });

      it("deactivates cleanly when the socket was already dropped by a close event", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();
        const tracking = (service as any).agentStateTracking.get("agent-1");

        // A normal close clears tracking.websocket but leaves the agent active.
        ws.onclose!({ code: 1000 });
        expect(tracking.websocket).toBeUndefined();
        expect(tracking.isActive).toBe(true);

        const stopPolling = tracking.stopPolling$;
        service.deactivateAgent("agent-1");

        expect(ws.close).not.toHaveBeenCalled();
        expect(tracking.isActive).toBe(false);
        expect(tracking.stopPolling$).not.toBe(stopPolling);
      });
    });

    describe("connection setup", () => {
      /** Swap window.location for the duration of fn; jsdom's own is not writable. */
      const withLocation = <T>(overrides: Partial<Location>, fn: () => T): T => {
        const original = window.location;
        Object.defineProperty(window, "location", {
          configurable: true,
          value: { ...original, ...overrides },
        });
        try {
          return fn();
        } finally {
          Object.defineProperty(window, "location", { configurable: true, value: original });
        }
      };

      it("switches to the wss scheme when the page is served over https", () => {
        seedAgent("agent-1");

        withLocation({ protocol: "https:", host: "texera.example.org" }, () => service.activateAgent("agent-1"));

        expect(FakeWebSocket.latest().url).toBe("wss://texera.example.org/api/agents/agent-1/react");
      });

      it("logs a payload that is not valid JSON instead of throwing out of the handler", () => {
        const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();

        expect(() => ws.onmessage!({ data: "<html>not json</html>" })).not.toThrow();

        expect(errSpy).toHaveBeenCalledWith("Failed to parse agent WebSocket message:", expect.any(SyntaxError));
        // The connection is left intact.
        expect((service as any).agentStateTracking.get("agent-1").websocket).toBe(ws);
      });

      it("logs socket transport errors", () => {
        const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const event = { type: "error" };

        FakeWebSocket.latest().onerror!(event);

        expect(errSpy).toHaveBeenCalledWith("Agent agent-1 WebSocket error:", event);
      });

      it("ignores a close event from a socket that has already been replaced", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const stale = FakeWebSocket.latest();
        const states: AgentState[] = [];
        service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));

        service.deactivateAgent("agent-1");
        service.activateAgent("agent-1");
        const current = FakeWebSocket.latest();
        expect(current).not.toBe(stale);

        // The stale socket reports an abnormal close after being swapped out:
        // the live connection survives and the state does not flip.
        stale.onclose!({ code: 1006 });

        expect((service as any).agentStateTracking.get("agent-1").websocket).toBe(current);
        expect(states).toEqual([AgentState.AVAILABLE]);
      });
    });

    describe("sendMessage", () => {
      it("reports an error for an unknown agent", () => {
        service.sendMessage("nope", "hi");
        expect(notification.error).toHaveBeenCalledWith("Agent with ID nope not found");
      });

      it("reports when no websocket connection is available", () => {
        seedAgent("agent-1");
        service.sendMessage("agent-1", "hi");
        expect(notification.error).toHaveBeenCalledWith("WebSocket connection not available");
      });

      it("sends a WsClientPromptCommand carrying the message source over an open socket", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();
        ws.readyState = FakeWebSocket.OPEN;

        service.sendMessage("agent-1", "hello");
        service.sendMessage("agent-1", "fix it", "feedback");

        expect(JSON.parse(ws.send.mock.calls[0][0])).toEqual({
          type: "WsClientPromptCommand",
          content: "hello",
          messageSource: "chat",
        });
        expect(JSON.parse(ws.send.mock.calls[1][0])).toEqual({
          type: "WsClientPromptCommand",
          content: "fix it",
          messageSource: "feedback",
        });
        expect(notification.error).not.toHaveBeenCalled();
      });

      it("surfaces send failures as a notification", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();
        ws.readyState = FakeWebSocket.OPEN;
        ws.send.mockImplementationOnce(() => {
          throw new Error("socket closed");
        });

        service.sendMessage("agent-1", "hello");

        expect(notification.error).toHaveBeenCalledWith("Failed to send message");
      });
    });

    describe("server events", () => {
      it("applies a WsServerSnapshotEvent: state, converted steps, HEAD and workflow", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();

        const states: AgentState[] = [];
        service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));
        let steps: ReActStep[] = [];
        service.getReActStepsObservable("agent-1").subscribe(s => (steps = s));
        let workflow: Workflow | null = null;
        service.getWorkflowObservable("agent-1").subscribe(w => (workflow = w));

        emit(ws, {
          type: "WsServerSnapshotEvent",
          state: "GENERATING",
          headId: "h1",
          workflowContent: { operators: ["op"] },
          steps: [
            {
              messageId: "m1",
              timestamp: "2026-06-11T00:00:00.000Z",
              toolCalls: [{ toolName: "addOperator" }],
              toolResults: [{ output: "out" }, { result: "res" }],
              operatorAccess: {
                "0": { viewedOperatorIds: ["v1"], addedOperatorIds: ["a1"], modifiedOperatorIds: [] },
              },
            },
          ],
        });

        expect(states[states.length - 1]).toBe(AgentState.GENERATING);
        expect(steps.length).toBe(1);
        const step = steps[0];
        // convertApiReActStep defaults: stepId 0, role "agent", empty content,
        // id falls back to `${messageId}-0`.
        expect(step.stepId).toBe(0);
        expect(step.role).toBe("agent");
        expect(step.content).toBe("");
        expect(step.id).toBe("m1-0");
        expect(step.timestamp.getTime()).toBe(new Date("2026-06-11T00:00:00.000Z").getTime());
        // output/result aliasing works in both directions.
        expect(step.toolResults?.[0]).toMatchObject({ output: "out", result: "out" });
        expect(step.toolResults?.[1]).toMatchObject({ output: "res", result: "res" });
        // operatorAccess arrives as an object and becomes a Map with int keys.
        expect(step.operatorAccess).toBeInstanceOf(Map);
        expect(step.operatorAccess?.get(0)?.viewedOperatorIds).toEqual(["v1"]);

        expect(service.getHeadId("agent-1")).toBe("h1");
        expect(workflow!.content).toEqual({ operators: ["op"] });
        expect((service as any).agentStateTracking.get("agent-1").wsWorkflowActive).toBe(true);

        // A follow-up status event flips only the state.
        emit(ws, { type: "WsServerStatusEvent", state: "AVAILABLE" });
        expect(states[states.length - 1]).toBe(AgentState.AVAILABLE);
      });

      it("appends, replaces in place and advances HEAD on WsServerStepEvent", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();

        let steps: ReActStep[] = [];
        service.getReActStepsObservable("agent-1").subscribe(s => (steps = s));
        let workflow: Workflow | null = null;
        service.getWorkflowObservable("agent-1").subscribe(w => (workflow = w));

        emit(ws, {
          type: "WsServerStepEvent",
          step: { messageId: "m1", stepId: 1, id: "s1", isEnd: false, timestamp: "2026-06-11T00:00:00.000Z" },
        });
        expect(steps.length).toBe(1);
        expect(steps[0].isEnd).toBe(false);
        expect(service.getHeadId("agent-1")).toBe("s1");

        // Same messageId + stepId: the existing step is replaced, not appended.
        emit(ws, {
          type: "WsServerStepEvent",
          step: { messageId: "m1", stepId: 1, id: "s1", isEnd: true, timestamp: "2026-06-11T00:00:01.000Z" },
        });
        expect(steps.length).toBe(1);
        expect(steps[0].isEnd).toBe(true);
        expect(service.getVisibleSteps("agent-1").length).toBe(1);

        // A new step with afterWorkflowContent also updates the workflow.
        emit(ws, {
          type: "WsServerStepEvent",
          step: {
            messageId: "m1",
            stepId: 2,
            id: "s2",
            timestamp: "2026-06-11T00:00:02.000Z",
            afterWorkflowContent: { operators: [1] },
          },
        });
        expect(steps.length).toBe(2);
        expect(service.getHeadId("agent-1")).toBe("s2");
        expect(workflow!.content).toEqual({ operators: [1] });
        expect((service as any).agentStateTracking.get("agent-1").wsWorkflowActive).toBe(true);
      });

      it("cleans up the agent when the backend reports 'Agent not found'", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();
        const states: AgentState[] = [];
        service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));
        let changes = 0;
        service.agentChange$.subscribe(() => changes++);

        emit(ws, { type: "WsServerErrorEvent", error: "Agent not found" });

        expect((service as any).agents.has("agent-1")).toBe(false);
        expect((service as any).agentStateTracking.has("agent-1")).toBe(false);
        expect(states[states.length - 1]).toBe(AgentState.UNAVAILABLE);
        expect(notification.warning).toHaveBeenCalledWith("Agent was removed (backend may have restarted)");
        expect(changes).toBe(1);
      });

      it("shows other websocket errors as notifications and keeps the agent", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");

        emit(FakeWebSocket.latest(), { type: "WsServerErrorEvent", error: "boom" });

        expect(notification.error).toHaveBeenCalledWith("boom");
        expect((service as any).agents.has("agent-1")).toBe(true);
      });

      it("marks the agent UNAVAILABLE only on abnormal socket close", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const states: AgentState[] = [];
        service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));

        // Normal close (code 1000): the socket is dropped, the state stays put.
        FakeWebSocket.latest().onclose!({ code: 1000 });
        expect((service as any).agentStateTracking.get("agent-1").websocket).toBeUndefined();
        expect(states).toEqual([AgentState.AVAILABLE]);

        // Reconnect, then close abnormally: state flips to UNAVAILABLE.
        service.activateAgent("agent-1");
        FakeWebSocket.latest().onclose!({ code: 1006 });
        expect(states[states.length - 1]).toBe(AgentState.UNAVAILABLE);
      });

      it("leaves the workflow stream alone when a snapshot carries no workflow content", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const states: AgentState[] = [];
        service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));
        const workflows: (Workflow | null)[] = [];
        service.getWorkflowObservable("agent-1").subscribe(w => workflows.push(w));

        emit(FakeWebSocket.latest(), { type: "WsServerSnapshotEvent", state: "GENERATING" });

        expect(states[states.length - 1]).toBe(AgentState.GENERATING);
        expect(workflows).toEqual([null]);
        // Workflow polling stays in charge while the socket sends no content.
        expect((service as any).agentStateTracking.get("agent-1").wsWorkflowActive).toBe(false);
      });

      it("ignores a step event that carries no step", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        let steps: ReActStep[] = [];
        service.getReActStepsObservable("agent-1").subscribe(s => (steps = s));

        emit(FakeWebSocket.latest(), { type: "WsServerStepEvent" });

        expect(steps).toEqual([]);
        expect(service.getHeadId("agent-1")).toBeNull();
      });

      it("advances HEAD to the synthesised id when the step carries none", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");

        emit(FakeWebSocket.latest(), {
          type: "WsServerStepEvent",
          step: { messageId: "m9", timestamp: "2026-06-11T00:00:00.000Z" },
        });

        // convertApiReActStep falls back to `${messageId}-${stepId || 0}`, which is
        // always a non-empty string, so a step never fails the HEAD guard.
        expect(service.getHeadId("agent-1")).toBe("m9-0");
      });

      it("ignores a status event that carries no state", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const states: AgentState[] = [];
        service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));

        emit(FakeWebSocket.latest(), { type: "WsServerStatusEvent" });

        expect(states).toEqual([AgentState.AVAILABLE]);
      });

      it("falls back to a generic notification when the error event carries no message", () => {
        vi.spyOn(console, "error").mockImplementation(() => {});
        seedAgent("agent-1");
        service.activateAgent("agent-1");

        emit(FakeWebSocket.latest(), { type: "WsServerErrorEvent" });

        expect(notification.error).toHaveBeenCalledWith("Agent error occurred");
        expect((service as any).agents.has("agent-1")).toBe(true);
      });

      it("warns about an unrecognised message type and changes nothing", () => {
        const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const states: AgentState[] = [];
        service.getAgentStateObservable("agent-1").subscribe(s => states.push(s));
        let steps: ReActStep[] = [];
        service.getReActStepsObservable("agent-1").subscribe(s => (steps = s));

        emit(FakeWebSocket.latest(), { type: "WsServerSomethingElseEvent", state: "GENERATING" });

        expect(warnSpy).toHaveBeenCalledWith("Unknown agent WebSocket message type:", "WsServerSomethingElseEvent");
        expect(states).toEqual([AgentState.AVAILABLE]);
        expect(steps).toEqual([]);
        expect(notification.error).not.toHaveBeenCalled();
      });
    });

    describe("stopStatePolling", () => {
      // stopStatePolling is internal; deleteAgent is the public entry point that
      // reaches it for both a tracked and an untracked agent.
      it("closes the socket and drops the tracking entry for a tracked agent", () => {
        seedAgent("agent-1");
        service.activateAgent("agent-1");
        const ws = FakeWebSocket.latest();
        const tracking = (service as any).agentStateTracking.get("agent-1");
        let stopped = false;
        tracking.stopPolling$.subscribe({ complete: () => (stopped = true) });

        let deleted: boolean | undefined;
        service.deleteAgent("agent-1").subscribe(d => (deleted = d));
        httpMock.expectOne(r => r.method === "DELETE" && r.url === "/api/agents/agent-1").flush({ deleted: true });

        expect(deleted).toBe(true);
        expect(ws.close).toHaveBeenCalledTimes(1);
        expect(tracking.websocket).toBeUndefined();
        expect(stopped).toBe(true);
        expect((service as any).agentStateTracking.has("agent-1")).toBe(false);
      });

      it("is a no-op for an agent that was never tracked", () => {
        let deleted: boolean | undefined;
        service.deleteAgent("ghost").subscribe(d => (deleted = d));
        httpMock.expectOne(r => r.method === "DELETE" && r.url === "/api/agents/ghost").flush({ deleted: true });

        expect(deleted).toBe(true);
        expect((service as any).agentStateTracking.has("ghost")).toBe(false);
      });
    });

    describe("workflow polling", () => {
      it("polls the workflow every second until websocket updates suppress it", () => {
        vi.useFakeTimers();
        seedAgent("agent-1", 42);
        service.activateAgent("agent-1");
        let workflow: Workflow | null = null;
        service.getWorkflowObservable("agent-1").subscribe(w => (workflow = w));

        vi.advanceTimersByTime(1000);
        expect(workflowPersist.retrieveWorkflow).toHaveBeenCalledWith(42);
        expect(workflow).toBe(stubWorkflow);

        // A snapshot carrying workflowContent flips wsWorkflowActive and
        // suppresses subsequent polls.
        FakeWebSocket.latest().onmessage!({
          data: JSON.stringify({ type: "WsServerSnapshotEvent", workflowContent: { operators: [] } }),
        });
        workflowPersist.retrieveWorkflow.mockClear();
        vi.advanceTimersByTime(3000);
        expect(workflowPersist.retrieveWorkflow).not.toHaveBeenCalled();

        service.deactivateAgent("agent-1");
      });

      it("swallows polling errors and keeps the interval alive", () => {
        vi.useFakeTimers();
        workflowPersist.retrieveWorkflow.mockReturnValue(throwError(() => new Error("db down")));
        seedAgent("agent-1", 42);
        service.activateAgent("agent-1");
        let workflow: Workflow | null = null;
        service.getWorkflowObservable("agent-1").subscribe(w => (workflow = w));

        vi.advanceTimersByTime(1000);
        expect(workflowPersist.retrieveWorkflow).toHaveBeenCalledWith(42);
        expect(workflow).toBeNull();

        // The next tick after recovery delivers a workflow again.
        workflowPersist.retrieveWorkflow.mockReturnValue(of(stubWorkflow));
        vi.advanceTimersByTime(1000);
        expect(workflow).toBe(stubWorkflow);

        service.deactivateAgent("agent-1");
      });
    });
  });

  describe("clearMessages", () => {
    it("resets the step stream on success", () => {
      seedAgent("agent-1");
      const tracking = (service as any).agentStateTracking.get("agent-1");
      tracking.reActStepsSubject.next([{ messageId: "m1" } as unknown as ReActStep]);

      service.clearMessages("agent-1");
      httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/clear").flush({});

      expect(tracking.reActStepsSubject.getValue()).toEqual([]);
    });

    it("keeps the steps when the clear request fails", () => {
      seedAgent("agent-1");
      const tracking = (service as any).agentStateTracking.get("agent-1");
      tracking.reActStepsSubject.next([{ messageId: "m1" } as unknown as ReActStep]);

      service.clearMessages("agent-1");
      httpMock
        .expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/clear")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(tracking.reActStepsSubject.getValue().length).toBe(1);
    });
  });

  describe("setHoveredMessage", () => {
    it("emits the deduplicated union of operator ids across tool calls", () => {
      let latest:
        | { viewedOperatorIds: string[]; addedOperatorIds: string[]; modifiedOperatorIds: string[] }
        | undefined;
      // Subscribing creates the tracking entry.
      service.getHoveredMessageOperatorsObservable("agent-1").subscribe(v => (latest = v));

      const step = {
        operatorAccess: new Map([
          [0, { viewedOperatorIds: ["a", "b"], addedOperatorIds: ["x"], modifiedOperatorIds: ["m"] }],
          [1, { viewedOperatorIds: ["b", "c"], addedOperatorIds: ["x", "y"], modifiedOperatorIds: [] }],
        ]),
      } as unknown as ReActStep;
      service.setHoveredMessage("agent-1", step);

      expect(latest).toEqual({
        viewedOperatorIds: ["a", "b", "c"],
        addedOperatorIds: ["x", "y"],
        modifiedOperatorIds: ["m"],
      });
    });

    it("emits empty arrays when the hovered step is cleared", () => {
      let latest:
        | { viewedOperatorIds: string[]; addedOperatorIds: string[]; modifiedOperatorIds: string[] }
        | undefined;
      service.getHoveredMessageOperatorsObservable("agent-1").subscribe(v => (latest = v));

      service.setHoveredMessage("agent-1", null);

      expect(latest).toEqual({ viewedOperatorIds: [], addedOperatorIds: [], modifiedOperatorIds: [] });
    });
  });

  describe("getReActStepsByOperatorAccess", () => {
    it("collects viewing and modifying steps exactly once each", () => {
      let result: { viewedBy: ReActStep[]; modifiedBy: ReActStep[] } | undefined;
      service.getReActStepsByOperatorAccess("agent-1", "op-7").subscribe(r => (result = r));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps")
        .flush({
          state: "AVAILABLE",
          steps: [
            {
              messageId: "viewer",
              timestamp: "2026-06-11T00:00:00.000Z",
              // Two accesses mention op-7: the step must appear only once.
              operatorAccess: {
                "0": { viewedOperatorIds: ["op-7"], addedOperatorIds: [], modifiedOperatorIds: [] },
                "1": { viewedOperatorIds: ["op-7"], addedOperatorIds: [], modifiedOperatorIds: [] },
              },
            },
            {
              messageId: "modifier",
              timestamp: "2026-06-11T00:00:00.000Z",
              operatorAccess: {
                "0": { viewedOperatorIds: [], addedOperatorIds: [], modifiedOperatorIds: ["op-7"] },
              },
            },
            {
              messageId: "bystander",
              timestamp: "2026-06-11T00:00:00.000Z",
              operatorAccess: {
                "0": { viewedOperatorIds: ["other"], addedOperatorIds: [], modifiedOperatorIds: [] },
              },
            },
          ],
        });

      expect(result?.viewedBy.map(s => s.messageId)).toEqual(["viewer"]);
      expect(result?.modifiedBy.map(s => s.messageId)).toEqual(["modifier"]);
    });
  });

  describe("agent request routing and fallbacks", () => {
    it("attaches X-Agent-Workflow-Id when the agent's tracking has a workflowId", () => {
      service.ensureWorkflowPolling("agent-1", 42);

      service.getAgentSettings("agent-1").subscribe();
      const settingsReq = httpMock.expectOne(r => r.url === "/api/agents/agent-1/settings");
      expect(settingsReq.request.headers.get("X-Agent-Workflow-Id")).toBe("42");
      settingsReq.flush({ maxSteps: 5 });

      service.getSystemInfo("agent-1").subscribe();
      const sysReq = httpMock.expectOne(r => r.url === "/api/agents/agent-1/system-info");
      expect(sysReq.request.headers.get("X-Agent-Workflow-Id")).toBe("42");
      sysReq.flush({ systemPrompt: "p", tools: [] });
    });

    it("omits the header when no workflow is associated", () => {
      service.getAgentSettings("agent-2").subscribe();
      const req = httpMock.expectOne(r => r.url === "/api/agents/agent-2/settings");
      expect(req.request.headers.get("X-Agent-Workflow-Id")).toBeNull();
      req.flush({});
    });

    it("falls back to the documented default settings when the fetch fails", () => {
      let settings: AgentSettingsApi | undefined;
      service.getAgentSettings("agent-1").subscribe(s => (settings = s));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/settings")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(settings).toEqual({
        maxOperatorResultCharLimit: 20000,
        maxOperatorResultCellCharLimit: 4000,
        toolTimeoutSeconds: 120,
        executionTimeoutMinutes: 10,
        disabledTools: [],
        maxSteps: 10,
        allowedOperatorTypes: [],
      });
    });

    it("updates the cached agent's settings on a successful PATCH", () => {
      seedAgent("agent-1");
      let updated: AgentSettingsApi | undefined;
      service.updateAgentSettings("agent-1", { maxSteps: 3 }).subscribe(s => (updated = s));

      const req = httpMock.expectOne(r => r.method === "PATCH" && r.url === "/api/agents/agent-1/settings");
      expect(req.request.body).toEqual({ maxSteps: 3 });
      req.flush({ maxSteps: 3, disabledTools: [] });

      expect(updated).toEqual({ maxSteps: 3, disabledTools: [] });
      expect((service as any).agents.get("agent-1").settings).toEqual({ maxSteps: 3, disabledTools: [] });
    });

    it("notifies and rethrows when the settings update fails", () => {
      let message: string | undefined;
      service
        .updateAgentSettings("agent-1", { maxSteps: 3 })
        .subscribe({ error: (e: unknown) => (message = (e as Error).message) });

      httpMock
        .expectOne(r => r.method === "PATCH" && r.url === "/api/agents/agent-1/settings")
        .flush({ error: "nope" }, { status: 400, statusText: "Bad Request" });

      expect(message).toBe("nope");
      expect(notification.error).toHaveBeenCalledWith("nope");
    });

    it("posts operator ids and converts the returned steps", () => {
      service.ensureWorkflowPolling("agent-1", 42);
      let result: { steps: ReActStep[] } | undefined;
      service.getStepsByOperatorIds("agent-1", ["op-1"]).subscribe(r => (result = r));

      const req = httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/steps-by-operators");
      expect(req.request.body).toEqual({ operatorIds: ["op-1"] });
      expect(req.request.headers.get("X-Agent-Workflow-Id")).toBe("42");
      req.flush({ steps: [{ messageId: "m1", timestamp: "2026-06-11T00:00:00.000Z" }] });

      expect(result?.steps.length).toBe(1);
      expect(result?.steps[0].id).toBe("m1-0");
    });

    it("falls back for system-info, operator-types and steps-by-operators failures", () => {
      let systemInfo: { systemPrompt: string; tools: unknown[] } | undefined;
      service.getSystemInfo("agent-1").subscribe(s => (systemInfo = s));
      httpMock
        .expectOne(r => r.url === "/api/agents/agent-1/system-info")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(systemInfo).toEqual({ systemPrompt: "Unable to retrieve system prompt", tools: [] });

      let operatorTypes: Array<{ type: string; description: string }> | undefined;
      service.getAvailableOperatorTypes("agent-1").subscribe(t => (operatorTypes = t));
      httpMock
        .expectOne(r => r.url === "/api/agents/agent-1/operator-types")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(operatorTypes).toEqual([]);

      let steps: { steps: ReActStep[] } | undefined;
      service.getStepsByOperatorIds("agent-1", ["op-1"]).subscribe(s => (steps = s));
      httpMock
        .expectOne(r => r.url === "/api/agents/agent-1/steps-by-operators")
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(steps).toEqual({ steps: [] });
    });
  });

  describe("canvas annotation toggles", () => {
    it("togglePortShapes drives showPortShapes$ and the synchronous getter", () => {
      const seen: boolean[] = [];
      service.showPortShapes$.subscribe(v => seen.push(v));

      service.togglePortShapes(false);

      expect(seen).toEqual([true, false]);
      expect(service.getShowPortShapes()).toBe(false);
    });

    it("requestScrollToStep broadcasts the scroll target", () => {
      let target: { agentId: string; messageId: string; stepId: number } | undefined;
      service.scrollToStep$.subscribe(t => (target = t));

      service.requestScrollToStep("agent-1", "m1", 4);

      expect(target).toEqual({ agentId: "agent-1", messageId: "m1", stepId: 4 });
    });
  });

  // ---------------------------------------------------------------------------
  // State accessors: the untracked side of each (an id with no tracking entry)
  // ---------------------------------------------------------------------------
  describe("state accessors without tracking", () => {
    it("getAgentState returns the tracked value, or UNAVAILABLE for an unknown id", () => {
      // Subscribing to the observable getter is what creates the tracking entry.
      service.getAgentStateObservable("agent-1").subscribe();
      (service as any).agentStateTracking.get("agent-1").stateSubject.next(AgentState.GENERATING);

      let tracked: AgentState | undefined;
      service.getAgentState("agent-1").subscribe(s => (tracked = s));
      expect(tracked).toBe(AgentState.GENERATING);

      let unknown: AgentState | undefined;
      service.getAgentState("ghost").subscribe(s => (unknown = s));
      expect(unknown).toBe(AgentState.UNAVAILABLE);
    });

    it("isAgentConnected is false for an unavailable/unknown agent and true otherwise", () => {
      let unknown: boolean | undefined;
      service.isAgentConnected("ghost").subscribe(v => (unknown = v));
      expect(unknown).toBe(false);

      service.getAgentStateObservable("agent-1").subscribe();
      (service as any).agentStateTracking.get("agent-1").stateSubject.next(AgentState.AVAILABLE);
      let connected: boolean | undefined;
      service.isAgentConnected("agent-1").subscribe(v => (connected = v));
      expect(connected).toBe(true);
    });

    it("getHeadId / getHeadIdObservable return the tracked HEAD, or null when untracked", () => {
      const emitted: (string | null)[] = [];
      service.getHeadIdObservable("agent-1").subscribe(v => emitted.push(v));
      (service as any).agentStateTracking.get("agent-1").headIdSubject.next("m1-2");

      expect(emitted).toEqual([null, "m1-2"]);
      expect(service.getHeadId("agent-1")).toBe("m1-2");
      expect(service.getHeadId("ghost")).toBeNull();
    });

    it("getVisibleSteps returns the tracked snapshot, or [] when untracked", () => {
      service.getReActStepsObservable("agent-1").subscribe();
      const step = { messageId: "m1", stepId: 0 } as unknown as ReActStep;
      (service as any).agentStateTracking.get("agent-1").reActStepsSubject.next([step]);

      expect(service.getVisibleSteps("agent-1")).toEqual([step]);
      expect(service.getVisibleSteps("ghost")).toEqual([]);
    });

    it("getWorkflowObservable emits the tracked workflow, or null when untracked", () => {
      service.getAgentStateObservable("agent-1").subscribe();
      (service as any).agentStateTracking.get("agent-1").workflowSubject.next(stubWorkflow);
      let tracked: Workflow | null | undefined;
      service.getWorkflowObservable("agent-1").subscribe(w => (tracked = w));
      expect(tracked).toBe(stubWorkflow);

      let untracked: Workflow | null | undefined = stubWorkflow;
      service.getWorkflowObservable("ghost").subscribe(w => (untracked = w));
      expect(untracked).toBeNull();
    });

    it("getAgentWorkflowId walks the delegate chain: missing agent, no delegate, full", () => {
      expect(service.getAgentWorkflowId("ghost")).toBeUndefined();

      (service as any).agents.set("no-delegate", { id: "no-delegate", name: "x" });
      expect(service.getAgentWorkflowId("no-delegate")).toBeUndefined();

      seedAgent("with-wf", 42);
      expect(service.getAgentWorkflowId("with-wf")).toBe(42);
    });

    it("getAgentCount reports the number of registered agents", () => {
      let empty: number | undefined;
      service.getAgentCount().subscribe(c => (empty = c));
      expect(empty).toBe(0);

      seedAgent("agent-1");
      seedAgent("agent-2");
      let count: number | undefined;
      service.getAgentCount().subscribe(c => (count = c));
      expect(count).toBe(2);
    });
  });

  describe("setHoveredMessage guards", () => {
    it("emits empty arrays for a non-null step that carries no operator access", () => {
      let latest:
        | { viewedOperatorIds: string[]; addedOperatorIds: string[]; modifiedOperatorIds: string[] }
        | undefined;
      service.getHoveredMessageOperatorsObservable("agent-1").subscribe(v => (latest = v));

      // A real step with no operatorAccess map takes the else-branch, distinct from
      // the null-step case already covered above.
      service.setHoveredMessage("agent-1", { messageId: "m1" } as unknown as ReActStep);

      expect(latest).toEqual({ viewedOperatorIds: [], addedOperatorIds: [], modifiedOperatorIds: [] });
    });

    it("is a no-op when the agent has no tracking", () => {
      // No tracking entry exists for this id, so the method must return without throwing.
      expect(() => service.setHoveredMessage("ghost", { messageId: "m1" } as unknown as ReActStep)).not.toThrow();
    });
  });

  describe("getReActStepsByOperatorAccess without operator access", () => {
    it("skips steps whose operatorAccess is absent", () => {
      let result: { viewedBy: ReActStep[]; modifiedBy: ReActStep[] } | undefined;
      service.getReActStepsByOperatorAccess("agent-1", "op-7").subscribe(r => (result = r));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps")
        .flush({
          state: "AVAILABLE",
          steps: [{ messageId: "no-access", timestamp: "2026-06-11T00:00:00.000Z" }],
        });

      expect(result).toEqual({ viewedBy: [], modifiedBy: [] });
    });
  });

  describe("mapStateToAgentState", () => {
    it("maps STOPPING, UNAVAILABLE and unrecognised backend states", () => {
      let mapped: AgentInfo[] | undefined;
      service.getAllAgents().subscribe(a => (mapped = a));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush({
          agents: [
            { ...apiAgent, id: "s", state: "STOPPING" },
            { ...apiAgent, id: "u", state: "UNAVAILABLE" },
            { ...apiAgent, id: "z", state: "NOT_A_REAL_STATE" },
          ],
        });

      const byId = new Map(mapped!.map(a => [a.id, a.state]));
      expect(byId.get("s")).toBe(AgentState.STOPPING);
      expect(byId.get("u")).toBe(AgentState.UNAVAILABLE);
      // Anything unrecognised falls through the default arm to UNAVAILABLE.
      expect(byId.get("z")).toBe(AgentState.UNAVAILABLE);
    });
  });

  // ---------------------------------------------------------------------------
  // HTTP failure paths and their fallback chains
  // ---------------------------------------------------------------------------
  describe("createAgent failure fallback chain", () => {
    it("prefers the nested error.error message", () => {
      let message: string | undefined;
      service.createAgent("gpt-5-mini").subscribe({ error: (e: unknown) => (message = (e as Error).message) });
      httpMock
        .expectOne(r => r.method === "POST" && r.url === "/api/agents")
        .flush({ error: "nested boom" }, { status: 400, statusText: "Bad Request" });

      expect(message).toBe("nested boom");
      expect(notification.error).toHaveBeenCalledWith("nested boom");
    });

    it("falls back to err.message when there is no nested error", () => {
      let message: string | undefined;
      service.createAgent("gpt-5-mini").subscribe({ error: (e: unknown) => (message = (e as Error).message) });
      // A body with no `.error` field leaves err.error?.error undefined, so the
      // HttpErrorResponse's own message is used.
      httpMock
        .expectOne(r => r.method === "POST" && r.url === "/api/agents")
        .flush({}, { status: 500, statusText: "Server Error" });

      expect(message).toContain("Http failure response");
      expect(notification.error).toHaveBeenCalledWith(message);
    });

    it("falls back to the default label when the error carries neither field", () => {
      // An HttpErrorResponse always has a message, so the default literal is only
      // reachable for a non-HTTP error; a bare object exercises that last arm.
      vi.spyOn((service as any).http, "post").mockReturnValue(throwError(() => ({})));

      let message: string | undefined;
      service.createAgent("gpt-5-mini").subscribe({ error: (e: unknown) => (message = (e as Error).message) });

      expect(message).toBe("Failed to create agent");
      expect(notification.error).toHaveBeenCalledWith("Failed to create agent");
    });
  });

  describe("updateAgentSettings edge cases", () => {
    it("returns the response without touching the cache when the agent is unknown", () => {
      let updated: AgentSettingsApi | undefined;
      service.updateAgentSettings("uncached", { maxSteps: 9 }).subscribe(s => (updated = s));

      httpMock.expectOne(r => r.method === "PATCH" && r.url === "/api/agents/uncached/settings").flush({ maxSteps: 9 });

      expect(updated).toEqual({ maxSteps: 9 });
      expect((service as any).agents.has("uncached")).toBe(false);
    });

    it("falls back to the default label when the failure carries neither field", () => {
      vi.spyOn((service as any).http, "patch").mockReturnValue(throwError(() => ({})));

      let message: string | undefined;
      service
        .updateAgentSettings("agent-1", { maxSteps: 3 })
        .subscribe({ error: (e: unknown) => (message = (e as Error).message) });

      expect(message).toBe("Failed to update agent settings");
      expect(notification.error).toHaveBeenCalledWith("Failed to update agent settings");
    });
  });

  describe("getReActSteps failure", () => {
    it("emits an empty array instead of propagating the error", () => {
      let steps: ReActStep[] | undefined;
      service.getReActSteps("agent-1").subscribe(s => (steps = s));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents/agent-1/react-steps")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(steps).toEqual([]);
    });
  });

  describe("syncAgentsWithBackend branches", () => {
    it("swallows a failed sync and treats the backend as empty", () => {
      seedAgent("agent-1");

      (service as any).syncAgentsWithBackend();
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush("boom", { status: 500, statusText: "Server Error" });

      // catchError substitutes { agents: [] }, so the local agent is evicted.
      expect((service as any).agents.size).toBe(0);
    });

    it("does not add a backend agent that is not already cached locally", () => {
      seedAgent("agent-1");

      (service as any).syncAgentsWithBackend();
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush({ agents: [{ ...apiAgent, id: "agent-2", state: "GENERATING" }] });

      // agent-1 is gone (not on the backend) and agent-2 is not merged in: sync only
      // updates agents it already knows about.
      expect((service as any).agents.has("agent-1")).toBe(false);
      expect((service as any).agents.has("agent-2")).toBe(false);
    });

    it("updates an existing agent's state even when it has no tracking entry", () => {
      // An agent in the cache with no tracking entry exercises the `if (tracking)` guard's
      // false side while still taking the `if (existingAgent)` true side.
      (service as any).agents.set("agent-1", { id: "agent-1", name: "Bob", state: AgentState.AVAILABLE });

      (service as any).syncAgentsWithBackend();
      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush({ agents: [{ ...apiAgent, id: "agent-1", state: "GENERATING" }] });

      expect((service as any).agents.get("agent-1").state).toBe(AgentState.GENERATING);
    });
  });

  describe("getAllAgents keeps agents still present on the backend", () => {
    it("does not prune a local agent that the backend still reports", () => {
      seedAgent("agent-1");
      let result: AgentInfo[] | undefined;
      service.getAllAgents().subscribe(r => (result = r));

      httpMock
        .expectOne(r => r.method === "GET" && r.url === "/api/agents")
        .flush({
          agents: [
            { ...apiAgent, id: "agent-1", state: "AVAILABLE" },
            { ...apiAgent, id: "agent-2", state: "GENERATING" },
          ],
        });

      expect(result?.map(a => a.id).sort()).toEqual(["agent-1", "agent-2"]);
      expect((service as any).agents.has("agent-1")).toBe(true);
    });
  });

  describe("stopGeneration error handlers", () => {
    it("logs when the websocket send throws", () => {
      const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      (service as any).agentStateTracking.set("agent-1", {
        websocket: {
          readyState: WebSocket.OPEN,
          send: vi.fn(() => {
            throw new Error("send failed");
          }),
        },
      });

      service.stopGeneration("agent-1");

      expect(errSpy).toHaveBeenCalledWith("Failed to send stop command:", expect.any(Error));
      errSpy.mockRestore();
    });

    it("logs when the REST fallback fails", () => {
      const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      (service as any).agentStateTracking.set("agent-1", {
        websocket: { readyState: WebSocket.CLOSED, send: vi.fn() },
      });

      service.stopGeneration("agent-1");
      httpMock
        .expectOne(r => r.method === "POST" && r.url === "/api/agents/agent-1/stop")
        .flush("boom", { status: 500, statusText: "Server Error" });

      expect(errSpy).toHaveBeenCalledWith("Error stopping agent agent-1:", expect.anything());
      errSpy.mockRestore();
    });
  });

  describe("getOrCreateStateTracking back-fills a workflow id", () => {
    it("adds a workflow id onto tracking that was created without one", () => {
      // First create tracking with no workflow id.
      service.getAgentStateObservable("agent-1").subscribe();
      expect((service as any).agentStateTracking.get("agent-1").workflowId).toBeUndefined();

      // ensureWorkflowPolling re-enters getOrCreateStateTracking with an id, hitting the
      // else-if that back-fills it.
      service.ensureWorkflowPolling("agent-1", 55);
      expect((service as any).agentStateTracking.get("agent-1").workflowId).toBe(55);

      // The back-filled id now rides on outgoing requests.
      service.getAgentSettings("agent-1").subscribe();
      const req = httpMock.expectOne(r => r.url === "/api/agents/agent-1/settings");
      expect(req.request.headers.get("X-Agent-Workflow-Id")).toBe("55");
      req.flush({});
    });
  });
});
