import { TestBed } from "@angular/core/testing";
import { UdfDebugService } from "./udf-debug.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { Observable, Subject } from "rxjs";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { WorkflowGraphReadonly } from "../workflow-graph/model/workflow-graph";
import { mockPoint, mockPythonUDFPredicate } from "../workflow-graph/model/mock-workflow-data";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import * as Y from "yjs";
import { ConsoleUpdateEvent } from "../../types/workflow-common.interface";
import { TexeraWebsocketEvent } from "../../types/workflow-websocket.interface";

describe("UdfDebugServiceSpec", () => {
  let service: UdfDebugService;
  let workflowActionService: WorkflowActionService;
  let mockWorkflowWebsocketService: jasmine.SpyObj<WorkflowWebsocketService>;
  let mockWorkflowStatusService: jasmine.SpyObj<WorkflowStatusService>;
  let mockExecuteWorkflowService: jasmine.SpyObj<ExecuteWorkflowService>;
  let statusUpdateStream: Subject<Record<string, OperatorStatistics>>;
  let consoleUpdateEventStream: Subject<ConsoleUpdateEvent>;
  let texeraGraph: WorkflowGraphReadonly;
  let stubWorker = "worker1";

  beforeEach(() => {
    // Create mock services
    mockWorkflowWebsocketService = jasmine.createSpyObj("WorkflowWebsocketService", ["send", "subscribeToEvent"]);
    mockWorkflowStatusService = jasmine.createSpyObj("WorkflowStatusService", ["getStatusUpdateStream"]);
    mockExecuteWorkflowService = jasmine.createSpyObj("ExecuteWorkflowService", ["getWorkerIds"]);

    // Initialize the mock streams
    statusUpdateStream = new Subject();
    consoleUpdateEventStream = new Subject();

    // Set mock return values
    mockWorkflowStatusService.getStatusUpdateStream.and.returnValue(statusUpdateStream.asObservable());
    mockWorkflowWebsocketService.subscribeToEvent.and.returnValue(
      consoleUpdateEventStream.asObservable() as Observable<TexeraWebsocketEvent>
    );
    mockExecuteWorkflowService.getWorkerIds.and.returnValue([stubWorker]);

    // Configure the TestBed
    TestBed.configureTestingModule({
      providers: [
        UdfDebugService,
        WorkflowActionService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: WorkflowWebsocketService, useValue: mockWorkflowWebsocketService },
        { provide: WorkflowStatusService, useValue: mockWorkflowStatusService },
        { provide: ExecuteWorkflowService, useValue: mockExecuteWorkflowService },
      ],
    });

    workflowActionService = TestBed.inject(WorkflowActionService);
    texeraGraph = workflowActionService.getTexeraGraph();
    workflowActionService.addOperator(mockPythonUDFPredicate, mockPoint);
    // Spy on the necessary methods
    spyOn(texeraGraph, "createOperatorDebugState").and.callThrough();
    spyOn(texeraGraph, "getOperatorDebugState").and.callThrough();

    service = TestBed.inject(UdfDebugService);
  });

  afterEach(() => {
    // Clean up the streams after each test
    statusUpdateStream.complete();
    consoleUpdateEventStream.complete();
  });

  it("should initialize debug handlers on service creation", () => {
    expect(texeraGraph.createOperatorDebugState).toHaveBeenCalledWith(mockPythonUDFPredicate.operatorID);
  });

  it("should retrieve the debug state of an operator", () => {
    const state = service.getDebugState(mockPythonUDFPredicate.operatorID);
    expect(state).toBeInstanceOf(Y.Map);
    expect(state.size).toBe(0); // Initially empty
  });

  it("should get the condition of a breakpoint", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });

    const condition = service.getCondition(mockPythonUDFPredicate.operatorID, 1);
    expect(condition).toBe("x > 5");
  });

  it("should return empty string if condition does not exist", () => {
    const condition = service.getCondition(mockPythonUDFPredicate.operatorID, 2);
    expect(condition).toBe("");
  });

  it("should update the breakpoint condition if different", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });

    service.doUpdateBreakpointCondition(mockPythonUDFPredicate.operatorID, 1, "x < 10");

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "condition 1 x < 10",
    });

    expect(debugState.get("1")?.condition).toBe("x < 10");
  });

  it("should not update the breakpoint condition if it is the same", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });

    service.doUpdateBreakpointCondition(mockPythonUDFPredicate.operatorID, 1, "x > 5");

    expect(mockWorkflowWebsocketService.send).not.toHaveBeenCalled();
  });

  it("should modify a breakpoint (remove existing)", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "", hit: false });

    service.doModifyBreakpoint(mockPythonUDFPredicate.operatorID, 1);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "clear 1",
    });

    expect(debugState.has("1")).toBeTrue(); // The state is supposed to be cleared later by console update events.
  });

  it("should modify a breakpoint (add new)", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);

    service.doModifyBreakpoint(mockPythonUDFPredicate.operatorID, 10);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "break 10",
    });

    // it should change the state yet
    expect(debugState.has("10")).toBeFalse();
  });

  it("should continue the workflow execution", () => {
    service.doContinue(mockPythonUDFPredicate.operatorID, stubWorker);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "continue",
    });
  });

  it("should step through the workflow execution", () => {
    service.doStep(mockPythonUDFPredicate.operatorID, stubWorker);

    expect(mockWorkflowWebsocketService.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: mockPythonUDFPredicate.operatorID,
      workerId: stubWorker,
      cmd: "next",
    });
  });

  it("should clear the debug state on state change to Uninitialized", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    const operatorId = mockPythonUDFPredicate.operatorID;
    debugState.set(operatorId, { breakpointId: 1, condition: "x > 5", hit: false });
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Uninitialized,
        aggregatedInputRowCount: 0,
        aggregatedOutputRowCount: 0,
      },
    });

    expect(debugState.size).toBe(0);
  });

  it("should handle console update events (breakpoint creation)", () => {
    const message: ConsoleUpdateEvent = {
      operatorId: mockPythonUDFPredicate.operatorID,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "Breakpoint 1 at /path/to/file.py:10",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    consoleUpdateEventStream.next(message);

    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    expect(debugState.get("10")).toEqual({ breakpointId: 1, condition: "", hit: false });
  });

  it("should handle console update events (breakpoint deletion)", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("10", { breakpointId: 1, condition: "", hit: false });

    const message = {
      operatorId: mockPythonUDFPredicate.operatorID,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "Deleted breakpoint 1 at /path/to/file.py:10",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    consoleUpdateEventStream.next(message);

    expect(debugState.has("10")).toBeFalse();
  });

  it("should handle console update events (stepping message)", () => {
    spyOn(service as any, "markBreakpointAsHit").and.callThrough();

    const message = {
      operatorId: mockPythonUDFPredicate.operatorID,
      messages: [
        {
          workerId: stubWorker,
          timestamp: { nanos: 0, seconds: 0 },
          title: "> /path/to/file.py(10)<module>()",
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          message: "",
        },
      ],
    };

    consoleUpdateEventStream.next(message);

    expect((service as UdfDebugService)["markBreakpointAsHit"]).toHaveBeenCalledWith(
      mockPythonUDFPredicate.operatorID,
      10
    );
  });

  it("should mark a breakpoint as hit", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);

    service["markBreakpointAsHit"](mockPythonUDFPredicate.operatorID, 10);

    expect(debugState.get("10")).toEqual({ breakpointId: undefined, condition: "", hit: true });
  });

  it("should mark continue by resetting hit statuses and removing temporary breakpoints", () => {
    const debugState = service.getDebugState(mockPythonUDFPredicate.operatorID);
    debugState.set("1", { breakpointId: 1, condition: "x > 5", hit: false });
    debugState.set("2", { breakpointId: undefined, condition: "", hit: true }); // Temporary breakpoint

    service["markContinue"](mockPythonUDFPredicate.operatorID);

    expect(debugState.get("1")).toEqual({ breakpointId: 1, condition: "x > 5", hit: false });
    expect(debugState.has("2")).toBeFalse();
  });
});
