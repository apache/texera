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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { of, throwError } from "rxjs";
import { ComputingUnitStatusService } from "./computing-unit-status.service";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { WorkflowWebsocketService } from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import { WorkflowStatusService } from "../../../../workspace/service/workflow-status/workflow-status.service";
import { UserService } from "../../user/user.service";
import { StubUserService } from "../../user/stub-user.service";
import { AuthService } from "../../user/auth.service";
import { StubAuthService } from "../../user/stub-auth.service";
import { DashboardWorkflowComputingUnit } from "../../../type/workflow-computing-unit";
import { ComputingUnitState } from "../../../type/computing-unit-connection.interface";
import { commonTestProviders } from "../../../testing/test-utils";

describe("ComputingUnitStatusService", () => {
  let service: ComputingUnitStatusService;
  let websocketService: WorkflowWebsocketService;

  const mockUnit = (cuid: number) => ({ computingUnit: { cuid } }) as unknown as DashboardWorkflowComputingUnit;

  beforeEach(() => {
    const managingStub = {
      listComputingUnits: () => of([]),
      getComputingUnit: (cuid: number) => of(mockUnit(cuid)),
      terminateComputingUnit: () => of(undefined),
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        ComputingUnitStatusService,
        WorkflowWebsocketService,
        WorkflowStatusService,
        { provide: WorkflowComputingUnitManagingService, useValue: managingStub },
        { provide: UserService, useClass: StubUserService },
        { provide: AuthService, useClass: StubAuthService },
        ...commonTestProviders,
      ],
    });

    service = TestBed.inject(ComputingUnitStatusService);
    websocketService = TestBed.inject(WorkflowWebsocketService);
  });

  afterEach(() => {
    // tear down the interval poll started by selectComputingUnit() so it can't outlive the test
    service.ngOnDestroy();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("reconnects when re-selecting the same workflow after disconnect (regression #3120)", () => {
    const openSpy = vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
    const closeSpy = vi.spyOn(websocketService, "closeWebsocket");
    (service as any).allComputingUnitsSubject.next([mockUnit(7)]);

    // Enter workflow 5 on computing unit 7 → opens the websocket once.
    service.selectComputingUnit(5, 7);
    expect(openSpy).toHaveBeenCalledTimes(1);

    // User returns to the dashboard.
    service.disconnect();
    expect(closeSpy).toHaveBeenCalled();

    // Re-enter the SAME workflow (the `wid -> null -> wid` pattern): without the
    // cleanup, the retained currentConnectedWid/Cuid would suppress the reconnect.
    service.selectComputingUnit(5, 7);
    expect(openSpy).toHaveBeenCalledTimes(2);
  });

  it("disconnect() clears the selected computing unit", () => {
    vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
    (service as any).allComputingUnitsSubject.next([mockUnit(7)]);
    service.selectComputingUnit(5, 7);

    let latest: DashboardWorkflowComputingUnit | null = mockUnit(7);
    service.getSelectedComputingUnit().subscribe(unit => (latest = unit));
    expect(latest).not.toBeNull();

    service.disconnect();
    expect(latest).toBeNull();
  });

  it("emits a connection-reset signal when switching to a different computing unit (issue #3120)", () => {
    let connected = false;
    vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {
      connected = true;
    });
    vi.spyOn(websocketService, "closeWebsocket").mockImplementation(() => {
      connected = false;
    });
    vi.spyOn(websocketService, "isConnected", "get").mockImplementation(() => connected);
    (service as any).allComputingUnitsSubject.next([mockUnit(7), mockUnit(8)]);

    let resetCount = 0;
    service.getConnectionResetStream().subscribe(() => resetCount++);

    // First connection on unit 7: nothing to tear down yet → no signal.
    service.selectComputingUnit(5, 7);
    expect(resetCount).toBe(0);

    // Switch to a different unit while connected → tear-down signal fires once.
    service.selectComputingUnit(5, 8);
    expect(resetCount).toBe(1);
  });

  it("emits a connection-reset signal when switching units even if the socket already dropped (issue #3120)", () => {
    vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
    vi.spyOn(websocketService, "closeWebsocket").mockImplementation(() => {});
    // socket reports disconnected throughout, e.g. the previous unit was terminated
    vi.spyOn(websocketService, "isConnected", "get").mockReturnValue(false);
    (service as any).allComputingUnitsSubject.next([mockUnit(7), mockUnit(8)]);

    let resetCount = 0;
    service.getConnectionResetStream().subscribe(() => resetCount++);

    // First connection on unit 7: nothing to tear down yet → no signal.
    service.selectComputingUnit(5, 7);
    expect(resetCount).toBe(0);

    // Switch units while disconnected: unit 7's stale state must still be cleared.
    service.selectComputingUnit(5, 8);
    expect(resetCount).toBe(1);
  });

  it("getAllComputingUnits() replays the current list and forwards later updates", () => {
    const emissions: DashboardWorkflowComputingUnit[][] = [];
    service.getAllComputingUnits().subscribe(units => emissions.push(units));

    // allComputingUnitsSubject is a BehaviorSubject initialized with [], so it replays that value.
    expect(emissions[0]).toEqual([]);

    const units = [mockUnit(1), mockUnit(2)];
    (service as any).allComputingUnitsSubject.next(units);
    expect(emissions[emissions.length - 1]).toBe(units);
  });

  it("getSelectedComputingUnitValue() returns null before any unit is selected", () => {
    expect(service.getSelectedComputingUnitValue()).toBeNull();
  });

  it("getSelectedComputingUnitValue() reflects the unit chosen via selectComputingUnit()", () => {
    vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
    const unit = mockUnit(7);
    (service as any).allComputingUnitsSubject.next([unit]);

    service.selectComputingUnit(5, 7);

    expect(service.getSelectedComputingUnitValue()).toBe(unit);
  });

  it("getStatus() maps a null selection to NoComputingUnit", () => {
    let status: ComputingUnitState | undefined;
    service.getStatus().subscribe(s => (status = s));
    expect(status).toBe(ComputingUnitState.NoComputingUnit);
  });

  it("getStatus() maps a Running unit to ComputingUnitState.Running", () => {
    (service as any).selectedUnitSubject.next({
      computingUnit: { cuid: 1 },
      status: "Running",
    } as unknown as DashboardWorkflowComputingUnit);

    let status: ComputingUnitState | undefined;
    service.getStatus().subscribe(s => (status = s));
    expect(status).toBe(ComputingUnitState.Running);
  });

  it("getStatus() maps a Pending unit to ComputingUnitState.Pending", () => {
    (service as any).selectedUnitSubject.next({
      computingUnit: { cuid: 1 },
      status: "Pending",
    } as unknown as DashboardWorkflowComputingUnit);

    let status: ComputingUnitState | undefined;
    service.getStatus().subscribe(s => (status = s));
    expect(status).toBe(ComputingUnitState.Pending);
  });

  it("getStatus() maps an unrecognized status to Pending (default branch)", () => {
    (service as any).selectedUnitSubject.next({
      computingUnit: { cuid: 1 },
      status: "Terminating",
    } as unknown as DashboardWorkflowComputingUnit);

    let status: ComputingUnitState | undefined;
    service.getStatus().subscribe(s => (status = s));
    expect(status).toBe(ComputingUnitState.Pending);
  });

  it("refreshComputingUnitList() re-fetches the list and pushes it to subscribers", () => {
    const managing = TestBed.inject(WorkflowComputingUnitManagingService);
    const newUnits = [mockUnit(42)];
    const listSpy = vi.spyOn(managing, "listComputingUnits").mockReturnValue(of(newUnits));

    let latest: DashboardWorkflowComputingUnit[] = [];
    service.getAllComputingUnits().subscribe(units => (latest = units));

    service.refreshComputingUnitList();

    expect(listSpy).toHaveBeenCalled();
    expect(latest).toEqual(newUnits);
  });

  it("updateUnitInList replaces the matching unit and leaves the others untouched", () => {
    const unitA = mockUnit(1);
    const unitB = mockUnit(2);
    (service as any).allComputingUnitsSubject.next([unitA, unitB]);

    const updatedA = { computingUnit: { cuid: 1 }, status: "Running" } as unknown as DashboardWorkflowComputingUnit;
    (service as any).updateUnitInList(updatedA);

    expect((service as any).allComputingUnitsSubject.value).toEqual([updatedA, unitB]);
  });

  it("setComputingUnitsState refreshes the selected unit when it is still present in the new list", () => {
    (service as any).selectedUnitSubject.next(mockUnit(7));

    const updated = { computingUnit: { cuid: 7 }, status: "Running" } as unknown as DashboardWorkflowComputingUnit;
    (service as any).setComputingUnitsState([updated]);

    expect(service.getSelectedComputingUnitValue()).toBe(updated);
  });

  it("setComputingUnitsState clears the selection and stops polling when the selected unit disappears", () => {
    (service as any).selectedUnitSubject.next(mockUnit(7));
    const stopSpy = vi.spyOn(service as any, "stopPollingSelectedUnit");

    (service as any).setComputingUnitsState([mockUnit(8)]);

    expect(service.getSelectedComputingUnitValue()).toBeNull();
    expect(stopSpy).toHaveBeenCalled();
  });

  it("startPollingSelectedUnit polls the unit on each interval tick and merges the result", () => {
    vi.useFakeTimers();
    try {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const polled = { computingUnit: { cuid: 3 }, status: "Running" } as unknown as DashboardWorkflowComputingUnit;
      const getSpy = vi.spyOn(managing, "getComputingUnit").mockReturnValue(of(polled));
      (service as any).allComputingUnitsSubject.next([mockUnit(3)]);

      (service as any).startPollingSelectedUnit(3);
      // interval() fires only after the first period elapses
      expect(getSpy).not.toHaveBeenCalled();

      vi.advanceTimersByTime((service as any).REFRESH_INTERVAL_MS);

      expect(getSpy).toHaveBeenCalledWith(3);
      expect((service as any).allComputingUnitsSubject.value).toEqual([polled]);
    } finally {
      // Stop the interval poll here so the test is self-contained rather than
      // relying on afterEach's ngOnDestroy to tear it down.
      (service as any).stopPollingSelectedUnit();
      vi.useRealTimers();
    }
  });

  it("stopPollingSelectedUnit halts further polling", () => {
    vi.useFakeTimers();
    try {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const getSpy = vi.spyOn(managing, "getComputingUnit").mockReturnValue(of(mockUnit(3)));

      (service as any).startPollingSelectedUnit(3);
      (service as any).stopPollingSelectedUnit();

      vi.advanceTimersByTime(10000);

      expect(getSpy).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  describe("selectComputingUnit() cache-miss and guard branches", () => {
    it("refreshes then selects a unit that is not yet in the cache", () => {
      const openSpy = vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const unit9 = mockUnit(9);
      // The cache starts empty, so cuid 9 is a miss: selection must trigger a
      // refresh and wait for the unit to appear before opening the socket.
      vi.spyOn(managing, "listComputingUnits").mockReturnValue(of([unit9]));

      service.selectComputingUnit(5, 9);

      const uid = TestBed.inject(UserService).getCurrentUser()?.uid;
      expect(uid).not.toBeUndefined();
      expect(openSpy).toHaveBeenCalledWith(5, uid, 9);
      expect(service.getSelectedComputingUnitValue()).toBe(unit9);
    });

    it("does not reopen the websocket when the identical unit is re-selected", () => {
      const openSpy = vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
      (service as any).allComputingUnitsSubject.next([mockUnit(7)]);

      service.selectComputingUnit(5, 7);
      // Same wid + cuid → shouldReconnect is false → the guarded block is skipped.
      service.selectComputingUnit(5, 7);

      expect(openSpy).toHaveBeenCalledTimes(1);
    });

    it("does nothing when the workflow id is undefined", () => {
      const openSpy = vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
      (service as any).allComputingUnitsSubject.next([mockUnit(7)]);

      // The unit is cached and shouldReconnect is true, so only the isDefined(wid)
      // guard can stop the selection: no socket is opened and nothing is selected.
      service.selectComputingUnit(undefined, 7);

      expect(openSpy).not.toHaveBeenCalled();
      expect(service.getSelectedComputingUnitValue()).toBeNull();
      expect((service as any).currentConnectedWid).toBeUndefined();
      expect((service as any).currentConnectedCuid).toBeUndefined();
    });

    it("reconnects when the same unit is opened under a different workflow (wid change alone)", () => {
      const openSpy = vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
      (service as any).allComputingUnitsSubject.next([mockUnit(7)]);

      service.selectComputingUnit(5, 7);
      expect(openSpy).toHaveBeenCalledTimes(1);

      // Same cuid (7) but a different wid (6): the currentConnectedCuid check is
      // false, so the currentConnectedWid !== wid operand must force the reconnect.
      service.selectComputingUnit(6, 7);
      expect(openSpy).toHaveBeenCalledTimes(2);
      expect((service as any).currentConnectedWid).toBe(6);
    });
  });

  it("startRefreshInterval() tears down an existing refresh subscription before creating a new one", () => {
    // The constructor already started one refresh subscription.
    const previous = (service as any).refreshSubscription;
    expect(previous).toBeTruthy();
    expect(previous.closed).toBe(false);

    (service as any).startRefreshInterval();

    // The prior subscription is unsubscribed and replaced with a fresh one.
    expect(previous.closed).toBe(true);
    expect((service as any).refreshSubscription).not.toBe(previous);
    expect((service as any).refreshSubscription.closed).toBe(false);
  });

  describe("terminateComputingUnit()", () => {
    it("closes the socket and clears status when terminating the connected selection", () => {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const workflowStatusService = TestBed.inject(WorkflowStatusService);
      const closeSpy = vi.spyOn(websocketService, "closeWebsocket").mockImplementation(() => {});
      const clearSpy = vi.spyOn(workflowStatusService, "clearStatus").mockImplementation(() => {});
      vi.spyOn(websocketService, "isConnected", "get").mockReturnValue(true);
      const termSpy = vi.spyOn(managing, "terminateComputingUnit").mockReturnValue(of({} as Response));
      const refreshSpy = vi.spyOn(service, "refreshComputingUnitList").mockImplementation(() => {});
      (service as any).selectedUnitSubject.next(mockUnit(7));

      let result: boolean | undefined;
      service.terminateComputingUnit(7).subscribe(r => (result = r));

      expect(closeSpy).toHaveBeenCalled();
      expect(clearSpy).toHaveBeenCalled();
      expect(termSpy).toHaveBeenCalledWith(7);
      // The tap() side effect requests a single list refresh.
      expect(refreshSpy).toHaveBeenCalled();
      expect(result).toBe(true);
    });

    it("leaves the socket untouched when terminating a non-selected unit", () => {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const closeSpy = vi.spyOn(websocketService, "closeWebsocket").mockImplementation(() => {});
      // isConnected is true, but isSelected short-circuits the guard to false.
      vi.spyOn(websocketService, "isConnected", "get").mockReturnValue(true);
      vi.spyOn(managing, "terminateComputingUnit").mockReturnValue(of({} as Response));
      vi.spyOn(service, "refreshComputingUnitList").mockImplementation(() => {});
      (service as any).selectedUnitSubject.next(mockUnit(8));

      let result: boolean | undefined;
      service.terminateComputingUnit(7).subscribe(r => (result = r));

      expect(closeSpy).not.toHaveBeenCalled();
      expect(result).toBe(true);
    });

    it("skips socket teardown when the selected unit is not connected", () => {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const closeSpy = vi.spyOn(websocketService, "closeWebsocket").mockImplementation(() => {});
      // isSelected is true, but the socket is down → the guarded teardown is skipped.
      vi.spyOn(websocketService, "isConnected", "get").mockReturnValue(false);
      vi.spyOn(managing, "terminateComputingUnit").mockReturnValue(of({} as Response));
      vi.spyOn(service, "refreshComputingUnitList").mockImplementation(() => {});
      (service as any).selectedUnitSubject.next(mockUnit(7));

      let result: boolean | undefined;
      service.terminateComputingUnit(7).subscribe(r => (result = r));

      expect(closeSpy).not.toHaveBeenCalled();
      expect(result).toBe(true);
    });

    it("resolves to false when the termination request errors", () => {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      vi.spyOn(websocketService, "isConnected", "get").mockReturnValue(false);
      vi.spyOn(managing, "terminateComputingUnit").mockReturnValue(throwError(() => new Error("boom")));
      const refreshSpy = vi.spyOn(service, "refreshComputingUnitList").mockImplementation(() => {});
      (service as any).selectedUnitSubject.next(mockUnit(7));

      let result: boolean | undefined;
      service.terminateComputingUnit(7).subscribe(r => (result = r));

      // The error is swallowed by catchError, which emits false; no refresh runs.
      expect(result).toBe(false);
      expect(refreshSpy).not.toHaveBeenCalled();
    });
  });
});
