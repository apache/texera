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
import { HttpErrorResponse } from "@angular/common/http";
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

  const mockUnit = (cuid: number, status?: string) =>
    (status
      ? { computingUnit: { cuid }, status }
      : { computingUnit: { cuid } }) as unknown as DashboardWorkflowComputingUnit;
  // What HttpClient delivers for a failed request; a dropped connection is the same type with status 0.
  const httpFailure = () =>
    throwError(() => new HttpErrorResponse({ status: 500, statusText: "Internal Server Error" }));
  const spyOpenWs = () => vi.spyOn(websocketService, "openWebsocket").mockImplementation(() => {});
  const seedUnits = (units: DashboardWorkflowComputingUnit[], selected?: DashboardWorkflowComputingUnit): void => {
    (service as any).allComputingUnitsSubject.next(units);
    if (selected !== undefined) {
      (service as any).selectedUnitSubject.next(selected);
    }
  };
  const withFakePolling = (run: () => void): void => {
    vi.useFakeTimers();
    try {
      run();
    } finally {
      (service as any).stopPollingSelectedUnit();
      vi.useRealTimers();
    }
  };

  beforeEach(() => {
    const managingStub = {
      listComputingUnits: () => of([]),
      getComputingUnit: (cuid: number) => of(mockUnit(cuid)),
      terminateComputingUnit: () => of(undefined),
    };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
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

  (
    [
      [undefined, ComputingUnitState.NoComputingUnit, "getStatus() maps a null selection to NoComputingUnit"],
      ["Running", ComputingUnitState.Running, "getStatus() maps a Running unit to ComputingUnitState.Running"],
      ["Pending", ComputingUnitState.Pending, "getStatus() maps a Pending unit to ComputingUnitState.Pending"],
      ["Failed", ComputingUnitState.Failed, "getStatus() maps a Failed unit to ComputingUnitState.Failed"],
      ["Unknown", ComputingUnitState.Unknown, "getStatus() maps an Unknown unit to ComputingUnitState.Unknown"],
      [
        "Terminating",
        ComputingUnitState.Terminating,
        "getStatus() maps a Terminating unit to ComputingUnitState.Terminating",
      ],
      [
        "FutureStatus",
        ComputingUnitState.Pending,
        "getStatus() keeps an unrecognized future status on the Pending fallback",
      ],
    ] as const
  ).forEach(([unitStatus, expected, title]) => {
    it(title, () => {
      if (unitStatus !== undefined) {
        (service as any).selectedUnitSubject.next(mockUnit(1, unitStatus));
      }
      let status: ComputingUnitState | undefined;
      service.getStatus().subscribe(s => (status = s));
      expect(status).toBe(expected);
    });
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

  it("keeps refreshing the list through failed fetches: nothing on a failure, the good list on the next success", () => {
    const managing = TestBed.inject(WorkflowComputingUnitManagingService);
    const goodUnits = [mockUnit(42)];
    const listSpy = vi
      .spyOn(managing, "listComputingUnits")
      .mockReturnValueOnce(httpFailure())
      .mockReturnValueOnce(httpFailure())
      .mockReturnValue(of(goodUnits));

    const emissions: DashboardWorkflowComputingUnit[][] = [];
    service.getAllComputingUnits().subscribe(units => emissions.push(units));
    // The BehaviorSubject replays its current list on subscribe.
    expect(emissions.length).toBe(1);

    service.refreshComputingUnitList();
    service.refreshComputingUnitList();
    // A failed fetch produces no list emission at all, not an empty or null list.
    expect(emissions.length).toBe(1);

    service.refreshComputingUnitList();
    // The failed fetches did not end the refresh stream: every signal still fetched.
    expect(listSpy).toHaveBeenCalledTimes(3);
    expect(emissions.length).toBe(2);
    expect(emissions[1]).toEqual(goodUnits);
  });

  it("logs a non-HTTP list failure and keeps refreshing, but stays silent for an HTTP failure", () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const goodUnits = [mockUnit(42)];
      const bug = new TypeError("cannot read resource");
      const listSpy = vi
        .spyOn(managing, "listComputingUnits")
        .mockReturnValueOnce(throwError(() => bug))
        .mockReturnValueOnce(httpFailure())
        .mockReturnValue(of(goodUnits));

      let latest: DashboardWorkflowComputingUnit[] = [];
      service.getAllComputingUnits().subscribe(units => (latest = units));

      service.refreshComputingUnitList();
      // A failure outside HTTP, such as in the response mapping, is a bug and must stay visible.
      expect(errorSpy).toHaveBeenCalledTimes(1);
      expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("list refresh"), bug);

      service.refreshComputingUnitList();
      // An HTTP failure is expected while the backend restarts and is not logged.
      expect(errorSpy).toHaveBeenCalledTimes(1);

      service.refreshComputingUnitList();
      expect(listSpy).toHaveBeenCalledTimes(3);
      expect(latest).toEqual(goodUnits);
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("updateUnitInList replaces the matching unit and leaves the others untouched", () => {
    const unitA = mockUnit(1);
    const unitB = mockUnit(2);
    seedUnits([unitA, unitB]);

    const updatedA = mockUnit(1, "Running");
    (service as any).updateUnitInList(updatedA);

    expect((service as any).allComputingUnitsSubject.value).toEqual([updatedA, unitB]);
  });

  it("setComputingUnitsState refreshes the selected unit when it is still present in the new list", () => {
    seedUnits([], mockUnit(7));

    const updated = mockUnit(7, "Running");
    (service as any).setComputingUnitsState([updated]);

    expect(service.getSelectedComputingUnitValue()).toBe(updated);
  });

  it("setComputingUnitsState clears the selection and stops polling when the selected unit disappears", () => {
    seedUnits([], mockUnit(7));
    const stopSpy = vi.spyOn(service as any, "stopPollingSelectedUnit");

    (service as any).setComputingUnitsState([mockUnit(8)]);

    expect(service.getSelectedComputingUnitValue()).toBeNull();
    expect(stopSpy).toHaveBeenCalled();
  });

  it("startPollingSelectedUnit polls the unit on each interval tick and merges the result", () => {
    withFakePolling(() => {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const polled = mockUnit(3, "Running");
      const getSpy = vi.spyOn(managing, "getComputingUnit").mockReturnValue(of(polled));
      seedUnits([mockUnit(3)]);

      (service as any).startPollingSelectedUnit(3);
      // interval() fires only after the first period elapses
      expect(getSpy).not.toHaveBeenCalled();

      vi.advanceTimersByTime((service as any).REFRESH_INTERVAL_MS);

      expect(getSpy).toHaveBeenCalledWith(3);
      expect((service as any).allComputingUnitsSubject.value).toEqual([polled]);
    });
  });

  it("stopPollingSelectedUnit halts further polling", () => {
    withFakePolling(() => {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const getSpy = vi.spyOn(managing, "getComputingUnit").mockReturnValue(of(mockUnit(3)));

      (service as any).startPollingSelectedUnit(3);
      (service as any).stopPollingSelectedUnit();

      vi.advanceTimersByTime(10000);

      expect(getSpy).not.toHaveBeenCalled();
    });
  });

  it("keeps polling the selected unit through a failed fetch and leaves the selection intact", () => {
    withFakePolling(() => {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const original = mockUnit(3);
      const polled = mockUnit(3, "Running");
      const getSpy = vi
        .spyOn(managing, "getComputingUnit")
        .mockReturnValueOnce(httpFailure())
        .mockReturnValue(of(polled));
      const stopSpy = vi.spyOn(service as any, "stopPollingSelectedUnit");
      seedUnits([original], original);

      (service as any).startPollingSelectedUnit(3);
      vi.advanceTimersByTime((service as any).REFRESH_INTERVAL_MS);
      expect(getSpy).toHaveBeenCalledTimes(1);

      // The failed poll leaves the selected unit object as it was and the poll subscription open.
      expect(service.getSelectedComputingUnitValue()).toBe(original);
      expect(stopSpy).not.toHaveBeenCalled();
      expect((service as any).selectedUnitPoll.closed).toBe(false);

      vi.advanceTimersByTime((service as any).REFRESH_INTERVAL_MS * 4);
      // Every later tick still fetches, and the next good result replaces the selection.
      expect(getSpy).toHaveBeenCalledTimes(5);
      expect(service.getSelectedComputingUnitValue()).toBe(polled);
    });
  });

  it("logs a non-HTTP poll failure and keeps polling, but stays silent for an HTTP failure", () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      withFakePolling(() => {
        const managing = TestBed.inject(WorkflowComputingUnitManagingService);
        const polled = mockUnit(3, "Running");
        const bug = new TypeError("cannot read resource");
        const getSpy = vi
          .spyOn(managing, "getComputingUnit")
          .mockReturnValueOnce(throwError(() => bug))
          .mockReturnValueOnce(httpFailure())
          .mockReturnValue(of(polled));
        seedUnits([mockUnit(3)], mockUnit(3));

        (service as any).startPollingSelectedUnit(3);
        vi.advanceTimersByTime((service as any).REFRESH_INTERVAL_MS);
        // A failure outside HTTP is a bug and must stay visible.
        expect(errorSpy).toHaveBeenCalledTimes(1);
        expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("status poll"), bug);

        vi.advanceTimersByTime((service as any).REFRESH_INTERVAL_MS);
        // An HTTP failure is not logged.
        expect(errorSpy).toHaveBeenCalledTimes(1);

        vi.advanceTimersByTime((service as any).REFRESH_INTERVAL_MS);
        expect(getSpy).toHaveBeenCalledTimes(3);
        expect(service.getSelectedComputingUnitValue()).toBe(polled);
      });
    } finally {
      errorSpy.mockRestore();
    }
  });

  describe("selectComputingUnit() cache-miss and guard branches", () => {
    it("refreshes then selects a unit that is not yet in the cache", () => {
      const openSpy = spyOpenWs();
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
      const openSpy = spyOpenWs();
      seedUnits([mockUnit(7)]);

      service.selectComputingUnit(5, 7);
      // Same wid + cuid → shouldReconnect is false → the guarded block is skipped.
      service.selectComputingUnit(5, 7);

      expect(openSpy).toHaveBeenCalledTimes(1);
    });

    it("does nothing when the workflow id is undefined", () => {
      const openSpy = spyOpenWs();
      seedUnits([mockUnit(7)]);

      // The unit is cached and shouldReconnect is true, so only the isDefined(wid)
      // guard can stop the selection: no socket is opened and nothing is selected.
      service.selectComputingUnit(undefined, 7);

      expect(openSpy).not.toHaveBeenCalled();
      expect(service.getSelectedComputingUnitValue()).toBeNull();
      expect((service as any).currentConnectedWid).toBeUndefined();
      expect((service as any).currentConnectedCuid).toBeUndefined();
    });

    it("reconnects when the same unit is opened under a different workflow (wid change alone)", () => {
      const openSpy = spyOpenWs();
      seedUnits([mockUnit(7)]);

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
    function runTerminate(connected: boolean, selectedCuid: number, response = of({} as Response)) {
      const managing = TestBed.inject(WorkflowComputingUnitManagingService);
      const workflowStatusService = TestBed.inject(WorkflowStatusService);
      const closeSpy = vi.spyOn(websocketService, "closeWebsocket").mockImplementation(() => {});
      const clearSpy = vi.spyOn(workflowStatusService, "clearStatus").mockImplementation(() => {});
      vi.spyOn(websocketService, "isConnected", "get").mockReturnValue(connected);
      const termSpy = vi.spyOn(managing, "terminateComputingUnit").mockReturnValue(response);
      const refreshSpy = vi.spyOn(service, "refreshComputingUnitList").mockImplementation(() => {});
      seedUnits([], mockUnit(selectedCuid));

      let result: boolean | undefined;
      service.terminateComputingUnit(7).subscribe(r => (result = r));
      return { closeSpy, clearSpy, termSpy, refreshSpy, result };
    }

    it("closes the socket and clears status when terminating the connected selection", () => {
      const { closeSpy, clearSpy, termSpy, refreshSpy, result } = runTerminate(true, 7);

      expect(closeSpy).toHaveBeenCalled();
      expect(clearSpy).toHaveBeenCalled();
      expect(termSpy).toHaveBeenCalledWith(7);
      // The tap() side effect requests a single list refresh.
      expect(refreshSpy).toHaveBeenCalled();
      expect(result).toBe(true);
    });

    it("leaves the socket untouched when terminating a non-selected unit", () => {
      // isConnected is true, but isSelected short-circuits the guard to false.
      const { closeSpy, result } = runTerminate(true, 8);

      expect(closeSpy).not.toHaveBeenCalled();
      expect(result).toBe(true);
    });

    it("skips socket teardown when the selected unit is not connected", () => {
      // isSelected is true, but the socket is down → the guarded teardown is skipped.
      const { closeSpy, result } = runTerminate(false, 7);

      expect(closeSpy).not.toHaveBeenCalled();
      expect(result).toBe(true);
    });

    it("resolves to false when the termination request errors", () => {
      const { refreshSpy, result } = runTerminate(
        false,
        7,
        throwError(() => new Error("boom"))
      );

      // The error is swallowed by catchError, which emits false; no refresh runs.
      expect(result).toBe(false);
      expect(refreshSpy).not.toHaveBeenCalled();
    });
  });
});
