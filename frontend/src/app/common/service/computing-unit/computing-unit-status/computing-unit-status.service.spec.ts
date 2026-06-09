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
import { of } from "rxjs";
import { ComputingUnitStatusService } from "./computing-unit-status.service";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { WorkflowWebsocketService } from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import { WorkflowStatusService } from "../../../../workspace/service/workflow-status/workflow-status.service";
import { UserService } from "../../user/user.service";
import { StubUserService } from "../../user/stub-user.service";
import { AuthService } from "../../user/auth.service";
import { StubAuthService } from "../../user/stub-auth.service";
import { DashboardWorkflowComputingUnit } from "../../../type/workflow-computing-unit";
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
});
