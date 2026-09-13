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
import { of, throwError } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { ComputingUnitActionsService, StartComputingUnitRequest } from "./computing-unit-actions.service";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { NotificationService } from "../../notification/notification.service";
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";

describe("ComputingUnitActionsService", () => {
  let service: ComputingUnitActionsService;
  let modalService: { create: ReturnType<typeof vi.fn>; confirm: ReturnType<typeof vi.fn> };
  let computingUnitService: {
    createKubernetesBasedComputingUnit: ReturnType<typeof vi.fn>;
    createLocalComputingUnit: ReturnType<typeof vi.fn>;
  };
  let notificationService: { error: ReturnType<typeof vi.fn>; success: ReturnType<typeof vi.fn> };
  let statusService: { terminateComputingUnit: ReturnType<typeof vi.fn> };

  const baseRequest: StartComputingUnitRequest = {
    type: "kubernetes",
    name: "unit",
    cpu: "2",
    memory: "4G",
    gpu: "1",
    jvmMemorySize: "1G",
    shmSize: "64M",
    localUri: "http://localhost:8080",
  };

  beforeEach(() => {
    modalService = { create: vi.fn(), confirm: vi.fn() };
    computingUnitService = {
      createKubernetesBasedComputingUnit: vi.fn().mockReturnValue(of({} as any)),
      createLocalComputingUnit: vi.fn().mockReturnValue(of({} as any)),
    };
    notificationService = { error: vi.fn(), success: vi.fn() };
    statusService = { terminateComputingUnit: vi.fn().mockReturnValue(of(true)) };

    TestBed.configureTestingModule({
      providers: [
        ComputingUnitActionsService,
        { provide: NzModalService, useValue: modalService },
        { provide: WorkflowComputingUnitManagingService, useValue: computingUnitService },
        { provide: NotificationService, useValue: notificationService },
        { provide: ComputingUnitStatusService, useValue: statusService },
      ],
    });
    service = TestBed.inject(ComputingUnitActionsService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("create", () => {
    it("routes a kubernetes request to createKubernetesBasedComputingUnit", () => {
      service.create({ ...baseRequest, type: "kubernetes" });

      expect(computingUnitService.createKubernetesBasedComputingUnit).toHaveBeenCalledWith(
        "unit",
        "2",
        "4G",
        "1",
        "1G",
        "64M"
      );
      expect(computingUnitService.createLocalComputingUnit).not.toHaveBeenCalled();
    });

    it("routes a local request to createLocalComputingUnit with name and localUri", () => {
      service.create({ ...baseRequest, type: "local" });

      expect(computingUnitService.createLocalComputingUnit).toHaveBeenCalledWith("unit", "http://localhost:8080");
    });

    it("throws for an unsupported computing unit type", () => {
      expect(() => service.create({ ...baseRequest, type: "quantum" as any })).toThrowError(
        "Unsupported computing unit type"
      );
    });
  });

  describe("openShareAccessModal", () => {
    it("opens the share modal seeded with the computing unit id and workspace flag", () => {
      service.openShareAccessModal(55, false);

      expect(modalService.create).toHaveBeenCalledTimes(1);
      const config = modalService.create.mock.calls[0][0];
      expect(config.nzData).toMatchObject({ type: "computing-unit", id: 55, inWorkspace: false, writeAccess: true });
    });

    it("defaults inWorkspace to true", () => {
      service.openShareAccessModal(1);
      expect(modalService.create.mock.calls[0][0].nzData.inWorkspace).toBe(true);
    });
  });

  describe("confirmAndTerminate", () => {
    const unit = (overrides: any = {}) =>
      ({ computingUnit: { uri: "http://x", name: "unit1", type: "kubernetes", ...overrides } }) as any;

    it("errors and skips the modal when the unit has no uri", () => {
      service.confirmAndTerminate(1, unit({ uri: "" }));

      expect(notificationService.error).toHaveBeenCalledWith("Invalid computing unit.");
      expect(modalService.confirm).not.toHaveBeenCalled();
    });

    it("opens a confirmation modal for a valid unit", () => {
      service.confirmAndTerminate(1, unit());
      expect(modalService.confirm).toHaveBeenCalledTimes(1);
    });

    it("notifies success when the confirmed termination succeeds", () => {
      statusService.terminateComputingUnit.mockReturnValue(of(true));
      service.confirmAndTerminate(7, unit());

      modalService.confirm.mock.calls[0][0].nzOnOk();

      expect(statusService.terminateComputingUnit).toHaveBeenCalledWith(7);
      expect(notificationService.success).toHaveBeenCalledWith("Terminated Computing Unit: unit1");
    });

    it("notifies an error when the confirmed termination reports failure", () => {
      statusService.terminateComputingUnit.mockReturnValue(of(false));
      service.confirmAndTerminate(7, unit());

      modalService.confirm.mock.calls[0][0].nzOnOk();

      expect(notificationService.error).toHaveBeenCalledWith("Failed to terminate computing unit");
    });

    it("uses the disconnect wording and shows no destructive warning for a local unit", () => {
      service.confirmAndTerminate(3, unit({ type: "local", name: "local1" }));

      const config = modalService.confirm.mock.calls[0][0];
      expect(config.nzTitle).toBe("Disconnect from Local Computing Unit");
      expect(config.nzOkText).toBe("Disconnect");
      expect(config.nzContent).toContain("disconnect from <strong>local1</strong>");
      // A local unit holds no execution results, so the data-loss warning must not appear.
      // Assert the copy itself, not only its colour, so restyling the shared warning
      // (a CSS class, a theme token) cannot silently void this guard.
      expect(config.nzContent).not.toContain("Warning:");
      expect(config.nzContent).not.toContain("will be lost");
      expect(config.nzContent).not.toContain("#ff4d4f");
      // the cancel label is a fixed word; it must not co-vary with the unit type
      expect(config.nzCancelText).toBe("Cancel");
      expect(config.nzOkType).toBe("primary");
    });

    it("falls back to the kubernetes copy when the unit carries no type", () => {
      // `type` is declared non-optional, so this pins the explicit `|| "kubernetes"` fallback.
      // The name deliberately reuses the local case's "local1" so that `type` and `name` are not
      // correlated across the suite: the copy cannot be keyed off the unit's name instead.
      service.confirmAndTerminate(4, unit({ type: undefined, name: "local1" }));

      const config = modalService.confirm.mock.calls[0][0];
      expect(config.nzTitle).toBe("Terminate Computing Unit");
      expect(config.nzOkText).toBe("Terminate");
      expect(config.nzContent).toContain("terminate <strong>local1</strong>");
      expect(config.nzContent).toContain("Warning:");
      expect(config.nzContent).toContain("#ff4d4f");
      expect(config.nzCancelText).toBe("Cancel");
      expect(config.nzOkType).toBe("primary");
    });

    it("notifies an error with the extracted message when the termination observable errors", () => {
      statusService.terminateComputingUnit.mockReturnValue(throwError(() => new Error("kaboom")));
      service.confirmAndTerminate(7, unit());

      modalService.confirm.mock.calls[0][0].nzOnOk();

      expect(notificationService.error).toHaveBeenCalledWith("Failed to terminate computing unit: kaboom");
    });
  });
});
