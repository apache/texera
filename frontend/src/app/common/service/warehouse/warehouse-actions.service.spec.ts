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
import { Observable, Subject, firstValueFrom, of, throwError } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { WarehouseActionsService } from "./warehouse-actions.service";
import { WarehouseService } from "./warehouse.service";
import { NotificationService } from "../notification/notification.service";
import { DashboardWarehouse } from "../../type/warehouse";

describe("WarehouseActionsService", () => {
  let service: WarehouseActionsService;
  let modalService: { confirm: ReturnType<typeof vi.fn> };
  let warehouseService: { createWarehouse: ReturnType<typeof vi.fn>; deleteWarehouse: ReturnType<typeof vi.fn> };
  let notificationService: { error: ReturnType<typeof vi.fn>; success: ReturnType<typeof vi.fn> };

  const warehouse: DashboardWarehouse = {
    whid: 3,
    name: "sales",
    lakekeeperWarehouseName: "user-1-3",
    flavor: "local",
    createdAtMillis: 0,
    ownerName: "Alice",
    ownerAvatar: null,
  };

  // The dialog is the only way in: every case goes through the confirm config
  // nz-modal was handed, so name that once instead of reaching into the mock.
  const confirmConfig = () => modalService.confirm.mock.calls[0][0];
  const clickDelete = () => confirmConfig().nzOnOk();

  beforeEach(() => {
    modalService = { confirm: vi.fn() };
    warehouseService = {
      createWarehouse: vi.fn().mockReturnValue(of(warehouse)),
      deleteWarehouse: vi.fn().mockReturnValue(of(void 0)),
    };
    notificationService = { error: vi.fn(), success: vi.fn() };

    TestBed.configureTestingModule({
      providers: [
        WarehouseActionsService,
        { provide: NzModalService, useValue: modalService },
        { provide: WarehouseService, useValue: warehouseService },
        { provide: NotificationService, useValue: notificationService },
      ],
    });
    service = TestBed.inject(WarehouseActionsService);
  });

  describe("create", () => {
    it("toasts the success and hands the created warehouse to the caller", async () => {
      const seen = firstValueFrom(service.create("sales"));

      expect(warehouseService.createWarehouse).toHaveBeenCalledWith("sales");
      expect(notificationService.success).toHaveBeenCalledWith('Warehouse "sales" created.');
      expect(await seen).toEqual(warehouse);
    });

    it("toasts the backend message when the create fails", () => {
      warehouseService.createWarehouse.mockReturnValue(throwError(() => ({ error: "'sales' already exists" })));

      service.create("sales").subscribe({ error: () => {} });

      expect(notificationService.error).toHaveBeenCalledWith("Failed to create warehouse: 'sales' already exists");
    });

    it("a failed create completes the caller's stream empty instead of erroring it", () => {
      // A replayed error would land in relays that have no error path and
      // surface as an unhandled RxJS error; the failure was already toasted.
      warehouseService.createWarehouse.mockReturnValue(throwError(() => ({ error: "boom" })));
      const next = vi.fn();
      const error = vi.fn();
      const complete = vi.fn();

      service.create("sales").subscribe({ next, error, complete });

      expect(error).not.toHaveBeenCalled();
      expect(next).not.toHaveBeenCalled();
      expect(complete).toHaveBeenCalled();
      expect(notificationService.error).toHaveBeenCalledWith("Failed to create warehouse: boom");
    });

    it("the request and its toast outlive the caller's subscription", () => {
      // The dialog (or its whole page) can be destroyed mid-create; the
      // service still owns the request, so nothing is aborted or swallowed.
      const inFlight = new Subject<DashboardWarehouse>();
      warehouseService.createWarehouse.mockReturnValue(inFlight.asObservable());

      const callerSub = service.create("sales").subscribe();
      callerSub.unsubscribe();
      inFlight.next(warehouse);
      inFlight.complete();

      expect(notificationService.success).toHaveBeenCalledWith('Warehouse "sales" created.');
    });

    it("issues exactly one request even with the service and the caller both subscribed", () => {
      // HttpClient observables are cold — every subscription is its own HTTP
      // request — so count subscriptions to the source, not mock calls.
      let subscriptions = 0;
      warehouseService.createWarehouse.mockReturnValue(
        new Observable<DashboardWarehouse>(() => {
          subscriptions++;
        })
      );

      service.create("sales").subscribe();

      expect(subscriptions).toBe(1);
    });
  });

  it("creates through the warehouse service, so create and delete share one entry point", async () => {
    const created = await firstValueFrom(service.create("mybucket"));

    expect(warehouseService.createWarehouse).toHaveBeenCalledWith("mybucket");
    expect(created).toEqual(warehouse);
  });

  it("asks for confirmation with the danger wording, without deleting yet", () => {
    const onDeleted = vi.fn();

    service.confirmAndDelete(warehouse, onDeleted);

    expect(modalService.confirm).toHaveBeenCalledTimes(1);
    const config = confirmConfig();
    expect(config.nzTitle).toBe('Delete warehouse "sales"?');
    expect(config.nzOkText).toBe("Delete");
    expect(config.nzOkDanger).toBe(true);
    expect(warehouseService.deleteWarehouse).not.toHaveBeenCalled();
    expect(onDeleted).not.toHaveBeenCalled();
  });

  it("deletes on confirm, then notifies and runs onDeleted", async () => {
    const onDeleted = vi.fn();
    service.confirmAndDelete(warehouse, onDeleted);

    await clickDelete();

    expect(warehouseService.deleteWarehouse).toHaveBeenCalledWith(3);
    expect(notificationService.success).toHaveBeenCalledWith("Warehouse deleted.");
    expect(onDeleted).toHaveBeenCalledTimes(1);
  });

  it("keeps the dialog busy until the delete settles", async () => {
    // The backend waits out Lakekeeper's asynchronous purge (#7742), so nzOnOk must
    // hand nz-modal a promise — that is what keeps the OK button spinning instead of
    // closing the dialog onto a row that is still there.
    const deleted = new Subject<void>();
    warehouseService.deleteWarehouse.mockReturnValue(deleted.asObservable());
    const onDeleted = vi.fn();
    service.confirmAndDelete(warehouse, onDeleted);

    const pending = clickDelete();
    expect(pending).toBeInstanceOf(Promise);
    expect(onDeleted).not.toHaveBeenCalled();

    deleted.next();
    deleted.complete();
    await pending;

    expect(onDeleted).toHaveBeenCalledTimes(1);
  });

  it("does not report a failure when the refresh callback throws after a successful delete", async () => {
    // The delete succeeded; a refresh that blows up is logged, not turned into a
    // failure toast, and it must not reject the dialog either.
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const onDeleted = vi.fn(() => {
      throw new Error("refresh blew up");
    });
    service.confirmAndDelete(warehouse, onDeleted);

    await clickDelete();

    expect(notificationService.success).toHaveBeenCalledWith("Warehouse deleted.");
    expect(notificationService.error).not.toHaveBeenCalled();
    expect(consoleErrorSpy).toHaveBeenCalled();
    consoleErrorSpy.mockRestore();
  });

  it("surfaces the backend message and skips onDeleted when the delete fails", async () => {
    warehouseService.deleteWarehouse.mockReturnValue(throwError(() => ({ error: "Lakekeeper unreachable" })));
    const onDeleted = vi.fn();
    service.confirmAndDelete(warehouse, onDeleted);

    // Resolves rather than rejects: the dialog closes and the error rides a toast.
    await clickDelete();

    expect(notificationService.error).toHaveBeenCalledWith("Failed to delete warehouse: Lakekeeper unreachable");
    expect(notificationService.success).not.toHaveBeenCalled();
    expect(onDeleted).not.toHaveBeenCalled();
  });
});
