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
import { Subject, of, throwError } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { WarehouseActionsService } from "./warehouse-actions.service";
import { WarehouseService } from "./warehouse.service";
import { NotificationService } from "../notification/notification.service";
import { DashboardWarehouse } from "../../type/warehouse";

describe("WarehouseActionsService", () => {
  let service: WarehouseActionsService;
  let modalService: { confirm: ReturnType<typeof vi.fn> };
  let warehouseService: { deleteWarehouse: ReturnType<typeof vi.fn> };
  let notificationService: { error: ReturnType<typeof vi.fn>; success: ReturnType<typeof vi.fn> };

  const warehouse: DashboardWarehouse = {
    whid: 3,
    name: "sales",
    lakekeeperWarehouseName: "user-1-sales",
    flavor: "local",
    createdAtMillis: 0,
    ownerName: "Alice",
    ownerAvatar: "",
  };

  beforeEach(() => {
    modalService = { confirm: vi.fn() };
    warehouseService = { deleteWarehouse: vi.fn().mockReturnValue(of(void 0)) };
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

  it("asks for confirmation with the danger wording, without deleting yet", () => {
    const onDeleted = vi.fn();

    service.confirmAndDelete(warehouse, onDeleted);

    expect(modalService.confirm).toHaveBeenCalledTimes(1);
    const config = modalService.confirm.mock.calls[0][0];
    expect(config.nzTitle).toBe('Delete warehouse "sales"?');
    expect(config.nzOkText).toBe("Delete");
    expect(config.nzOkDanger).toBe(true);
    expect(warehouseService.deleteWarehouse).not.toHaveBeenCalled();
    expect(onDeleted).not.toHaveBeenCalled();
  });

  it("deletes on confirm, then notifies and runs onDeleted", async () => {
    const onDeleted = vi.fn();
    service.confirmAndDelete(warehouse, onDeleted);

    await modalService.confirm.mock.calls[0][0].nzOnOk();

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

    const pending = modalService.confirm.mock.calls[0][0].nzOnOk();
    expect(pending).toBeInstanceOf(Promise);
    expect(onDeleted).not.toHaveBeenCalled();

    deleted.next();
    deleted.complete();
    await pending;

    expect(onDeleted).toHaveBeenCalledTimes(1);
  });

  it("surfaces the backend message and skips onDeleted when the delete fails", async () => {
    warehouseService.deleteWarehouse.mockReturnValue(throwError(() => ({ error: "Lakekeeper unreachable" })));
    const onDeleted = vi.fn();
    service.confirmAndDelete(warehouse, onDeleted);

    // Resolves rather than rejects: the dialog closes and the error rides a toast.
    await modalService.confirm.mock.calls[0][0].nzOnOk();

    expect(notificationService.error).toHaveBeenCalledWith("Failed to delete warehouse: Lakekeeper unreachable");
    expect(notificationService.success).not.toHaveBeenCalled();
    expect(onDeleted).not.toHaveBeenCalled();
  });
});
