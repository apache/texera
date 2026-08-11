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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { DeleteOutline, FileAddOutline } from "@ant-design/icons-angular/icons";
import { NzIconModule } from "ng-zorro-antd/icon";
import { ModalOptions, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";

import { UserWarehouseComponent } from "./user-warehouse.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WarehouseService } from "../../../../common/service/warehouse/warehouse.service";
import { DashboardWarehouse } from "../../../../common/type/warehouse";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("UserWarehouseComponent", () => {
  let component: UserWarehouseComponent;
  let fixture: ComponentFixture<UserWarehouseComponent>;

  let warehouseServiceSpy: {
    getStatus: ReturnType<typeof vi.fn>;
    createWarehouse: ReturnType<typeof vi.fn>;
    deleteWarehouse: ReturnType<typeof vi.fn>;
  };
  let notificationSpy: {
    error: ReturnType<typeof vi.fn>;
    success: ReturnType<typeof vi.fn>;
    info: ReturnType<typeof vi.fn>;
    warning: ReturnType<typeof vi.fn>;
  };
  let confirmSpy: ReturnType<typeof vi.spyOn>;
  let capturedConfirmConfig: ModalOptions | undefined;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  const warehouse = (whid: number, name: string): DashboardWarehouse => ({
    whid,
    name,
    warehouseName: `user-1-${name}`,
    flavor: "local",
    createdAtMillis: 1723300000000,
  });

  beforeEach(async () => {
    warehouseServiceSpy = {
      getStatus: vi.fn().mockReturnValue(of({ enabled: true, warehouses: [] })),
      createWarehouse: vi.fn().mockReturnValue(of(warehouse(1, "mybucket"))),
      deleteWarehouse: vi.fn().mockReturnValue(of(undefined)),
    };
    notificationSpy = {
      error: vi.fn(),
      success: vi.fn(),
      info: vi.fn(),
      warning: vi.fn(),
    };
    consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

    await TestBed.configureTestingModule({
      imports: [UserWarehouseComponent, NoopAnimationsModule, NzIconModule.forChild([FileAddOutline, DeleteOutline])],
      providers: [
        NzModalService,
        { provide: WarehouseService, useValue: warehouseServiceSpy as unknown as WarehouseService },
        { provide: NotificationService, useValue: notificationSpy as unknown as NotificationService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    // Use the real NzModalService (the rendered <nz-modal> relies on it) but capture the
    // confirm() config so tests can drive its nzOnOk callback without opening an overlay.
    capturedConfirmConfig = undefined;
    confirmSpy = vi.spyOn(TestBed.inject(NzModalService), "confirm").mockImplementation((options?: ModalOptions) => {
      capturedConfirmConfig = options;
      return {} as NzModalRef;
    });

    fixture = TestBed.createComponent(UserWarehouseComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    consoleErrorSpy?.mockRestore();
    confirmSpy?.mockRestore();
    fixture?.destroy();
  });

  it("shows the disabled notice when the deployment reports the feature off", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: false, warehouses: [] }));

    fixture.detectChanges();

    expect(component.warehouseEnabled).toBe(false);
    const notice = fixture.nativeElement.querySelector(".warehouse-page-empty");
    expect(notice?.textContent).toContain("disabled in this deployment");
    expect(fixture.nativeElement.querySelector(".warehouse-page-list")).toBeNull();
  });

  it("shows the create hint while the user has no warehouses", () => {
    fixture.detectChanges();

    const notice = fixture.nativeElement.querySelector(".warehouse-page-empty");
    expect(notice?.textContent).toContain("No warehouses yet");
  });

  it("lists the user's warehouses with their catalog name as tooltip", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(
      of({ enabled: true, warehouses: [warehouse(1, "first"), warehouse(2, "second")] })
    );

    fixture.detectChanges();

    const items = fixture.nativeElement.querySelectorAll(".warehouse-page-item");
    expect(items.length).toBe(2);
    expect(items[0].textContent).toContain("first");
    expect(items[1].textContent).toContain("second");
    expect(fixture.nativeElement.querySelector(".warehouse-page-empty")).toBeNull();
  });

  it("surfaces a failed status fetch as a notification", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(throwError(() => new Error("boom")));

    fixture.detectChanges();

    expect(notificationSpy.error).toHaveBeenCalledWith("Failed to fetch warehouses.");
  });

  describe("createWarehouse", () => {
    it("creates with the trimmed name, closes the modal, and refreshes the list", () => {
      fixture.detectChanges();
      component.showCreateModal();
      component.newWarehouseName = "  mybucket  ";
      warehouseServiceSpy.getStatus.mockClear();

      component.createWarehouse();

      expect(warehouseServiceSpy.createWarehouse).toHaveBeenCalledWith("mybucket");
      expect(component.createModalVisible).toBe(false);
      expect(component.creating).toBe(false);
      expect(notificationSpy.success).toHaveBeenCalledWith('Warehouse "mybucket" created.');
      expect(warehouseServiceSpy.getStatus).toHaveBeenCalledTimes(1);
    });

    it("does nothing for a blank name", () => {
      fixture.detectChanges();
      component.newWarehouseName = "   ";

      component.createWarehouse();

      expect(warehouseServiceSpy.createWarehouse).not.toHaveBeenCalled();
    });

    it("keeps the modal open and surfaces the backend message on failure", () => {
      fixture.detectChanges();
      component.showCreateModal();
      component.newWarehouseName = "dup";
      warehouseServiceSpy.createWarehouse.mockReturnValue(
        throwError(() => ({ error: "a warehouse named 'dup' already exists" }))
      );

      component.createWarehouse();

      expect(component.createModalVisible).toBe(true);
      expect(component.creating).toBe(false);
      expect(notificationSpy.error).toHaveBeenCalledWith("a warehouse named 'dup' already exists");
    });
  });

  describe("confirmDelete", () => {
    it("builds a danger confirm whose nzOnOk deletes the warehouse and refreshes", () => {
      warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: true, warehouses: [warehouse(3, "doomed")] }));
      fixture.detectChanges();
      warehouseServiceSpy.getStatus.mockClear();

      component.confirmDelete(warehouse(3, "doomed"));

      expect(confirmSpy).toHaveBeenCalledTimes(1);
      expect(capturedConfirmConfig?.nzTitle).toBe('Delete warehouse "doomed"?');
      expect(capturedConfirmConfig?.nzOkDanger).toBe(true);

      (capturedConfirmConfig?.nzOnOk as () => void)();
      expect(warehouseServiceSpy.deleteWarehouse).toHaveBeenCalledWith(3);
      expect(notificationSpy.success).toHaveBeenCalledWith("Warehouse deleted.");
      expect(warehouseServiceSpy.getStatus).toHaveBeenCalledTimes(1);
    });

    it("surfaces a failed delete as a notification", () => {
      fixture.detectChanges();
      warehouseServiceSpy.deleteWarehouse.mockReturnValue(throwError(() => ({ error: "Lakekeeper unreachable" })));

      component.confirmDelete(warehouse(3, "doomed"));
      (capturedConfirmConfig?.nzOnOk as () => void)();

      expect(notificationSpy.error).toHaveBeenCalledWith("Lakekeeper unreachable");
    });
  });
});
