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
import { By } from "@angular/platform-browser";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { CloudServerOutline, DeleteOutline, FileAddOutline } from "@ant-design/icons-angular/icons";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzModalService } from "ng-zorro-antd/modal";
import { NEVER, Subject, of, throwError } from "rxjs";

import { UserWarehouseComponent } from "./user-warehouse.component";
import { WarehouseCreateModalComponent } from "../../../../common/component/warehouse-create-modal/warehouse-create-modal.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WarehouseActionsService } from "../../../../common/service/warehouse/warehouse-actions.service";
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
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  const warehouse = (whid: number, name: string): DashboardWarehouse => ({
    whid,
    name,
    lakekeeperWarehouseName: `user-1-${name}`,
    flavor: "local",
    createdAtMillis: 1723300000000,
    ownerName: "Alice",
    ownerAvatar: "",
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
      imports: [
        UserWarehouseComponent,
        NoopAnimationsModule,
        NzIconModule.forChild([FileAddOutline, CloudServerOutline, DeleteOutline]),
      ],
      providers: [
        NzModalService,
        { provide: WarehouseService, useValue: warehouseServiceSpy as unknown as WarehouseService },
        { provide: NotificationService, useValue: notificationSpy as unknown as NotificationService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(UserWarehouseComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    consoleErrorSpy?.mockRestore();
    fixture?.destroy();
  });

  it("shows the disabled notice when the deployment reports the feature off", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: false, warehouses: [] }));

    fixture.detectChanges();

    expect(component.warehouseEnabled).toBe(false);
    const notice = fixture.nativeElement.querySelector(".warehouse-page-empty");
    expect(notice?.textContent).toContain("disabled in this deployment");
    expect(fixture.nativeElement.querySelectorAll("texera-user-warehouse-list-item").length).toBe(0);
  });

  it("says nothing about the feature while the status request is still in flight", () => {
    // A plain false would render the "disabled in this deployment" notice, naming
    // the wrong cause while the request is pending or after it failed.
    warehouseServiceSpy.getStatus.mockReturnValue(NEVER);

    fixture.detectChanges();

    expect(component.warehouseEnabled).toBeUndefined();
    expect(fixture.nativeElement.querySelector(".warehouse-page-empty")).toBeNull();
  });

  it("shows the create hint while the user has no warehouses", () => {
    fixture.detectChanges();

    const notice = fixture.nativeElement.querySelector(".warehouse-page-empty");
    expect(notice?.textContent).toContain("No warehouses yet");
    expect(fixture.nativeElement.querySelectorAll("texera-user-warehouse-list-item").length).toBe(0);
  });

  it("lists the user's warehouses in the virtual-scroll list", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(
      of({ enabled: true, warehouses: [warehouse(1, "first"), warehouse(2, "second")] })
    );

    fixture.detectChanges();

    // The virtual-scroll viewport has no height in jsdom, so no rows are laid
    // out here; the row markup is covered by the list item's own spec. What this
    // asserts is the state the template iterates and the absence of any notice.
    expect(component.warehouses.map(w => w.whid)).toEqual([1, 2]);
    expect(fixture.nativeElement.querySelector(".warehouse-page-empty")).toBeNull();
  });

  it("renders the scroll viewport only when there are rows to show", () => {
    // In the disabled/failed/empty states the viewport would otherwise render
    // as an empty bordered box below the notice.
    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: false, warehouses: [] }));
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector("cdk-virtual-scroll-viewport")).toBeNull();

    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: true, warehouses: [warehouse(1, "first")] }));
    component.retry();
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector("cdk-virtual-scroll-viewport")).toBeTruthy();
  });

  it("opens the create modal from the header button, which is disabled while the feature is off", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: false, warehouses: [] }));
    fixture.detectChanges();

    const createButton = fixture.debugElement.query(By.css(".create-btn"));
    expect(createButton.nativeElement.disabled).toBe(true);

    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: true, warehouses: [] }));
    component.ngOnInit();
    fixture.detectChanges();
    expect(createButton.nativeElement.disabled).toBe(false);

    createButton.triggerEventHandler("click", null);
    fixture.detectChanges();

    expect(component.addWarehouseModalVisible).toBe(true);
    // The flag has to reach the embedded modal, not just the component field.
    expect(fixture.debugElement.query(By.directive(WarehouseCreateModalComponent)).componentInstance.visible).toBe(
      true
    );
  });

  it("offers a retry when the status request fails, and recovers on success", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(throwError(() => new Error("boom")));
    fixture.detectChanges();

    expect(component.loadFailed).toBe(true);
    const notice = fixture.nativeElement.querySelector(".warehouse-page-empty");
    expect(notice?.textContent).toContain("Could not load your warehouses");

    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: true, warehouses: [warehouse(1, "first")] }));
    fixture.debugElement.query(By.css(".warehouse-page-empty button")).triggerEventHandler("click", null);
    fixture.detectChanges();

    expect(component.loadFailed).toBe(false);
    expect(component.warehouses.map(w => w.whid)).toEqual([1]);
  });

  it("clears the stale status when a later refresh fails, leaving only the retry state", () => {
    // Otherwise a failed refresh right after a delete would show the failure
    // notice beside an outdated list still containing the deleted row, with
    // Create still enabled.
    warehouseServiceSpy.getStatus.mockReturnValue(of({ enabled: true, warehouses: [warehouse(1, "first")] }));
    fixture.detectChanges();
    expect(component.warehouses.length).toBe(1);

    warehouseServiceSpy.getStatus.mockReturnValue(throwError(() => new Error("boom")));
    component.retry();
    fixture.detectChanges();

    expect(component.warehouseEnabled).toBeUndefined();
    expect(component.warehouses).toEqual([]);
    const notices = fixture.nativeElement.querySelectorAll(".warehouse-page-empty");
    expect(notices.length).toBe(1);
    expect(notices[0].textContent).toContain("Could not load your warehouses");
    expect(fixture.debugElement.query(By.css(".create-btn")).nativeElement.disabled).toBe(true);
  });

  it("a late response from a superseded refresh cannot overwrite the newer state", () => {
    const first = new Subject<{ enabled: boolean; warehouses: DashboardWarehouse[] }>();
    const second = new Subject<{ enabled: boolean; warehouses: DashboardWarehouse[] }>();
    warehouseServiceSpy.getStatus.mockReturnValueOnce(first.asObservable()).mockReturnValueOnce(second.asObservable());

    fixture.detectChanges();
    component.retry();

    second.next({ enabled: true, warehouses: [warehouse(2, "kept")] });
    second.complete();
    // The older request settles last; switchMap must already have dropped it.
    first.next({ enabled: true, warehouses: [warehouse(1, "stale")] });
    first.complete();

    expect(component.warehouses.map(w => w.name)).toEqual(["kept"]);
  });

  it("surfaces a failed status fetch as a notification", () => {
    warehouseServiceSpy.getStatus.mockReturnValue(throwError(() => new Error("boom")));

    fixture.detectChanges();

    expect(notificationSpy.error).toHaveBeenCalledWith("Failed to fetch warehouses.");
    // Still undefined, so the page reports the failure rather than claiming the
    // feature is disabled.
    expect(component.warehouseEnabled).toBeUndefined();
    expect(fixture.nativeElement.querySelector(".warehouse-page-empty")?.textContent).toContain(
      "Could not load your warehouses"
    );
  });

  it("hands the warehouse to the actions service, and refreshes once it reports the delete", () => {
    const actionsService = TestBed.inject(WarehouseActionsService);
    const confirmAndDeleteSpy = vi.spyOn(actionsService, "confirmAndDelete").mockImplementation(() => {});
    fixture.detectChanges();
    warehouseServiceSpy.getStatus.mockClear();
    const doomed = warehouse(3, "doomed");

    component.deleteWarehouse(doomed);

    expect(confirmAndDeleteSpy).toHaveBeenCalledTimes(1);
    expect(confirmAndDeleteSpy.mock.calls[0][0]).toEqual(doomed);
    const onDeleted = confirmAndDeleteSpy.mock.calls[0][1] as () => void;
    onDeleted();
    expect(warehouseServiceSpy.getStatus).toHaveBeenCalledTimes(1);
  });

  it("refreshes the warehouse list when the modal emits warehouseCreated", () => {
    fixture.detectChanges();
    warehouseServiceSpy.getStatus.mockClear();

    const modal = fixture.debugElement.query(By.directive(WarehouseCreateModalComponent)).componentInstance;
    modal.warehouseCreated.emit(warehouse(1, "mybucket"));

    expect(warehouseServiceSpy.getStatus).toHaveBeenCalledTimes(1);
  });

  it("syncs visibility when the embedded modal closes itself", () => {
    fixture.detectChanges();
    component.showAddWarehouseModalVisible();
    expect(component.addWarehouseModalVisible).toBe(true);

    const modal = fixture.debugElement.query(By.directive(WarehouseCreateModalComponent)).componentInstance;
    modal.visibleChange.emit(false);

    expect(component.addWarehouseModalVisible).toBe(false);
  });
});
