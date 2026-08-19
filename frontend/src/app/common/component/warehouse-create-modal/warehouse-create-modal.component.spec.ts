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

import { SimpleChange } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";
import { WarehouseCreateModalComponent } from "./warehouse-create-modal.component";
import { NotificationService } from "../../service/notification/notification.service";
import { WarehouseService } from "../../service/warehouse/warehouse.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { commonTestProviders } from "../../testing/test-utils";

describe("WarehouseCreateModalComponent", () => {
  let fixture: ComponentFixture<WarehouseCreateModalComponent>;
  let component: WarehouseCreateModalComponent;
  let warehouseService: { createWarehouse: ReturnType<typeof vi.fn> };
  let notificationService: { error: ReturnType<typeof vi.fn>; success: ReturnType<typeof vi.fn> };

  const created: DashboardWarehouse = {
    whid: 7,
    name: "mybucket",
    warehouseName: "user-1-mybucket",
    flavor: "local",
    createdAtMillis: 0,
  };

  beforeEach(async () => {
    warehouseService = { createWarehouse: vi.fn().mockReturnValue(of(created)) };
    notificationService = { error: vi.fn(), success: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [WarehouseCreateModalComponent, NoopAnimationsModule, HttpClientTestingModule],
      providers: [
        // The rendered <nz-modal> injects NzModalService itself.
        NzModalService,
        { provide: WarehouseService, useValue: warehouseService },
        { provide: NotificationService, useValue: notificationService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(WarehouseCreateModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
  });

  it("renders the name input and a disabled Create button once opened", () => {
    component.visible = true;
    fixture.detectChanges();

    // nz-modal renders into the CDK overlay on document, not into the fixture.
    expect(document.querySelector("input[nz-input]")).toBeTruthy();
    const createButton = document.querySelector<HTMLButtonElement>("#confirm-create-warehouse-btn");
    expect(createButton?.disabled).toBe(true);

    component.newWarehouseName = "mybucket";
    fixture.detectChanges();
    expect(createButton?.disabled).toBe(false);
  });

  it("creates the trimmed name, then emits the warehouse and closes", () => {
    const createdSpy = vi.fn();
    const visibleSpy = vi.fn();
    component.warehouseCreated.subscribe(createdSpy);
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;
    component.newWarehouseName = "  mybucket  ";

    component.createWarehouse();

    expect(warehouseService.createWarehouse).toHaveBeenCalledWith("mybucket");
    expect(notificationService.success).toHaveBeenCalledWith('Warehouse "mybucket" created.');
    expect(createdSpy).toHaveBeenCalledWith(created);
    expect(component.visible).toBe(false);
    expect(visibleSpy).toHaveBeenCalledWith(false);
    expect(component.creating).toBe(false);
  });

  it("does nothing for a blank name", () => {
    component.newWarehouseName = "   ";

    component.createWarehouse();

    expect(warehouseService.createWarehouse).not.toHaveBeenCalled();
  });

  it("does not double-submit while a create is in flight", () => {
    component.newWarehouseName = "mybucket";
    component.creating = true;

    component.createWarehouse();

    expect(warehouseService.createWarehouse).not.toHaveBeenCalled();
  });

  it("keeps the modal open and surfaces the backend message when the create fails", () => {
    warehouseService.createWarehouse.mockReturnValue(
      throwError(() => ({ error: "a warehouse named 'mybucket' already exists" }))
    );
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;
    component.newWarehouseName = "mybucket";

    component.createWarehouse();

    expect(component.visible).toBe(true);
    expect(visibleSpy).not.toHaveBeenCalled();
    expect(component.creating).toBe(false);
    expect(notificationService.error).toHaveBeenCalledWith("a warehouse named 'mybucket' already exists");
  });

  it("clears the previous name when the modal opens", () => {
    component.newWarehouseName = "leftover";
    component.visible = true;

    component.ngOnChanges({ visible: new SimpleChange(false, true, false) });

    expect(component.newWarehouseName).toBe("");
  });

  it("cancel closes without creating", () => {
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;

    component.handleCreateWarehouseModalCancel();

    expect(component.visible).toBe(false);
    expect(visibleSpy).toHaveBeenCalledWith(false);
    expect(warehouseService.createWarehouse).not.toHaveBeenCalled();
  });
});
