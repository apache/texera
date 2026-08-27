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
import { Subject, of, throwError } from "rxjs";
import { WarehouseCreateModalComponent } from "./warehouse-create-modal.component";
import { NotificationService } from "../../service/notification/notification.service";
import { WarehouseActionsService } from "../../service/warehouse/warehouse-actions.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { commonTestProviders } from "../../testing/test-utils";

describe("WarehouseCreateModalComponent", () => {
  let fixture: ComponentFixture<WarehouseCreateModalComponent>;
  let component: WarehouseCreateModalComponent;
  let warehouseActions: { create: ReturnType<typeof vi.fn> };
  let notificationService: { error: ReturnType<typeof vi.fn>; success: ReturnType<typeof vi.fn> };

  const created: DashboardWarehouse = {
    whid: 7,
    name: "mybucket",
    lakekeeperWarehouseName: "user-1-mybucket",
    flavor: "local",
    createdAtMillis: 0,
    ownerName: "Alice",
    ownerAvatar: "",
  };

  beforeEach(async () => {
    warehouseActions = { create: vi.fn().mockReturnValue(of(created)) };
    notificationService = { error: vi.fn(), success: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [WarehouseCreateModalComponent, NoopAnimationsModule, HttpClientTestingModule],
      providers: [
        // The rendered <nz-modal> injects NzModalService itself.
        NzModalService,
        { provide: WarehouseActionsService, useValue: warehouseActions },
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

  it("renders nothing while closed", () => {
    // detectChanges already ran in beforeEach with visible=false.
    expect(document.querySelector("#confirm-create-warehouse-btn")).toBeNull();
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

  it("drives create from the dialog's own controls, not just the component method", async () => {
    // The buttons and the Enter key are the only paths a user has; asserting on
    // the component method alone leaves those bindings unverified.
    component.visible = true;
    component.newWarehouseName = "mybucket";
    fixture.detectChanges();

    document.querySelector<HTMLButtonElement>("#confirm-create-warehouse-btn")!.click();
    expect(warehouseActions.create).toHaveBeenCalledWith("mybucket");

    warehouseActions.create.mockClear();
    component.visible = true;
    component.newWarehouseName = "again";
    fixture.detectChanges();
    document
      .querySelector<HTMLInputElement>("input[nz-input]")!
      .dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", bubbles: true }));
    expect(warehouseActions.create).toHaveBeenCalledWith("again");
  });

  it("takes what the user types through the two-way binding", async () => {
    component.visible = true;
    fixture.detectChanges();

    const input = document.querySelector<HTMLInputElement>("input[nz-input]")!;
    input.value = "typed-in";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    await fixture.whenStable();

    expect(component.newWarehouseName).toBe("typed-in");
  });

  it("closes from the dialog's Cancel button", () => {
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;
    fixture.detectChanges();

    const cancel = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find(
      b => b.textContent?.trim() === "Cancel"
    )!;
    cancel.click();

    expect(component.visible).toBe(false);
    expect(visibleSpy).toHaveBeenCalledWith(false);
    expect(warehouseActions.create).not.toHaveBeenCalled();
  });

  it("leaves the form alone when a change does not open the dialog", () => {
    component.newWarehouseName = "typing";

    component.ngOnChanges({});

    expect(component.newWarehouseName).toBe("typing");
  });

  it("creates the trimmed name, then emits the warehouse and closes", () => {
    const createdSpy = vi.fn();
    const visibleSpy = vi.fn();
    component.warehouseCreated.subscribe(createdSpy);
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;
    component.newWarehouseName = "  mybucket  ";

    component.createWarehouse();

    expect(warehouseActions.create).toHaveBeenCalledWith("mybucket");
    expect(notificationService.success).toHaveBeenCalledWith('Warehouse "mybucket" created.');
    expect(createdSpy).toHaveBeenCalledWith(created);
    expect(component.visible).toBe(false);
    expect(visibleSpy).toHaveBeenCalledWith(false);
    expect(component.creating).toBe(false);
  });

  it("does nothing for a blank name", () => {
    component.newWarehouseName = "   ";

    component.createWarehouse();

    expect(warehouseActions.create).not.toHaveBeenCalled();
  });

  it("does not double-submit while a create is in flight", () => {
    component.newWarehouseName = "mybucket";
    component.creating = true;

    component.createWarehouse();

    expect(warehouseActions.create).not.toHaveBeenCalled();
  });

  it("keeps the modal open and surfaces the backend message when the create fails", () => {
    warehouseActions.create.mockReturnValue(
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
    expect(notificationService.error).toHaveBeenCalledWith(
      "Failed to create warehouse: a warehouse named 'mybucket' already exists"
    );
  });

  it("clears the previous name and any stuck loading state when the modal opens", () => {
    component.newWarehouseName = "leftover";
    // Cancelling mid-flight leaves creating set; reopening must not show a Create
    // button stuck in its loading state.
    component.creating = true;
    component.visible = true;

    component.ngOnChanges({ visible: new SimpleChange(false, true, false) });

    expect(component.newWarehouseName).toBe("");
    expect(component.creating).toBe(false);
  });

  it("cancel abandons an in-flight create instead of letting it land later", () => {
    // The component outlives the dialog, so without an explicit teardown the
    // request would still succeed: creating the warehouse the user cancelled and
    // closing the dialog they had already reopened.
    const inFlight = new Subject<DashboardWarehouse>();
    warehouseActions.create.mockReturnValue(inFlight.asObservable());
    const createdSpy = vi.fn();
    component.warehouseCreated.subscribe(createdSpy);
    component.visible = true;
    component.newWarehouseName = "first";
    component.createWarehouse();

    component.handleCreateWarehouseModalCancel();
    inFlight.next(created);
    inFlight.complete();

    expect(createdSpy).not.toHaveBeenCalled();
    expect(notificationService.success).not.toHaveBeenCalled();
    expect(component.creating).toBe(false);
  });

  it("a host-driven close abandons the in-flight create like Cancel does", () => {
    // The workspace picker (#7817) will close this dialog by flipping
    // [(visible)] itself, without going through the Cancel handler.
    const inFlight = new Subject<DashboardWarehouse>();
    warehouseActions.create.mockReturnValue(inFlight.asObservable());
    const createdSpy = vi.fn();
    component.warehouseCreated.subscribe(createdSpy);
    component.visible = true;
    component.newWarehouseName = "first";
    component.createWarehouse();

    component.visible = false;
    component.ngOnChanges({ visible: new SimpleChange(true, false, false) });
    inFlight.next(created);
    inFlight.complete();

    expect(createdSpy).not.toHaveBeenCalled();
    expect(notificationService.success).not.toHaveBeenCalled();
  });

  it("cancel closes without creating", () => {
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;

    component.handleCreateWarehouseModalCancel();

    expect(component.visible).toBe(false);
    expect(visibleSpy).toHaveBeenCalledWith(false);
    expect(warehouseActions.create).not.toHaveBeenCalled();
  });
});
