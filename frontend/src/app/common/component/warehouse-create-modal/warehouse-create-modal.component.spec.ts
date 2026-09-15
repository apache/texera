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
import { Subject, of } from "rxjs";
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

  it("takes what the user types through the two-way binding", async () => {
    component.visible = true;
    fixture.detectChanges();

    const input = document.querySelector<HTMLInputElement>("input[nz-input]")!;
    input.value = "typed-in";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    await fixture.whenStable();

    expect(component.newWarehouseName).toBe("typed-in");
  });

  it("create fires the trimmed request and closes at once, like the computing-unit dialog", () => {
    // The outcome arrives later as a toast; the dialog does not wait for it.
    warehouseActions.create.mockReturnValue(new Subject<DashboardWarehouse>().asObservable());
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;
    component.newWarehouseName = "  mybucket  ";
    fixture.detectChanges();

    document.querySelector<HTMLButtonElement>("#confirm-create-warehouse-btn")!.click();

    expect(warehouseActions.create).toHaveBeenCalledWith("mybucket");
    expect(component.visible).toBe(false);
    expect(visibleSpy).toHaveBeenCalledWith(false);
  });

  it("the Enter key submits and closes the same way", () => {
    warehouseActions.create.mockReturnValue(new Subject<DashboardWarehouse>().asObservable());
    component.visible = true;
    component.newWarehouseName = "again";
    fixture.detectChanges();

    document
      .querySelector<HTMLInputElement>("input[nz-input]")!
      .dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", bubbles: true }));

    expect(warehouseActions.create).toHaveBeenCalledWith("again");
    expect(component.visible).toBe(false);
  });

  it("Enter on a blank name does nothing and keeps the dialog open", () => {
    component.visible = true;
    component.newWarehouseName = "   ";
    fixture.detectChanges();

    document
      .querySelector<HTMLInputElement>("input[nz-input]")!
      .dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", bubbles: true }));

    expect(warehouseActions.create).not.toHaveBeenCalled();
    expect(component.visible).toBe(true);
  });

  it("a create that lands after the close reports itself without touching a reopened dialog", () => {
    // This is what makes close-and-forget safe: the user who cancelled watching
    // still sees the toast and the refreshed list, while the dialog they have
    // since reopened — mid-typing — is left alone.
    const inFlight = new Subject<DashboardWarehouse>();
    warehouseActions.create.mockReturnValue(inFlight.asObservable());
    const createdSpy = vi.fn();
    component.warehouseCreated.subscribe(createdSpy);
    component.visible = true;
    component.newWarehouseName = "first";
    component.handleCreateWarehouseModalOk();

    component.visible = true;
    component.newWarehouseName = "second-in-progress";
    inFlight.next(created);
    inFlight.complete();

    expect(notificationService.success).toHaveBeenCalledWith('Warehouse "mybucket" created.');
    expect(createdSpy).toHaveBeenCalledWith(created);
    expect(component.visible).toBe(true);
    expect(component.newWarehouseName).toBe("second-in-progress");
  });

  it("a create that fails after the close surfaces the backend message as a toast", () => {
    const inFlight = new Subject<DashboardWarehouse>();
    warehouseActions.create.mockReturnValue(inFlight.asObservable());
    component.visible = true;
    component.newWarehouseName = "mybucket";
    component.handleCreateWarehouseModalOk();

    inFlight.error({ error: "a warehouse named 'mybucket' already exists" });

    expect(notificationService.error).toHaveBeenCalledWith(
      "Failed to create warehouse: a warehouse named 'mybucket' already exists"
    );
  });

  it("clears the previous name when the modal opens", () => {
    component.newWarehouseName = "leftover";
    component.visible = true;

    component.ngOnChanges({ visible: new SimpleChange(false, true, false) });

    expect(component.newWarehouseName).toBe("");
  });

  it("leaves the form alone when a change does not open the dialog", () => {
    component.newWarehouseName = "typing";

    component.ngOnChanges({});

    expect(component.newWarehouseName).toBe("typing");
  });

  it("cancel closes without creating", () => {
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
});
