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
import { WarehouseActionsService } from "../../service/warehouse/warehouse-actions.service";
import { DashboardWarehouse } from "../../type/warehouse";
import { commonTestProviders } from "../../testing/test-utils";

describe("WarehouseCreateModalComponent", () => {
  let fixture: ComponentFixture<WarehouseCreateModalComponent>;
  let component: WarehouseCreateModalComponent;
  let warehouseActions: { create: ReturnType<typeof vi.fn> };

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

    await TestBed.configureTestingModule({
      imports: [WarehouseCreateModalComponent, NoopAnimationsModule, HttpClientTestingModule],
      providers: [
        // The rendered <nz-modal> injects NzModalService itself.
        NzModalService,
        { provide: WarehouseActionsService, useValue: warehouseActions },
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

  it("disables Create until the name matches the backend's naming rule", () => {
    component.visible = true;
    fixture.detectChanges();

    const createButton = document.querySelector<HTMLButtonElement>("#confirm-create-warehouse-btn")!;
    expect(createButton.disabled).toBe(true);

    for (const invalid of ["my bucket", "-starts-wrong", "no/slash"]) {
      component.newWarehouseName = invalid;
      fixture.detectChanges();
      expect(createButton.disabled).toBe(true);
    }

    component.newWarehouseName = "my-bucket_1";
    fixture.detectChanges();
    expect(createButton.disabled).toBe(false);
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

  it("create fires the trimmed request, clears the name, and closes at once", () => {
    warehouseActions.create.mockReturnValue(new Subject<DashboardWarehouse>().asObservable());
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);
    component.visible = true;
    component.newWarehouseName = "  mybucket  ";
    fixture.detectChanges();

    document.querySelector<HTMLButtonElement>("#confirm-create-warehouse-btn")!.click();

    expect(warehouseActions.create).toHaveBeenCalledWith("mybucket");
    expect(component.newWarehouseName).toBe("");
    expect(component.visible).toBe(false);
    expect(visibleSpy).toHaveBeenCalledWith(false);
  });

  it("a second rapid click cannot fire a duplicate create", () => {
    // The dialog stays clickable through its close animation.
    warehouseActions.create.mockReturnValue(new Subject<DashboardWarehouse>().asObservable());
    component.visible = true;
    component.newWarehouseName = "mybucket";
    fixture.detectChanges();

    const createButton = document.querySelector<HTMLButtonElement>("#confirm-create-warehouse-btn")!;
    createButton.click();
    createButton.click();

    expect(warehouseActions.create).toHaveBeenCalledTimes(1);
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

  it("Enter on an invalid name does nothing and keeps the dialog open", () => {
    component.visible = true;
    fixture.detectChanges();

    for (const invalid of ["   ", "bad name"]) {
      component.newWarehouseName = invalid;
      document
        .querySelector<HTMLInputElement>("input[nz-input]")!
        .dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", bubbles: true }));
    }

    expect(warehouseActions.create).not.toHaveBeenCalled();
    expect(component.visible).toBe(true);
  });

  it("relays a create that lands after the close without touching a reopened dialog", () => {
    // The toasts belong to the actions service; what the dialog owes its host
    // is the (warehouseCreated) relay — and hands off the dialog the user has
    // since reopened mid-typing.
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

    expect(createdSpy).toHaveBeenCalledWith(created);
    expect(component.visible).toBe(true);
    expect(component.newWarehouseName).toBe("second-in-progress");
  });

  it("a create that fails after the close never surfaces as an unhandled RxJS error", () => {
    // RxJS reports an error hitting a subscriber without an error path via a
    // thrown setTimeout; flushing fake timers makes that deterministic.
    vi.useFakeTimers();
    try {
      const inFlight = new Subject<DashboardWarehouse>();
      warehouseActions.create.mockReturnValue(inFlight.asObservable());
      const createdSpy = vi.fn();
      component.warehouseCreated.subscribe(createdSpy);
      component.visible = true;
      component.newWarehouseName = "first";
      component.handleCreateWarehouseModalOk();

      inFlight.error({ error: "boom" });

      expect(() => vi.runAllTimers()).not.toThrow();
      expect(createdSpy).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
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
