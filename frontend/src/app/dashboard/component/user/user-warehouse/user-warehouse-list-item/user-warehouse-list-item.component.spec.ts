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
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { CloudServerOutline, DeleteOutline } from "@ant-design/icons-angular/icons";
import { UserWarehouseListItemComponent } from "./user-warehouse-list-item.component";
import { DashboardWarehouse } from "../../../../../common/type/warehouse";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

describe("UserWarehouseListItemComponent", () => {
  let fixture: ComponentFixture<UserWarehouseListItemComponent>;

  const warehouse: DashboardWarehouse = {
    whid: 3,
    name: "sales",
    lakekeeperWarehouseName: "user-1-sales",
    flavor: "local",
    createdAtMillis: 0,
    ownerName: "Alice",
    ownerAvatar: "",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [UserWarehouseListItemComponent, NzIconModule.forChild([CloudServerOutline, DeleteOutline])],
      providers: [NzModalService, ...commonTestProviders],
    }).compileComponents();
  });

  it("throws when rendered without a warehouse", () => {
    const item = new UserWarehouseListItemComponent({} as NzModalService);

    expect(() => item.warehouse).toThrowError("warehouse property must be provided to UserWarehouseListItemComponent.");
  });

  it("renders the id, the name with its full-name tooltip, and the metadata columns", () => {
    fixture = TestBed.createComponent(UserWarehouseListItemComponent);
    fixture.componentInstance.warehouse = warehouse;
    fixture.detectChanges();

    const element: HTMLElement = fixture.nativeElement;
    expect(element.querySelector(".warehouse-id")?.textContent).toContain("#3");
    expect(element.querySelector(".resource-name")?.textContent).toContain("sales");
    const nameTooltip = fixture.debugElement.query(By.css(".resource-name")).injector.get(NzTooltipDirective);
    expect(nameTooltip.title).toBe("sales");
    // The relative "Created" value depends on the clock; assert the labels only.
    expect(element.textContent).toContain("Created:");
    expect(element.textContent).toContain("Flavor:");
    expect(element.textContent).toContain("local");
  });

  it("emits deleted when the delete button is clicked", () => {
    fixture = TestBed.createComponent(UserWarehouseListItemComponent);
    fixture.componentInstance.warehouse = warehouse;
    fixture.detectChanges();
    const deletedSpy = vi.fn();
    fixture.componentInstance.deleted.subscribe(deletedSpy);

    fixture.debugElement.query(By.css(".button-group button")).triggerEventHandler("click", null);

    expect(deletedSpy).toHaveBeenCalledTimes(1);
  });

  it("opens the metadata modal when the name is clicked", () => {
    fixture = TestBed.createComponent(UserWarehouseListItemComponent);
    fixture.componentInstance.warehouse = warehouse;
    fixture.detectChanges();
    const createSpy = vi.spyOn(TestBed.inject(NzModalService), "create").mockReturnValue({} as NzModalRef);

    fixture.debugElement.query(By.css(".resource-name")).triggerEventHandler("click", null);

    expect(createSpy).toHaveBeenCalledTimes(1);
    const config = createSpy.mock.calls[0][0];
    expect(config.nzTitle).toBe("Warehouse Information");
    expect(config.nzData).toEqual(warehouse);
    expect(config.nzFooter).toBeNull();
  });
});
