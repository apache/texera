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
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { WarehouseMetadataComponent } from "./warehouse-metadata.component";
import { DashboardWarehouse } from "../../type/warehouse";

describe("WarehouseMetadataComponent", () => {
  const warehouse: DashboardWarehouse = {
    whid: 3,
    name: "sales",
    lakekeeperWarehouseName: "user-1-sales",
    flavor: "local",
    createdAtMillis: 1723300000000,
    ownerName: "Alice",
    ownerAvatar: "",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [WarehouseMetadataComponent],
      providers: [{ provide: NZ_MODAL_DATA, useValue: warehouse }],
    }).compileComponents();
  });

  it("renders every field of the injected warehouse", () => {
    const fixture = TestBed.createComponent(WarehouseMetadataComponent);
    fixture.detectChanges();

    const text = fixture.nativeElement.textContent;
    expect(text).toContain("sales");
    expect(text).toContain("Alice");
    expect(text).toContain("local");
    // The absolute timestamp is locale-dependent; assert against the same
    // conversion the component performs rather than a hard-coded string.
    expect(text).toContain(new Date(1723300000000).toLocaleString());
  });

  it("shows a dash for an owner with no display name", () => {
    TestBed.overrideProvider(NZ_MODAL_DATA, { useValue: { ...warehouse, ownerName: null } });
    const fixture = TestBed.createComponent(WarehouseMetadataComponent);
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).toContain("\u2014");
  });

  it("leaves out the catalog name, which locates data the user cannot reach yet", () => {
    const fixture = TestBed.createComponent(WarehouseMetadataComponent);
    fixture.detectChanges();

    expect(fixture.nativeElement.textContent).not.toContain("user-1-sales");
  });
});
