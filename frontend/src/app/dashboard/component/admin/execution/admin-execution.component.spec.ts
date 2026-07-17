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

import { ComponentFixture, inject, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { AdminExecutionComponent } from "./admin-execution.component";
import { AdminExecutionService } from "../../../service/admin/execution/admin-execution.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Execution } from "../../../../common/type/execution";
import { NzTableQueryParams } from "ng-zorro-antd/table";
import { of } from "rxjs";

describe("AdminDashboardComponent", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [AdminExecutionService, ...commonTestProviders],
      imports: [AdminExecutionComponent, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => fixture.destroy());

  it("should create", inject([HttpTestingController], () => {
    expect(component).toBeTruthy();
  }));

  it("renders the workflow link to /user/workflow/<id> when the admin has access", () => {
    component.listOfExecutions = [
      {
        access: true,
        workflowId: 42,
        workflowName: "demo workflow",
        executionId: 1,
        executionName: "exec",
        userName: "alice",
        executionStatus: "COMPLETED",
      } as unknown as Execution,
    ];
    component.isLoading = false;
    fixture.detectChanges();

    const anchor = fixture.debugElement.query(By.css('a[href="/user/workflow/42"]'));
    expect(anchor).toBeTruthy();
  });
});

describe("AdminExecutionComponent pagination (#3586)", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;
  let service: AdminExecutionService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [AdminExecutionService, ...commonTestProviders],
      imports: [AdminExecutionComponent, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();

    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    service = TestBed.inject(AdminExecutionService);
    // Keep any data fetch inert and synchronous so the page-bar state can be asserted directly.
    // Note: we deliberately do NOT call fixture.detectChanges() (no ngOnInit/auto-refresh) and
    // drive onQueryParamsChange() directly, the way ng-zorro's nz-table does on a page click.
    vi.spyOn(service, "getExecutionList").mockReturnValue(of([]));
    vi.spyOn(service, "getTotalWorkflows").mockImplementation(() => of(component.totalWorkflows));
  });

  afterEach(() => fixture.destroy());

  function changeParams(pageSize: number, pageIndex: number): void {
    component.onQueryParamsChange({ pageSize, pageIndex, sort: [], filter: [] } as NzTableQueryParams);
  }

  it("moves to the page the user clicks (page 1 -> page 5)", () => {
    component.totalWorkflows = 65; // 13 pages at size 5
    component.pageSize = 5;
    component.currentPageIndex = 0;

    changeParams(5, 5);

    expect(component.pageSize).toBe(5);
    expect(component.currentPageIndex).toBe(4); // 0-indexed page 5
  });

  it("follows the emitted page index when the page size changes (reset to first page)", () => {
    // On a page-size change ng-zorro emits the new size together with pageIndex=1.
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 4; // currently on page 5

    changeParams(20, 1);

    expect(component.pageSize).toBe(20);
    expect(component.currentPageIndex).toBe(0); // must follow pageIndex=1, not stay on page 5
  });

  it("syncs both page size and page index from a single event, order-independently", () => {
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 4;

    changeParams(20, 3); // size and index change together

    expect(component.pageSize).toBe(20);
    expect(component.currentPageIndex).toBe(2); // page 3 at the new size
  });

  it("clamps to the last existing page when a larger page size removes pages", () => {
    component.totalWorkflows = 65; // 2 pages at size 50
    component.pageSize = 5;
    component.currentPageIndex = 12; // last page at size 5

    changeParams(50, 13);

    expect(component.pageSize).toBe(50);
    expect(component.currentPageIndex).toBe(1); // clamp to page 2 (the last page)
  });

  it("handles single-page results (stays on the only page)", () => {
    component.totalWorkflows = 3; // 1 page
    component.pageSize = 5;
    component.currentPageIndex = 0;

    changeParams(5, 1);

    expect(component.currentPageIndex).toBe(0);
  });

  it("clamps to the first page for empty results even if a later page is requested", () => {
    component.totalWorkflows = 0;
    component.pageSize = 5;
    component.currentPageIndex = 0;

    changeParams(5, 5); // requesting page 5 with no data at all

    expect(component.currentPageIndex).toBe(0);
  });

  it("does not refetch when neither page size nor page index changed (sort/filter handled elsewhere)", () => {
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 4; // page 5
    vi.mocked(service.getExecutionList).mockClear();

    changeParams(5, 5); // page 5 again, same size -> no-op for pagination

    expect(service.getExecutionList).not.toHaveBeenCalled();
  });

  it("fetches exactly once for a real page change", () => {
    component.totalWorkflows = 65;
    component.pageSize = 5;
    component.currentPageIndex = 0;
    vi.mocked(service.getExecutionList).mockClear();

    changeParams(5, 5);

    expect(service.getExecutionList).toHaveBeenCalledTimes(1);
  });
});
