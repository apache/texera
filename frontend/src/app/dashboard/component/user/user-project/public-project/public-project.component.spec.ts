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
import { of } from "rxjs";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { PublicProjectComponent } from "./public-project.component";
import { PublicProjectService } from "../../../../service/user/public-project/public-project.service";
import { PublicProject } from "../../../../type/dashboard-project.interface";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

describe("PublicProjectComponent", () => {
  let fixture: ComponentFixture<PublicProjectComponent>;
  let component: PublicProjectComponent;

  const sampleProjects: PublicProject[] = [
    { pid: 1, name: "Alpha", owner: "alice", creationTime: 1_000 },
    { pid: 2, name: "Bravo", owner: "bob", creationTime: 2_000 },
    { pid: 3, name: "Charlie", owner: "carol", creationTime: 3_000 },
  ];

  let mockPublicProjectService: {
    getPublicProjects: ReturnType<typeof vi.fn>;
    addPublicProjects: ReturnType<typeof vi.fn>;
  };
  let modalRef: { destroy: ReturnType<typeof vi.fn> };
  let disabledList: Set<number>;

  beforeEach(async () => {
    mockPublicProjectService = {
      getPublicProjects: vi.fn().mockReturnValue(of(sampleProjects)),
      addPublicProjects: vi.fn().mockReturnValue(of(undefined)),
    };
    modalRef = { destroy: vi.fn() };
    // pid 2 is disabled to verify the component reads disabledList from NZ_MODAL_DATA.
    disabledList = new Set<number>([2]);

    await TestBed.configureTestingModule({
      imports: [PublicProjectComponent, NoopAnimationsModule],
      providers: [
        ...commonTestProviders,
        { provide: PublicProjectService, useValue: mockPublicProjectService },
        { provide: NzModalRef, useValue: modalRef },
        { provide: NZ_MODAL_DATA, useValue: { disabledList } },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(PublicProjectComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(c => (c.innerHTML = ""));
  });

  it("should create and read disabledList from NZ_MODAL_DATA", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
    expect(component.disabledList).toBe(disabledList);
    expect(component.disabledList.has(2)).toBe(true);
  });

  it("ngOnInit loads publicProjectEntries from getPublicProjects and renders the rows", () => {
    fixture.detectChanges();
    expect(mockPublicProjectService.getPublicProjects).toHaveBeenCalledTimes(1);
    expect(component.publicProjectEntries).toEqual(sampleProjects);

    const rows = (fixture.nativeElement as HTMLElement).querySelectorAll("tbody tr");
    expect(rows.length).toBe(sampleProjects.length);
    const text = (fixture.nativeElement as HTMLElement).textContent ?? "";
    expect(text).toContain("Alpha");
    expect(text).toContain("alice");
    expect(text).toContain("Selected 0 items");
  });

  it("updateCheckedSet adds an id when checked and removes it when unchecked", () => {
    fixture.detectChanges();

    component.updateCheckedSet(1, true);
    expect(component.checkedList.has(1)).toBe(true);

    component.updateCheckedSet(1, false);
    expect(component.checkedList.has(1)).toBe(false);
  });

  it("onItemChecked updates the set and refreshes the checked status", () => {
    fixture.detectChanges();
    const refreshSpy = vi.spyOn(component, "refreshCheckedStatus");

    component.onItemChecked(1, true);

    expect(component.checkedList.has(1)).toBe(true);
    expect(refreshSpy).toHaveBeenCalledTimes(1);
    // Only 1 of 3 selected => indeterminate, not fully checked.
    expect(component.checked).toBe(false);
    expect(component.indeterminate).toBe(true);

    component.onItemChecked(1, false);
    expect(component.checkedList.has(1)).toBe(false);
    expect(refreshSpy).toHaveBeenCalledTimes(2);
    expect(component.indeterminate).toBe(false);
  });

  it("onAllChecked(true) selects every entry", () => {
    fixture.detectChanges();

    component.onAllChecked(true);

    sampleProjects.forEach(p => expect(component.checkedList.has(p.pid)).toBe(true));
    expect(component.checkedList.size).toBe(sampleProjects.length);
    expect(component.checked).toBe(true);
    expect(component.indeterminate).toBe(false);
  });

  it("onAllChecked(false) unchecks all entries", () => {
    fixture.detectChanges();
    component.onAllChecked(true);

    component.onAllChecked(false);

    expect(component.checkedList.size).toBe(0);
    expect(component.checked).toBe(false);
    expect(component.indeterminate).toBe(false);
  });

  describe("refreshCheckedStatus", () => {
    beforeEach(() => fixture.detectChanges());

    it("all selected => checked true, indeterminate false", () => {
      sampleProjects.forEach(p => component.checkedList.add(p.pid));

      component.refreshCheckedStatus();

      expect(component.checked).toBe(true);
      expect(component.indeterminate).toBe(false);
    });

    it("none selected => checked false, indeterminate false", () => {
      component.checkedList.clear();

      component.refreshCheckedStatus();

      expect(component.checked).toBe(false);
      expect(component.indeterminate).toBe(false);
    });

    it("some selected => checked false, indeterminate true", () => {
      component.checkedList.add(sampleProjects[0].pid);

      component.refreshCheckedStatus();

      expect(component.checked).toBe(false);
      expect(component.indeterminate).toBe(true);
    });
  });

  it("addPublicProjects calls the service with the checked ids and destroys the modal on success", () => {
    fixture.detectChanges();
    component.checkedList = new Set<number>([1, 3]);

    component.addPublicProjects();

    expect(mockPublicProjectService.addPublicProjects).toHaveBeenCalledTimes(1);
    const [[checkedIds]] = mockPublicProjectService.addPublicProjects.mock.calls;
    expect(checkedIds).toHaveLength(2);
    expect(checkedIds).toEqual(expect.arrayContaining([1, 3]));
    expect(modalRef.destroy).toHaveBeenCalledTimes(1);
  });
});
