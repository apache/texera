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
import { NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { SortButtonComponent } from "./sort-button.component";
import { SortMethod } from "../../../type/sort-method";

describe("SortButtonComponent", () => {
  let component: SortButtonComponent;
  let fixture: ComponentFixture<SortButtonComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SortButtonComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SortButtonComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create the component with correct default sorting method", () => {
    expect(component).toBeTruthy();
    expect(component.sortMethod).toBe(SortMethod.EditTimeDesc);
  });

  it("should handle lastSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.lastSort();
    expect(component.sortMethod).toBe(SortMethod.EditTimeDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.EditTimeDesc);
  });

  it("should handle dateSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.dateSort();
    expect(component.sortMethod).toBe(SortMethod.CreateTimeDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.CreateTimeDesc);
  });

  it("should handle ascSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.ascSort();
    expect(component.sortMethod).toBe(SortMethod.NameAsc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.NameAsc);
  });

  it("should handle dscSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.dscSort();
    expect(component.sortMethod).toBe(SortMethod.NameDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.NameDesc);
  });

  it("should handle execSort() correctly", () => {
    const emitSpy = vi.spyOn(component.sortMethodChange, "emit");
    component.execSort();
    expect(component.sortMethod).toBe(SortMethod.ExecutionTimeDesc);
    expect(emitSpy).toHaveBeenCalledWith(SortMethod.ExecutionTimeDesc);
  });

  it("shows the edit-time and execution-time options by default (e.g. for workflows)", () => {
    expect(component.showEditTime).toBe(true);
    expect(component.showExecutionTime).toBe(true);
  });

  it("can hide edit-time and execution-time options (e.g. for datasets, which have neither)", () => {
    component.showEditTime = false;
    component.showExecutionTime = false;
    fixture.detectChanges();
    expect(component.showEditTime).toBe(false);
    expect(component.showExecutionTime).toBe(false);
  });

  /**
   * The options live in an nz-dropdown-menu, whose content is an <ng-template>
   * that only mounts into a CDK overlay when the dropdown opens — jsdom never
   * drives that, so none of the menu ever rendered and the tests above could
   * only check the component's own methods. Instantiating the menu template
   * directly puts the rows in the fixture's DOM, so the @if guards, the labels
   * and the per-row (click) bindings all really run: re-pointing a row at the
   * wrong sort method, or dropping a guard, fails here.
   */
  describe("rendered sort menu", () => {
    /** Mounts the dropdown menu template into the fixture and returns its rows. */
    function renderMenu(): HTMLButtonElement[] {
      const menu = fixture.debugElement.query(By.directive(NzDropdownMenuComponent))
        .componentInstance as NzDropdownMenuComponent;
      menu.viewContainerRef.createEmbeddedView(menu.templateRef);
      fixture.detectChanges();
      return Array.from(fixture.nativeElement.querySelectorAll("li[nz-menu-item] button"));
    }

    const labelsOf = (rows: HTMLButtonElement[]): string[] => rows.map(row => row.textContent!.trim());

    it("lists every sort option when the resource has both timestamps", () => {
      expect(labelsOf(renderMenu())).toEqual([
        "By Edit Time",
        "By Create Time",
        "By Execution Time",
        "A -> Z",
        "Z -> A",
      ]);
    });

    it("drops only the edit-time option when the resource has no edit timestamp", () => {
      component.showEditTime = false;

      expect(labelsOf(renderMenu())).toEqual(["By Create Time", "By Execution Time", "A -> Z", "Z -> A"]);
    });

    it("drops only the execution-time option when the resource has no execution timestamp", () => {
      component.showExecutionTime = false;

      expect(labelsOf(renderMenu())).toEqual(["By Edit Time", "By Create Time", "A -> Z", "Z -> A"]);
    });

    it("emits the sort method that matches the clicked row", () => {
      const emitted: SortMethod[] = [];
      component.sortMethodChange.subscribe(method => emitted.push(method));
      const rows = renderMenu();
      const labels = labelsOf(rows);

      // Click every row in turn: each label must reach its own sort method, so
      // two rows wired to the same handler cannot pass.
      rows.forEach(row => row.click());

      expect(labels).toHaveLength(5);
      expect(emitted).toEqual([
        SortMethod.EditTimeDesc,
        SortMethod.CreateTimeDesc,
        SortMethod.ExecutionTimeDesc,
        SortMethod.NameAsc,
        SortMethod.NameDesc,
      ]);
      // ... and the last click is the state the button keeps.
      expect(component.sortMethod).toBe(SortMethod.NameDesc);
    });
  });
});
