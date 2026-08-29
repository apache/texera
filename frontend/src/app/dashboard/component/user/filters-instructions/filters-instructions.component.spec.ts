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

import { FiltersInstructionsComponent } from "./filters-instructions.component";
import { NzPopoverDirective, NzPopoverModule } from "ng-zorro-antd/popover";

describe("FiltersInstructionsComponent", () => {
  let component: FiltersInstructionsComponent;
  let fixture: ComponentFixture<FiltersInstructionsComponent>;

  /** Collapses the template's indentation so each item can be compared as one line. */
  function listItems(list: Element): string[] {
    return Array.from(list.querySelectorAll("li")).map(li => (li.textContent ?? "").replace(/\s+/g, " ").trim());
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      // NoopAnimationsModule keeps the popover's zoom transition out of the test, so
      // the panel is in the DOM as soon as it opens and gone as soon as it closes.
      imports: [FiltersInstructionsComponent, NzPopoverModule, NoopAnimationsModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FiltersInstructionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    // Destroying the fixture disposes the CDK overlay too, so nothing this spec
    // rendered into document.body survives into the next test.
    fixture.destroy();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("instructions popover", () => {
    // The help text lives in an <ng-template> that ng-zorro only stamps out once the
    // popover opens, and it lands in the CDK overlay under document.body rather than
    // in the fixture's own host element. Opening it through the directive keeps the
    // test off the hover trigger's mouse-enter/leave delay timers.
    function openPopover(): HTMLElement {
      const trigger = fixture.debugElement.query(By.directive(NzPopoverDirective));
      trigger.injector.get(NzPopoverDirective).show();
      fixture.detectChanges();
      const panel = document.querySelector(".ant-popover");
      expect(panel, "expected the popover panel to be rendered").toBeTruthy();
      return panel as HTMLElement;
    }

    it("renders only the trigger icon while the popover is closed", () => {
      expect(fixture.nativeElement.querySelector("i.search-info-box")).toBeTruthy();
      expect(document.querySelector(".ant-popover")).toBeNull();
    });

    it("lists every supported search criterion once opened", () => {
      const panel = openPopover();

      expect(panel.textContent).toContain("Filter Instructions");
      expect(panel.textContent).toContain("We support the following search criteria:");

      expect(listItems(panel.querySelectorAll("ul")[0])).toEqual([
        "Search by Workflow Name: workflowName",
        "Search by Workflow Creation Time: ctime: yyyy-MM-dd",
        "Search by Workflow Modification Time: mtime: yyyy-MM-dd",
        "Search by Workflow Owner: owner: John",
        "Search by Workflow Id: id: workflowId",
        "Search by Workflows' Operators: operator: operatorName",
      ]);
    });

    it("explains how to change the search parameters and shows a worked example", () => {
      const panel = openPopover();

      expect(panel.textContent).toContain("You can change search parameters by:");

      expect(listItems(panel.querySelectorAll("ul")[1])).toEqual([
        "selecting/unselecting dropdown menu options",
        "manually typing parameters into search bar",
        "clicking the X on a tag or the search bar, to clear one or all tags",
      ]);

      expect(panel.textContent).toContain('Example: "Untitled Workflow" id:1 owner:John');
      expect(panel.textContent).toContain(
        "Meaning: Search for the workflow with name Untitled Workflow, id 1, and the owner called John."
      );
    });
  });
});
