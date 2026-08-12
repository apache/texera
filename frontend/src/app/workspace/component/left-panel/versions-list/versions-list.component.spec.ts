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
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { DEFAULT_WORKFLOW } from "../../../service/workflow-graph/model/workflow-action.service";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyModule } from "@ngx-formly/core";
import { TEXERA_FORMLY_CONFIG } from "../../../../common/formly/formly-config";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { VersionsListComponent } from "./versions-list.component";
import { RouterTestingModule } from "@angular/router/testing";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";
import { WorkflowVersionEntry } from "../../../../dashboard/type/workflow-version-entry";
import { Workflow } from "../../../../common/type/workflow";
import { of } from "rxjs";

describe("VersionsListComponent", () => {
  let component: VersionsListComponent;
  let fixture: ComponentFixture<VersionsListComponent>;
  let workflowActionService: WorkflowActionService;
  let workflowVersionService: WorkflowVersionService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [WorkflowActionService, ...commonTestProviders],
      imports: [
        VersionsListComponent,
        BrowserAnimationsModule,
        FormsModule,
        FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
        ReactiveFormsModule,
        HttpClientTestingModule,
        RouterTestingModule.withRoutes([]),
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(VersionsListComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowVersionService = TestBed.inject(WorkflowVersionService);
    // Intentionally do NOT call fixture.detectChanges() here: the ngOnInit specs configure
    // their spies and then invoke ngOnInit() themselves, so this lets them observe the
    // first/only invocation. Tests that need the rendered template call detectChanges() locally.
  });

  afterEach(() => {
    fixture.destroy();
    vi.restoreAllMocks();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  // Helper: build a collapsable-entry list directly on the component.
  const makeEntry = (vId: number, importance: boolean, expand = false) => ({
    vId,
    creationTime: vId * 1000,
    content: `content-${vId}`,
    importance,
    expand,
  });

  describe("getDisplayedVersionId", () => {
    it("should compute the descending displayed id as count - index", () => {
      expect(component.getDisplayedVersionId(0, 5)).toBe(5);
      expect(component.getDisplayedVersionId(2, 5)).toBe(3);
      expect(component.getDisplayedVersionId(4, 5)).toBe(1);
    });
  });

  describe("collapse", () => {
    it("should do nothing when versionsList is undefined", () => {
      component.versionsList = undefined;
      // must not throw when there is no list to walk
      expect(() => component.collapse(0, true)).not.toThrow();
      expect(component.versionsList).toBeUndefined();
    });

    it("should expand the trailing non-important entries when $event is true", () => {
      component.versionsList = [
        makeEntry(5, true),
        makeEntry(4, false),
        makeEntry(3, false),
        makeEntry(2, true),
        makeEntry(1, false),
      ];

      component.collapse(0, true);

      // entries 1 and 2 (non-important) are expanded, then the walk stops at the
      // next important entry (index 3); index 3 and 4 are left untouched.
      expect(component.versionsList[1].expand).toBe(true);
      expect(component.versionsList[2].expand).toBe(true);
      expect(component.versionsList[3].expand).toBe(false);
      expect(component.versionsList[4].expand).toBe(false);
    });

    it("should collapse the trailing non-important entries when $event is false", () => {
      component.versionsList = [
        makeEntry(5, true),
        makeEntry(4, false, true),
        makeEntry(3, false, true),
        makeEntry(2, true, true),
        makeEntry(1, false, true),
      ];

      component.collapse(0, false);

      expect(component.versionsList[1].expand).toBe(false);
      expect(component.versionsList[2].expand).toBe(false);
      // stops at the important entry, so its expand state is preserved
      expect(component.versionsList[3].expand).toBe(true);
      expect(component.versionsList[4].expand).toBe(true);
    });

    it("should stop immediately when the next entry is important", () => {
      component.versionsList = [makeEntry(3, false), makeEntry(2, true), makeEntry(1, false)];

      component.collapse(0, true);

      // the very next entry (index 1) is important, so nothing is changed
      expect(component.versionsList[1].expand).toBe(false);
      expect(component.versionsList[2].expand).toBe(false);
    });
  });

  describe("ngOnInit", () => {
    it("should unhighlight the currently highlighted elements", () => {
      const wrapper = workflowActionService.getJointGraphWrapper();
      const highlights = { operators: ["op-1"], groups: [], links: [], commentBoxes: [], ports: [] };
      vi.spyOn(wrapper, "getCurrentHighlights").mockReturnValue(highlights as any);
      const unhighlightSpy = vi.spyOn(wrapper, "unhighlightElements").mockImplementation(() => {});

      component.ngOnInit();

      expect(unhighlightSpy).toHaveBeenCalledWith(highlights);
    });

    it("should not retrieve versions when the route has no workflow id", () => {
      (component.route.snapshot.params as any).id = undefined;
      const retrieveSpy = vi.spyOn(workflowVersionService, "retrieveVersionsOfWorkflow");
      component.versionsList = undefined;

      component.ngOnInit();

      expect(retrieveSpy).not.toHaveBeenCalled();
      expect(component.versionsList).toBeUndefined();
    });

    it("should load and map the retrieved versions with expand defaulting to false", () => {
      const entries: WorkflowVersionEntry[] = [
        { vId: 10, creationTime: 1000, content: "c10", importance: true },
        { vId: 9, creationTime: 900, content: "c9", importance: false },
      ];
      (component.route.snapshot.params as any).id = 42;
      const retrieveSpy = vi.spyOn(workflowVersionService, "retrieveVersionsOfWorkflow").mockReturnValue(of(entries));

      component.ngOnInit();

      expect(retrieveSpy).toHaveBeenCalledWith(42);
      expect(component.versionsList).toEqual([
        { vId: 10, creationTime: 1000, content: "c10", importance: true, expand: false },
        { vId: 9, creationTime: 900, content: "c9", importance: false, expand: false },
      ]);
    });
  });

  describe("getVersion", () => {
    const mockWorkflow = { content: {} } as unknown as Workflow;

    it("should select the row, fetch the version and display it", () => {
      const retrieveSpy = vi
        .spyOn(workflowVersionService, "retrieveWorkflowByVersion")
        .mockReturnValue(of(mockWorkflow));
      const displaySpy = vi.spyOn(workflowVersionService, "displayParticularVersion").mockImplementation(() => {});

      component.getVersion(7, 3, 2);

      // selected row is recorded immediately (synchronously)
      expect(component.selectedRowIndex).toBe(2);
      // wid comes from the workflow metadata (default is 0)
      expect(retrieveSpy).toHaveBeenCalledWith(0, 7);
      expect(displaySpy).toHaveBeenCalledWith(mockWorkflow, 7, 3);
    });

    it("should use the current workflow metadata wid when fetching the version", () => {
      workflowActionService.setWorkflowMetadata({ ...DEFAULT_WORKFLOW, wid: 99 });
      const retrieveSpy = vi
        .spyOn(workflowVersionService, "retrieveWorkflowByVersion")
        .mockReturnValue(of(mockWorkflow));
      vi.spyOn(workflowVersionService, "displayParticularVersion").mockImplementation(() => {});

      component.getVersion(4, 1, 0);

      expect(retrieveSpy).toHaveBeenCalledWith(99, 4);
      expect(component.selectedRowIndex).toBe(0);
    });
  });
  /**
   * The table's own logic lives in the template: which rows survive the collapse predicate, the
   * descending version number, and the selection highlight. The class-level specs above drive
   * collapse() and getDisplayedVersionId() directly and never render, so none of it was pinned.
   */
  describe("rendered table", () => {
    /** Renders the given entries and returns the rows that survived the *ngIf. */
    function renderRows(entries: ReturnType<typeof makeEntry>[]): HTMLTableRowElement[] {
      component.versionsList = entries as any;
      fixture.detectChanges();
      return Array.from(fixture.nativeElement.querySelectorAll("tbody tr"));
    }

    it("renders no table at all until the versions have loaded", () => {
      component.versionsList = undefined as any;
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector("#versions-list")).toBeNull();
    });

    it("labels the columns from versionTableHeaders", () => {
      renderRows([makeEntry(1, true)]);

      const headers = Array.from(fixture.nativeElement.querySelectorAll("thead th")).map(th =>
        (th as HTMLElement).textContent?.trim()
      );
      expect(headers).toEqual(component.versionTableHeaders);
    });

    it("hides an unimportant version while it is collapsed", () => {
      // The predicate is (!importance && expand) || importance: minor versions stay folded away
      // until their important parent is expanded, which is the whole point of the collapse.
      const rows = renderRows([makeEntry(3, true), makeEntry(2, false, false), makeEntry(1, false, false)]);

      expect(rows).toHaveLength(1);
    });

    it("reveals an unimportant version once it is expanded", () => {
      const rows = renderRows([makeEntry(3, true), makeEntry(2, false, true), makeEntry(1, false, false)]);

      expect(rows).toHaveLength(2);
    });

    it("numbers the versions downwards, newest first", () => {
      const rows = renderRows([makeEntry(3, true), makeEntry(2, true), makeEntry(1, true)]);

      const numbers = rows.map(r => r.querySelectorAll("td")[0].textContent?.trim());
      expect(numbers).toEqual(["3", "2", "1"]);
    });

    it("marks only the selected row", () => {
      component.selectedRowIndex = 1;
      const rows = renderRows([makeEntry(2, true), makeEntry(1, true)]);

      expect(rows[0].classList).not.toContain("selected-row");
      expect(rows[1].classList).toContain("selected-row");
    });

    it("offers the expand control only on an important version", () => {
      const rows = renderRows([makeEntry(2, true), makeEntry(1, false, true)]);

      expect(rows[0].querySelector("[nztableexpand], .ant-table-row-expand-icon")).not.toBeNull();
      expect(rows[1].querySelector(".ant-table-row-expand-icon")).toBeNull();
    });

    it("asks for the version behind the row that was clicked", () => {
      const getVersion = vi.spyOn(component, "getVersion").mockImplementation(() => {});
      const rows = renderRows([makeEntry(30, true), makeEntry(20, true)]);

      rows[1].querySelector<HTMLButtonElement>("button.version-link")!.click();

      // vId of the clicked row, its displayed (descending) number, and its index.
      expect(getVersion).toHaveBeenCalledWith(20, 1, 1);
    });

    it("shows the timestamp in the compact date format", () => {
      const rows = renderRows([makeEntry(1, true)]);

      expect(rows[0].querySelector("button.version-link")!.textContent?.trim()).toMatch(
        /^\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}$/
      );
    });
  });
});
