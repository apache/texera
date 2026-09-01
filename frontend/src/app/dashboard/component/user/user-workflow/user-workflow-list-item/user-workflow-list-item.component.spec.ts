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

import { Component, ViewChild } from "@angular/core";
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { UserWorkflowListItemComponent } from "./user-workflow-list-item.component";
import { FileSaverService } from "../../../../service/user/file/file-saver.service";
import { testWorkflow1, testWorkflowEntries } from "../../../user-dashboard-test-fixtures";
import { By } from "@angular/platform-browser";
import { StubWorkflowPersistService } from "../../../../../common/service/workflow-persist/stub-workflow-persist.service";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../../common/service/workflow-persist/workflow-persist.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { WorkflowExecutionHistoryComponent } from "../ngbd-modal-workflow-executions/workflow-execution-history.component";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { Workflow } from "../../../../../common/type/workflow";
import { of } from "rxjs";
import { NzListComponent } from "ng-zorro-antd/list";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { provideRouter } from "@angular/router";
import { DashboardEntry } from "../../../../type/dashboard-entry";
import { NzTooltipDirective, NzTooltipModule } from "ng-zorro-antd/tooltip";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { GuiConfigService } from "../../../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../../../common/service/gui-config.service.mock";

// UserWorkflowListItemComponent is rooted at <nz-list-item>; instantiating it
// outside an <nz-list> host throws "No provider found for NzListComponent".
@Component({
  standalone: true,
  imports: [NzListComponent, UserWorkflowListItemComponent],
  template: `
    <nz-list>
      <texera-user-workflow-list-item
        [entry]="entry"
        [editable]="editable"></texera-user-workflow-list-item>
    </nz-list>
  `,
})
class TestHostComponent {
  entry!: DashboardEntry;
  editable = true;
  @ViewChild(UserWorkflowListItemComponent, { static: true }) inner!: UserWorkflowListItemComponent;
}

// A fresh DashboardEntry per call so methods that mutate the workflow (rename)
// cannot leak into the shared testWorkflowEntries fixture.
function makeWorkflowEntry(workflowOverrides: Partial<Workflow> = {}): DashboardEntry {
  return new DashboardEntry({
    workflow: { ...testWorkflow1, ...workflowOverrides },
    isOwner: true,
    ownerName: "Texera",
    accessLevel: "Write",
    ownerId: 1,
    coverImage: null,
  });
}

describe("UserWorkflowListItemComponent", () => {
  let component: UserWorkflowListItemComponent;
  let fixture: ComponentFixture<TestHostComponent>;
  const fileSaverServiceSpy = { saveAs: vi.fn() } as unknown as Mocked<FileSaverService>;
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TestHostComponent, NzModalModule, HttpClientTestingModule, NzTooltipModule],
      providers: [
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: FileSaverService, useValue: fileSaverServiceSpy },
        provideRouter([]),
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TestHostComponent);
    fixture.componentInstance.entry = testWorkflowEntries[0];
    fixture.componentInstance.editable = true;
    fixture.detectChanges();
    component = fixture.componentInstance.inner;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("sends http request to backend to retrieve export json", () => {
    // Test the workflow download button.
    component.onClickDownloadWorkfllow();
    expect(fileSaverServiceSpy.saveAs).toHaveBeenCalledExactlyOnceWith(
      new Blob([JSON.stringify(testWorkflowEntries[0].workflow.workflow.content)], {
        type: "text/plain;charset=utf-8",
      }),
      "workflow 1.json"
    );
  });

  it("adding a workflow description adds a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      let addWorkflowDescriptionBtn = fixture.debugElement.query(By.css(".add-description-btn"));
      expect(addWorkflowDescriptionBtn).toBeTruthy();
      addWorkflowDescriptionBtn.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput = fixture.debugElement.nativeElement.querySelector(".workflow-editable-description");
      expect(editableDescriptionInput).toBeTruthy();
      vi.spyOn(component, "confirmUpdateWorkflowCustomDescription");
      sendInput(editableDescriptionInput, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  it("Editing a workflow description edits a description to the workflow", waitForAsync(() => {
    fixture.whenStable().then(() => {
      const workflowDescriptionLabel = fixture.debugElement.query(By.css(".workflow-description"));
      expect(workflowDescriptionLabel).toBeTruthy();
      workflowDescriptionLabel.triggerEventHandler("click", null);
      fixture.detectChanges();
      let editableDescriptionInput1 = fixture.debugElement.nativeElement.querySelector(
        ".workflow-editable-description"
      );
      expect(editableDescriptionInput1).toBeTruthy();
      vi.spyOn(component, "confirmUpdateWorkflowCustomDescription");
      sendInput(editableDescriptionInput1, "dummy description added by focusing out the input element.").then(() => {
        fixture.detectChanges();
        editableDescriptionInput1.dispatchEvent(new Event("focusout"));
        fixture.detectChanges();
        expect(component.confirmUpdateWorkflowCustomDescription).toHaveBeenCalledTimes(1);
      });
    });
  }));

  describe("method coverage", () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    describe("confirmUpdateWorkflowCustomName", () => {
      it("persists the new name, updates the workflow, and stops editing", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        // The stub lacks updateWorkflowName; give this fresh-per-test instance a spy.
        const spy = ((persist as any).updateWorkflowName = vi.fn().mockReturnValue(of(undefined)));
        component.entry = makeWorkflowEntry({ wid: 5, name: "old" });
        component.editingName = true;

        component.confirmUpdateWorkflowCustomName("new name");

        expect(spy).toHaveBeenCalledWith(5, "new name");
        expect(component.workflow.name).toBe("new name");
        expect(component.editingName).toBe(false);
      });

      it("falls back to the default name when the input is empty", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowName = vi.fn().mockReturnValue(of(undefined)));
        component.entry = makeWorkflowEntry({ wid: 5 });

        component.confirmUpdateWorkflowCustomName("");

        expect(spy).toHaveBeenCalledWith(5, DEFAULT_WORKFLOW_NAME);
        expect(component.workflow.name).toBe(DEFAULT_WORKFLOW_NAME);
      });

      it("is a no-op when the workflow has no id", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowName = vi.fn());
        component.entry = makeWorkflowEntry({ wid: undefined });

        component.confirmUpdateWorkflowCustomName("x");

        expect(spy).not.toHaveBeenCalled();
      });
    });

    describe("confirmUpdateWorkflowCustomDescription", () => {
      it("persists the new description and stops editing", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowDescription = vi.fn().mockReturnValue(of(undefined)));
        component.entry = makeWorkflowEntry({ wid: 5 });
        component.editingDescription = true;

        component.confirmUpdateWorkflowCustomDescription("new desc");

        expect(spy).toHaveBeenCalledWith(5, "new desc");
        expect(component.workflow.description).toBe("new desc");
        expect(component.editingDescription).toBe(false);
      });

      it("is a no-op when the workflow has no id", () => {
        const persist = TestBed.inject(WorkflowPersistService);
        const spy = ((persist as any).updateWorkflowDescription = vi.fn());
        component.entry = makeWorkflowEntry({ wid: undefined });

        component.confirmUpdateWorkflowCustomDescription("x");

        expect(spy).not.toHaveBeenCalled();
      });
    });

    it("onClickGetWorkflowExecutions opens the execution-history modal for the workflow", () => {
      const modal = TestBed.inject(NzModalService);
      const spy = vi.spyOn(modal, "create").mockReturnValue({} as any);
      component.entry = makeWorkflowEntry({ wid: 9, name: "wf" });

      component.onClickGetWorkflowExecutions();

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({
          nzContent: WorkflowExecutionHistoryComponent,
          nzData: { wid: 9 },
          nzTitle: "Execution results of Workflow: wf",
        })
      );
    });

    describe("onClickDownloadWorkfllow", () => {
      it("delegates to the download service with the workflow id and name", () => {
        const download = TestBed.inject(DownloadService);
        const spy = vi.spyOn(download, "downloadWorkflow").mockReturnValue(of(undefined) as any);
        component.entry = makeWorkflowEntry({ wid: 9, name: "wf" });

        component.onClickDownloadWorkfllow();

        expect(spy).toHaveBeenCalledWith(9, "wf");
      });

      it("does nothing when the workflow has no id", () => {
        const download = TestBed.inject(DownloadService);
        const spy = vi.spyOn(download, "downloadWorkflow");
        component.entry = makeWorkflowEntry({ wid: undefined });

        component.onClickDownloadWorkfllow();

        expect(spy).not.toHaveBeenCalled();
      });
    });

    it("opens the share modal for this row's workflow, carrying its write access and the owner list", async () => {
      const entry = makeWorkflowEntry({ wid: 21, name: "wf" });
      entry.workflow.accessLevel = "WRITE";
      component.entry = entry;
      const persist = TestBed.inject(WorkflowPersistService);
      // The owner list is the autocomplete the dialog exists to offer; it is the one awaited
      // value in the method, so nothing else proves it is resolved before the modal opens.
      vi.spyOn(persist, "retrieveOwners").mockReturnValue(of(["a@example.com", "b@example.com"]));
      const modal = TestBed.inject(NzModalService);
      const spy = vi.spyOn(modal, "create").mockReturnValue({} as any);

      await component.onClickOpenShareAccess();

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({
          nzContent: ShareAccessComponent,
          nzData: expect.objectContaining({
            writeAccess: true,
            type: "workflow",
            id: 21,
            allOwners: ["a@example.com", "b@example.com"],
          }),
        })
      );
    });

    it("marks the share modal read-only for a row the viewer can only read", async () => {
      const entry = makeWorkflowEntry({ wid: 21 });
      entry.workflow.accessLevel = "READ";
      component.entry = entry;
      const modal = TestBed.inject(NzModalService);
      const spy = vi.spyOn(modal, "create").mockReturnValue({} as any);

      await component.onClickOpenShareAccess();

      expect(spy).toHaveBeenCalledWith(
        expect.objectContaining({ nzData: expect.objectContaining({ writeAccess: false }) })
      );
    });

    describe("mis-wired inputs", () => {
      // Both accessors are read from the template on every change-detection pass, so a silent
      // undefined would surface as an unrelated crash deep in ng-zorro instead of here.
      it("refuses to read the entry before one has been provided", () => {
        component.entry = undefined as any;

        expect(() => component.entry).toThrowError("entry property must be provided to UserWorkflowListItemComponent.");
      });

      it("refuses to read a workflow off an entry that carries no workflow payload", () => {
        // The guard tests for the payload, not for entry.type: an entry of any kind that does
        // carry a workflow passes it, so this must not claim to be a kind check.
        component.entry = { name: "ds" } as unknown as DashboardEntry;

        expect(() => component.workflow).toThrowError(/Entry must be workflow/);
      });
    });
  });

  function sendInput(editableDescriptionInput: HTMLInputElement, text: string) {
    // Helper function to change the workflow description textbox.
    editableDescriptionInput.value = text;
    editableDescriptionInput.dispatchEvent(new Event("input"));
    fixture.detectChanges();
    return fixture.whenStable();
  }
});
/**
 * The list item's template carries decisions the class does not: which control a click reaches, what
 * value an edit forwards, whether a read-only row may still be edited inline, and which actions the
 * row offers. The suite above calls the component's methods directly, so none of that was rendered.
 */
describe("UserWorkflowListItemComponent rendering", () => {
  let fixture: ComponentFixture<TestHostComponent>;
  let component: UserWorkflowListItemComponent;
  let persistService: { updateWorkflowName: ReturnType<typeof vi.fn> };

  async function setup(opts: { executionsTracking?: boolean } = {}) {
    // StubWorkflowPersistService does not declare updateWorkflowName, so it cannot be spied on.
    persistService = { updateWorkflowName: vi.fn(() => of({} as Response)) };
    TestBed.resetTestingModule();
    await TestBed.configureTestingModule({
      imports: [TestHostComponent, NzModalModule, HttpClientTestingModule, NzTooltipModule],
      providers: [
        { provide: WorkflowPersistService, useValue: persistService },
        { provide: FileSaverService, useValue: { saveAs: vi.fn() } },
        provideRouter([]),
        ...commonTestProviders,
      ],
    }).compileComponents();
    if (opts.executionsTracking) {
      (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
        workflowExecutionsTrackingEnabled: true,
      });
    }
  }

  /** Renders one row. */
  function render(entry: DashboardEntry, editable = true): HTMLElement {
    fixture = TestBed.createComponent(TestHostComponent);
    fixture.componentInstance.entry = entry;
    fixture.componentInstance.editable = editable;
    fixture.detectChanges();
    component = fixture.componentInstance.inner;
    return fixture.nativeElement as HTMLElement;
  }

  /**
   * Tooltip titles on the row, read off the directive: ng-zorro consumes the nz-tooltip attribute,
   * and an interpolated title is a property binding that never appears in the DOM at all.
   */
  function tooltipTitles(): unknown[] {
    return fixture.debugElement
      .queryAll(By.directive(NzTooltipDirective))
      .map(d => (d.injector.get(NzTooltipDirective) as NzTooltipDirective).directiveTitle);
  }

  /** The elements whose tooltip title satisfies the predicate, in document order. */
  function byTooltip(pred: (title: string) => boolean): HTMLElement[] {
    return fixture.debugElement
      .queryAll(By.directive(NzTooltipDirective))
      .filter(d => {
        const t = (d.injector.get(NzTooltipDirective) as NzTooltipDirective).directiveTitle;
        return typeof t === "string" && pred(t);
      })
      .map(d => d.nativeElement as HTMLElement);
  }

  /**
   * The one element whose tooltip title satisfies the predicate. Indexing byTooltip() directly
   * reports a missing or duplicated action as a TypeError on `undefined.click()` — or, worse,
   * silently clicks the first of several; asserting the match is unique names the real problem.
   */
  function onlyByTooltip(pred: (title: string) => boolean): HTMLElement {
    const matches = byTooltip(pred);
    expect(matches).toHaveLength(1);
    return matches[0];
  }

  beforeEach(async () => {
    await setup();
  });

  describe("renaming", () => {
    it("forwards the text that was typed, not the name it started with", () => {
      // The input is seeded with the current name, so binding workflow.name instead of the input's
      // value would still look right on screen while silently discarding every rename.
      render(makeWorkflowEntry({ wid: 7, name: "before" }));

      component.editingName = true;
      fixture.detectChanges();

      const input = (fixture.nativeElement as HTMLElement).querySelector<HTMLInputElement>("input")!;
      expect(input.value).toBe("before");
      input.value = "after";
      input.dispatchEvent(new Event("focusout"));

      expect(persistService.updateWorkflowName).toHaveBeenCalledWith(7, "after");
    });
  });

  describe("inline description editing", () => {
    it("opens for a viewer who may edit", () => {
      const el = render(makeWorkflowEntry(), true);

      el.querySelector<HTMLElement>(".workflow-description")?.click();
      fixture.detectChanges();

      expect(component.editingDescription).toBe(true);
    });

    it("stays shut for a read-only viewer", () => {
      // The gate is the template expression editingDescription = editable; without it a shared
      // read-only row opens an editor whose save the backend then rejects.
      const el = render(makeWorkflowEntry(), false);

      el.querySelector<HTMLElement>(".workflow-description")?.click();
      fixture.detectChanges();

      expect(component.editingDescription).toBe(false);
    });
  });

  describe("ownership", () => {
    it("tells a non-owner what access they have and who shared it", () => {
      const entry = makeWorkflowEntry();
      entry.workflow.isOwner = false;
      entry.workflow.accessLevel = "READ";
      entry.workflow.ownerName = "Bob";
      render(entry);

      // Pins the order of the two interpolations.
      expect(tooltipTitles()).toContain("READ access shared by Bob");
    });

    it("shows no shared-access marker to the owner", () => {
      const entry = makeWorkflowEntry();
      entry.workflow.isOwner = true;
      render(entry);

      expect(tooltipTitles().some(t => typeof t === "string" && t.includes("access shared by"))).toBe(false);
    });

    it("disables deleting a workflow the viewer does not own", () => {
      const entry = makeWorkflowEntry();
      entry.workflow.isOwner = false;
      const el = render(entry);

      expect(el.querySelector<HTMLButtonElement>("button[nz-popconfirm]")?.disabled).toBe(true);
    });
  });

  describe("row actions", () => {
    it("withholds the executions action while execution tracking is off", () => {
      render(makeWorkflowEntry());

      expect(tooltipTitles().some(t => typeof t === "string" && t.startsWith("Executions of the workflow"))).toBe(
        false
      );
    });

    it("offers the executions action once execution tracking is on", async () => {
      await setup({ executionsTracking: true });
      render(makeWorkflowEntry());

      expect(tooltipTitles().some(t => typeof t === "string" && t.startsWith("Executions of the workflow"))).toBe(true);
    });

    it("keeps duplicate and delete on their own outputs", () => {
      // Adjacent icon buttons; emitting the wrong one of these would be destructive.
      render(makeWorkflowEntry());
      const duplicated = vi.fn();
      const deleted = vi.fn();
      component.duplicated.subscribe(duplicated);
      component.deleted.subscribe(deleted);

      const dup = byTooltip(t => t.startsWith("Duplicate"));
      expect(dup.length).toBe(1);
      dup[0].click();
      expect(duplicated).toHaveBeenCalledTimes(1);
      expect(deleted).not.toHaveBeenCalled();

      fixture.debugElement.query(By.css("button[nz-popconfirm]")).triggerEventHandler("nzOnConfirm", null);
      expect(deleted).toHaveBeenCalledTimes(1);
      expect(duplicated).toHaveBeenCalledTimes(1);
    });

    it("opens the executions modal from the history action", async () => {
      await setup({ executionsTracking: true });
      render(makeWorkflowEntry({ wid: 11, name: "wf" }));
      const modal = TestBed.inject(NzModalService);
      const create = vi.spyOn(modal, "create").mockReturnValue({} as any);

      onlyByTooltip(t => t.startsWith("Executions of the workflow")).click();

      expect(create).toHaveBeenCalledWith(
        expect.objectContaining({ nzContent: WorkflowExecutionHistoryComponent, nzData: { wid: 11 } })
      );
    });
  });

  describe("inline name and description editors", () => {
    it("puts name editing behind the pencil and description editing behind the plus", () => {
      // Two adjacent icon buttons on the same toolbar; swapping them would open the wrong editor.
      render(makeWorkflowEntry());
      expect(component.editingName).toBe(false);
      expect(component.editingDescription).toBe(false);

      onlyByTooltip(t => t === "Customize Workflow Name").click();

      expect(component.editingName).toBe(true);
      expect(component.editingDescription).toBe(false);

      onlyByTooltip(t => t === "Add Description").click();

      expect(component.editingDescription).toBe(true);
    });
  });

  describe("selection checkbox", () => {
    it("indents the avatar only when the checkbox is absent", () => {
      const withBox = render(makeWorkflowEntry(), true).querySelector<HTMLElement>("nz-list-item-meta-avatar")!;
      expect(withBox.style.marginLeft).not.toBe("16px");

      const withoutBox = render(makeWorkflowEntry(), false).querySelector<HTMLElement>("nz-list-item-meta-avatar")!;
      expect(withoutBox.style.marginLeft).toBe("16px");
      expect(withoutBox.querySelector(".workflow-item-checkbox")).toBeNull();
    });

    it("records the selection on the entry", () => {
      const entry = makeWorkflowEntry();
      render(entry, true);

      fixture.debugElement.query(By.css(".workflow-item-checkbox")).triggerEventHandler("ngModelChange", true);

      expect(entry.checked).toBe(true);
    });
  });
});
