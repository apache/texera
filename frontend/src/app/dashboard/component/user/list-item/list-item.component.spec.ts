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
import { ListItemComponent } from "./list-item.component";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "src/app/common/service/workflow-persist/workflow-persist.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, Subject, throwError } from "rxjs";
import { ActionType, HubService } from "../../../../hub/service/hub.service";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { DatasetService, DEFAULT_DATASET_NAME } from "../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_WORKSPACE,
} from "../../../../app-routing.constant";

describe("ListItemComponent", () => {
  let component: ListItemComponent;
  let fixture: ComponentFixture<ListItemComponent>;
  let workflowPersistService: Mocked<WorkflowPersistService>;
  let datasetService: Mocked<DatasetService>;
  let hubService: HubService;
  let modalService: NzModalService;

  beforeEach(async () => {
    const workflowPersistServiceSpy = { updateWorkflowName: vi.fn(), updateWorkflowDescription: vi.fn() };
    const datasetServiceSpy = { updateDatasetName: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [ListItemComponent, HttpClientTestingModule, BrowserAnimationsModule, RouterTestingModule],
      providers: [
        { provide: WorkflowPersistService, useValue: workflowPersistServiceSpy },
        { provide: DatasetService, useValue: datasetServiceSpy },
        { provide: UserService, useClass: StubUserService },
        NzModalService,
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ListItemComponent);
    component = fixture.componentInstance;
    workflowPersistService = TestBed.inject(WorkflowPersistService) as unknown as Mocked<WorkflowPersistService>;
    datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
    hubService = TestBed.inject(HubService);
    modalService = TestBed.inject(NzModalService);
    // initializeEntry() needs a fully-formed workflow entry to avoid throwing
    // when the template renders for the first time. Each test below overwrites
    // component.entry directly, which exercises confirm methods without going
    // back through change detection.
    component.entry = {
      id: 0,
      name: "default",
      description: "",
      type: "workflow",
      workflow: { isOwner: true },
      accessibleUserIds: [],
      likeCount: 0,
      viewCount: 0,
      isLiked: false,
      size: 0,
    } as unknown as DashboardEntry;
    fixture.detectChanges();
  });

  it("should update workflow name successfully", () => {
    const newName = "New Workflow Name";
    component.entry = { id: 1, name: "Old Name", type: "workflow" } as unknown as DashboardEntry;
    workflowPersistService.updateWorkflowName.mockReturnValue(of({} as Response));

    component.confirmUpdateCustomName(newName);

    expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, newName);
    expect(component.entry.name).toBe(newName);
    expect(component.editingName).toBe(false);
  });

  it("should handle error when updating workflow name", () => {
    const newName = "New Workflow Name";
    component.entry = { id: 1, name: "Old Name", type: "workflow" } as unknown as DashboardEntry;
    component.originalName = "Old Name";
    workflowPersistService.updateWorkflowName.mockReturnValue(throwError(() => new Error("Error")));

    component.confirmUpdateCustomName(newName);

    expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, newName);
    expect(component.entry.name).toBe("Old Name");
    expect(component.editingName).toBe(false);
  });

  it("should update workflow description successfully", () => {
    const newDescription = "New Description";
    component.entry = { id: 1, description: "Old Description", type: "workflow" } as unknown as DashboardEntry;
    workflowPersistService.updateWorkflowDescription.mockReturnValue(of({} as Response));

    component.confirmUpdateCustomDescription(newDescription);

    expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, newDescription);
    expect(component.entry.description).toBe(newDescription);
    expect(component.editingDescription).toBe(false);
  });

  it("should handle error when updating workflow description", () => {
    const newDescription = "New Description";
    component.entry = { id: 1, description: "Old Description", type: "workflow" } as unknown as DashboardEntry;
    component.originalDescription = "Old Description";
    workflowPersistService.updateWorkflowDescription.mockReturnValue(throwError(() => new Error("Error")));

    component.confirmUpdateCustomDescription(newDescription);

    expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, newDescription);
    expect(component.entry.description).toBe("Old Description");
    expect(component.editingDescription).toBe(false);
  });

  describe("initializeEntry routes", () => {
    const baseStats = { likeCount: 0, viewCount: 0, isLiked: false };

    it("routes owned workflows to the user workspace", () => {
      component.currentUid = 1;
      component.entry = {
        id: 100,
        type: "workflow",
        workflow: { isOwner: true },
        accessibleUserIds: [1],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([USER_WORKSPACE, "100"]);
    });

    it("routes non-owned workflows to the hub workflow detail page", () => {
      component.currentUid = 1;
      component.entry = {
        id: 101,
        type: "workflow",
        workflow: { isOwner: false },
        accessibleUserIds: [2],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([HUB_WORKFLOW_RESULT_DETAIL, "101"]);
    });

    it("routes owned datasets to the user dataset page", () => {
      component.currentUid = 1;
      component.entry = {
        id: 300,
        type: "dataset",
        dataset: { isOwner: true },
        accessibleUserIds: [1],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([USER_DATASET, "300"]);
    });

    it("routes non-owned datasets to the hub dataset detail page", () => {
      component.currentUid = 1;
      component.entry = {
        id: 301,
        type: "dataset",
        dataset: { isOwner: false },
        accessibleUserIds: [2],
        ...baseStats,
      } as unknown as DashboardEntry;
      component.initializeEntry();
      expect(component.entryLink).toEqual([HUB_DATASET_RESULT_DETAIL, "301"]);
    });
  });

  it("should reject an invalid dataset name, revert to original, and exit editing", () => {
    component.entry = {
      id: 5,
      name: "has space",
      type: "dataset",
    } as unknown as DashboardEntry;
    component.originalName = "original-name";
    component.editingName = true;
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");

    component.confirmUpdateCustomName("has space");

    expect(datasetService.updateDatasetName).not.toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalled();
    expect(component.entry.name).toBe("original-name");
    expect(component.editingName).toBe(false);
  });

  it("should call the dataset service for a valid dataset rename", () => {
    component.entry = {
      id: 5,
      name: "new-valid-name",
      type: "dataset",
    } as unknown as DashboardEntry;
    component.originalName = "old-name";
    datasetService.updateDatasetName.mockReturnValue(of({} as any));

    component.confirmUpdateCustomName("new-valid-name");

    expect(datasetService.updateDatasetName).toHaveBeenCalledWith(5, "new-valid-name");
  });

  it("should surface the error message and revert the name when a dataset rename fails", () => {
    component.entry = {
      id: 5,
      name: "new-valid-name",
      type: "dataset",
    } as unknown as DashboardEntry;
    component.originalName = "old-name";
    component.editingName = true;
    datasetService.updateDatasetName.mockReturnValue(throwError(() => new Error("boom")));
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");

    component.confirmUpdateCustomName("new-valid-name");

    expect(errorSpy).toHaveBeenCalledWith("boom");
    expect(component.entry.name).toBe("old-name");
    expect(component.editingName).toBe(false);
  });

  describe("edit + interaction handlers", () => {
    it("onEditName captures the original name and enters edit mode", () => {
      component.entry = { id: 1, name: "My Name", type: "workflow" } as unknown as DashboardEntry;

      component.onEditName();

      expect(component.editingName).toBe(true);
      expect(component.originalName).toBe("My Name");
    });

    it("onEditDescription opens the edit modal and applies the change, then closes it", () => {
      component.editable = true;
      component.entry = { id: 1, description: "old", type: "workflow" } as unknown as DashboardEntry;
      const descriptionChange = new Subject<string>();
      const modalRef = { componentInstance: { descriptionChange }, destroy: vi.fn() };
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue(modalRef as any);
      const confirmSpy = vi.spyOn(component, "confirmUpdateCustomDescription").mockImplementation(() => {});

      component.onEditDescription();
      expect(createSpy).toHaveBeenCalled();

      descriptionChange.next("new description");
      expect(confirmSpy).toHaveBeenCalledWith("new description");
      expect(modalRef.destroy).toHaveBeenCalled();
    });

    it("onEditDescription is a no-op when the entry is not editable", () => {
      component.editable = false;
      const createSpy = vi.spyOn(modalService, "create");

      component.onEditDescription();

      expect(createSpy).not.toHaveBeenCalled();
    });

    it("onCheckboxChange toggles the entry's checked flag and emits the change", () => {
      const entry = { checked: false } as unknown as DashboardEntry;
      const emitSpy = vi.fn();
      component.checkboxChanged.subscribe(emitSpy);

      component.onCheckboxChange(entry);

      expect(entry.checked).toBe(true);
      expect(emitSpy).toHaveBeenCalled();
    });

    it("toggleLike likes the entry and refreshes the like count on success", () => {
      component.currentUid = 1;
      component.entry = { id: 5, type: "workflow" } as unknown as DashboardEntry;
      component.isLiked = false;
      vi.spyOn(hubService, "postLike").mockReturnValue(of(true));
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { like: 3 } }] as any));

      component.toggleLike();

      expect(hubService.postLike).toHaveBeenCalledWith(5, "workflow");
      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(3);
    });

    it("toggleLike unlikes the entry and refreshes the like count on success", () => {
      component.currentUid = 1;
      component.entry = { id: 5, type: "workflow" } as unknown as DashboardEntry;
      component.isLiked = true;
      vi.spyOn(hubService, "postUnlike").mockReturnValue(of(true));
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { like: 1 } }] as any));

      component.toggleLike();

      expect(hubService.postUnlike).toHaveBeenCalledWith(5, "workflow");
      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(1);
    });

    it("toggleLike does nothing when there is no current user", () => {
      component.currentUid = undefined;
      component.entry = { id: 5, type: "workflow" } as unknown as DashboardEntry;
      const postLikeSpy = vi.spyOn(hubService, "postLike");

      component.toggleLike();

      expect(postLikeSpy).not.toHaveBeenCalled();
    });

    it("openDetailModal opens the detail modal and increments the view count", () => {
      component.entry = { id: 9, type: "workflow" } as unknown as DashboardEntry;
      const modalRef = { componentInstance: {}, destroy: vi.fn() };
      vi.spyOn(modalService, "create").mockReturnValue(modalRef as any);
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { view: 4 } }] as any));

      component.openDetailModal(9);

      expect(modalService.create).toHaveBeenCalled();
      expect(hubService.getCounts).toHaveBeenCalledWith(["workflow"], [9], [ActionType.View]);
      expect(component.viewCount).toBe(5); // 4 + 1
    });
  });

  /**
   * The suite above feeds one workflow entry; the component dispatches on `entry.type` in
   * several places, so these hand it the other kinds. Each test builds its own entry — the
   * rename and description handlers mutate it in place.
   */
  describe("per-entry-type dispatch", () => {
    function entryOf(overrides: Partial<Record<string, unknown>>): DashboardEntry {
      return {
        id: 7,
        name: "item",
        description: "",
        accessibleUserIds: [],
        likeCount: 0,
        viewCount: 0,
        isLiked: false,
        size: 0,
        ...overrides,
      } as unknown as DashboardEntry;
    }

    /** Re-runs the input pipeline the way an @Input change would. */
    function feed(entry: DashboardEntry): void {
      component.entry = entry;
      component.ngOnChanges({ entry: {} as any });
    }

    afterEach(() => {
      vi.restoreAllMocks();
    });

    it("refuses to be read before an entry is supplied", () => {
      const bare = TestBed.createComponent(ListItemComponent).componentInstance;

      expect(() => bare.entry).toThrowError("entry property must be provided.");
    });

    it("picks an icon per entry kind", () => {
      feed(entryOf({ type: "workflow", workflow: { isOwner: true } }));
      expect(component.iconType).toBe("project");

      feed(entryOf({ type: "dataset", dataset: { isOwner: true } }));
      expect(component.iconType).toBe("database");

      feed(entryOf({ type: "file" }));
      expect(component.iconType).toBe("folder-open");
    });

    it("refuses an entry kind it does not know", () => {
      expect(() => feed(entryOf({ type: "quantum" }))).toThrowError("Unexpected type in DashboardEntry.");
    });

    it("leaves a dataset without a numeric id unrouted", () => {
      // The dataset arm reads isOwner and the link only for a persisted entry.
      feed(entryOf({ type: "dataset", id: undefined, dataset: { isOwner: false } }));

      expect(component.entryLink).toEqual([]);
      expect(component.iconType).not.toBe("database");
    });

    it("reduces a description to a plain preview, and blanks an empty one", () => {
      feed(entryOf({ type: "file", description: undefined }));
      expect(component.renderedDescription).toBe("");

      feed(entryOf({ type: "file", description: "   " }));
      expect(component.renderedDescription).toBe("");

      feed(entryOf({ type: "file", description: "# Title with [a link](http://x)  and\n*emphasis*" }));
      expect(component.renderedDescription).toBe("Title with a link and emphasis");
    });

    describe("share access", () => {
      /** A modal handle whose componentInstance re-emits on demand. */
      function modalReturning(refresh: Subject<void> | undefined) {
        return {
          componentInstance: refresh === undefined ? undefined : { refresh },
        } as any;
      }

      it("opens the workflow share dialog and re-emits its refresh", async () => {
        const refresh = new Subject<void>();
        const create = vi.spyOn(modalService, "create").mockReturnValue(modalReturning(refresh));
        // The shared stub carries only the update methods; the share dialog also asks for
        // the owner list.
        (workflowPersistService as any).retrieveOwners = vi.fn().mockReturnValue(of([]));
        let refreshed = false;
        component.refresh.subscribe(() => (refreshed = true));
        feed(entryOf({ type: "workflow", workflow: { isOwner: true, accessLevel: "WRITE" } }));

        await component.onClickOpenShareAccess();

        expect(create).toHaveBeenCalledWith(
          expect.objectContaining({
            nzTitle: "Share this workflow with others",
            nzData: expect.objectContaining({ type: "workflow", writeAccess: true, id: 7 }),
          })
        );

        refresh.next();
        expect(refreshed).toBe(true);
      });

      it("opens the dataset share dialog with the dataset's owners", async () => {
        const create = vi.spyOn(modalService, "create").mockReturnValue(modalReturning(new Subject<void>()));
        (datasetService as any).retrieveOwners = vi.fn().mockReturnValue(of([]));
        feed(entryOf({ type: "dataset", dataset: { isOwner: true }, accessLevel: "READ" }));

        await component.onClickOpenShareAccess();

        expect(create).toHaveBeenCalledWith(
          expect.objectContaining({
            nzTitle: "Share this dataset with others",
            nzData: expect.objectContaining({ type: "dataset", writeAccess: false }),
          })
        );
      });

      it("opens nothing for an entry kind that cannot be shared", async () => {
        const create = vi.spyOn(modalService, "create");
        feed(entryOf({ type: "file" }));

        await component.onClickOpenShareAccess();

        expect(create).not.toHaveBeenCalled();
      });
    });

    describe("editing", () => {
      it("focuses the name box once it exists, and copes when it does not", () => {
        vi.useRealTimers();
        feed(entryOf({ type: "workflow", workflow: { isOwner: true }, name: "before" }));

        // No view child yet: entering edit mode must not throw.
        (component as any).nameInput = undefined;
        expect(() => component.onEditName()).not.toThrow();
        expect(component.originalName).toBe("before");
        expect(component.editingName).toBe(true);

        const input = { value: "before", focus: vi.fn(), setSelectionRange: vi.fn() };
        (component as any).nameInput = { nativeElement: input };
        component.onEditName();

        // The focus is scheduled on a task the component owns; run it directly rather than
        // waiting on a timer.
        const scheduled = vi.spyOn(globalThis, "setTimeout");
        component.onEditName();
        const callback = scheduled.mock.calls.at(-1)?.[0] as () => void;
        callback();

        expect(input.focus).toHaveBeenCalled();
        expect(input.setSelectionRange).toHaveBeenCalledWith("before".length, "before".length);
      });

      it("reports a missing id instead of updating", () => {
        const notify = vi.spyOn((component as any).notificationService, "error").mockImplementation(() => {});
        feed(entryOf({ type: "workflow", id: 0, workflow: { isOwner: true } }));

        component.confirmUpdateCustomName("renamed");

        expect(notify).toHaveBeenCalledWith("Id is missing");
        expect(workflowPersistService.updateWorkflowName).not.toHaveBeenCalled();
      });

      it("falls back to the default name per entry kind when the new name is empty", () => {
        workflowPersistService.updateWorkflowName.mockReturnValue(of({} as Response));
        feed(entryOf({ type: "workflow", workflow: { isOwner: true } }));

        component.confirmUpdateCustomName("");

        expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(7, DEFAULT_WORKFLOW_NAME);
      });

      it("treats an absent description as an empty one", () => {
        workflowPersistService.updateWorkflowDescription.mockReturnValue(of({} as Response));
        feed(entryOf({ type: "workflow", workflow: { isOwner: true } }));

        component.confirmUpdateCustomDescription(undefined);

        expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(7, "");
      });

      it("sends a dataset description to the dataset service", () => {
        (datasetService as any).updateDatasetDescription = vi.fn().mockReturnValue(of(undefined));
        feed(entryOf({ type: "dataset", dataset: { isOwner: true } }));

        component.confirmUpdateCustomDescription("about this set");

        expect((datasetService as any).updateDatasetDescription).toHaveBeenCalledWith(7, "about this set");
      });
    });

    it("ignores a change that is not the entry", () => {
      feed(entryOf({ type: "file", description: "kept" }));
      const before = component.renderedDescription;

      component.ngOnChanges({ editable: {} as any });

      expect(component.renderedDescription).toBe(before);
    });

    it("falls back to the dataset default name when a dataset rename is blank", () => {
      (datasetService as any).updateDatasetName = vi.fn().mockReturnValue(of({} as Response));
      feed(entryOf({ type: "dataset", dataset: { isOwner: true } }));

      component.confirmUpdateCustomName("");

      expect((datasetService as any).updateDatasetName).toHaveBeenCalledWith(7, DEFAULT_DATASET_NAME);
    });

    describe("download", () => {
      it("downloads a workflow by id and name", () => {
        const download = vi
          .spyOn((component as any).downloadService, "downloadWorkflow")
          .mockReturnValue(of(undefined));
        feed(entryOf({ type: "workflow", workflow: { isOwner: true, workflow: { name: "flow" } } }));

        component.onClickDownload();

        expect(download).toHaveBeenCalledWith(7, "flow");
      });

      it("downloads a dataset by id and name", () => {
        const download = vi.spyOn((component as any).downloadService, "downloadDataset").mockReturnValue(of(undefined));
        feed(entryOf({ type: "dataset", dataset: { isOwner: true }, name: "set" }));

        component.onClickDownload();

        expect(download).toHaveBeenCalledWith(7, "set");
      });

      it("downloads nothing for an entry that was never persisted", () => {
        const workflow = vi.spyOn((component as any).downloadService, "downloadWorkflow");
        feed(entryOf({ type: "file", id: 0 }));

        component.onClickDownload();

        expect(workflow).not.toHaveBeenCalled();
      });
    });
  });

  /**
   * The suites above call the handlers directly; these fire them from the rendered
   * markup, so a control that loses its binding fails here.
   */
  describe("rendered controls", () => {
    const q = (selector: string) => fixture.debugElement.query(By.css(selector));
    const button = (title: string) => q(`button[title="${title}"]`);

    /**
     * Renders the card for one entry. The entry is rebuilt per call because the
     * rename path mutates `entry.name` in place.
     */
    function render(overrides: Record<string, unknown> = {}, isPrivateSearch = true): void {
      component.entry = {
        id: 7,
        name: "item",
        description: "",
        type: "workflow",
        workflow: { isOwner: true },
        dataset: { isOwner: true },
        accessibleUserIds: [],
        likeCount: 0,
        viewCount: 0,
        isLiked: false,
        size: 0,
        ...overrides,
      } as unknown as DashboardEntry;
      component.isPrivateSearch = isPrivateSearch;
      component.ngOnChanges({ entry: {} as any });
      fixture.detectChanges();
    }

    afterEach(() => {
      fixture.destroy();
      vi.restoreAllMocks();
    });

    it("renames through the inline input, confirming on blur and on enter", () => {
      const confirm = vi.spyOn(component, "confirmUpdateCustomName").mockImplementation(() => {});
      render();

      expect(q("input.resource-name-edit-input")).toBeNull();
      button("Rename").triggerEventHandler("click", new MouseEvent("click"));
      fixture.detectChanges();

      const input = q("input.resource-name-edit-input");
      expect(input).not.toBeNull();
      input.nativeElement.value = "renamed";
      input.nativeElement.dispatchEvent(new Event("input"));
      fixture.detectChanges();
      // The two-way binding writes straight back onto the entry.
      expect(component.entry.name).toBe("renamed");

      input.triggerEventHandler("blur", null);
      expect(confirm).toHaveBeenLastCalledWith("renamed");

      input.triggerEventHandler("keydown.enter", null);
      expect(confirm).toHaveBeenCalledTimes(2);

      // Clicking inside the input must not bubble to the row's routerLink.
      const click = { stopPropagation: vi.fn() };
      input.triggerEventHandler("click", click);
      expect(click.stopPropagation).toHaveBeenCalledTimes(1);
    });

    it("opens the description editor from its button and from the description line", () => {
      const edit = vi.spyOn(component, "onEditDescription").mockImplementation(() => {});
      render({ description: "hello" });

      button("Edit Description").triggerEventHandler("click", new MouseEvent("click"));
      q(".resource-description").triggerEventHandler("click", new MouseEvent("click"));

      expect(edit).toHaveBeenCalledTimes(2);
    });

    it("tracks hover over the row", () => {
      render();
      const row = q("div[nz-row]");

      row.triggerEventHandler("mouseenter", null);
      expect(component.hovering).toBe(true);

      row.triggerEventHandler("mouseleave", null);
      expect(component.hovering).toBe(false);
    });

    it("toggles the row checkbox of a private workflow entry", () => {
      let changes = 0;
      component.checkboxChanged.subscribe(() => changes++);
      render();

      const checkbox = q("input.large-checkbox");
      expect(checkbox).not.toBeNull();
      checkbox.triggerEventHandler("change", null);
      fixture.detectChanges();

      expect(component.entry.checked).toBe(true);
      expect(changes).toBe(1);
      expect(q("input.large-checkbox").nativeElement.checked).toBe(true);

      // Ticking the box must not bubble to the row's routerLink.
      const click = { stopPropagation: vi.fn() };
      q("input.large-checkbox").triggerEventHandler("click", click);
      expect(click.stopPropagation).toHaveBeenCalledTimes(1);
    });

    it("wires the detail, share, copy and delete controls", () => {
      const detail = vi.spyOn(component, "openDetailModal").mockImplementation(() => {});
      const share = vi.spyOn(component, "onClickOpenShareAccess").mockResolvedValue(undefined);
      let duplicated = 0;
      let deleted = 0;
      component.duplicated.subscribe(() => duplicated++);
      component.deleted.subscribe(() => deleted++);
      render();

      button("Detail").triggerEventHandler("click", null);
      button("Share").triggerEventHandler("click", null);
      button("Copy").triggerEventHandler("click", null);
      // The popconfirm popup itself needs a CDK overlay, which jsdom never attaches;
      // the confirmation output is bound on the button, so it is fired directly.
      button("Delete").triggerEventHandler("nzOnConfirm", null);

      expect(detail).toHaveBeenCalledWith(7);
      expect(share).toHaveBeenCalledTimes(1);
      expect(duplicated).toBe(1);
      expect(deleted).toBe(1);
    });

    it("offers the download button to workflows and datasets only", () => {
      const download = vi.spyOn(component, "onClickDownload").mockImplementation(() => {});

      render();
      button("Download").triggerEventHandler("click", null);
      expect(download).toHaveBeenCalledTimes(1);

      render({ type: "dataset" });
      expect(button("Download")).not.toBeNull();

      render({ type: "file" });
      expect(button("Download")).toBeNull();
    });

    it("likes from the public card and disables the button without a signed-in user", () => {
      const like = vi.spyOn(component, "toggleLike").mockImplementation(() => {});
      render({ likeCount: 12 }, false);

      // No current user: the control renders but is disabled.
      expect(q("button.like-button").nativeElement.disabled).toBe(true);

      component.currentUid = 1;
      fixture.detectChanges();
      const likeButton = q("button.like-button");
      expect(likeButton.nativeElement.disabled).toBe(false);
      likeButton.triggerEventHandler("click", new MouseEvent("click"));

      expect(like).toHaveBeenCalledTimes(1);
      expect(likeButton.nativeElement.textContent).toContain("12");
    });
  });
});
