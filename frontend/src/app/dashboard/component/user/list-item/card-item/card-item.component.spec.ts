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

import { ComponentFixture, TestBed, fakeAsync, tick } from "@angular/core/testing";
import { CardItemComponent } from "./card-item.component";
import { ActionType, EntityType, HubService } from "src/app/hub/service/hub.service";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "src/app/common/service/workflow-persist/workflow-persist.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, throwError, Subject } from "rxjs";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { By } from "@angular/platform-browser";
import { RouterTestingModule } from "@angular/router/testing";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_WORKSPACE,
} from "../../../../../app-routing.constant";
import { WorkflowCoverService } from "src/app/dashboard/service/user/workflow-cover/workflow-cover.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DatasetService, DEFAULT_DATASET_NAME } from "../../../../service/user/dataset/dataset.service";
import { DownloadService } from "src/app/dashboard/service/user/download/download.service";

function makeWorkflowEntry(overrides: Partial<DashboardEntry> = {}): DashboardEntry {
  return {
    id: 1,
    name: "wf",
    description: "",
    type: "workflow",
    workflow: { isOwner: true },
    accessibleUserIds: [],
    likeCount: 0,
    viewCount: 0,
    isLiked: false,
    size: 0,
    ...overrides,
  } as unknown as DashboardEntry;
}

function makeDatasetEntry(overrides: Partial<DashboardEntry> = {}): DashboardEntry {
  return {
    id: 5,
    name: "ds",
    description: "",
    type: "dataset",
    dataset: { isOwner: true },
    accessibleUserIds: [],
    likeCount: 0,
    viewCount: 0,
    isLiked: false,
    size: 0,
    ...overrides,
  } as unknown as DashboardEntry;
}

describe("CardItemComponent", () => {
  let component: CardItemComponent;
  let fixture: ComponentFixture<CardItemComponent>;
  let workflowPersistService: Mocked<WorkflowPersistService>;
  let workflowCoverService: Mocked<WorkflowCoverService>;
  let datasetService: Mocked<DatasetService>;

  beforeEach(async () => {
    const workflowPersistServiceSpy = { updateWorkflowName: vi.fn(), updateWorkflowDescription: vi.fn() };
    const workflowCoverServiceSpy = {
      getCover: vi.fn().mockReturnValue(of(undefined)),
      setCoverFromFile: vi.fn(),
      clearCover: vi.fn().mockReturnValue(of(undefined)),
    };
    const datasetServiceSpy = { getDatasetCoverUrl: vi.fn(), updateDatasetName: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [CardItemComponent, HttpClientTestingModule, BrowserAnimationsModule, RouterTestingModule],
      providers: [
        { provide: WorkflowPersistService, useValue: workflowPersistServiceSpy },
        { provide: WorkflowCoverService, useValue: workflowCoverServiceSpy },
        { provide: DatasetService, useValue: datasetServiceSpy },
        { provide: UserService, useClass: StubUserService },
        NzModalService,
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(CardItemComponent);
    component = fixture.componentInstance;
    workflowPersistService = TestBed.inject(WorkflowPersistService) as unknown as Mocked<WorkflowPersistService>;
    workflowCoverService = TestBed.inject(WorkflowCoverService) as unknown as Mocked<WorkflowCoverService>;
    datasetService = TestBed.inject(DatasetService) as unknown as Mocked<DatasetService>;
    component.entry = makeWorkflowEntry();
    fixture.detectChanges();
  });

  it("should update workflow name successfully", () => {
    const newName = "New Workflow Name";
    component.entry = makeWorkflowEntry({ id: 1, name: "Old Name" });
    workflowPersistService.updateWorkflowName.mockReturnValue(of({} as Response));

    component.confirmUpdateCustomName(newName);

    expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, newName);
    expect(component.entry.name).toBe(newName);
    expect(component.editingName).toBe(false);
  });

  it("should revert the name and exit edit mode when the update fails", () => {
    component.entry = makeWorkflowEntry({ id: 1, name: "Old Name" });
    component.originalName = "Old Name";
    workflowPersistService.updateWorkflowName.mockReturnValue(throwError(() => new Error("Error")));

    component.confirmUpdateCustomName("New Workflow Name");

    expect(component.entry.name).toBe("Old Name");
    expect(component.editingName).toBe(false);
  });

  it("should reject an invalid dataset name, revert to original, and exit editing", () => {
    component.entry = makeDatasetEntry({ id: 5, name: "invalid name" });
    component.originalName = "original-name";
    component.editingName = true;
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");

    component.confirmUpdateCustomName("invalid name");

    expect(datasetService.updateDatasetName).not.toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalled();
    expect(component.entry.name).toBe("original-name");
    expect(component.editingName).toBe(false);
  });

  it("should call the dataset service for a valid dataset rename", () => {
    component.entry = makeDatasetEntry({ id: 5, name: "new-valid-name" });
    component.originalName = "old-name";
    datasetService.updateDatasetName.mockReturnValue(of({} as any));

    component.confirmUpdateCustomName("new-valid-name");

    expect(datasetService.updateDatasetName).toHaveBeenCalledWith(5, "new-valid-name");
  });

  it("should surface the error message and revert the name when a dataset rename fails", () => {
    component.entry = makeDatasetEntry({ id: 5, name: "new-valid-name" });
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

  it("should update workflow description successfully", () => {
    component.entry = makeWorkflowEntry({ id: 1, description: "Old Description" });
    workflowPersistService.updateWorkflowDescription.mockReturnValue(of({} as Response));

    component.confirmUpdateCustomDescription("New Description");

    expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, "New Description");
    expect(component.entry.description).toBe("New Description");
    expect(component.editingDescription).toBe(false);
  });

  it("should revert the description and exit edit mode when the update fails", () => {
    component.entry = makeWorkflowEntry({ id: 1, description: "Old Description" });
    component.originalDescription = "Old Description";
    workflowPersistService.updateWorkflowDescription.mockReturnValue(throwError(() => new Error("Error")));

    component.confirmUpdateCustomDescription("New Description");

    expect(component.entry.description).toBe("Old Description");
    expect(component.editingDescription).toBe(false);
  });

  it("should route owners to the workspace and non-owners to the hub detail view", () => {
    component.currentUid = 42;
    component.entry = makeWorkflowEntry({ id: 7, accessibleUserIds: [42] });
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });
    expect(component.entryLink).toEqual([USER_WORKSPACE, "7"]);

    component.entry = makeWorkflowEntry({ id: 7, accessibleUserIds: [99] });
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });
    expect(component.entryLink).toEqual([HUB_WORKFLOW_RESULT_DETAIL, "7"]);
  });

  it("should format counts as kilo for values >= 1000", () => {
    expect(component.formatCount(999)).toBe("999");
    expect(component.formatCount(1500)).toBe("1.5k");
    expect(component.formatCount(0)).toBe("0");
  });

  it("should return 'Unknown' for undefined timestamps", () => {
    expect(component.formatTime(undefined)).toBe("Unknown");
  });

  it("should emit deleted when the parent triggers the delete confirmation", () => {
    const spy = vi.fn();
    component.deleted.subscribe(spy);
    component.deleted.emit();
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("should toggle the entry checked flag and emit checkboxChanged", () => {
    const entry = makeWorkflowEntry({ checked: false } as any);
    component.entry = entry;
    const spy = vi.fn();
    component.checkboxChanged.subscribe(spy);

    component.onCheckboxChange(entry);

    expect((entry as any).checked).toBe(true);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("should show cover controls only for an owned workflow in private search", () => {
    component.isPrivateSearch = true;
    component.entry = makeWorkflowEntry({ workflow: { isOwner: true } } as any);
    expect(component.canEditCover).toBe(true);

    component.entry = makeWorkflowEntry({ workflow: { isOwner: false } } as any);
    expect(component.canEditCover).toBe(false);

    component.entry = makeWorkflowEntry({ workflow: { isOwner: true } } as any);
    component.isPrivateSearch = false;
    expect(component.canEditCover).toBe(false);
  });

  it("should use the stored cover image on initialization", () => {
    const cover = "data:image/jpeg;base64,abc";
    const entry = makeWorkflowEntry({ id: 7 });
    entry.coverImageUrl = cover;

    component.entry = entry;
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });

    expect(workflowCoverService.getCover).not.toHaveBeenCalled();
    expect(component.hasCustomImage).toBe(true);
    expect(component.coverImageSrc).toBe(cover);
  });

  it("should fall back to the default preview image when no cover is set", () => {
    const entry = makeWorkflowEntry({ id: 7 });
    entry.coverImageUrl = undefined;

    component.entry = entry;
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });

    expect(workflowCoverService.getCover).not.toHaveBeenCalled();
    expect(component.hasCustomImage).toBe(false);
    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("should upload a selected image and use the returned data URL as the cover", async () => {
    const dataUrl = "data:image/jpeg;base64,xyz";
    workflowCoverService.setCoverFromFile.mockResolvedValue(dataUrl);
    component.entry = makeWorkflowEntry({ id: 7 });
    const file = new File(["x"], "pic.png", { type: "image/png" });

    await component.onImageSelected({ target: { files: [file], value: "pic.png" } } as any);

    expect(workflowCoverService.setCoverFromFile).toHaveBeenCalledWith(7, file);
    expect(component.coverImageSrc).toBe(dataUrl);
    expect(component.hasCustomImage).toBe(true);
  });

  it("should reject a non-image file and not upload it", async () => {
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");
    const file = new File(["x"], "notes.txt", { type: "text/plain" });

    await component.onImageSelected({ target: { files: [file], value: "notes.txt" } } as any);

    expect(workflowCoverService.setCoverFromFile).not.toHaveBeenCalled();
    expect(errorSpy).toHaveBeenCalled();
  });

  it("should notify on upload failure and keep the previous preview image", async () => {
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");
    workflowCoverService.setCoverFromFile.mockRejectedValue(new Error("boom"));
    component.entry = makeWorkflowEntry({ id: 7 });
    const file = new File(["x"], "pic.png", { type: "image/png" });

    await component.onImageSelected({ target: { files: [file], value: "pic.png" } } as any);

    expect(errorSpy).toHaveBeenCalled();
    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("should clear the cover and revert to the default image on reset", () => {
    workflowCoverService.clearCover.mockReturnValue(of(undefined));
    component.entry = makeWorkflowEntry({ id: 7 });
    (component as any).customImage = "data:image/jpeg;base64,abc";

    component.resetImage();

    expect(workflowCoverService.clearCover).toHaveBeenCalledWith(7);
    expect(component.hasCustomImage).toBe(false);
    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("should notify and keep the cover when reset fails", () => {
    const notificationService = TestBed.inject(NotificationService);
    const errorSpy = vi.spyOn(notificationService, "error");
    workflowCoverService.clearCover.mockReturnValue(throwError(() => new Error("boom")));
    component.entry = makeWorkflowEntry({ id: 7 });
    (component as any).customImage = "data:image/jpeg;base64,abc";

    component.resetImage();

    expect(errorSpy).toHaveBeenCalled();
    expect(component.hasCustomImage).toBe(true);
  });

  it("openImagePicker clicks the hidden file input", () => {
    const clickSpy = vi.fn();
    (component as any).backgroundInput = { nativeElement: { click: clickSpy } };
    component.openImagePicker();
    expect(clickSpy).toHaveBeenCalledTimes(1);
  });

  it("openImagePicker is a no-op when the file input is absent", () => {
    (component as any).backgroundInput = undefined;
    expect(() => component.openImagePicker()).not.toThrow();
  });

  it("should do nothing when no file is selected", async () => {
    await component.onImageSelected({ target: { files: [], value: "" } } as any);
    expect(workflowCoverService.setCoverFromFile).not.toHaveBeenCalled();
  });

  it("should not upload when the entry id is not numeric", async () => {
    component.entry = makeWorkflowEntry({ id: "not-a-number" as any });
    const file = new File(["x"], "pic.png", { type: "image/png" });
    await component.onImageSelected({ target: { files: [file], value: "pic.png" } } as any);
    expect(workflowCoverService.setCoverFromFile).not.toHaveBeenCalled();
  });

  it("resetImage does nothing when the entry id is not numeric", () => {
    component.entry = makeWorkflowEntry({ id: "not-a-number" as any });
    component.resetImage();
    expect(workflowCoverService.clearCover).not.toHaveBeenCalled();
  });

  it("should load the dataset cover into the preview when the entry has a cover", () => {
    datasetService.getDatasetCoverUrl.mockReturnValue(of({ url: "https://cover.example/img.png" }));
    component.entry = makeDatasetEntry({ id: 5, coverImageUrl: "cover/path.png" });
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });

    expect(datasetService.getDatasetCoverUrl).toHaveBeenCalledWith(5);
    expect(component.coverImageSrc).toBe("https://cover.example/img.png");
  });

  it("should fall back to the default preview when the cover fetch fails", () => {
    datasetService.getDatasetCoverUrl.mockReturnValue(throwError(() => new Error("cover fetch failed")));
    component.entry = makeDatasetEntry({ coverImageUrl: "cover/path.png" });
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });

    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("should reset the preview to the default image on cover load error", () => {
    component.coverImageSrc = "https://cover.example/img.png";
    component.onCoverError();
    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("should keep the default preview for non-dataset entries", () => {
    component.entry = makeWorkflowEntry();
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });
    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("should not fetch a cover when the dataset has no cover image", () => {
    component.entry = makeDatasetEntry({ coverImageUrl: undefined });
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });

    expect(datasetService.getDatasetCoverUrl).not.toHaveBeenCalled();
    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("should use the default preview when the cover url resolves to null", () => {
    datasetService.getDatasetCoverUrl.mockReturnValue(of({ url: null }));
    component.entry = makeDatasetEntry({ coverImageUrl: "cover/path.png" });
    component.ngOnChanges({ entry: { currentValue: component.entry } as any });

    expect(component.coverImageSrc).toBe(CardItemComponent.DEFAULT_PREVIEW_IMAGE);
  });

  it("initializeEntry configures workflow metadata: disableDelete, workspace link, icon, counts", () => {
    component.currentUid = 42;
    component.entry = makeWorkflowEntry({
      id: 7,
      workflow: { isOwner: false },
      accessibleUserIds: [42],
      size: 123,
      likeCount: 9,
      viewCount: 4,
      isLiked: true,
    } as any);

    component.initializeEntry();

    expect(component.disableDelete).toBe(true); // !entry.workflow.isOwner
    expect(component.entryLink).toEqual([USER_WORKSPACE, "7"]); // currentUid is an accessible owner
    expect(component.size).toBe(123);
    expect(component.iconType).toBe("project");
    expect(component.likeCount).toBe(9);
    expect(component.viewCount).toBe(4);
    expect(component.isLiked).toBe(true);
  });

  it("initializeEntry uses the folder-open icon for a file entry", () => {
    component.entry = {
      id: 8,
      name: "f",
      type: "file",
      likeCount: 0,
      viewCount: 0,
      isLiked: false,
    } as unknown as DashboardEntry;

    component.initializeEntry();

    expect(component.iconType).toBe("folder-open");
  });

  it("initializeEntry throws for an unexpected entry type", () => {
    component.entry = {
      id: 1,
      name: "x",
      type: "bogus",
      likeCount: 0,
      viewCount: 0,
      isLiked: false,
    } as unknown as DashboardEntry;

    expect(() => component.initializeEntry()).toThrow("Unexpected type in DashboardEntry.");
  });

  it("onEditName captures the original name, enters edit mode, and focuses the input at the caret end", fakeAsync(() => {
    component.entry = makeWorkflowEntry({ name: "Current Name" }); // length 12
    const focus = vi.fn();
    const setSelectionRange = vi.fn();
    component.nameInput = { nativeElement: { value: "Current Name", focus, setSelectionRange } } as any;

    component.onEditName();

    expect(component.originalName).toBe("Current Name");
    expect(component.editingName).toBe(true);

    tick(0); // flush the focus timer

    expect(focus).toHaveBeenCalledTimes(1);
    expect(setSelectionRange).toHaveBeenCalledWith(12, 12);
  }));

  it("onEditName enters edit mode without throwing when the input is not rendered", fakeAsync(() => {
    component.entry = makeWorkflowEntry({ name: "n" });
    component.nameInput = undefined as any;

    component.onEditName();

    expect(component.editingName).toBe(true);
    expect(component.originalName).toBe("n");
    expect(() => tick(0)).not.toThrow(); // timer guard skips the missing input
  }));

  it("onEditDescription captures the original description, enters edit mode, and focuses the textarea at the caret end", fakeAsync(() => {
    component.entry = makeWorkflowEntry({ description: "Some desc" }); // length 9
    const focus = vi.fn();
    const setSelectionRange = vi.fn();
    component.descriptionInput = { nativeElement: { value: "Some desc", focus, setSelectionRange } } as any;

    component.onEditDescription();

    expect(component.originalDescription).toBe("Some desc");
    expect(component.editingDescription).toBe(true);

    tick(0);

    expect(focus).toHaveBeenCalledTimes(1);
    expect(setSelectionRange).toHaveBeenCalledWith(9, 9);
  }));

  it("openDetailModal opens the workflow detail modal and bumps the view count", () => {
    const modalService = TestBed.inject(NzModalService);
    const hubService = TestBed.inject(HubService);
    const createSpy = vi.spyOn(modalService, "create").mockReturnValue({ componentInstance: {} } as any);
    const getCountsSpy = vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { view: 5 } }] as any));

    component.entry = makeWorkflowEntry({ id: 7 }); // type defaults to "workflow"
    component.openDetailModal(7);

    expect(createSpy).toHaveBeenCalledTimes(1);
    const cfg = createSpy.mock.calls[0][0];
    expect(cfg.nzData).toEqual({ wid: 7 });
    expect(cfg.nzFooter).toBeNull();
    expect(getCountsSpy).toHaveBeenCalledWith([EntityType.Workflow], [7], [ActionType.View]);
    expect(component.viewCount).toBe(6); // (5 view count) + 1
  });

  it("openDetailModal defaults nzData wid to 0 and skips the count fetch when wid is undefined", () => {
    const modalService = TestBed.inject(NzModalService);
    const hubService = TestBed.inject(HubService);
    const createSpy = vi.spyOn(modalService, "create").mockReturnValue({ componentInstance: {} } as any);
    const getCountsSpy = vi.spyOn(hubService, "getCounts");

    component.openDetailModal(undefined);

    expect(createSpy.mock.calls[0][0].nzData).toEqual({ wid: 0 });
    expect(getCountsSpy).not.toHaveBeenCalled();
  });

  it("openDetailModal skips the count fetch when the modal has no component instance", () => {
    const modalService = TestBed.inject(NzModalService);
    const hubService = TestBed.inject(HubService);
    vi.spyOn(modalService, "create").mockReturnValue({ componentInstance: null } as any);
    const getCountsSpy = vi.spyOn(hubService, "getCounts");

    component.openDetailModal(7);

    expect(getCountsSpy).not.toHaveBeenCalled();
  });

  it("toggleLike posts a like and refreshes the count when the entry is not yet liked", () => {
    const hubService = TestBed.inject(HubService);
    const postLikeSpy = vi.spyOn(hubService, "postLike").mockReturnValue(of(true));
    const getCountsSpy = vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { like: 10 } }] as any));

    component.currentUid = 42;
    component.entry = makeWorkflowEntry({ id: 7 });
    component.isLiked = false;

    component.toggleLike();

    expect(postLikeSpy).toHaveBeenCalledWith(7, EntityType.Workflow);
    expect(getCountsSpy).toHaveBeenCalledWith([EntityType.Workflow], [7], [ActionType.Like]);
    expect(component.isLiked).toBe(true);
    expect(component.likeCount).toBe(10);
  });

  it("toggleLike posts an unlike and refreshes the count when the entry is already liked", () => {
    const hubService = TestBed.inject(HubService);
    const postUnlikeSpy = vi.spyOn(hubService, "postUnlike").mockReturnValue(of(true));
    const getCountsSpy = vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: { like: 3 } }] as any));

    component.currentUid = 42;
    component.entry = makeWorkflowEntry({ id: 7 });
    component.isLiked = true;

    component.toggleLike();

    expect(postUnlikeSpy).toHaveBeenCalledWith(7, EntityType.Workflow);
    expect(getCountsSpy).toHaveBeenCalledWith([EntityType.Workflow], [7], [ActionType.Like]);
    expect(component.isLiked).toBe(false);
    expect(component.likeCount).toBe(3);
  });

  it("toggleLike leaves state unchanged and skips the count fetch when the like request reports failure", () => {
    const hubService = TestBed.inject(HubService);
    vi.spyOn(hubService, "postLike").mockReturnValue(of(false));
    const getCountsSpy = vi.spyOn(hubService, "getCounts");

    component.currentUid = 42;
    component.entry = makeWorkflowEntry({ id: 7 });
    component.isLiked = false;

    component.toggleLike();

    expect(component.isLiked).toBe(false);
    expect(getCountsSpy).not.toHaveBeenCalled();
  });

  it("toggleLike is a no-op when there is no current user", () => {
    const hubService = TestBed.inject(HubService);
    const postLikeSpy = vi.spyOn(hubService, "postLike");
    const postUnlikeSpy = vi.spyOn(hubService, "postUnlike");

    component.currentUid = undefined;
    component.entry = makeWorkflowEntry({ id: 7 });

    component.toggleLike();

    expect(postLikeSpy).not.toHaveBeenCalled();
    expect(postUnlikeSpy).not.toHaveBeenCalled();
  });

  it("toggleLike is a no-op when the entry has no id", () => {
    const hubService = TestBed.inject(HubService);
    const postLikeSpy = vi.spyOn(hubService, "postLike");

    component.currentUid = 42;
    component.entry = makeWorkflowEntry({ id: undefined });
    component.isLiked = false;

    component.toggleLike();

    expect(postLikeSpy).not.toHaveBeenCalled();
  });

  describe("extended coverage", () => {
    it("entry getter throws when no entry has been provided", () => {
      component.entry = undefined as any;
      expect(() => component.entry).toThrow("entry property must be provided.");
    });

    it("initializeEntry routes an owning dataset user to the user dataset view", () => {
      component.currentUid = 42;
      component.entry = makeDatasetEntry({
        id: 5,
        dataset: { isOwner: true },
        accessibleUserIds: [42],
        coverImageUrl: undefined, // skips the cover fetch
        size: 55,
      } as any);

      component.initializeEntry();

      expect(component.entryLink).toEqual([USER_DATASET, "5"]);
      expect(component.iconType).toBe("database");
      expect(component.disableDelete).toBe(false); // owner
      expect(component.size).toBe(55);
    });

    it("initializeEntry routes a non-owning dataset user to the hub dataset detail view", () => {
      component.currentUid = 42;
      component.entry = makeDatasetEntry({
        id: 5,
        dataset: { isOwner: false },
        accessibleUserIds: [99],
        coverImageUrl: undefined,
      } as any);

      component.initializeEntry();

      expect(component.entryLink).toEqual([HUB_DATASET_RESULT_DETAIL, "5"]);
      expect(component.disableDelete).toBe(true); // !isOwner
    });

    it("onClickDownload downloads a workflow via the download service", () => {
      const downloadService = TestBed.inject(DownloadService);
      const downloadWorkflowSpy = vi.spyOn(downloadService, "downloadWorkflow").mockReturnValue(of({} as any));
      component.entry = makeWorkflowEntry({ id: 7, name: "myflow" });

      component.onClickDownload();

      expect(downloadWorkflowSpy).toHaveBeenCalledWith(7, "myflow");
    });

    it("onClickDownload downloads a dataset via the download service", () => {
      const downloadService = TestBed.inject(DownloadService);
      const downloadDatasetSpy = vi.spyOn(downloadService, "downloadDataset").mockReturnValue(of(new Blob()));
      component.entry = makeDatasetEntry({ id: 5, name: "mydataset", coverImageUrl: undefined });

      component.onClickDownload();

      expect(downloadDatasetSpy).toHaveBeenCalledWith(5, "mydataset");
    });

    it("onClickDownload is a no-op when the entry has no id", () => {
      const downloadService = TestBed.inject(DownloadService);
      const downloadWorkflowSpy = vi.spyOn(downloadService, "downloadWorkflow");
      const downloadDatasetSpy = vi.spyOn(downloadService, "downloadDataset");
      component.entry = makeWorkflowEntry({ id: undefined });

      component.onClickDownload();

      expect(downloadWorkflowSpy).not.toHaveBeenCalled();
      expect(downloadDatasetSpy).not.toHaveBeenCalled();
    });

    it("onClickOpenShareAccess opens the workflow share modal and forwards refresh events", async () => {
      const modalService = TestBed.inject(NzModalService);
      const refresh$ = new Subject<void>();
      const createSpy = vi
        .spyOn(modalService, "create")
        .mockReturnValue({ componentInstance: { refresh: refresh$ } } as any);
      (workflowPersistService as any).retrieveOwners = vi.fn().mockReturnValue(of(["alice", "bob"]));
      component.entry = makeWorkflowEntry({ id: 7, workflow: { isOwner: true, accessLevel: "WRITE" } } as any);

      await component.onClickOpenShareAccess();

      expect(createSpy).toHaveBeenCalledTimes(1);
      const cfg = createSpy.mock.calls[0][0];
      expect(cfg.nzData).toEqual({
        writeAccess: true,
        type: "workflow",
        id: 7,
        allOwners: ["alice", "bob"],
        inWorkspace: false,
      });
      expect(cfg.nzTitle).toBe("Share this workflow with others");

      const refreshSpy = vi.fn();
      const refreshSub = component.refresh.subscribe(refreshSpy);
      refresh$.next();
      expect(refreshSpy).toHaveBeenCalledTimes(1);
      refreshSub.unsubscribe();
    });

    it("onClickOpenShareAccess opens the dataset share modal with dataset-specific data", async () => {
      const modalService = TestBed.inject(NzModalService);
      const createSpy = vi
        .spyOn(modalService, "create")
        .mockReturnValue({ componentInstance: { refresh: new Subject<void>() } } as any);
      (datasetService as any).retrieveOwners = vi.fn().mockReturnValue(of(["carol"]));
      component.entry = makeDatasetEntry({ id: 5, accessLevel: "READ", coverImageUrl: undefined } as any);

      await component.onClickOpenShareAccess();

      expect(createSpy).toHaveBeenCalledTimes(1);
      const cfg = createSpy.mock.calls[0][0];
      expect(cfg.nzData).toEqual({
        writeAccess: false, // accessLevel is READ, not WRITE
        type: "dataset",
        id: 5,
        allOwners: ["carol"],
      });
      expect(cfg.nzTitle).toBe("Share this dataset with others");
    });

    it("onClickOpenShareAccess does not open a modal for a non-shareable entry type", async () => {
      const modalService = TestBed.inject(NzModalService);
      const createSpy = vi.spyOn(modalService, "create");
      component.entry = {
        id: 3,
        name: "f",
        type: "file",
        likeCount: 0,
        viewCount: 0,
        isLiked: false,
      } as unknown as DashboardEntry;

      await component.onClickOpenShareAccess();

      expect(createSpy).not.toHaveBeenCalled();
    });

    it("confirmUpdateCustomName surfaces a missing-id error and skips the update", () => {
      const notificationService = TestBed.inject(NotificationService);
      const errorSpy = vi.spyOn(notificationService, "error");
      component.entry = makeWorkflowEntry({ id: undefined, name: "current" });
      component.originalName = "old"; // differs from current, so the update path runs

      component.confirmUpdateCustomName("new-name");

      expect(errorSpy).toHaveBeenCalledWith("Id is missing");
      expect(workflowPersistService.updateWorkflowName).not.toHaveBeenCalled();
    });

    it("confirmUpdateCustomName falls back to the default workflow name when the new name is blank", () => {
      component.entry = makeWorkflowEntry({ id: 1, name: "current" });
      component.originalName = "old";
      workflowPersistService.updateWorkflowName.mockReturnValue(of({} as Response));

      component.confirmUpdateCustomName("");

      expect(workflowPersistService.updateWorkflowName).toHaveBeenCalledWith(1, DEFAULT_WORKFLOW_NAME);
    });

    it("confirmUpdateCustomName falls back to the default dataset name when the new name is blank", () => {
      component.entry = makeDatasetEntry({ id: 5, name: "current" });
      component.originalName = "old";
      datasetService.updateDatasetName.mockReturnValue(of({} as any));

      component.confirmUpdateCustomName("");

      expect(datasetService.updateDatasetName).toHaveBeenCalledWith(5, DEFAULT_DATASET_NAME);
    });

    it("confirmUpdateCustomDescription updates a dataset description via the dataset service", () => {
      (datasetService as any).updateDatasetDescription = vi.fn().mockReturnValue(of({} as any));
      component.entry = makeDatasetEntry({ id: 5, description: "current" });
      component.originalDescription = "old";

      component.confirmUpdateCustomDescription("new description");

      expect((datasetService as any).updateDatasetDescription).toHaveBeenCalledWith(5, "new description");
      expect(component.entry.description).toBe("new description");
      expect(component.editingDescription).toBe(false);
    });

    it("confirmUpdateCustomDescription writes an empty string when the description is undefined", () => {
      component.entry = makeWorkflowEntry({ id: 1, description: "current" });
      component.originalDescription = "old";
      workflowPersistService.updateWorkflowDescription.mockReturnValue(of({} as Response));

      component.confirmUpdateCustomDescription(undefined);

      expect(workflowPersistService.updateWorkflowDescription).toHaveBeenCalledWith(1, "");
    });

    it("confirmUpdateCustomDescription falls back to an empty string when the update fails without an original", () => {
      component.entry = makeWorkflowEntry({ id: 1, description: "current" });
      component.originalDescription = undefined;
      workflowPersistService.updateWorkflowDescription.mockReturnValue(throwError(() => new Error("boom")));

      component.confirmUpdateCustomDescription("new description");

      expect(component.entry.description).toBe(""); // originalValue undefined -> ""
      expect(component.editingDescription).toBe(false);
    });

    it("openDetailModal defaults the bumped view count to 1 when no counts are returned", () => {
      const modalService = TestBed.inject(NzModalService);
      const hubService = TestBed.inject(HubService);
      vi.spyOn(modalService, "create").mockReturnValue({ componentInstance: {} } as any);
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([] as any));

      component.entry = makeWorkflowEntry({ id: 7 });
      component.viewCount = 99;
      component.openDetailModal(7);

      expect(component.viewCount).toBe(1); // (undefined ?? 0) + 1
    });

    it("toggleLike defaults the like count to 0 when the refreshed count is missing", () => {
      const hubService = TestBed.inject(HubService);
      vi.spyOn(hubService, "postLike").mockReturnValue(of(true));
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: {} }] as any));

      component.currentUid = 42;
      component.entry = makeWorkflowEntry({ id: 7 });
      component.isLiked = false;
      component.likeCount = 5;

      component.toggleLike();

      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(0);
    });

    it("toggleLike defaults the like count to 0 after an unlike when the refreshed count is missing", () => {
      const hubService = TestBed.inject(HubService);
      vi.spyOn(hubService, "postUnlike").mockReturnValue(of(true));
      vi.spyOn(hubService, "getCounts").mockReturnValue(of([{ counts: {} }] as any));

      component.currentUid = 42;
      component.entry = makeWorkflowEntry({ id: 7 });
      component.isLiked = true;
      component.likeCount = 5;

      component.toggleLike();

      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(0);
    });
  });

  describe("template rendering", () => {
    // Query, assert the element is present, then dispatch the event — a real
    // MouseEvent for clicks so handlers calling stopPropagation()/preventDefault() work.
    const fire = (css: string, event: string, payload: unknown): void => {
      const el = fixture.debugElement.query(By.css(css));
      expect(el).toBeTruthy();
      el.triggerEventHandler(event, payload);
    };

    it("renders the full private-search action set for an owned workflow (and no like button)", () => {
      component.entry = makeWorkflowEntry();
      component.isPrivateSearch = true;
      component.currentUid = 1;
      component.initializeEntry(); // the Download button reads a per-kind capability off the entry
      fixture.detectChanges();

      const de = fixture.debugElement;
      expect(de.query(By.css(".card-checkbox"))).toBeTruthy();
      expect(de.query(By.css(".edit-btn"))).toBeTruthy();
      expect(de.query(By.css('button[title="Detail"]'))).toBeTruthy();
      expect(de.query(By.css('button[title="Share"]'))).toBeTruthy();
      expect(de.query(By.css('button[title="Copy"]'))).toBeTruthy();
      expect(de.query(By.css('button[title="Download"]'))).toBeTruthy();
      expect(de.query(By.css(".delete-btn"))).toBeTruthy();
      // the like button is only rendered in non-private mode
      expect(de.query(By.css(".like-btn"))).toBeNull();
    });

    it("wires each private-search action click to its handler / output", () => {
      component.entry = makeWorkflowEntry();
      component.isPrivateSearch = true;
      component.currentUid = 1;
      component.initializeEntry();
      fixture.detectChanges();

      const detailSpy = vi.spyOn(component, "openDetailModal").mockImplementation(() => {});
      const shareSpy = vi.spyOn(component, "onClickOpenShareAccess").mockImplementation(async () => {});
      const downloadSpy = vi.spyOn(component, "onClickDownload").mockImplementation(async () => {});
      let duplicated = false;
      component.duplicated.subscribe(() => (duplicated = true));
      let deleted = false;
      component.deleted.subscribe(() => (deleted = true));

      fire('button[title="Detail"]', "click", new MouseEvent("click"));
      fire('button[title="Share"]', "click", new MouseEvent("click"));
      fire('button[title="Download"]', "click", new MouseEvent("click"));
      fire('button[title="Copy"]', "click", new MouseEvent("click"));
      fire(".delete-btn", "nzOnConfirm", undefined);

      expect(detailSpy).toHaveBeenCalled();
      expect(shareSpy).toHaveBeenCalled();
      expect(downloadSpy).toHaveBeenCalled();
      expect(duplicated).toBe(true);
      expect(deleted).toBe(true);
    });

    it("enters name-editing mode from the edit button and swaps the display for the input", () => {
      component.entry = makeWorkflowEntry();
      component.isPrivateSearch = true;
      fixture.detectChanges();

      // Trigger via the DOM; onEditName sets editingName synchronously (its setTimeout
      // focus callback never runs in this synchronous test — no fake timers needed).
      fire(".edit-btn", "click", new MouseEvent("click"));
      expect(component.editingName).toBe(true);

      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css(".resource-name-edit-input"))).toBeTruthy();
      expect(fixture.debugElement.query(By.css(".resource-name"))).toBeNull();

      // pressing Enter in the edit input confirms the rename (real key event so the
      // Angular keydown.enter binding fires through its event plugin)
      const confirmSpy = vi.spyOn(component, "confirmUpdateCustomName").mockImplementation(() => {});
      const editInput = fixture.debugElement.query(By.css(".resource-name-edit-input"))
        .nativeElement as HTMLInputElement;
      editInput.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter" }));
      expect(confirmSpy).toHaveBeenCalled();
    });

    it("renders and wires the cover-image controls when the cover is editable", () => {
      // A workflow entry with a cover url makes the component compute hasCustomImage = true
      // through its public entry input, so we don't reach into the private customImage field.
      component.entry = makeWorkflowEntry({ coverImageUrl: "http://example.com/cover.png" });
      component.isPrivateSearch = true;
      component.initializeEntry(); // process the entry input (mirrors the ngOnChanges path)
      fixture.detectChanges();
      expect(component.canEditCover).toBe(true);
      expect(component.hasCustomImage).toBe(true);

      const cameraSpy = vi.spyOn(component, "openImagePicker").mockImplementation(() => {});
      const resetSpy = vi.spyOn(component, "resetImage").mockImplementation(() => {});
      fire('button[title="Change cover image"]', "click", new MouseEvent("click"));
      fire('button[title="Reset to default image"]', "click", new MouseEvent("click"));

      expect(cameraSpy).toHaveBeenCalled();
      expect(resetSpy).toHaveBeenCalled();

      // selecting a file fires the hidden input's (change) handler
      const imageSelectedSpy = vi.spyOn(component, "onImageSelected").mockImplementation(async () => {});
      fire('input[type="file"]', "change", { target: { files: [] } });
      expect(imageSelectedSpy).toHaveBeenCalled();
    });

    it("renders the like button in non-private mode and toggles like on click", () => {
      component.entry = makeWorkflowEntry();
      component.isPrivateSearch = false;
      component.currentUid = 1;
      component.isLiked = false;
      fixture.detectChanges();

      const likeBtn = fixture.debugElement.query(By.css(".like-btn"));
      expect(likeBtn).toBeTruthy();
      expect(fixture.debugElement.query(By.css(".card-checkbox"))).toBeNull();
      expect(fixture.debugElement.query(By.css(".private-actions"))).toBeNull();

      const toggleSpy = vi.spyOn(component, "toggleLike").mockImplementation(() => {});
      likeBtn.triggerEventHandler("click", new MouseEvent("click"));
      expect(toggleSpy).toHaveBeenCalled();
    });

    it("reflects liked state and disables the like button without a current user", () => {
      component.entry = makeWorkflowEntry();
      component.isPrivateSearch = false;
      component.isLiked = true;
      component.currentUid = undefined;
      fixture.detectChanges();

      const likeBtn = fixture.debugElement.query(By.css(".like-btn")).nativeElement as HTMLButtonElement;
      expect(likeBtn.classList.contains("liked")).toBe(true);
      expect(likeBtn.disabled).toBe(true);
    });

    it("shows Download but hides Detail/Copy/checkbox for a dataset in private mode", () => {
      component.entry = makeDatasetEntry();
      component.isPrivateSearch = true;
      component.initializeEntry();
      fixture.detectChanges();

      const de = fixture.debugElement;
      expect(de.query(By.css('button[title="Download"]'))).toBeTruthy();
      expect(de.query(By.css('button[title="Share"]'))).toBeTruthy();
      expect(de.query(By.css('button[title="Detail"]'))).toBeNull();
      expect(de.query(By.css('button[title="Copy"]'))).toBeNull();
      expect(de.query(By.css(".card-checkbox"))).toBeNull();
    });

    it("renders the size row when a size is set and handles a cover-image load error", () => {
      component.entry = makeWorkflowEntry();
      component.size = 2048;
      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css('span[title="Size"]'))).toBeTruthy();

      const errorSpy = vi.spyOn(component, "onCoverError").mockImplementation(() => {});
      fire(".card-preview-image", "error", {});
      expect(errorSpy).toHaveBeenCalled();
    });

    it("writes what was typed in the name editor back onto the entry", () => {
      // The editor is seeded from entry.name; with a one-way binding it would look right on screen
      // while the confirmed rename kept sending the name the card started with.
      const entry = makeWorkflowEntry({ name: "before" });
      component.entry = entry;
      component.isPrivateSearch = true;
      component.editingName = true;
      fixture.detectChanges();

      const input = fixture.debugElement.query(By.css(".resource-name-edit-input"));
      expect(input).toBeTruthy();
      input.triggerEventHandler("ngModelChange", "after");

      expect(entry.name).toBe("after");
    });

    it("keeps a click inside the name editor from opening the card", () => {
      // The whole header is a routerLink, so without stopPropagation every click meant for the
      // caret would navigate away mid-rename.
      component.entry = makeWorkflowEntry();
      component.isPrivateSearch = true;
      component.editingName = true;
      fixture.detectChanges();

      const header = fixture.debugElement.query(By.css(".card-header")).nativeElement as HTMLElement;
      const reachedHeader = vi.fn();
      header.addEventListener("click", reachedHeader);

      const input = fixture.debugElement.query(By.css(".resource-name-edit-input")).nativeElement as HTMLInputElement;
      input.dispatchEvent(new MouseEvent("click", { bubbles: true }));

      expect(reachedHeader).not.toHaveBeenCalled();
    });
  });

  describe("guard paths", () => {
    /** Files are the one registered kind with neither a rename nor a description endpoint. */
    function makeFileEntry(overrides: Partial<DashboardEntry> = {}): DashboardEntry {
      return {
        id: 3,
        name: "notes.txt",
        description: "",
        type: EntityType.File,
        accessibleUserIds: [],
        likeCount: 0,
        viewCount: 0,
        isLiked: false,
        size: 0,
        ...overrides,
      } as unknown as DashboardEntry;
    }

    it("ngOnChanges ignores a change set that does not carry the entry", () => {
      // initializeEntry resets the cover and the counters; re-running it on an unrelated input
      // change would discard a cover that had just finished loading.
      const initialize = vi.spyOn(component, "initializeEntry");

      component.ngOnChanges({ currentUid: { currentValue: 3 } as any });

      expect(initialize).not.toHaveBeenCalled();
    });

    it("onEditDescription tolerates a textarea that has not rendered yet", fakeAsync(() => {
      // The caret is placed in a timer callback, which can outlive the element it was queued for.
      component.entry = makeWorkflowEntry({ description: "some text" });
      component.descriptionInput = undefined as any;

      component.onEditDescription();

      expect(component.editingDescription).toBe(true);
      expect(() => tick(0)).not.toThrow();
    }));

    it("does not attempt a rename for a kind that has no rename endpoint", () => {
      // Whatever the fixture types stays typed no matter which branch runs, so the editor state is
      // what separates this early return from the two that close the editor. The rename endpoint is
      // mocked so a regression fails on the assertion rather than on a TypeError out of
      // updateProperty.
      workflowPersistService.updateWorkflowName.mockReturnValue(of({} as Response));
      component.entry = makeFileEntry({ name: "typed" });
      component.originalName = "notes.txt";
      component.editingName = true;

      component.confirmUpdateCustomName("typed");

      expect(workflowPersistService.updateWorkflowName).not.toHaveBeenCalled();
      expect(datasetService.updateDatasetName).not.toHaveBeenCalled();
      expect(component.editingName).toBe(true);
    });

    it("does not attempt a description update for a kind that has no description endpoint", () => {
      // Mocked so that a regression here fails on the assertion below rather than on a TypeError
      // thrown out of updateProperty.
      workflowPersistService.updateWorkflowDescription.mockReturnValue(of({} as Response));
      component.entry = makeFileEntry({ description: "typed" });
      component.originalDescription = "";
      component.editingDescription = true;

      component.confirmUpdateCustomDescription("typed");

      expect(workflowPersistService.updateWorkflowDescription).not.toHaveBeenCalled();
      expect(component.editingDescription).toBe(true);
    });

    it("toggleLike leaves the liked state and the count alone when the unlike reports failure", () => {
      const hubService = TestBed.inject(HubService);
      vi.spyOn(hubService, "postUnlike").mockReturnValue(of(false));
      const getCountsSpy = vi.spyOn(hubService, "getCounts");

      component.currentUid = 42;
      component.entry = makeWorkflowEntry({ id: 7 });
      component.isLiked = true;
      component.likeCount = 5;

      component.toggleLike();

      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(5);
      expect(getCountsSpy).not.toHaveBeenCalled();
    });
  });
});
