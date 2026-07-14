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
import { CardItemComponent } from "./card-item.component";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { of, throwError } from "rxjs";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterTestingModule } from "@angular/router/testing";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { HUB_WORKFLOW_RESULT_DETAIL, USER_WORKSPACE } from "../../../../../app-routing.constant";
import { WorkflowCoverService } from "src/app/dashboard/service/user/workflow-cover/workflow-cover.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DatasetService } from "../../../../service/user/dataset/dataset.service";

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
    const datasetServiceSpy = { getDatasetCoverUrl: vi.fn() };

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
});
