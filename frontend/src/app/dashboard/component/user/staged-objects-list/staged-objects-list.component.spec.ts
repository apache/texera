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
import { EventEmitter } from "@angular/core";
import { By } from "@angular/platform-browser";
import { CdkVirtualScrollViewport } from "@angular/cdk/scrolling";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { of, throwError } from "rxjs";
import { StagedObjectsListComponent } from "./staged-objects-list.component";
import { StagedFileService } from "../../../service/user/file-resource/staged-file.service";
import { DATASET_FILE_RESOURCE_ENDPOINT } from "../../../service/user/file-resource/file-resource-endpoint";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";

describe("StagedObjectsListComponent", () => {
  let fixture: ComponentFixture<StagedObjectsListComponent>;
  let component: StagedObjectsListComponent;
  let getDiffSpy: ReturnType<typeof vi.fn>;
  let resetFileDiffSpy: ReturnType<typeof vi.fn>;

  const renderList = async () => {
    component.resourceId = 1;
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
  };

  const stagedObjects: DatasetStagedObject[] = [
    { path: "dir/a.txt", pathType: "file", diffType: "added", sizeBytes: 1 },
    { path: "dir/b.txt", pathType: "file", diffType: "removed" },
  ];

  beforeEach(() => {
    getDiffSpy = vi.fn(() => of(stagedObjects));
    resetFileDiffSpy = vi.fn(() => of({}));

    TestBed.configureTestingModule({
      imports: [StagedObjectsListComponent, ...commonTestImports],
      providers: [
        {
          provide: StagedFileService,
          useValue: { getDiff: getDiffSpy, resetFileDiff: resetFileDiffSpy },
        },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn() } },
        ...commonTestProviders,
      ],
    });

    fixture = TestBed.createComponent(StagedObjectsListComponent);
    component = fixture.componentInstance;
  });

  it("fetches staged objects on init and emits them", () => {
    component.resourceId = 1;
    const emitted: DatasetStagedObject[][] = [];
    component.stagedObjectsChanged.subscribe((objects: DatasetStagedObject[]) => emitted.push(objects));

    component.ngOnInit();

    expect(getDiffSpy).toHaveBeenCalledWith(DATASET_FILE_RESOURCE_ENDPOINT, 1);
    expect(component.stagedObjects).toEqual(stagedObjects);
    expect(emitted).toEqual([stagedObjects]);
  });

  it("does not fetch staged objects when resourceId is undefined", () => {
    component.resourceId = undefined;

    component.ngOnInit();

    expect(getDiffSpy).not.toHaveBeenCalled();
    expect(component.stagedObjects).toEqual([]);
  });

  // #5586: one change event per finished file must not mean one dataset-diff
  // request per file.
  it("coalesces bursts of change events into one refetch per audit window", () => {
    vi.useFakeTimers();
    try {
      component.resourceId = 1;
      const changes = new EventEmitter<void>();
      component.userMakeChangesEvent = changes;

      for (let i = 0; i < 50; i++) {
        changes.emit();
      }
      expect(getDiffSpy).not.toHaveBeenCalled();

      vi.advanceTimersByTime(StagedObjectsListComponent.REFRESH_AUDIT_TIME_MS);
      expect(getDiffSpy).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("identifies staged objects by diff type and path in trackByStagedObject", () => {
    expect(component.trackByStagedObject(0, stagedObjects[0])).toBe("added:dir/a.txt");
    expect(component.trackByStagedObject(1, stagedObjects[1])).toBe("removed:dir/b.txt");
  });

  it("renders staged object rows inside the virtual scroll viewport", async () => {
    await renderList();

    const viewport = fixture.nativeElement.querySelector("cdk-virtual-scroll-viewport");
    expect(viewport).not.toBeNull();
    const rows = fixture.nativeElement.querySelectorAll("nz-list-item.staged-object-row");
    expect(rows.length).toBe(stagedObjects.length);
    expect(rows[0].textContent).toContain("dir/a.txt");
    expect(rows[1].textContent).toContain("dir/b.txt");
  });

  it("reverts a staged object from its row's delete button", async () => {
    await renderList();

    (fixture.nativeElement.querySelector(".delete-button") as HTMLButtonElement).click();

    expect(resetFileDiffSpy).toHaveBeenCalledWith(DATASET_FILE_RESOURCE_ENDPOINT, 1, "dir/a.txt");
    expect(getDiffSpy).toHaveBeenCalledTimes(2);
  });

  it("shows the full path and upload time in the row tooltip", async () => {
    component.uploadTimeMap = new Map([["a.txt", 5]]);
    await renderList();

    fixture.debugElement.query(By.css(".truncate-file-path")).injector.get(NzTooltipDirective).show();
    fixture.detectChanges();

    const overlayText = document.querySelector(".cdk-overlay-container")?.textContent ?? "";
    expect(overlayText).toContain("dir/a.txt");
    expect(overlayText).toContain("Upload time");
  });

  it("re-measures the viewport on request", async () => {
    await renderList();
    const viewport = fixture.debugElement.query(By.directive(CdkVirtualScrollViewport)).componentInstance;
    const checkViewportSizeSpy = vi.spyOn(viewport, "checkViewportSize");

    component.remeasureViewport();
    await new Promise(resolve => setTimeout(resolve));

    expect(checkViewportSizeSpy).toHaveBeenCalled();
  });
  describe("branch coverage", () => {
    it("ignores a userMakeChangesEvent that is not provided", () => {
      // The setter guards on the event; assigning nothing must not subscribe or refetch.
      component.resourceId = 1;
      component.userMakeChangesEvent = undefined as unknown as EventEmitter<void>;
      expect(getDiffSpy).not.toHaveBeenCalled();
    });

    it("does not revert an object when no dataset id is set", () => {
      component.resourceId = undefined;
      component.onObjectReverted(stagedObjects[0]);
      expect(resetFileDiffSpy).not.toHaveBeenCalled();
    });

    it("notifies when reverting a staged object fails", () => {
      resetFileDiffSpy.mockReturnValue(throwError(() => new Error("boom")));
      const notificationService = TestBed.inject(NotificationService);
      component.resourceId = 1;

      component.onObjectReverted(stagedObjects[0]);

      expect(notificationService.error).toHaveBeenCalledWith("Failed to delete the file");
    });

    it("returns no upload time when there is no upload-time map", () => {
      component.uploadTimeMap = undefined;
      expect(component.getFileUploadTime("dir/a.txt")).toBeNull();
    });

    it("falls back to the whole path when the last segment is empty", () => {
      component.uploadTimeMap = new Map([["dir/", 111]]);
      // "dir/".split("/").pop() is "", so the lookup key falls back to the full path.
      expect(component.getFileUploadTime("dir/")).toBe(111);
    });

    it("returns null for a filename that is absent from the upload-time map", () => {
      component.uploadTimeMap = new Map([["a.txt", 222]]);
      expect(component.getFileUploadTime("dir/missing.txt")).toBeNull();
    });
  });
});
