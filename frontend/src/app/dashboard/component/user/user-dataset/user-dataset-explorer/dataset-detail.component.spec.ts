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
import { ActivatedRoute } from "@angular/router";
import { of, Subject } from "rxjs";
import { NzModalService } from "ng-zorro-antd/modal";
import { MarkdownService } from "ngx-markdown";
import { DatasetDetailComponent } from "./dataset-detail.component";
import { DatasetService, MultipartUploadProgress } from "../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { MOCK_USER, StubUserService } from "../../../../../common/service/user/stub-user.service";
import { HubService } from "../../../../../hub/service/hub.service";
import { AdminSettingsService } from "../../../../service/admin/settings/admin-settings.service";
import { FileUploadItem } from "../../../../type/dashboard-file.interface";
import { DatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../../common/type/dataset-staged-object";
import { commonTestImports, commonTestProviders } from "../../../../../common/testing/test-utils";

describe("DatasetDetailComponent upload queue", () => {
  let fixture: ComponentFixture<DatasetDetailComponent>;
  let component: DatasetDetailComponent;
  let uploadSubjects: Subject<MultipartUploadProgress>[];
  let uploadedPaths: string[];
  let multipartUploadSpy: ReturnType<typeof vi.fn>;

  const makeFileItem = (name: string): FileUploadItem => ({
    file: new File(["x"], name),
    name,
    description: "",
    uploadProgress: 0,
    isUploadingFlag: false,
    restart: false,
  });

  const dropFiles = (...names: string[]) => component.onNewUploadFilesChanged(names.map(makeFileItem));

  const finishUpload = (index: number, filePath: string, totalTime = 1) =>
    uploadSubjects[index].next({ filePath, percentage: 100, status: "finished", totalTime });

  beforeEach(() => {
    uploadSubjects = [];
    uploadedPaths = [];
    multipartUploadSpy = vi.fn((_ownerEmail: string, _datasetName: string, filePath: string) => {
      const progress = new Subject<MultipartUploadProgress>();
      uploadSubjects.push(progress);
      uploadedPaths.push(filePath);
      return progress.asObservable();
    });

    TestBed.configureTestingModule({
      imports: [DatasetDetailComponent, ...commonTestImports],
      providers: [
        { provide: ActivatedRoute, useValue: { params: of({ did: 1 }), data: of({}) } },
        { provide: NzModalService, useValue: {} },
        {
          provide: DatasetService,
          useValue: {
            multipartUpload: multipartUploadSpy,
            finalizeMultipartUpload: vi.fn(() => of({})),
            getDataset: vi.fn(() =>
              of({
                dataset: { name: "test-dataset", description: "", isPublic: false, isDownloadable: true },
                accessPrivilege: "WRITE",
                ownerEmail: "owner@texera.com",
                isOwner: true,
              })
            ),
            retrieveDatasetVersionList: vi.fn(() => of([])),
            getDatasetDiff: vi.fn(() => of([])),
            createDatasetVersion: vi.fn(() => of({})),
            deleteDatasetFile: vi.fn(() => of({})),
          },
        },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn(), info: vi.fn() } },
        { provide: DownloadService, useValue: {} },
        { provide: UserService, useClass: StubUserService },
        {
          provide: HubService,
          useValue: {
            getCounts: vi.fn(() => of([{ counts: { like: 0 } }])),
            postView: vi.fn(() => of(0)),
            isLiked: vi.fn(() => of([{ isLiked: false }])),
          },
        },
        { provide: AdminSettingsService, useValue: { getSetting: vi.fn(() => of("3")) } },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
        ...commonTestProviders,
      ],
    });

    fixture = TestBed.createComponent(DatasetDetailComponent);
    component = fixture.componentInstance;
    // Log in so ngOnInit reaches loadUploadSettings (maxConcurrentFiles = 3).
    (TestBed.inject(UserService) as unknown as StubUserService).userChangeSubject.next(MOCK_USER);
    fixture.detectChanges();
  });

  it("starts at most maxConcurrentFiles uploads immediately and queues the rest", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt", "f5.txt");

    expect(multipartUploadSpy).toHaveBeenCalledTimes(3);
    expect(uploadedPaths).toEqual(["f1.txt", "f2.txt", "f3.txt"]);
    expect(component.activeCount).toBe(3);
    expect(component.queuedCount).toBe(2);
    expect(component.queuedFileNames).toEqual(["f4.txt", "f5.txt"]);
  });

  it("does nothing when an empty file list is dropped", () => {
    dropFiles();

    expect(multipartUploadSpy).not.toHaveBeenCalled();
    expect(component.activeCount).toBe(0);
    expect(component.queuedCount).toBe(0);
    expect(component.queuedFileNames).toEqual([]);
  });

  it("starts the next queued upload when an active upload finishes", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt", "f5.txt");

    finishUpload(0, "f1.txt");

    expect(multipartUploadSpy).toHaveBeenCalledTimes(4);
    expect(uploadedPaths[3]).toBe("f4.txt");
    expect(component.activeCount).toBe(3);
    expect(component.queuedCount).toBe(1);
    expect(component.queuedFileNames).toEqual(["f5.txt"]);
  });

  it("removes a cancelled file from the pending queue without starting it", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt", "f5.txt");

    component.cancelExistingUpload("f4.txt");

    expect(multipartUploadSpy).toHaveBeenCalledTimes(3);
    expect(component.queuedCount).toBe(1);
    expect(component.queuedFileNames).toEqual(["f5.txt"]);
  });

  it("ignores cancellation of a file that is neither active nor queued", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt");

    component.cancelExistingUpload("missing.txt");

    expect(component.activeCount).toBe(3);
    expect(component.queuedCount).toBe(1);
    expect(component.queuedFileNames).toEqual(["f4.txt"]);
  });

  // #5586: the template reads queuedFileNames on every change-detection pass,
  // so it must not allocate a new array unless the queue changed.
  it("keeps the same queuedFileNames array reference while the queue is unchanged", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt", "f5.txt");

    const firstRead = component.queuedFileNames;

    expect(component.queuedFileNames).toBe(firstRead);
  });

  it("exposes a new queuedFileNames array after the queue changes", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt", "f5.txt");
    const beforeCancel = component.queuedFileNames;

    component.cancelExistingUpload("f4.txt");

    expect(component.queuedFileNames).not.toBe(beforeCancel);
    expect(component.queuedFileNames).toEqual(["f5.txt"]);
  });

  it("identifies pending queue entries by file name in trackByPendingFile", () => {
    expect(component.trackByPendingFile(0, "dir/a.txt")).toBe("dir/a.txt");
  });

  // A resumed upload with no missing parts finishes with totalTime exactly 0;
  // the slot must still be released.
  it("releases the concurrency slot when a finished upload reports totalTime 0", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt");

    finishUpload(0, "f1.txt", 0);

    expect(multipartUploadSpy).toHaveBeenCalledTimes(4);
    expect(uploadedPaths[3]).toBe("f4.txt");
    expect(component.activeCount).toBe(3);
    expect(component.queuedCount).toBe(0);
  });

  // The Pending header updates per file, so the Finished header must too — it
  // cannot wait for the throttled staged-objects refetch.
  it("updates the Finished count immediately when uploads finish", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt");
    expect(component.pendingChangesCount).toBe(0);

    finishUpload(0, "f1.txt");
    expect(component.pendingChangesCount).toBe(1);

    finishUpload(1, "f2.txt");
    expect(component.pendingChangesCount).toBe(2);
  });

  it("reconciles the optimistic Finished count with a diff response", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt");
    finishUpload(0, "f1.txt");
    finishUpload(1, "f2.txt");

    const diff: DatasetStagedObject[] = [{ path: "f1.txt", pathType: "file", diffType: "added", sizeBytes: 1 }];
    component.onStagedObjectsUpdated(diff);

    // f1 is confirmed by the response; f2 stays counted until a response includes it.
    expect(component.pendingChangesCount).toBe(2);

    component.onStagedObjectsUpdated([...diff, { path: "f2.txt", pathType: "file", diffType: "added", sizeBytes: 1 }]);
    expect(component.pendingChangesCount).toBe(2);
  });

  it("keeps an in-progress upload's slot while progress events stream in", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt");

    uploadSubjects[0].next({ filePath: "f1.txt", percentage: 50, status: "uploading" });

    expect(component.uploadTasks.find(t => t.filePath === "f1.txt")?.percentage).toBe(50);
    expect(component.activeCount).toBe(3);
    expect(component.queuedCount).toBe(1);
  });

  it("does not double-count a finished upload already confirmed by a diff response", () => {
    dropFiles("f1.txt");
    finishUpload(0, "f1.txt");
    component.onStagedObjectsUpdated([{ path: "f1.txt", pathType: "file", diffType: "added", sizeBytes: 1 }]);
    expect(component.pendingChangesCount).toBe(1);

    dropFiles("f1.txt"); // re-upload the already-staged file
    finishUpload(1, "f1.txt");

    expect(component.pendingChangesCount).toBe(1);
  });

  it("does not start queued uploads beyond a lowered concurrency limit", () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt");
    component.maxConcurrentFiles = 1;

    finishUpload(0, "f1.txt");

    expect(component.activeCount).toBe(2);
    expect(component.queuedCount).toBe(1);
    expect(multipartUploadSpy).toHaveBeenCalledTimes(3);
  });

  it("clears the Finished count when a version is created", () => {
    dropFiles("f1.txt");
    finishUpload(0, "f1.txt");
    expect(component.pendingChangesCount).toBe(1);

    component.versionName = "v1";
    component.onClickOpenVersionCreator();

    expect(component.pendingChangesCount).toBe(0);
  });

  it("does not remove a re-uploaded file's active task when hiding its finished predecessor", () => {
    vi.useFakeTimers();
    try {
      dropFiles("a.txt");
      finishUpload(0, "a.txt"); // schedules the finished row to hide in 5s

      dropFiles("a.txt"); // re-upload the same name within the 5s window
      vi.advanceTimersByTime(5000);

      expect(component.uploadTasks).toHaveLength(1);
      expect(component.uploadTasks[0].status).not.toBe("finished");
      expect(component.activeCount).toBe(1);

      finishUpload(1, "a.txt");
      expect(component.activeCount).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("renders the virtualized pending list and re-measures viewports on panel expand", async () => {
    dropFiles("f1.txt", "f2.txt", "f3.txt", "f4.txt", "f5.txt");
    fixture.detectChanges();
    // Flush the viewport's init microtask, then render the rows.
    await Promise.resolve();
    fixture.detectChanges();

    expect(component.pendingListHeightPx).toBe(2 * component.PENDING_ROW_HEIGHT_PX);
    const rows = fixture.nativeElement.querySelectorAll(".pending-file-row");
    expect(rows.length).toBe(2);

    // Expand the Pending / Uploading / Finished panels.
    const headers: NodeListOf<HTMLElement> = fixture.nativeElement.querySelectorAll(
      ".upload-status-panels .ant-collapse-header"
    );
    expect(headers.length).toBe(3);
    headers.forEach(header => header.click());
    fixture.detectChanges();
    // Flush the checkViewportSize timers.
    await new Promise(resolve => setTimeout(resolve));

    // Collapsing again must be a no-op for the re-measure handler.
    headers.forEach(header => header.click());
    fixture.detectChanges();

    // Cancel a queued file from its row.
    const cancelButton = fixture.nativeElement.querySelector(".pending-file-row button") as HTMLButtonElement;
    cancelButton.click();
    expect(component.queuedCount).toBe(1);
    expect(component.queuedFileNames).toEqual(["f5.txt"]);
  });

  it("counts a staged file deletion immediately", () => {
    const node: DatasetFileNode = { name: "a.txt", type: "file", parentDir: "/owner@texera.com/test-dataset/v1" };

    component.onPreviouslyUploadedFileDeleted(node);

    expect(component.pendingChangesCount).toBe(1);
  });
});
