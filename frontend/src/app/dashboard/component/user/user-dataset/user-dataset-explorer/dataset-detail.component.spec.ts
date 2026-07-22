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
import { of, Subject, throwError } from "rxjs";
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
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../../common/type/dataset-staged-object";
import { commonTestImports, commonTestProviders } from "../../../../../common/testing/test-utils";
import { Dataset, DatasetVersion } from "../../../../../common/type/dataset";
import { DashboardDataset } from "../../../../type/dashboard-dataset.interface";
import { HttpErrorResponse } from "@angular/common/http";
import { format } from "date-fns";

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
        { provide: AdminSettingsService, useValue: { getPublicSetting: vi.fn(() => of("3")) } },
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

describe("DatasetDetailComponent behavior", () => {
  let fixture: ComponentFixture<DatasetDetailComponent>;
  let component: DatasetDetailComponent;

  type MockService = Record<string, ReturnType<typeof vi.fn>>;
  let datasetServiceStub: MockService;
  let notificationServiceStub: MockService;
  let downloadServiceStub: MockService;
  let hubServiceStub: MockService;
  let adminSettingsServiceStub: MockService;

  const CREATION_TS = 1_700_000_000_000;

  const makeDataset = (overrides: Partial<Dataset> = {}): Dataset => ({
    did: 5,
    ownerUid: 9,
    name: "ds",
    isPublic: false,
    isDownloadable: true,
    storagePath: undefined,
    description: "desc",
    creationTime: undefined,
    coverImage: undefined,
    ...overrides,
  });

  const makeDashboardDataset = (overrides: Partial<DashboardDataset> = {}): DashboardDataset => ({
    isOwner: false,
    ownerEmail: "owner@texera.com",
    dataset: makeDataset(),
    accessPrivilege: "NONE",
    size: 0,
    ...overrides,
  });

  const makeVersion = (overrides: Partial<DatasetVersion> = {}): DatasetVersion => ({
    dvid: 1,
    did: 5,
    creatorUid: 9,
    name: "v1",
    versionHash: undefined,
    creationTime: undefined,
    fileNodes: undefined,
    ...overrides,
  });

  const fileLeaf = (name: string, parentDir: string, size: number): DatasetFileNode => ({
    name,
    type: "file",
    parentDir,
    size,
  });

  const createComponent = (params: Record<string, unknown> = { did: 5 }): void => {
    TestBed.configureTestingModule({
      imports: [DatasetDetailComponent, ...commonTestImports],
      providers: [
        { provide: ActivatedRoute, useValue: { params: of(params), data: of({}) } },
        { provide: NzModalService, useValue: {} },
        { provide: DatasetService, useValue: datasetServiceStub },
        { provide: NotificationService, useValue: notificationServiceStub },
        { provide: DownloadService, useValue: downloadServiceStub },
        { provide: UserService, useClass: StubUserService },
        { provide: HubService, useValue: hubServiceStub },
        { provide: AdminSettingsService, useValue: adminSettingsServiceStub },
        { provide: MarkdownService, useValue: { parse: vi.fn(() => "") } },
        ...commonTestProviders,
      ],
    });
    fixture = TestBed.createComponent(DatasetDetailComponent);
    component = fixture.componentInstance;
  };

  // The StubUserService emits MOCK_USER in its own constructor, before the
  // component subscribes, so currentUid starts undefined; re-emit to log in.
  const login = (): void => {
    (TestBed.inject(UserService) as unknown as StubUserService).userChangeSubject.next(MOCK_USER);
  };

  beforeEach(() => {
    datasetServiceStub = {
      getDataset: vi.fn(() => of(makeDashboardDataset())),
      retrieveDatasetVersionList: vi.fn(() => of([])),
      getDatasetCoverUrl: vi.fn(() => of({ url: "http://cover" })),
      retrieveDatasetVersionFileTree: vi.fn(() => of({ fileNodes: [fileLeaf("a.txt", "/root", 1)], size: 1 })),
      createDatasetVersion: vi.fn(() => of(makeVersion())),
      updateDatasetPublicity: vi.fn(() => of({})),
      updateDatasetDownloadable: vi.fn(() => of({})),
      updateDatasetCoverImage: vi.fn(() => of({})),
      updateDatasetDescription: vi.fn(() => of({})),
      deleteDatasetFile: vi.fn(() => of({})),
      getDatasetDiff: vi.fn(() => of([])),
      multipartUpload: vi.fn(() => of()),
      finalizeMultipartUpload: vi.fn(() => of({})),
    };
    notificationServiceStub = { success: vi.fn(), error: vi.fn(), info: vi.fn() };
    downloadServiceStub = {
      downloadDatasetVersion: vi.fn(() => of(new Blob())),
      downloadSingleFile: vi.fn(() => of(new Blob())),
    };
    hubServiceStub = {
      getCounts: vi.fn(() => of([{ counts: { like: 0 } }])),
      postView: vi.fn(() => of(0)),
      isLiked: vi.fn(() => of([{ isLiked: false }])),
      postLike: vi.fn(() => of(true)),
      postUnlike: vi.fn(() => of(true)),
    };
    adminSettingsServiceStub = { getPublicSetting: vi.fn(() => of("50")) };
  });

  describe("ngOnInit", () => {
    it("loads info, versions, like and view counts but skips liked/upload settings without a current user", () => {
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 7 } }]));
      hubServiceStub.postView.mockReturnValue(of(42));

      createComponent({ did: 5 });
      // Drive the genuine logged-out path rather than relying on StubUserService's emission
      // quirk (its user makes isLogin default to true).
      component.isLogin = false;
      fixture.detectChanges();

      expect(datasetServiceStub.getDataset).toHaveBeenCalled();
      expect(datasetServiceStub.retrieveDatasetVersionList).toHaveBeenCalled();
      expect(component.likeCount).toBe(7);
      expect(component.viewCount).toBe(42);
      expect(hubServiceStub.isLiked).not.toHaveBeenCalled();
      expect(adminSettingsServiceStub.getPublicSetting).not.toHaveBeenCalled();
    });

    it("fetches liked status and upload settings for a logged-in user", () => {
      hubServiceStub.isLiked.mockReturnValue(of([{ isLiked: true }]));

      createComponent({ did: 5 });
      login();
      fixture.detectChanges();

      expect(hubServiceStub.isLiked).toHaveBeenCalled();
      expect(component.isLiked).toBe(true);
      expect(adminSettingsServiceStub.getPublicSetting).toHaveBeenCalled();
    });

    it("keeps the default upload settings when the public settings are missing", () => {
      adminSettingsServiceStub.getPublicSetting.mockReturnValue(of(null));

      createComponent({ did: 5 });
      login();
      fixture.detectChanges();

      expect(component.chunkSizeMiB).toBe(50);
      expect(component.maxConcurrentChunks).toBe(10);
      expect(component.maxConcurrentFiles).toBe(3);
    });

    it("makes no hub calls when the route carries no did", () => {
      createComponent({});
      component.ngOnInit();

      expect(datasetServiceStub.getDataset).not.toHaveBeenCalled();
      expect(hubServiceStub.getCounts).not.toHaveBeenCalled();
      expect(hubServiceStub.postView).not.toHaveBeenCalled();
    });
  });

  describe("retrieveDatasetInfo", () => {
    it("maps dataset fields, formats numeric creation time, and resolves the cover image url", () => {
      const dashboard = makeDashboardDataset({
        isOwner: true,
        ownerEmail: "o@e.com",
        accessPrivilege: "WRITE",
        dataset: makeDataset({
          name: "N",
          description: "D",
          isPublic: true,
          isDownloadable: false,
          coverImage: "cover.png",
          creationTime: CREATION_TS,
        }),
      });
      datasetServiceStub.getDataset.mockReturnValue(of(dashboard));
      datasetServiceStub.getDatasetCoverUrl.mockReturnValue(of({ url: "http://c" }));

      createComponent();
      component.did = 5;
      component.retrieveDatasetInfo();

      expect(component.datasetName).toBe("N");
      expect(component.datasetDescription).toBe("D");
      expect(component.userDatasetAccessLevel).toBe("WRITE");
      expect(component.datasetIsPublic).toBe(true);
      expect(component.datasetIsDownloadable).toBe(false);
      expect(component.ownerEmail).toBe("o@e.com");
      expect(component.isOwner).toBe(true);
      expect(component.coverImageUrl).toBe("http://c");
      expect(component.datasetCreationTime).toEqual(format(new Date(CREATION_TS), "MM/dd/yyyy HH:mm:ss"));
      expect(component.datasetCreationTime).toMatch(/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/);
    });

    it("nulls the cover image url when its retrieval fails", () => {
      datasetServiceStub.getDataset.mockReturnValue(
        of(makeDashboardDataset({ dataset: makeDataset({ coverImage: "c.png" }) }))
      );
      datasetServiceStub.getDatasetCoverUrl.mockReturnValue(throwError(() => new Error("boom")));

      createComponent();
      component.did = 5;
      component.coverImageUrl = "stale";
      component.retrieveDatasetInfo();

      expect(component.coverImageUrl).toBeNull();
    });

    it("leaves the cover image url null and skips the cover fetch when there is no cover image", () => {
      datasetServiceStub.getDataset.mockReturnValue(
        of(makeDashboardDataset({ dataset: makeDataset({ coverImage: undefined }) }))
      );

      createComponent();
      component.did = 5;
      component.coverImageUrl = "stale";
      component.retrieveDatasetInfo();

      expect(component.coverImageUrl).toBeNull();
      expect(datasetServiceStub.getDatasetCoverUrl).not.toHaveBeenCalled();
    });
  });

  describe("retrieveDatasetVersionList", () => {
    it("selects the first version and delegates to onVersionSelected when the list is non-empty", () => {
      const v1 = makeVersion({ dvid: 10, name: "v10" });
      const v2 = makeVersion({ dvid: 9, name: "v9" });
      datasetServiceStub.retrieveDatasetVersionList.mockReturnValue(of([v1, v2]));

      createComponent();
      component.did = 5;
      const spy = vi.spyOn(component, "onVersionSelected");
      component.retrieveDatasetVersionList();

      expect(component.versions).toEqual([v1, v2]);
      expect(component.selectedVersion).toEqual(v1);
      expect(spy).toHaveBeenCalledWith(v1);
    });

    it("makes no selection when the version list is empty", () => {
      datasetServiceStub.retrieveDatasetVersionList.mockReturnValue(of([]));

      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;
      const spy = vi.spyOn(component, "onVersionSelected");
      component.retrieveDatasetVersionList();

      expect(component.versions).toEqual([]);
      expect(component.selectedVersion).toBeUndefined();
      expect(spy).not.toHaveBeenCalled();
    });
  });

  describe("onVersionSelected", () => {
    it("walks nested directories to the first file leaf and loads it", () => {
      const leaf = fileLeaf("c.txt", "/root/a", 42);
      const tree: DatasetFileNode[] = [{ name: "a", type: "directory", parentDir: "/root", children: [leaf] }];
      datasetServiceStub.retrieveDatasetVersionFileTree.mockReturnValue(of({ fileNodes: tree, size: 100 }));

      createComponent();
      component.did = 5;
      component.onVersionSelected(makeVersion({ dvid: 2, creationTime: CREATION_TS }));

      expect(component.fileTreeNodeList).toEqual(tree);
      expect(component.currentDatasetVersionSize).toBe(100);
      expect(component.currentDisplayedFileName).toBe(getFullPathFromDatasetFileNode(leaf));
      expect(component.currentFileSize).toBe(42);
      expect(component.selectedVersionCreationTime).toMatch(/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$/);
    });

    it("does not fetch a file tree for a version without a dvid", () => {
      createComponent();
      component.did = 5;
      component.onVersionSelected(makeVersion({ dvid: undefined }));

      expect(datasetServiceStub.retrieveDatasetVersionFileTree).not.toHaveBeenCalled();
    });
  });

  describe("isDownloadAllowed and userHasWriteAccess", () => {
    beforeEach(() => createComponent());

    it("always allows the owner to download, even when the dataset is not downloadable", () => {
      component.isOwner = true;
      component.datasetIsDownloadable = false;
      expect(component.isDownloadAllowed()).toBe(true);
    });

    it("allows a non-owner to download a public downloadable dataset without explicit access", () => {
      component.isOwner = false;
      component.datasetIsDownloadable = true;
      component.datasetIsPublic = true;
      component.userDatasetAccessLevel = "NONE";
      expect(component.isDownloadAllowed()).toBe(true);
    });

    it("blocks a non-owner from a private downloadable dataset without access", () => {
      component.isOwner = false;
      component.datasetIsDownloadable = true;
      component.datasetIsPublic = false;
      component.userDatasetAccessLevel = "NONE";
      expect(component.isDownloadAllowed()).toBe(false);
    });

    it("blocks download when the dataset is not downloadable", () => {
      component.isOwner = false;
      component.datasetIsDownloadable = false;
      component.datasetIsPublic = true;
      expect(component.isDownloadAllowed()).toBe(false);
    });

    it("reports write access only for the WRITE privilege", () => {
      component.userDatasetAccessLevel = "WRITE";
      expect(component.userHasWriteAccess()).toBe(true);
      component.userDatasetAccessLevel = "READ";
      expect(component.userHasWriteAccess()).toBe(false);
      component.userDatasetAccessLevel = "NONE";
      expect(component.userHasWriteAccess()).toBe(false);
    });
  });

  describe("publicity and downloadable toggles", () => {
    it("marks the dataset public and toasts on success", () => {
      createComponent();
      component.did = 5;
      component.datasetName = "MyDS";
      component.onPublicStatusChange(true);

      expect(component.datasetIsPublic).toBe(true);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset MyDS is now public");
    });

    it("keeps the public flag and toasts an error when the publicity update fails", () => {
      datasetServiceStub.updateDatasetPublicity.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetIsPublic = false;
      component.onPublicStatusChange(true);

      expect(component.datasetIsPublic).toBe(false);
      expect(notificationServiceStub.error).toHaveBeenCalledWith("Fail to change the dataset publicity");
    });

    it("marks downloads not-allowed and toasts on success", () => {
      createComponent();
      component.did = 5;
      component.onDownloadableStatusChange(false);

      expect(component.datasetIsDownloadable).toBe(false);
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Dataset downloads are now not allowed");
    });

    it("keeps the downloadable flag and toasts an error when the update fails", () => {
      datasetServiceStub.updateDatasetDownloadable.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetIsDownloadable = true;
      component.onDownloadableStatusChange(false);

      expect(component.datasetIsDownloadable).toBe(true);
      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to change the dataset download permission");
    });
  });

  describe("onClickOpenVersionCreator", () => {
    it("creates a version, clears the name, refreshes the list and emits a change on success", () => {
      datasetServiceStub.createDatasetVersion.mockReturnValue(of(makeVersion()));
      datasetServiceStub.retrieveDatasetVersionList.mockReturnValue(of([]));
      createComponent();
      component.did = 5;
      component.versionName = "v2";
      const emit = vi.fn();
      component.userMakeChanges.subscribe(emit);

      component.onClickOpenVersionCreator();

      expect(datasetServiceStub.createDatasetVersion).toHaveBeenCalledWith(5, "v2");
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Version Created");
      expect(component.versionName).toBe("");
      expect(component.isCreatingVersion).toBe(false);
      expect(datasetServiceStub.retrieveDatasetVersionList).toHaveBeenCalled();
      expect(emit).toHaveBeenCalled();
    });

    it("surfaces the backend message and resets the in-progress flag on failure", () => {
      datasetServiceStub.createDatasetVersion.mockReturnValue(throwError(() => ({ error: { message: "boom" } })));
      createComponent();
      component.did = 5;
      component.versionName = "v2";

      component.onClickOpenVersionCreator();

      expect(notificationServiceStub.error).toHaveBeenCalledWith("Version creation failed: boom");
      expect(component.isCreatingVersion).toBe(false);
    });

    it("ignores a second click while a version creation is already in progress", () => {
      datasetServiceStub.createDatasetVersion.mockReturnValue(new Subject());
      createComponent();
      component.did = 5;

      component.onClickOpenVersionCreator();
      component.onClickOpenVersionCreator();

      expect(datasetServiceStub.createDatasetVersion).toHaveBeenCalledTimes(1);
      expect(component.isCreatingVersion).toBe(true);
    });
  });

  describe("downloads", () => {
    it("downloads the selected version as a zip when did and dvid are present", () => {
      createComponent();
      component.did = 5;
      component.datasetName = "DS";
      component.selectedVersion = makeVersion({ dvid: 3, name: "v3" });

      component.onClickDownloadVersionAsZip();

      expect(downloadServiceStub.downloadDatasetVersion).toHaveBeenCalledWith(5, 3, "DS", "v3");
    });

    it("does not download a zip when no version is selected", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;

      component.onClickDownloadVersionAsZip();

      expect(downloadServiceStub.downloadDatasetVersion).not.toHaveBeenCalled();
    });

    it("uses the public endpoint to download the current file for a public non-owner dataset", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ dvid: 3 });
      component.datasetIsPublic = true;
      component.isOwner = false;
      component.currentDisplayedFileName = "/a/b/c.txt";

      component.onClickDownloadCurrentFile();

      expect(downloadServiceStub.downloadSingleFile).toHaveBeenCalledWith("/a/b/c.txt", false);
    });

    it("uses the authenticated endpoint to download the current file for the owner", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ dvid: 3 });
      component.datasetIsPublic = true;
      component.isOwner = true;
      component.currentDisplayedFileName = "/a/b/c.txt";

      component.onClickDownloadCurrentFile();

      expect(downloadServiceStub.downloadSingleFile).toHaveBeenCalledWith("/a/b/c.txt", true);
    });

    it("does not download the current file without a selected version dvid", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;

      component.onClickDownloadCurrentFile();

      expect(downloadServiceStub.downloadSingleFile).not.toHaveBeenCalled();
    });
  });

  describe("staged objects and view flags", () => {
    beforeEach(() => createComponent());

    it("tracks the pending-change count from staged objects", () => {
      const staged: DatasetStagedObject[] = [
        { path: "a", pathType: "file", diffType: "added", sizeBytes: 1 },
        { path: "b", pathType: "file", diffType: "added", sizeBytes: 1 },
      ];
      component.onStagedObjectsUpdated(staged);
      expect(component.pendingChangesCount).toBe(2);
      expect(component.userHasPendingChanges).toBe(true);

      component.onStagedObjectsUpdated([]);
      expect(component.pendingChangesCount).toBe(0);
      expect(component.userHasPendingChanges).toBe(false);
    });

    it("toggles the maximize, right-bar and precise-view-count flags", () => {
      expect(component.isMaximized).toBe(false);
      component.onClickScaleTheView();
      expect(component.isMaximized).toBe(true);

      expect(component.isRightBarCollapsed).toBe(false);
      component.onClickHideRightBar();
      expect(component.isRightBarCollapsed).toBe(true);

      expect(component.displayPreciseViewCount).toBe(false);
      component.changeViewDisplayStyle();
      expect(component.displayPreciseViewCount).toBe(true);
    });
  });

  describe("toggleLike", () => {
    it("unlikes and decrements the like count when currently liked", () => {
      hubServiceStub.postUnlike.mockReturnValue(of(true));
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 4 } }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = true;
      component.likeCount = 5;

      component.toggleLike();

      expect(hubServiceStub.postUnlike).toHaveBeenCalled();
      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(4);
    });

    it("likes and increments the like count when not currently liked", () => {
      hubServiceStub.postLike.mockReturnValue(of(true));
      hubServiceStub.getCounts.mockReturnValue(of([{ counts: { like: 6 } }]));
      createComponent();
      component.did = 5;
      component.currentUid = MOCK_USER.uid;
      component.isLiked = false;
      component.likeCount = 5;

      component.toggleLike();

      expect(hubServiceStub.postLike).toHaveBeenCalled();
      expect(component.isLiked).toBe(true);
      expect(component.likeCount).toBe(6);
    });

    it("does nothing when no user is logged in", () => {
      createComponent();
      component.did = 5;
      component.currentUid = undefined;

      component.toggleLike();

      expect(hubServiceStub.postLike).not.toHaveBeenCalled();
      expect(hubServiceStub.postUnlike).not.toHaveBeenCalled();
    });
  });

  describe("cover image and description persistence", () => {
    it("refreshes the cover url and toasts success after setting a cover image", () => {
      datasetServiceStub.updateDatasetCoverImage.mockReturnValue(of({}));
      datasetServiceStub.getDatasetCoverUrl.mockReturnValue(of({ url: "http://new" }));
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ name: "v1" });

      component.onSetCoverImage("img.png");

      expect(datasetServiceStub.updateDatasetCoverImage).toHaveBeenCalledWith(5, "v1/img.png");
      expect(component.coverImageUrl).toBe("http://new");
      expect(notificationServiceStub.success).toHaveBeenCalledWith("Cover image updated.");
    });

    it("surfaces the backend message when setting the cover image fails", () => {
      datasetServiceStub.updateDatasetCoverImage.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "nope" }, status: 400 }))
      );
      createComponent();
      component.did = 5;
      component.selectedVersion = makeVersion({ name: "v1" });

      component.onSetCoverImage("img.png");

      expect(notificationServiceStub.error).toHaveBeenCalledWith("nope");
    });

    it("does nothing when there is no selected version to attach the cover to", () => {
      createComponent();
      component.did = 5;
      component.selectedVersion = undefined;

      component.onSetCoverImage("img.png");

      expect(datasetServiceStub.updateDatasetCoverImage).not.toHaveBeenCalled();
    });

    it("persists a changed description and updates the field", () => {
      datasetServiceStub.updateDatasetDescription.mockReturnValue(of({}));
      createComponent();
      component.did = 5;
      component.datasetDescription = "old";

      component.onDatasetDescriptionChange("new");

      expect(datasetServiceStub.updateDatasetDescription).toHaveBeenCalledWith(5, "new");
      expect(component.datasetDescription).toBe("new");
    });

    it("skips the persistence call when the description is unchanged", () => {
      createComponent();
      component.did = 5;
      component.datasetDescription = "same";

      component.onDatasetDescriptionChange("same");

      expect(datasetServiceStub.updateDatasetDescription).not.toHaveBeenCalled();
    });

    it("reverts the description and toasts an error when persistence fails", () => {
      datasetServiceStub.updateDatasetDescription.mockReturnValue(throwError(() => new Error("boom")));
      createComponent();
      component.did = 5;
      component.datasetDescription = "old";

      component.onDatasetDescriptionChange("new");

      expect(component.datasetDescription).toBe("old");
      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to update dataset description");
    });
  });

  describe("copyCurrentFilePath", () => {
    let originalClipboardDescriptor: PropertyDescriptor | undefined;
    let writeText: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      // Capture the original own-property descriptor (undefined if navigator has no own
      // `clipboard`, e.g. under jsdom) so afterEach can restore the exact shape.
      originalClipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, "clipboard");
      writeText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
      createComponent();
    });

    afterEach(() => {
      if (originalClipboardDescriptor) {
        Object.defineProperty(navigator, "clipboard", originalClipboardDescriptor);
      } else {
        delete (navigator as any).clipboard;
      }
    });

    it("writes the displayed path to the clipboard and toasts success", async () => {
      component.currentDisplayedFileName = "/a/b/c.txt";

      await component.copyCurrentFilePath();

      expect(writeText).toHaveBeenCalledWith("/a/b/c.txt");
      expect(notificationServiceStub.success).toHaveBeenCalledWith("File path copied to clipboard");
    });

    it("does nothing when no file is displayed", async () => {
      component.currentDisplayedFileName = "";

      await component.copyCurrentFilePath();

      expect(writeText).not.toHaveBeenCalled();
    });

    it("toasts an error when the clipboard write rejects", async () => {
      writeText.mockRejectedValue(new Error("denied"));
      component.currentDisplayedFileName = "/a/b/c.txt";

      await component.copyCurrentFilePath();

      expect(notificationServiceStub.error).toHaveBeenCalledWith("Failed to copy file path");
    });
  });
});
