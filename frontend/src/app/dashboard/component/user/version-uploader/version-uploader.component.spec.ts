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
import { HttpErrorResponse, HttpStatusCode } from "@angular/common/http";
import { By } from "@angular/platform-browser";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { concat, Observable, of, Subject, throwError } from "rxjs";
import {
  ABORT_RETRY_BACKOFF_BASE_MS,
  ABORT_RETRY_MAX_ATTEMPTS,
  VersionUploaderComponent,
} from "./version-uploader.component";
import {
  MultipartUploadProgress,
  MultipartUploadService,
} from "../../../service/user/file-resource/multipart-upload.service";
import { StagedFileService } from "../../../service/user/file-resource/staged-file.service";
import {
  DATASET_FILE_RESOURCE_ENDPOINT,
  MODEL_FILE_RESOURCE_ENDPOINT,
} from "../../../service/user/file-resource/file-resource-endpoint";
import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";

describe("VersionUploaderComponent", () => {
  let fixture: ComponentFixture<VersionUploaderComponent>;
  let component: VersionUploaderComponent;
  let uploadSubjects: Subject<MultipartUploadProgress>[];
  let uploadedPaths: string[];
  let multipartUploadSpy: ReturnType<typeof vi.fn>;
  let createVersionSpy: ReturnType<typeof vi.fn>;
  const asCreateVersion = (spy: ReturnType<typeof vi.fn>) => spy as unknown as (name: string) => Observable<unknown>;

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
    multipartUploadSpy = vi.fn((_endpoint: unknown, _ownerEmail: string, _resourceName: string, filePath: string) => {
      const progress = new Subject<MultipartUploadProgress>();
      uploadSubjects.push(progress);
      uploadedPaths.push(filePath);
      return progress.asObservable();
    });
    createVersionSpy = vi.fn(() => of({}));

    TestBed.configureTestingModule({
      imports: [VersionUploaderComponent, ...commonTestImports],
      providers: [
        { provide: NzModalService, useValue: {} },
        {
          provide: MultipartUploadService,
          useValue: {
            multipartUpload: multipartUploadSpy,
            finalizeMultipartUpload: vi.fn(() => of({})),
            listMultipartUploads: vi.fn(() => of([])),
            findExistingUploadFiles: vi.fn(() => of([])),
          },
        },
        { provide: StagedFileService, useValue: { getDiff: vi.fn(() => of([])), resetFileDiff: vi.fn(() => of({})) } },
        { provide: NotificationService, useValue: { success: vi.fn(), error: vi.fn(), info: vi.fn() } },
        // maxConcurrentFiles becomes 3, the cap the queue tests below rely on.
        { provide: AdminSettingsService, useValue: { getPublicSetting: vi.fn(() => of("3")) } },
        ...commonTestProviders,
      ],
    });

    fixture = TestBed.createComponent(VersionUploaderComponent);
    component = fixture.componentInstance;
    component.resourceId = 1;
    component.ownerEmail = "owner@texera.com";
    component.resourceName = "test-dataset";
    component.endpoint = DATASET_FILE_RESOURCE_ENDPOINT;
    component.createVersion = asCreateVersion(createVersionSpy);
    fixture.detectChanges();
  });

  describe("a failed upload", () => {
    const notification = () => TestBed.inject(NotificationService) as unknown as { error: ReturnType<typeof vi.fn> };

    /** Fails the in-flight upload of `name` with the given HTTP status. */
    const failUpload = (index: number, status: number) =>
      uploadSubjects[index].error(new HttpErrorResponse({ status }));

    it("names the 409 conflict so the user knows to retry", () => {
      dropFiles("a.csv");

      failUpload(0, HttpStatusCode.Conflict);

      expect(notification().error).toHaveBeenCalledWith(expect.stringContaining("Upload blocked (409)"));
    });

    it("falls back to a generic message for any other failure", () => {
      dropFiles("a.csv");

      failUpload(0, HttpStatusCode.InternalServerError);

      expect(notification().error).toHaveBeenCalledWith("Upload failed. Please retry.");
    });

    it("marks the task failed and keeps its progress rather than showing it complete", () => {
      dropFiles("a.csv");
      // a partially-uploaded file: the bar must not jump to 100 when it fails
      uploadSubjects[0].next({ filePath: "a.csv", percentage: 42, status: "uploading" });

      failUpload(0, HttpStatusCode.InternalServerError);

      const task = component.uploadTasks.find(t => t.filePath === "a.csv");
      expect(task?.status).toBe("failed");
      expect(task?.percentage).toBe(42);
    });

    it("frees the concurrency slot so a queued upload can start", () => {
      // maxConcurrentFiles is 3, so a fourth file waits for a slot
      dropFiles("a.csv", "b.csv", "c.csv", "d.csv");
      expect(uploadedPaths).toEqual(["a.csv", "b.csv", "c.csv"]);

      failUpload(0, HttpStatusCode.InternalServerError);

      expect(uploadedPaths).toContain("d.csv");
    });

    it("still reports the failure when the task is no longer in the list", () => {
      dropFiles("a.csv");
      component.uploadTasks = []; // the taskIndex === -1 arm

      expect(() => failUpload(0, HttpStatusCode.InternalServerError)).not.toThrow();
      expect(notification().error).toHaveBeenCalled();
    });
  });

  /**
   * A progress event and the five-second hide timer both address a row by its index in
   * `uploadTasks`, and that row can already be gone — dismissed by the user — by the time
   * either arrives, so both lookups have to survive the miss. The completion path also has
   * to pick a key for `uploadTimeMap` out of a name that may carry directories.
   */
  describe("progress bookkeeping", () => {
    beforeEach(() => {
      // The completion path arms a 5s row-hide timer; keep it off the real clock.
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it("ignores progress for a row that is no longer listed", () => {
      dropFiles("a.csv");
      component.uploadTasks = []; // dismissed while a chunk was still in flight

      expect(() => uploadSubjects[0].next({ filePath: "a.csv", percentage: 50, status: "uploading" })).not.toThrow();
      // The late event must not resurrect the row or write a phantom index into the list.
      expect(component.uploadTasks).toEqual([]);
      expect(Object.keys(component.uploadTasks)).toHaveLength(0);
    });

    it("keys the upload time by the last path segment, falling back to the whole name", () => {
      // Taking the segment after the last "/" yields "" for a name that ends in one, and
      // keying the map under "" would collide every such upload onto one entry; the
      // fallback keeps the name the caller gave instead.
      dropFiles("nested/dir/");

      finishUpload(0, "nested/dir/", 7);

      expect(component.uploadTimeMap.get("nested/dir/")).toBe(7);
      expect(component.uploadTimeMap.has("")).toBe(false);

      // The reader of this map (staged-objects-list) looks a row up by
      // `filePath.split("/").pop() || filePath`, so an ordinary nested name has to be
      // keyed by its last segment here or the per-file time silently stops rendering.
      dropFiles("dir/sub/a.csv");

      finishUpload(1, "dir/sub/a.csv", 9);

      expect(component.uploadTimeMap.get("a.csv")).toBe(9);
      expect(component.uploadTimeMap.has("dir/sub/a.csv")).toBe(false);
    });

    it("ignores a hide request for a row that is gone", () => {
      // Every one of scheduleHide's call sites already checks the index, so the -1 arm
      // pins a defensive no-op rather than a reachable scenario: without the guard the
      // lookup would read `filePath` off undefined and throw. The valid-index call that
      // follows keeps a scheduleHide which does nothing at all from passing this test.
      dropFiles("a.csv");
      const before = [...component.uploadTasks];

      expect(() => (component as any).scheduleHide(-1)).not.toThrow();
      expect(component.uploadTasks).toEqual(before);

      (component as any).scheduleHide(0);
      vi.advanceTimersByTime(5000);

      expect(component.uploadTasks).toEqual([]);
    });
  });

  /**
   * Aborting an in-flight upload has to survive the backend still finalizing the previous attempt:
   * the abort call is retried on 409 up to ABORT_RETRY_MAX_ATTEMPTS, a 404 means it is already gone,
   * and the caller's callback must fire exactly once down every one of those paths.
   */
  describe("aborting an upload", () => {
    let finalize: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      vi.useFakeTimers();
      finalize = TestBed.inject(MultipartUploadService).finalizeMultipartUpload as unknown as ReturnType<typeof vi.fn>;
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    /** Starts an upload and reports progress, leaving one task in flight. */
    function inFlight(name = "a.txt") {
      dropFiles(name);
      uploadSubjects[0].next({ filePath: name, percentage: 10, status: "uploading", totalTime: 0 });
      return component.uploadTasks.find(t => t.filePath === name)!;
    }

    const conflict = () => throwError(() => ({ status: 409 }) as any);
    const gone = () => throwError(() => ({ status: 404 }) as any);

    it("marks the task aborted and tells the caller once", () => {
      const task = inFlight();
      const onAborted = vi.fn();

      component.onClickAbortUploadProgress(task as any, onAborted);

      expect(finalize).toHaveBeenCalledWith(
        DATASET_FILE_RESOURCE_ENDPOINT,
        "owner@texera.com",
        "test-dataset",
        "a.txt",
        true
      );
      expect(component.uploadTasks.find(t => t.filePath === "a.txt")!.status).toBe("aborted");
      expect(onAborted).toHaveBeenCalledTimes(1);

      // The aborted row goes on the same five-second hide timer a finished one does, so
      // it clears itself out of the list instead of sitting there for the rest of the session.
      vi.advanceTimersByTime(5000);

      expect(component.uploadTasks.find(t => t.filePath === "a.txt")).toBeUndefined();
    });

    it("stops listening to the upload it aborted", () => {
      const task = inFlight();

      component.onClickAbortUploadProgress(task as any);

      // The progress stream is unsubscribed, so a late event cannot resurrect the task.
      uploadSubjects[0].next({ filePath: "a.txt", percentage: 100, status: "finished", totalTime: 1 });
      expect(component.uploadTasks.find(t => t.filePath === "a.txt")!.status).toBe("aborted");
    });

    it("treats a 404 as already aborted rather than an error", () => {
      finalize.mockReturnValueOnce(gone());
      const task = inFlight();
      const onAborted = vi.fn();

      component.onClickAbortUploadProgress(task as any, onAborted);

      expect(onAborted).toHaveBeenCalledTimes(1);
      expect(finalize).toHaveBeenCalledTimes(1);
    });

    it("retries a 409 after a backoff and finishes once the server catches up", () => {
      // The server is still finalizing the previous attempt; the abort has to wait it out.
      finalize.mockReturnValueOnce(conflict());
      const task = inFlight();
      const onAborted = vi.fn();

      component.onClickAbortUploadProgress(task as any, onAborted);
      expect(onAborted).not.toHaveBeenCalled();

      vi.advanceTimersByTime(ABORT_RETRY_BACKOFF_BASE_MS);

      expect(finalize).toHaveBeenCalledTimes(2);
      expect(onAborted).toHaveBeenCalledTimes(1);
    });

    it("backs off further on each successive conflict", () => {
      finalize.mockReturnValue(conflict());
      const task = inFlight();

      component.onClickAbortUploadProgress(task as any);
      expect(finalize).toHaveBeenCalledTimes(1);

      // First wait is BASE * 1, the second BASE * 2, so BASE alone is not enough for the third call.
      vi.advanceTimersByTime(ABORT_RETRY_BACKOFF_BASE_MS);
      expect(finalize).toHaveBeenCalledTimes(2);

      vi.advanceTimersByTime(ABORT_RETRY_BACKOFF_BASE_MS);
      expect(finalize).toHaveBeenCalledTimes(2);

      vi.advanceTimersByTime(ABORT_RETRY_BACKOFF_BASE_MS);
      expect(finalize).toHaveBeenCalledTimes(3);
    });

    it("gives up after the attempt limit but still reports the abort", () => {
      // Without the bound this would retry forever against a permanently conflicted server.
      finalize.mockReturnValue(conflict());
      const task = inFlight();
      const onAborted = vi.fn();

      component.onClickAbortUploadProgress(task as any, onAborted);
      vi.advanceTimersByTime(ABORT_RETRY_BACKOFF_BASE_MS * ABORT_RETRY_MAX_ATTEMPTS * (ABORT_RETRY_MAX_ATTEMPTS + 1));

      expect(finalize).toHaveBeenCalledTimes(ABORT_RETRY_MAX_ATTEMPTS + 1);
      expect(onAborted).toHaveBeenCalledTimes(1);
    });

    it("reports the abort once even on an error the retry does not cover", () => {
      finalize.mockReturnValueOnce(throwError(() => ({ status: 500 }) as any));
      const task = inFlight();
      const onAborted = vi.fn();

      component.onClickAbortUploadProgress(task as any, onAborted);

      expect(onAborted).toHaveBeenCalledTimes(1);
      expect(finalize).toHaveBeenCalledTimes(1);
    });

    it("frees the concurrency slot so a queued upload can start", () => {
      // Aborting has to release the slot as an ordinary completion would; otherwise the queue
      // stalls behind an upload that is no longer running.
      dropFiles("a.txt", "b.txt", "c.txt", "d.txt");
      expect(uploadedPaths).toEqual(["a.txt", "b.txt", "c.txt"]);
      uploadSubjects[0].next({ filePath: "a.txt", percentage: 10, status: "uploading", totalTime: 0 });
      const task = component.uploadTasks.find(t => t.filePath === "a.txt")!;

      component.onClickAbortUploadProgress(task as any);

      expect(uploadedPaths).toContain("d.txt");
    });

    it("cancelExistingUpload aborts an upload that is still running", () => {
      inFlight("b.txt");
      const onCanceled = vi.fn();

      component.cancelExistingUpload("b.txt", onCanceled);

      expect(finalize).toHaveBeenCalledWith(
        DATASET_FILE_RESOURCE_ENDPOINT,
        "owner@texera.com",
        "test-dataset",
        "b.txt",
        true
      );
      expect(onCanceled).toHaveBeenCalledTimes(1);
    });

    it("frees the slot of an upload aborted before its first part went out", () => {
      // A task sits at "initializing" until the service reports its first progress.
      // Cancelling in that window still has to hand the slot to whatever is queued
      // behind it, or the queue stalls on an upload that never started.
      dropFiles("a.txt", "b.txt", "c.txt", "d.txt");
      expect(uploadedPaths).toEqual(["a.txt", "b.txt", "c.txt"]);
      const initializing = component.uploadTasks.find(t => t.filePath === "a.txt")!;
      expect(initializing.status).toBe("initializing");

      component.onClickAbortUploadProgress(initializing as any);

      expect(uploadedPaths).toContain("d.txt");
      expect(component.activeCount).toBe(3);
    });

    it("does not free a second slot when a finished upload's row is dismissed", () => {
      // The row's button becomes "Close" once the upload is done, and the slot was
      // already released by the completion; releasing it a second time would let a
      // fourth upload run past the concurrency cap.
      dropFiles("a.txt", "b.txt", "c.txt", "d.txt");
      finishUpload(0, "a.txt");
      expect(uploadedPaths).toContain("d.txt");
      const finished = component.uploadTasks.find(t => t.filePath === "a.txt")!;
      expect(finished.status).toBe("finished");

      component.onClickAbortUploadProgress(finished as any);

      expect(component.activeCount).toBe(3);
      const dismissed = component.uploadTasks.find(t => t.filePath === "a.txt")!;
      expect(dismissed.status).toBe("aborted");

      // The row lingers for five seconds after being dismissed, so the same X is
      // still there to be clicked again — and that click must not release either.
      component.onClickAbortUploadProgress(dismissed as any);

      expect(component.activeCount).toBe(3);
    });

    it("does not free a second slot when a failed upload's row is dismissed", () => {
      // The failure handler already released this upload's slot and let the queued
      // fourth file start; dismissing the row it left behind must not release a
      // second slot, or a fifth upload would run past the cap of three.
      dropFiles("a.txt", "b.txt", "c.txt", "d.txt");
      uploadSubjects[0].error(new HttpErrorResponse({ status: 500 }));
      expect(uploadedPaths).toContain("d.txt");
      const failed = component.uploadTasks.find(t => t.filePath === "a.txt")!;
      expect(failed.status).toBe("failed");

      component.onClickAbortUploadProgress(failed as any);

      expect(component.activeCount).toBe(3);
      expect(multipartUploadSpy).toHaveBeenCalledTimes(4);
    });

    it("cancelExistingUpload aborts an upload whose first part has not gone out", () => {
      // Until the service reports a first chunk the task sits at "initializing".
      // A re-drop in that window has to abort that attempt rather than fall
      // through and race a second multipart upload against it for the same path,
      // which is exactly the 409 the upload error handler warns about.
      dropFiles("b.txt");
      expect(component.uploadTasks.find(t => t.filePath === "b.txt")!.status).toBe("initializing");
      const onCanceled = vi.fn();

      component.cancelExistingUpload("b.txt", onCanceled);

      expect(finalize).toHaveBeenCalledWith(
        DATASET_FILE_RESOURCE_ENDPOINT,
        "owner@texera.com",
        "test-dataset",
        "b.txt",
        true
      );
      expect(component.uploadTasks.find(t => t.filePath === "b.txt")!.status).toBe("aborted");
      // The slot goes back to the queue instead of being held by an attempt that
      // is no longer running.
      expect(component.activeCount).toBe(0);
      expect(onCanceled).toHaveBeenCalledTimes(1);
    });

    it("tells the caller once even when the abort call reports more than once", () => {
      // The callback is latched so that it fires exactly once no matter how many of the
      // subscription's handlers reach it. HttpClient itself delivers a single response,
      // so this drives the latch directly: a response followed by a stream failure runs
      // the next handler and then the error handler, and both of them report done.
      finalize.mockReturnValueOnce(
        concat(
          of({}),
          throwError(() => ({ status: 500 }) as any)
        )
      );
      const task = inFlight();
      const onAborted = vi.fn();

      component.onClickAbortUploadProgress(task as any, onAborted);

      expect(onAborted).toHaveBeenCalledTimes(1);
    });

    it("aborts a task whose row was already dropped without resurrecting it", () => {
      const task = inFlight();
      component.uploadTasks = []; // the row was dismissed before the abort was clicked

      component.onClickAbortUploadProgress(task as any);

      expect(finalize).toHaveBeenCalledWith(
        DATASET_FILE_RESOURCE_ENDPOINT,
        "owner@texera.com",
        "test-dataset",
        "a.txt",
        true
      );
      // Writing "aborted" back at a missing index would leave a phantom "-1" property on
      // the array, which neither a throw nor `.length` would reveal.
      expect(component.uploadTasks).toEqual([]);
      expect(Object.keys(component.uploadTasks)).toHaveLength(0);
    });
  });

  const settingsStub = () =>
    TestBed.inject(AdminSettingsService) as unknown as { getPublicSetting: ReturnType<typeof vi.fn> };

  /** Rebuilds the fixture so a per-test settings stub is in place before ngOnInit runs. */
  const rebuild = (seed?: (c: VersionUploaderComponent) => void): void => {
    fixture.destroy();
    fixture = TestBed.createComponent(VersionUploaderComponent);
    component = fixture.componentInstance;
    component.resourceId = 1;
    component.ownerEmail = "owner@texera.com";
    component.resourceName = "test-dataset";
    component.endpoint = DATASET_FILE_RESOURCE_ENDPOINT;
    component.createVersion = asCreateVersion(createVersionSpy);
    seed?.(component);
    fixture.detectChanges();
  };

  /** The panel is the only thing that knows which resource family it is addressing. */
  describe("resource addressing", () => {
    it("passes its endpoint to the upload engine, whichever family it serves", () => {
      component.endpoint = MODEL_FILE_RESOURCE_ENDPOINT;
      component.resourceName = "resnet-50";

      dropFiles("weights/model.pt");

      expect(multipartUploadSpy.mock.calls[0][0]).toBe(MODEL_FILE_RESOURCE_ENDPOINT);
      expect(multipartUploadSpy.mock.calls[0][2]).toBe("resnet-50");
    });

    it("reads its upload tuning from its own family's settings keys", () => {
      const settings = TestBed.inject(AdminSettingsService) as unknown as {
        getPublicSetting: ReturnType<typeof vi.fn>;
      };

      // The last is the file picker's own per-file ceiling, which it reads off the same endpoint.
      expect(settings.getPublicSetting.mock.calls.map(call => call[0])).toEqual([
        DATASET_FILE_RESOURCE_ENDPOINT.chunkSizeSettingKey,
        DATASET_FILE_RESOURCE_ENDPOINT.maxConcurrentChunksSettingKey,
        DATASET_FILE_RESOURCE_ENDPOINT.maxConcurrentFilesSettingKey,
        DATASET_FILE_RESOURCE_ENDPOINT.maxFileSizeSettingKey,
      ]);
      expect(component.maxConcurrentFiles).toBe(3);
    });

    it("keeps the default upload settings when the public settings are missing", () => {
      settingsStub().getPublicSetting.mockReturnValue(of(null));
      rebuild();

      expect(component.chunkSizeMiB).toBe(50);
      expect(component.maxConcurrentChunks).toBe(10);
      expect(component.maxConcurrentFiles).toBe(3);
    });

    it("leaves the chunk size untouched when only that setting fails to load", () => {
      // A distinct value per key, so a setting that lands in the wrong field is visible:
      // 7 chunks and 2 files cannot stand in for one another.
      settingsStub().getPublicSetting.mockImplementation((key: string) =>
        key === DATASET_FILE_RESOURCE_ENDPOINT.chunkSizeSettingKey
          ? throwError(() => new Error("boom"))
          : of(key === DATASET_FILE_RESOURCE_ENDPOINT.maxConcurrentChunksSettingKey ? "7" : "2")
      );
      // A sentinel the class default cannot supply, so "the failed fetch wrote nothing" is
      // distinguishable from "it wrote the default back".
      rebuild(c => (c.chunkSizeMiB = 42));

      expect(component.chunkSizeMiB).toBe(42);
      expect(component.maxConcurrentChunks).toBe(7);
      expect(component.maxConcurrentFiles).toBe(2);
    });

    it("leaves both concurrency limits untouched when their settings fail to load", () => {
      settingsStub().getPublicSetting.mockImplementation((key: string) =>
        key === DATASET_FILE_RESOURCE_ENDPOINT.chunkSizeSettingKey ? of("128") : throwError(() => new Error("boom"))
      );
      rebuild(c => {
        c.maxConcurrentChunks = 41;
        c.maxConcurrentFiles = 40;
      });

      // A failed fetch that wrote anything here — a reset, or a NaN — would stall the queue
      // outright, since `activeUploads < NaN` is never true.
      expect(component.chunkSizeMiB).toBe(128);
      expect(component.maxConcurrentChunks).toBe(41);
      expect(component.maxConcurrentFiles).toBe(40);
    });
  });

  describe("in-flight signalling", () => {
    it("tells the host while an upload is running, so a rename cannot strand it", () => {
      const inFlight: boolean[] = [];
      component.uploadsInFlightChange.subscribe((v: boolean) => inFlight.push(v));

      dropFiles("a.csv");
      expect(inFlight.at(-1)).toBe(true);

      component.onClickAbortUploadProgress(component.uploadTasks[0]);
      expect(inFlight.at(-1)).toBe(false);
    });

    it("stays flagged until the last of several uploads finishes", () => {
      const inFlight: boolean[] = [];
      component.uploadsInFlightChange.subscribe((v: boolean) => inFlight.push(v));

      dropFiles("a.csv", "b.csv");
      finishUpload(0, "a.csv");
      expect(inFlight.at(-1)).toBe(true);

      finishUpload(1, "b.csv");
      expect(inFlight.at(-1)).toBe(false);
    });
  });

  describe("staged changes", () => {
    it("tracks the pending-change count from the diff response", () => {
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

    it("counts a change the host staged elsewhere, such as a file-tree deletion", () => {
      component.notePathStaged("nested/a.txt");

      expect(component.pendingChangesCount).toBe(1);
      expect(component.userHasPendingChanges).toBe(true);
    });
  });

  describe("creating a version", () => {
    it("commits through the host's callback, trimming the name", () => {
      component.versionName = "  v2  ";

      component.onClickCreateVersion();

      expect(createVersionSpy).toHaveBeenCalledWith("v2");
    });

    it("lets the backend name the version when the box is empty", () => {
      component.versionName = "   ";

      component.onClickCreateVersion();

      expect(createVersionSpy).toHaveBeenCalledWith("");
    });

    it("clears the staged state, tells the host to reload, and re-enables the button", () => {
      const versionCreated = vi.fn();
      component.versionCreated.subscribe(versionCreated);
      dropFiles("a.csv");
      finishUpload(0, "a.csv");
      expect(component.pendingChangesCount).toBe(1);

      component.onClickCreateVersion();

      expect(component.versionName).toBe("");
      expect(component.pendingChangesCount).toBe(0);
      expect(component.isCreatingVersion).toBe(false);
      expect(versionCreated).toHaveBeenCalledTimes(1);
    });

    it("keeps the staged changes and re-enables the button when creation is rejected", () => {
      createVersionSpy.mockReturnValue(throwError(() => new HttpErrorResponse({ status: 400 })));
      const versionCreated = vi.fn();
      component.versionCreated.subscribe(versionCreated);
      dropFiles("a.csv");
      finishUpload(0, "a.csv");

      component.onClickCreateVersion();

      expect(component.isCreatingVersion).toBe(false);
      // The user can retry, so what they staged must survive the failure.
      expect(component.pendingChangesCount).toBe(1);
      expect(versionCreated).not.toHaveBeenCalled();
    });

    it("ignores a second submit while one is in flight", () => {
      createVersionSpy.mockReturnValue(new Subject());

      component.onClickCreateVersion();
      component.onClickCreateVersion();

      expect(createVersionSpy).toHaveBeenCalledTimes(1);
    });

    it("commits nothing before the host supplies a resource id", () => {
      component.resourceId = undefined;

      component.onClickCreateVersion();

      expect(createVersionSpy).not.toHaveBeenCalled();
    });
  });

  describe("progress state for the template", () => {
    it("maps the upload status to a progress state", () => {
      expect(component.getUploadStatus("uploading")).toBe("active");
      expect(component.getUploadStatus("initializing")).toBe("active");
      expect(component.getUploadStatus("aborted")).toBe("exception");
      expect(component.getUploadStatus("failed")).toBe("exception");
      expect(component.getUploadStatus("finished")).toBe("success");
    });

    it("tracks a task by its file path", () => {
      const task = { filePath: "owner/data/file.csv" } as unknown as Parameters<typeof component.trackByTask>[1];
      expect(component.trackByTask(0, task)).toBe("owner/data/file.csv");
    });
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

  it("starts no upload at all before the host supplies a resource id", () => {
    // Every multipart call is addressed to one resource, so without an id there is
    // nowhere to upload into: the drop is refused outright rather than leaving
    // rows on the panel for uploads that were never started.
    component.resourceId = undefined;

    dropFiles("f1.txt", "f2.txt");

    expect(multipartUploadSpy).not.toHaveBeenCalled();
    expect(component.uploadTasks).toEqual([]);
    expect(component.activeCount).toBe(0);
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
    component.onClickCreateVersion();

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

    // Flush the viewport's init microtask, then render the rows.
    fixture.detectChanges();
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

  describe("rendered progress", () => {
    /**
     * Puts one task on the panel in the given state and opens it. The panel is gated on the
     * separate activeUploads counter rather than on uploadTasks, and ng-zorro collapses it by
     * default, so both have to be arranged before its body exists.
     */
    function withTask(over: Record<string, unknown>): HTMLElement {
      (component as any).activeUploads = 1;
      component.uploadTasks = [
        {
          filePath: "big.csv",
          percentage: 40,
          status: "uploading",
          uploadSpeed: 1024,
          totalTime: 12,
          estimatedTimeRemaining: 30,
          ...over,
        } as any,
      ];
      fixture.detectChanges();
      const el = fixture.nativeElement as HTMLElement;
      const header = Array.from(el.querySelectorAll<HTMLElement>(".ant-collapse-header")).find(h =>
        (h.textContent || "").includes("Uploading:")
      );
      header!.click();
      fixture.detectChanges();
      return el;
    }

    it("shows no statistics while an upload is still initializing", () => {
      // There is nothing to report yet; showing a 0 B/s row reads as a stalled upload.
      const el = withTask({ status: "initializing" });

      expect(el.querySelector(".upload-stats")).toBeNull();
    });

    it("reports speed and both timings while an upload runs", () => {
      const el = withTask({ status: "uploading" });

      const stats = el.querySelector(".upload-stats")!;
      expect(stats.textContent).toContain("elapsed");
      expect(stats.textContent).toContain("left");
      expect(stats.querySelector(".fixed-width-speed")).not.toBeNull();
    });

    it("replaces the live figures with a total once the upload finishes", () => {
      const el = withTask({ status: "finished" });

      const stats = el.querySelector(".upload-stats")!;
      expect(stats.textContent).toContain("Upload time:");
      expect(stats.textContent).not.toContain("left");
    });

    it("reports a total for an aborted upload too", () => {
      const el = withTask({ status: "aborted" });

      expect(el.querySelector(".upload-stats")!.textContent).toContain("Upload time:");
    });
  });

  describe("rendered panel", () => {
    const host = (): HTMLElement => fixture.nativeElement as HTMLElement;

    const q = <E extends Element>(root: ParentNode, selector: string): E => {
      const el = root.querySelector(selector);
      expect(el, `expected to find "${selector}"`).not.toBeNull();
      return el as unknown as E;
    };

    const text = (el: Element | null | undefined): string => (el?.textContent ?? "").replace(/\s+/g, " ").trim();

    /** ng-zorro collapses every status panel by default, so its body has to be opened first. */
    const openPanel = (header: string): void => {
      const found = Array.from(host().querySelectorAll<HTMLElement>(".ant-collapse-header")).find(h =>
        (h.textContent || "").includes(header)
      );
      expect(found, `expected a collapse panel headed "${header}"`).toBeDefined();
      found!.click();
      fixture.detectChanges();
    };

    describe("upload panel", () => {
      it("starts an upload for a file the uploader hands over", () => {
        const el = host();
        const uploader = fixture.debugElement.query(By.css("texera-user-files-uploader"));
        // Distinct tuning values, so the two cannot stand in for each other below.
        component.chunkSizeMiB = 50;
        component.maxConcurrentChunks = 10;

        uploader.triggerEventHandler("uploadedFiles", [makeFileItem("new.csv")]);
        fixture.detectChanges();

        // Both are plain numbers, so asserting their exact values is the only way to notice
        // them exchanged: 10-byte chunks, or 52 million parallel requests, would look
        // identical to expect.any(Number).
        expect(multipartUploadSpy).toHaveBeenCalledWith(
          DATASET_FILE_RESOURCE_ENDPOINT,
          "owner@texera.com",
          "test-dataset",
          "new.csv",
          expect.anything(),
          50 * 1024 * 1024,
          10,
          false
        );
        expect(text(el)).toContain("Uploading: 1 file(s)");
      });

      /** Renders the given in-flight tasks and expands the "Uploading" panel. */
      const withTasks = (...tasks: Array<Record<string, unknown>>): HTMLElement => {
        component.uploadTasks = tasks.map(t => ({
          percentage: 40,
          status: "uploading",
          uploadSpeed: 1024,
          totalTime: 12,
          estimatedTimeRemaining: 30,
          ...t,
        })) as never;
        (component as unknown as { activeUploads: number }).activeUploads = tasks.length;
        fixture.detectChanges();
        openPanel("Uploading:");
        return host();
      };

      it("aborts the upload whose own row button was clicked", () => {
        withTasks({ filePath: "first.csv" }, { filePath: "second.csv" });

        const rows = fixture.debugElement.queryAll(By.css(".upload-progress-wrapper > div"));
        expect(rows.length).toBe(2);
        // Each row has to name its own task: identifying the row by position alone
        // would not notice every row rendering the first task's name and status.
        expect(rows.map(row => text(row.query(By.css(".progress-header")).nativeElement))).toEqual([
          "uploading: first.csv",
          "uploading: second.csv",
        ]);

        const abort = rows[1].query(By.css(".progress-header button"));
        // A live upload is cancelled, not dismissed; the finished row below says "Close".
        expect(abort.injector.get(NzTooltipDirective).directiveTitle).toBe("Cancel the upload");

        abort.nativeElement.click();
        fixture.detectChanges();

        const finalize = TestBed.inject(MultipartUploadService).finalizeMultipartUpload as unknown as ReturnType<
          typeof vi.fn
        >;
        expect(finalize).toHaveBeenCalledTimes(1);
        expect(finalize).toHaveBeenCalledWith(
          DATASET_FILE_RESOURCE_ENDPOINT,
          "owner@texera.com",
          "test-dataset",
          "second.csv",
          true
        );
      });

      it("reports the elapsed time, the time remaining and the speed in their own slots", () => {
        // Distinguishable timings, so the two spans cannot stand in for each other:
        // showing 90s elapsed on a 12s-old upload is the defect this guards.
        const el = withTasks({
          filePath: "big.csv",
          totalTime: 12,
          estimatedTimeRemaining: 90,
          uploadSpeed: 5 * 1024 * 1024,
        });
        const stats = q<HTMLElement>(el, ".upload-stats");

        expect(Array.from(stats.querySelectorAll(".fixed-width-time")).map(text)).toEqual(["12s", "1m30s left"]);
        expect(text(q<HTMLElement>(stats, ".fixed-width-speed"))).toBe("5.0 MB/s");
      });

      it("floors both live timings at one second while an upload reports none", () => {
        const el = withTasks({ filePath: "big.csv", totalTime: undefined, estimatedTimeRemaining: undefined });

        const times = Array.from(q<HTMLElement>(el, ".upload-stats").querySelectorAll(".fixed-width-time")).map(text);
        expect(times).toEqual(["1s", "1s left"]);
      });

      it("reports the total time of a finished upload", () => {
        const el = withTasks({ filePath: "big.csv", status: "finished", totalTime: 75 });

        expect(text(q<HTMLElement>(el, ".upload-stats"))).toContain("Upload time: 1m15s");
        // A finished row is dismissed rather than cancelled.
        const button = fixture.debugElement.query(By.css(".upload-progress-wrapper > div .progress-header button"));
        expect(button.injector.get(NzTooltipDirective).directiveTitle).toBe("Close");
      });

      it("floors the total of a finished upload that timed nothing", () => {
        const el = withTasks({ filePath: "big.csv", status: "finished", totalTime: undefined });

        expect(text(q<HTMLElement>(el, ".upload-stats"))).toContain("Upload time: 1s");
      });
    });

    describe("version creator", () => {
      /** Renders the creator, which only appears with staged changes to commit. */
      const withPendingChanges = (): HTMLElement => {
        component.userHasPendingChanges = true;
        fixture.detectChanges();
        return host();
      };

      const typeName = (el: HTMLElement, value: string): HTMLInputElement => {
        const input = q<HTMLInputElement>(el, ".version-input");
        input.value = value;
        input.dispatchEvent(new Event("input"));
        fixture.detectChanges();
        return input;
      };

      it("offers the creator only once there is something to commit", () => {
        const el = host();
        expect(el.querySelector(".version-creator")).toBeNull();

        component.userHasPendingChanges = true;
        fixture.detectChanges();

        expect(el.querySelector(".version-creator")).not.toBeNull();
        expect(text(q<HTMLElement>(el, ".create-version-button"))).toBe("Submit");
      });

      it("creates a version named by the creator's own input", () => {
        const el = withPendingChanges();

        typeName(el, "second cut");
        q<HTMLButtonElement>(el, ".create-version-button").click();

        expect(createVersionSpy).toHaveBeenCalledWith("second cut");
      });

      it("submits the version straight from the name field with Enter", () => {
        const el = withPendingChanges();

        typeName(el, "from the keyboard").dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

        expect(createVersionSpy).toHaveBeenCalledWith("from the keyboard");
      });

      it("spins the submit button and locks the name field while a version is being created", async () => {
        const el = withPendingChanges();
        expect(q<HTMLButtonElement>(el, ".create-version-button").classList).not.toContain("ant-btn-loading");
        expect(q<HTMLInputElement>(el, ".version-input").disabled).toBe(false);

        component.isCreatingVersion = true;
        fixture.detectChanges();
        // NgModel routes the input's `disabled` binding through control.disable(),
        // which it defers to a microtask, so the DOM lags the render by one turn.
        await Promise.resolve();
        fixture.detectChanges();

        expect(q<HTMLButtonElement>(el, ".create-version-button").classList).toContain("ant-btn-loading");
        // Renaming a version mid-creation would be applied to nothing, so the
        // field is locked for as long as the request is in flight.
        expect(q<HTMLInputElement>(el, ".version-input").disabled).toBe(true);
      });
    });
  });
});
