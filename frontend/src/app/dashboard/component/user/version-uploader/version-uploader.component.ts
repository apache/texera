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

import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Observable, Subscription } from "rxjs";
import { HttpErrorResponse, HttpStatusCode } from "@angular/common/http";
import { NgFor, NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { CdkFixedSizeVirtualScroll, CdkVirtualForOf, CdkVirtualScrollViewport } from "@angular/cdk/scrolling";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { NzDividerComponent } from "ng-zorro-antd/divider";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzProgressComponent } from "ng-zorro-antd/progress";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";

import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import {
  MultipartUploadProgress,
  MultipartUploadService,
} from "../../../service/user/file-resource/multipart-upload.service";
import {
  DATASET_FILE_RESOURCE_ENDPOINT,
  FileResourceEndpoint,
} from "../../../service/user/file-resource/file-resource-endpoint";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { extractErrorMessage } from "../../../../common/util/error";
import { formatSpeed, formatTime, parseIntOrDefault } from "src/app/common/util/format.util";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { FilesUploaderComponent } from "../files-uploader/files-uploader.component";
import { StagedObjectsListComponent } from "../staged-objects-list/staged-objects-list.component";

export const ABORT_RETRY_MAX_ATTEMPTS = 10;
export const ABORT_RETRY_BACKOFF_BASE_MS = 100;
export const FINISHED_TASK_HIDE_DELAY_MS = 5000;

/**
 * The "Create New Version" panel of a versioned resource's detail page: pick files, watch them
 * upload, review what is staged, and commit it as a version. Resource-agnostic — addressing comes
 * from `endpoint`, and only the version-creation call is passed in, because each resource kind
 * types its own response payload.
 */
@UntilDestroy()
@Component({
  selector: "texera-version-uploader",
  templateUrl: "./version-uploader.component.html",
  styleUrls: ["./version-uploader.component.scss"],
  imports: [
    NgIf,
    NgFor,
    FormsModule,
    NzButtonComponent,
    NzCollapseComponent,
    NzCollapsePanelComponent,
    NzDividerComponent,
    NzIconDirective,
    NzInputDirective,
    NzProgressComponent,
    NzTagComponent,
    NzTooltipDirective,
    ɵNzTransitionPatchDirective,
    CdkVirtualScrollViewport,
    CdkFixedSizeVirtualScroll,
    CdkVirtualForOf,
    FilesUploaderComponent,
    StagedObjectsListComponent,
  ],
})
export class VersionUploaderComponent implements OnInit {
  @Input() resourceId: number | undefined;
  @Input() resourceName: string = "";
  @Input() ownerEmail: string = "";
  /** Which resource family the ids above belong to. */
  @Input() endpoint: FileResourceEndpoint = DATASET_FILE_RESOURCE_ENDPOINT;
  /** Commits the staged files as a new version. */
  @Input() createVersion!: (versionName: string) => Observable<unknown>;

  /** The host reloads its version list off this. */
  @Output() versionCreated = new EventEmitter<void>();
  /**
   * True while any upload is mid-flight. The engine captured the resource name when the upload
   * started, so a rename in that window strands the remaining part/finish calls under the old
   * name — and the abort, which reads the new one, cannot clean them up either.
   */
  @Output() uploadsInFlightChange = new EventEmitter<boolean>();

  userHasPendingChanges: boolean = false;
  pendingChangesCount: number = 0;
  // Staged paths from the last diff response, plus locally staged paths not yet in one: counted
  // together so the Finished header keeps pace with the real-time Pending header.
  private confirmedStagedPaths = new Set<string>();
  private unconfirmedStagedPaths = new Set<string>();

  // Upload tuning, overridden by this resource family's settings in Admin -> Settings.
  chunkSizeMiB: number = 50;
  maxConcurrentChunks: number = 10;
  maxConcurrentFiles: number = 3;
  private uploadSubscriptions = new Map<string, Subscription>();
  uploadTimeMap = new Map<string, number>();

  private activeUploads: number = 0;
  // FIFO queue of uploads waiting for a concurrency slot, keyed by file name.
  private pendingQueue = new Map<string, () => void>();
  private pendingQueueDirty = false;
  private queuedFileNamesSnapshot: string[] = [];

  // Row height must match .pending-file-row in the SCSS.
  readonly PENDING_ROW_HEIGHT_PX = 32;
  readonly PENDING_LIST_MAX_HEIGHT_PX = 160;

  @ViewChild(CdkVirtualScrollViewport) private pendingViewport?: CdkVirtualScrollViewport;

  versionName: string = "";
  isCreatingVersion: boolean = false;

  // One row per in-flight or recently finished upload, keyed by file path.
  uploadTasks: Array<MultipartUploadProgress & { filePath: string }> = [];

  // Coalesced by the staged list, which refetches the diff at most once per window.
  userMakeChanges = new EventEmitter<void>();

  formatTime = formatTime;
  formatSpeed = formatSpeed;

  constructor(
    private multipartUploadService: MultipartUploadService,
    private adminSettingsService: AdminSettingsService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.loadUploadSettings();
  }

  // A missing key or failed fetch keeps the field defaults; NaN here would silently stall the
  // upload queue (`activeUploads < NaN` is always false).
  private loadUploadSettings(): void {
    this.adminSettingsService
      .getPublicSetting(this.endpoint.chunkSizeSettingKey)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: value => (this.chunkSizeMiB = parseIntOrDefault(value, this.chunkSizeMiB)),
        error: () => {},
      });
    this.adminSettingsService
      .getPublicSetting(this.endpoint.maxConcurrentChunksSettingKey)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: value => (this.maxConcurrentChunks = parseIntOrDefault(value, this.maxConcurrentChunks)),
        error: () => {},
      });
    this.adminSettingsService
      .getPublicSetting(this.endpoint.maxConcurrentFilesSettingKey)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: value => (this.maxConcurrentFiles = parseIntOrDefault(value, this.maxConcurrentFiles)),
        error: () => {},
      });
  }

  onNewUploadFilesChanged(files: FileUploadItem[]): void {
    if (!this.resourceId) {
      return;
    }
    files.forEach(file => {
      const continueWithUpload = () => {
        const startUpload = () => {
          this.removeFromPendingQueue(file.name);
          this.uploadTasks.unshift({ filePath: file.name, percentage: 0, status: "initializing" });

          const subscription = this.multipartUploadService
            .multipartUpload(
              this.endpoint,
              this.ownerEmail,
              this.resourceName,
              file.name,
              file.file,
              this.chunkSizeMiB * 1024 * 1024,
              this.maxConcurrentChunks,
              file.restart
            )
            .pipe(untilDestroyed(this))
            .subscribe({
              next: progress => {
                const taskIndex = this.uploadTasks.findIndex(t => t.filePath === file.name);
                if (taskIndex === -1) {
                  return;
                }
                this.uploadTasks[taskIndex] = {
                  ...this.uploadTasks[taskIndex],
                  ...progress,
                  percentage: progress.percentage ?? this.uploadTasks[taskIndex].percentage ?? 0,
                };
                // totalTime may be exactly 0 (resumed upload with no missing parts); a truthiness
                // check would leak the concurrency slot.
                if (progress.status === "finished" && progress.totalTime !== undefined) {
                  const filename = file.name.split("/").pop() || file.name;
                  this.uploadTimeMap.set(filename, progress.totalTime);
                  this.notePathStaged(file.name);
                  this.scheduleHide(taskIndex);
                  this.onUploadComplete();
                }
              },
              error: (res: unknown) => {
                const err = res as HttpErrorResponse;
                if (err?.status === HttpStatusCode.Conflict) {
                  this.notificationService.error(
                    "Upload blocked (409). Another upload is likely in progress for this file (another tab/browser), or the server is finalizing a previous upload. Please retry in a moment."
                  );
                } else {
                  this.notificationService.error("Upload failed. Please retry.");
                }
                const taskIndex = this.uploadTasks.findIndex(t => t.filePath === file.name);
                if (taskIndex !== -1) {
                  this.uploadTasks[taskIndex] = {
                    ...this.uploadTasks[taskIndex],
                    percentage: this.uploadTasks[taskIndex].percentage ?? 0,
                    status: "failed",
                  };
                  this.scheduleHide(taskIndex);
                }
                this.onUploadComplete();
              },
              complete: () => {
                const taskIndex = this.uploadTasks.findIndex(t => t.filePath === file.name);
                if (taskIndex !== -1 && this.uploadTasks[taskIndex].status !== "finished") {
                  this.uploadTasks[taskIndex].status = "finished";
                  this.notePathStaged(file.name);
                  this.scheduleHide(taskIndex);
                  this.onUploadComplete();
                }
              },
            });
          this.uploadSubscriptions.set(file.name, subscription);
        };

        if (this.activeUploads < this.maxConcurrentFiles) {
          this.setActiveUploads(this.activeUploads + 1);
          startUpload();
        } else {
          this.pendingQueue.set(file.name, startUpload);
          this.pendingQueueDirty = true;
        }
      };

      this.cancelExistingUpload(file.name, continueWithUpload);
    });
  }

  cancelExistingUpload(fileName: string, onCanceled?: () => void): void {
    const task = this.uploadTasks.find(t => t.filePath === fileName);
    if (task && (task.status === "uploading" || task.status === "initializing")) {
      this.onClickAbortUploadProgress(task, onCanceled);
      return;
    }
    this.removeFromPendingQueue(fileName);
    onCanceled?.();
  }

  private processNextQueuedUpload(): void {
    if (this.activeUploads >= this.maxConcurrentFiles) {
      return;
    }
    const next = this.pendingQueue.entries().next();
    if (!next.done) {
      const [fileName, startUpload] = next.value;
      this.pendingQueue.delete(fileName);
      this.pendingQueueDirty = true;
      this.setActiveUploads(this.activeUploads + 1);
      startUpload();
    }
  }

  private onUploadComplete(): void {
    this.setActiveUploads(this.activeUploads - 1);
    this.processNextQueuedUpload();
  }

  private setActiveUploads(count: number): void {
    this.activeUploads = count;
    this.uploadsInFlightChange.emit(count > 0);
  }

  private removeFromPendingQueue(fileName: string): void {
    if (this.pendingQueue.delete(fileName)) {
      this.pendingQueueDirty = true;
    }
  }

  // Stable array for the template: rebuilt at most once per queue change so change detection does
  // not allocate a new array per pass (#5586).
  get queuedFileNames(): string[] {
    if (this.pendingQueueDirty) {
      this.queuedFileNamesSnapshot = Array.from(this.pendingQueue.keys());
      this.pendingQueueDirty = false;
    }
    return this.queuedFileNamesSnapshot;
  }

  get queuedCount(): number {
    return this.pendingQueue.size;
  }

  get activeCount(): number {
    return this.activeUploads;
  }

  get pendingListHeightPx(): number {
    return Math.min(this.queuedCount * this.PENDING_ROW_HEIGHT_PX, this.PENDING_LIST_MAX_HEIGHT_PX);
  }

  get hasAnyActivity(): boolean {
    return this.pendingChangesCount > 0 || this.activeCount > 0 || this.queuedCount > 0;
  }

  // The viewport initializes inside the collapsed (display: none) panel and measures height 0; the
  // CDK only re-measures on window resize.
  onPendingPanelActiveChange(active: boolean): void {
    if (active) {
      setTimeout(() => this.pendingViewport?.checkViewportSize());
    }
  }

  // Hide a finished, failed or aborted row after a short delay.
  private scheduleHide(idx: number): void {
    if (idx === -1) {
      return;
    }
    const task = this.uploadTasks[idx];
    this.uploadSubscriptions.delete(task.filePath);
    // Remove by identity, not filePath: a same-named re-upload within the window has its own row,
    // which must survive this timer.
    setTimeout(() => {
      this.uploadTasks = this.uploadTasks.filter(t => t !== task);
    }, FINISHED_TASK_HIDE_DELAY_MS);
  }

  onClickAbortUploadProgress(task: MultipartUploadProgress & { filePath: string }, onAborted?: () => void): void {
    const subscription = this.uploadSubscriptions.get(task.filePath);
    if (subscription) {
      subscription.unsubscribe();
      this.uploadSubscriptions.delete(task.filePath);
    }

    if (task.status === "uploading" || task.status === "initializing") {
      this.onUploadComplete();
    }

    let doneCalled = false;
    const done = () => {
      if (doneCalled) {
        return;
      }
      doneCalled = true;
      onAborted?.();
    };

    const abortWithRetry = (attempt: number) => {
      this.multipartUploadService
        .finalizeMultipartUpload(this.endpoint, this.ownerEmail, this.resourceName, task.filePath, true)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.notificationService.info(`${task.filePath} uploading has been terminated`);
            done();
          },
          error: (res: unknown) => {
            const err = res as HttpErrorResponse;
            // Already gone, treat as done.
            if (err.status === HttpStatusCode.NotFound) {
              done();
              return;
            }
            // Backend is still finalizing/aborting; retry with a tiny backoff.
            if (err.status === HttpStatusCode.Conflict && attempt < ABORT_RETRY_MAX_ATTEMPTS) {
              setTimeout(() => abortWithRetry(attempt + 1), ABORT_RETRY_BACKOFF_BASE_MS * (attempt + 1));
              return;
            }
            done();
          },
        });
    };

    abortWithRetry(0);

    const idx = this.uploadTasks.findIndex(t => t.filePath === task.filePath);
    if (idx !== -1) {
      this.uploadTasks[idx] = { ...this.uploadTasks[idx], status: "aborted" };
      this.scheduleHide(idx);
    }
  }

  getUploadStatus(status: MultipartUploadProgress["status"]): "active" | "exception" | "success" {
    return status === "uploading" || status === "initializing"
      ? "active"
      : status === "aborted" || status === "failed"
        ? "exception"
        : "success";
  }

  trackByTask(_: number, task: MultipartUploadProgress & { filePath: string }): string {
    return task.filePath;
  }

  trackByPendingFile(_: number, fileName: string): string {
    return fileName;
  }

  onStagedObjectsUpdated(stagedObjects: DatasetStagedObject[]): void {
    this.confirmedStagedPaths = new Set(stagedObjects.map(obj => obj.path));
    for (const path of this.confirmedStagedPaths) {
      this.unconfirmedStagedPaths.delete(path);
    }
    this.refreshPendingChanges();
  }

  /**
   * Counts a change staged outside this panel — the host stages a deletion from the file tree —
   * in the Finished header immediately, ahead of the next diff response.
   */
  notePathStaged(path: string): void {
    if (!this.confirmedStagedPaths.has(path)) {
      this.unconfirmedStagedPaths.add(path);
    }
    this.refreshPendingChanges();
    this.userMakeChanges.emit();
  }

  private refreshPendingChanges(): void {
    this.pendingChangesCount = this.confirmedStagedPaths.size + this.unconfirmedStagedPaths.size;
    this.userHasPendingChanges = this.pendingChangesCount > 0;
  }

  onClickCreateVersion(): void {
    if (!this.resourceId || this.isCreatingVersion) {
      return;
    }
    this.isCreatingVersion = true;
    this.createVersion(this.versionName?.trim() || "")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success("Version Created");
          this.isCreatingVersion = false;
          this.versionName = "";
          // A new version consumes all staged changes.
          this.confirmedStagedPaths.clear();
          this.unconfirmedStagedPaths.clear();
          this.refreshPendingChanges();
          this.versionCreated.emit();
          this.userMakeChanges.emit();
        },
        error: (err: unknown) => {
          this.notificationService.error(`Version creation failed: ${extractErrorMessage(err)}`);
          this.isCreatingVersion = false;
        },
      });
  }
}
