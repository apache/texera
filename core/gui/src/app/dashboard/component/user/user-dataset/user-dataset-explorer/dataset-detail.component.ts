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

import { Component, EventEmitter, OnInit, Output } from "@angular/core";
import { ActivatedRoute } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetService, MultipartUploadProgress } from "../../../../service/user/dataset/dataset.service";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import {
  DatasetFileNode,
  getFullPathFromDatasetFileNode,
  getRelativePathFromDatasetFileNode,
} from "../../../../../common/type/datasetVersionFileTree";
import { DatasetVersion } from "../../../../../common/type/dataset";
import { switchMap, throttleTime } from "rxjs/operators";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { UserService } from "../../../../../common/service/user/user.service";
import { isDefined } from "../../../../../common/util/predicate";
import { ActionType, EntityType, HubService, LikedStatus } from "../../../../../hub/service/hub.service";
import { FileUploadItem } from "../../../../type/dashboard-file.interface";
import { DatasetStagedObject } from "../../../../../common/type/dataset-staged-object";
import { NzModalService } from "ng-zorro-antd/modal";
import { UserDatasetVersionCreatorComponent } from "./user-dataset-version-creator/user-dataset-version-creator.component";
import { AdminSettingsService } from "../../../../service/admin/settings/admin-settings.service";
import { HttpErrorResponse } from "@angular/common/http";
import { Subscription } from "rxjs";
import { formatSpeed, formatTime } from "src/app/common/util/format.util";
import { format } from "date-fns";

export const THROTTLE_TIME_MS = 1000;

@UntilDestroy()
@Component({
  templateUrl: "./dataset-detail.component.html",
  styleUrls: ["./dataset-detail.component.scss"],
})
export class DatasetDetailComponent implements OnInit {
  public did: number | undefined;
  public datasetName: string = "";
  public datasetDescription: string = "";
  public datasetCreationTime: string = "";
  public datasetCreationTimeTooltip: string = "";
  public datasetIsPublic: boolean = false;
  public datasetIsDownloadable: boolean = true;
  public userDatasetAccessLevel: "READ" | "WRITE" | "NONE" = "NONE";
  public ownerEmail: string = "";
  public isOwner: boolean = false;

  public currentDisplayedFileName: string = "";
  public currentFileSize: number | undefined;
  public currentDatasetVersionSize: number | undefined;

  public isRightBarCollapsed = false;
  public isMaximized = false;

  public versions: ReadonlyArray<DatasetVersion> = [];
  public selectedVersion: DatasetVersion | undefined;
  public fileTreeNodeList: DatasetFileNode[] = [];
  public selectedVersionCreationTime: string = "";

  public versionCreatorBaseVersion: DatasetVersion | undefined;
  public isLogin: boolean = this.userService.isLogin();

  public isLiked: boolean = false;
  public likeCount: number = 0;
  public currentUid: number | undefined;
  public viewCount: number = 0;
  public displayPreciseViewCount = false;

  userHasPendingChanges: boolean = false;
  pendingChangesCount: number = 0;

  // Uploading setting
  chunkSizeMiB: number = 50;
  maxConcurrentChunks: number = 10;
  private uploadSubscriptions = new Map<string, Subscription>();
  uploadTimeMap = new Map<string, number>();

  // Cap number of concurrent files uploads
  maxConcurrentFiles: number = 3;
  private activeUploads: number = 0;
  private pendingQueue: Array<{ fileName: string; startUpload: () => void }> = [];

  versionName: string = "";
  isCreatingVersion: boolean = false;

  //  List of upload tasks – each task tracked by its filePath
  public uploadTasks: Array<
    MultipartUploadProgress & {
      filePath: string;
    }
  > = [];

  @Output() userMakeChanges = new EventEmitter<void>();

  constructor(
    private route: ActivatedRoute,
    private modalService: NzModalService,
    private datasetService: DatasetService,
    private notificationService: NotificationService,
    private downloadService: DownloadService,
    private userService: UserService,
    private hubService: HubService,
    private adminSettingsService: AdminSettingsService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentUid = this.userService.getCurrentUser()?.uid;
        this.isLogin = this.userService.isLogin();
      });
  }

  // item for control the resizeable sider
  MAX_SIDER_WIDTH = 600;
  MIN_SIDER_WIDTH = 150;
  siderWidth = 400;
  id = -1;
  onSideResize({ width }: NzResizeEvent): void {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.siderWidth = width!;
    });
  }

  ngOnInit(): void {
    this.route.params
      .pipe(
        switchMap(params => {
          this.did = params["did"];
          this.retrieveDatasetInfo();
          this.retrieveDatasetVersionList();
          return this.route.data; // or some other observable
        }),
        untilDestroyed(this)
      )
      .subscribe();

    if (!isDefined(this.did)) {
      return;
    }

    this.hubService
      .getCounts([EntityType.Dataset], [this.did], [ActionType.Like])
      .pipe(untilDestroyed(this))
      .subscribe(counts => {
        this.likeCount = counts[0].counts.like ?? 0;
      });

    this.hubService
      .postView(this.did, this.currentUid ? this.currentUid : 0, EntityType.Dataset)
      .pipe(throttleTime(THROTTLE_TIME_MS))
      .pipe(untilDestroyed(this))
      .subscribe(count => {
        this.viewCount = count;
      });

    if (!isDefined(this.currentUid)) {
      return;
    }

    this.hubService
      .isLiked([this.did], [EntityType.Dataset])
      .pipe(untilDestroyed(this))
      .subscribe((isLiked: LikedStatus[]) => {
        this.isLiked = isLiked.length > 0 ? isLiked[0].isLiked : false;
      });

    this.loadUploadSettings();
  }

  public onClickOpenVersionCreator() {
    if (this.did && !this.isCreatingVersion) {
      this.isCreatingVersion = true;

      this.datasetService
        .createDatasetVersion(this.did, this.versionName?.trim() || "")
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => {
            this.notificationService.success("Version Created");
            this.isCreatingVersion = false;
            this.versionName = "";
            this.retrieveDatasetVersionList();
            this.userMakeChanges.emit();
          },
          error: (res: unknown) => {
            const err = res as HttpErrorResponse;
            this.notificationService.error(`Version creation failed: ${err.error.message}`);
            this.isCreatingVersion = false;
          },
        });
    }
  }

  public onClickDownloadVersionAsZip() {
    if (this.did && this.selectedVersion && this.selectedVersion.dvid) {
      this.downloadService
        .downloadDatasetVersion(this.did, this.selectedVersion.dvid, this.datasetName, this.selectedVersion.name)
        .pipe(untilDestroyed(this))
        .subscribe();
    }
  }

  onPublicStatusChange(checked: boolean): void {
    // Handle the change in dataset public status
    if (this.did) {
      this.datasetService
        .updateDatasetPublicity(this.did)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.datasetIsPublic = checked;
            let state = "public";
            if (!this.datasetIsPublic) {
              state = "private";
            }
            this.notificationService.success(`Dataset ${this.datasetName} is now ${state}`);
          },
          error: (err: unknown) => {
            this.notificationService.error("Fail to change the dataset publicity");
          },
        });
    }
  }

  onDownloadableStatusChange(checked: boolean): void {
    // Handle the change in dataset downloadable status
    if (this.did) {
      this.datasetService
        .updateDatasetDownloadable(this.did)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.datasetIsDownloadable = checked;
            let state = "allowed";
            if (!this.datasetIsDownloadable) {
              state = "not allowed";
            }
            this.notificationService.success(`Dataset downloads are now ${state}`);
          },
          error: (err: unknown) => {
            this.notificationService.error("Failed to change the dataset download permission");
          },
        });
    }
  }

  retrieveDatasetInfo() {
    if (this.did) {
      this.datasetService
        .getDataset(this.did, this.isLogin)
        .pipe(untilDestroyed(this))
        .subscribe(dashboardDataset => {
          const dataset = dashboardDataset.dataset;
          this.datasetName = dataset.name;
          this.datasetDescription = dataset.description;
          this.userDatasetAccessLevel = dashboardDataset.accessPrivilege;
          this.datasetIsPublic = dataset.isPublic;
          this.datasetIsDownloadable = dataset.isDownloadable;
          this.ownerEmail = dashboardDataset.ownerEmail;
          this.isOwner = dashboardDataset.isOwner;
          if (typeof dataset.creationTime === "number") {
            const date = new Date(dataset.creationTime);
            this.datasetCreationTime = format(date, "MM/dd/yyyy HH:mm:ss");
            const timeZoneName =
              new Intl.DateTimeFormat("en-US", {
                timeZoneName: "long",
              })
                .format(date)
                .split(", ")
                .pop() || "";
            this.datasetCreationTimeTooltip = `${format(date, "zzzz")} (${timeZoneName})`;
          }
        });
    }
  }

  retrieveDatasetVersionList() {
    if (this.did) {
      this.datasetService
        .retrieveDatasetVersionList(this.did, this.isLogin)
        .pipe(untilDestroyed(this))
        .subscribe(versionNames => {
          this.versions = versionNames;
          // by default, the selected version is the 1st element in the retrieved list
          // which is guaranteed(by the backend) to be the latest created version.
          if (this.versions.length > 0) {
            this.selectedVersion = this.versions[0];
            this.onVersionSelected(this.selectedVersion);
          }
        });
    }
  }

  loadFileContent(node: DatasetFileNode) {
    this.currentDisplayedFileName = getFullPathFromDatasetFileNode(node);
    this.currentFileSize = node.size;
  }

  onClickDownloadCurrentFile = (): void => {
    if (!this.did || !this.selectedVersion?.dvid) return;
    // For public datasets accessed by non-owners, use public endpoint
    const shouldUsePublicEndpoint = this.datasetIsPublic && !this.isOwner;
    this.downloadService
      .downloadSingleFile(this.currentDisplayedFileName, !shouldUsePublicEndpoint)
      .pipe(untilDestroyed(this))
      .subscribe();
  };

  onClickScaleTheView() {
    this.isMaximized = !this.isMaximized;
  }

  onClickHideRightBar() {
    this.isRightBarCollapsed = !this.isRightBarCollapsed;
  }

  onStagedObjectsUpdated(stagedObjects: DatasetStagedObject[]) {
    this.userHasPendingChanges = stagedObjects.length > 0;
    this.pendingChangesCount = stagedObjects.length;
  }

  onVersionSelected(version: DatasetVersion): void {
    this.selectedVersion = version;
    if (this.did && this.selectedVersion.dvid)
      this.datasetService
        .retrieveDatasetVersionFileTree(this.did, this.selectedVersion.dvid, this.isLogin)
        .pipe(untilDestroyed(this))
        .subscribe(data => {
          this.fileTreeNodeList = data.fileNodes;
          this.currentDatasetVersionSize = data.size;
          if (typeof version.creationTime === "number") {
            const date = new Date(version.creationTime);
            this.selectedVersionCreationTime = format(date, "MM/dd/yyyy HH:mm:ss");
          }
          let currentNode = this.fileTreeNodeList[0];
          while (currentNode.type === "directory" && currentNode.children) {
            currentNode = currentNode.children[0];
          }
          this.loadFileContent(currentNode);
        });
  }

  onVersionFileTreeNodeSelected(node: DatasetFileNode) {
    this.loadFileContent(node);
  }

  userHasWriteAccess(): boolean {
    return this.userDatasetAccessLevel == "WRITE";
  }

  isDownloadAllowed(): boolean {
    // Owners can always download
    if (this.isOwner) {
      return true;
    }
    // Non-owners can download if dataset is downloadable and they have access
    // For public datasets, users have access even if userDatasetAccessLevel is 'NONE'
    // For private datasets, users need explicit access (userDatasetAccessLevel !== 'NONE')
    return this.datasetIsDownloadable && (this.datasetIsPublic || this.userDatasetAccessLevel !== "NONE");
  }

  // Track multiple file by unique key
  trackByTask(_: number, task: MultipartUploadProgress & { filePath: string }): string {
    return task.filePath;
  }

  private loadUploadSettings(): void {
    this.adminSettingsService
      .getSetting("multipart_upload_chunk_size_mib")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.chunkSizeMiB = parseInt(value)));
    this.adminSettingsService
      .getSetting("max_number_of_concurrent_uploading_file_chunks")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.maxConcurrentChunks = parseInt(value)));
    this.adminSettingsService
      .getSetting("max_number_of_concurrent_uploading_file")
      .pipe(untilDestroyed(this))
      .subscribe(value => {
        this.maxConcurrentFiles = parseInt(value);
      });
  }

  onNewUploadFilesChanged(files: FileUploadItem[]) {
    if (this.did) {
      files.forEach(file => {
        // Check if currently uploading
        this.cancelExistingUpload(file.name);

        // Create upload function
        const startUpload = () => {
          this.pendingQueue = this.pendingQueue.filter(item => item.fileName !== file.name);

          // Add an initializing task placeholder to uploadTasks
          this.uploadTasks.unshift({
            filePath: file.name,
            percentage: 0,
            status: "initializing",
            uploadId: "",
            physicalAddress: "",
          });
          // Start multipart upload
          const subscription = this.datasetService
            .multipartUpload(
              this.ownerEmail,
              this.datasetName,
              file.name,
              file.file,
              this.chunkSizeMiB * 1024 * 1024,
              this.maxConcurrentChunks
            )
            .pipe(untilDestroyed(this))
            .subscribe({
              next: progress => {
                // Find the task
                const taskIndex = this.uploadTasks.findIndex(t => t.filePath === file.name);

                if (taskIndex !== -1) {
                  // Update the task with new progress info
                  this.uploadTasks[taskIndex] = {
                    ...this.uploadTasks[taskIndex],
                    ...progress,
                    percentage: progress.percentage ?? this.uploadTasks[taskIndex].percentage ?? 0,
                  };

                  // Auto-hide when upload is truly finished
                  if (progress.status === "finished" && progress.totalTime) {
                    const filename = file.name.split("/").pop() || file.name;
                    this.uploadTimeMap.set(filename, progress.totalTime);
                    this.userMakeChanges.emit();
                    this.scheduleHide(taskIndex);
                    this.onUploadComplete();
                  }
                }
              },
              error: () => {
                // Handle upload error
                const taskIndex = this.uploadTasks.findIndex(t => t.filePath === file.name);

                if (taskIndex !== -1) {
                  this.uploadTasks[taskIndex] = {
                    ...this.uploadTasks[taskIndex],
                    percentage: 100,
                    status: "aborted",
                  };
                  this.scheduleHide(taskIndex);
                }
                this.onUploadComplete();
              },
              complete: () => {
                const taskIndex = this.uploadTasks.findIndex(t => t.filePath === file.name);
                if (taskIndex !== -1 && this.uploadTasks[taskIndex].status !== "finished") {
                  this.uploadTasks[taskIndex].status = "finished";
                  this.userMakeChanges.emit();
                  this.scheduleHide(taskIndex);
                  this.onUploadComplete();
                }
              },
            });
          // Store the subscription for later cleanup
          this.uploadSubscriptions.set(file.name, subscription);
        };

        // Queue management
        if (this.activeUploads < this.maxConcurrentFiles) {
          this.activeUploads++;
          startUpload();
        } else {
          this.pendingQueue.push({ fileName: file.name, startUpload });
        }
      });
    }
  }

  private cancelExistingUpload(fileName: string): void {
    const isUploading = this.uploadTasks.some(
      t => t.filePath === fileName && (t.status === "uploading" || t.status === "initializing")
    );
    this.uploadSubscriptions.get(fileName)?.unsubscribe();
    this.uploadSubscriptions.delete(fileName);
    this.uploadTasks = this.uploadTasks.filter(t => t.filePath !== fileName);

    // Process next in queue if this was active
    if (isUploading) {
      this.onUploadComplete();
    }
    // Remove from pending queue if present
    this.pendingQueue = this.pendingQueue.filter(item => item.fileName !== fileName);
  }

  private processNextQueuedUpload(): void {
    if (this.pendingQueue.length > 0 && this.activeUploads < this.maxConcurrentFiles) {
      const next = this.pendingQueue.shift();
      if (next) {
        this.activeUploads++;
        next.startUpload();
      }
    }
  }

  private onUploadComplete(): void {
    this.activeUploads--;
    this.processNextQueuedUpload();
  }

  get queuedFileNames(): string[] {
    return this.pendingQueue.map(item => item.fileName);
  }

  get queuedCount(): number {
    return this.pendingQueue.length;
  }

  get activeCount(): number {
    return this.activeUploads;
  }

  // Hide a task row after 5s
  private scheduleHide(idx: number) {
    if (idx === -1) {
      return;
    }
    const key = this.uploadTasks[idx].filePath;
    this.uploadSubscriptions.delete(key);
    setTimeout(() => {
      this.uploadTasks = this.uploadTasks.filter(t => t.filePath !== key);
    }, 5000);
  }

  onClickAbortUploadProgress(task: MultipartUploadProgress & { filePath: string }) {
    const subscription = this.uploadSubscriptions.get(task.filePath);
    if (subscription) {
      subscription.unsubscribe();
      this.uploadSubscriptions.delete(task.filePath);
    }

    if (task.status === "uploading" || task.status === "initializing") {
      this.onUploadComplete();
    }

    this.datasetService
      .finalizeMultipartUpload(
        this.ownerEmail,
        this.datasetName,
        task.filePath,
        task.uploadId,
        [],
        task.physicalAddress,
        true // abort flag
      )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.notificationService.info(`${task.filePath} uploading has been terminated`);
      });
    // Remove the aborted task immediately
    this.uploadTasks = this.uploadTasks.filter(t => t.filePath !== task.filePath);
  }

  getUploadStatus(status: "initializing" | "uploading" | "finished" | "aborted"): "active" | "exception" | "success" {
    return status === "uploading" || status === "initializing"
      ? "active"
      : status === "aborted"
        ? "exception"
        : "success";
  }

  onPreviouslyUploadedFileDeleted(node: DatasetFileNode) {
    if (this.did) {
      this.datasetService
        .deleteDatasetFile(this.did, getRelativePathFromDatasetFileNode(node))
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.notificationService.success(
              `File ${node.name} is successfully deleted. You may finalize it or revert it at the "Create Version" panel`
            );
            this.userMakeChanges.emit();
          },
          error: (err: unknown) => {
            this.notificationService.error("Failed to delete the file");
          },
        });
    }
  }

  // alias for formatSize
  formatSize = formatSize;

  formatCount(count: number): string {
    if (count >= 1000) {
      return (count / 1000).toFixed(1) + "k";
    }
    return count.toString();
  }
  formatTime = formatTime;
  formatSpeed = formatSpeed;

  toggleLike(): void {
    const userId = this.currentUid;
    if (!isDefined(userId) || !isDefined(this.did)) {
      return;
    }

    if (this.isLiked) {
      this.hubService
        .postUnlike(this.did, EntityType.Dataset)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = false;
            this.hubService
              .getCounts([EntityType.Dataset], [this.did!], [ActionType.Like])
              .pipe(untilDestroyed(this))
              .subscribe(counts => {
                this.likeCount = counts[0].counts.like ?? 0;
              });
          }
        });
    } else {
      this.hubService
        .postLike(this.did, EntityType.Dataset)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = true;
            this.hubService
              .getCounts([EntityType.Dataset], [this.did!], [ActionType.Like])
              .pipe(untilDestroyed(this))
              .subscribe(counts => {
                this.likeCount = counts[0].counts.like ?? 0;
              });
          }
        });
    }
  }

  changeViewDisplayStyle() {
    this.displayPreciseViewCount = !this.displayPreciseViewCount;
  }
}
