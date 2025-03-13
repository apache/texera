import { Component, EventEmitter, OnInit, Output } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetService, MultipartUploadProgress } from "../../../../service/user/dataset/dataset.service";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import {
  DatasetFileNode,
  getFullPathFromDatasetFileNode,
  getPathsUnderOrEqualDatasetFileNode,
  getRelativePathFromDatasetFileNode,
} from "../../../../../common/type/datasetVersionFileTree";
import { DatasetVersion } from "../../../../../common/type/dataset";
import { switchMap, throttleTime } from "rxjs/operators";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { DASHBOARD_USER_DATASET } from "../../../../../app-routing.constant";
import { UserService } from "../../../../../common/service/user/user.service";
import { isDefined } from "../../../../../common/util/predicate";
import { HubService } from "../../../../../hub/service/hub.service";
import { FileUploadItem } from "../../../../type/dashboard-file.interface";
import { file } from "jszip";
import { DatasetStagedObject } from "../../../../../common/type/dataset-staged-object";
import { NzModalService } from "ng-zorro-antd/modal";
import { UserDatasetVersionCreatorComponent } from "./user-dataset-version-creator/user-dataset-version-creator.component";
import { DashboardDataset } from "../../../../type/dashboard-dataset.interface";

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
  public datasetIsPublic: boolean = false;
  public userDatasetAccessLevel: "READ" | "WRITE" | "NONE" = "NONE";

  public currentDisplayedFileName: string = "";
  public currentFileSize: number | undefined;
  public currentDatasetVersionSize: number | undefined;

  public isRightBarCollapsed = false;
  public isMaximized = false;

  public versions: ReadonlyArray<DatasetVersion> = [];
  public selectedVersion: DatasetVersion | undefined;
  public fileTreeNodeList: DatasetFileNode[] = [];

  public versionCreatorBaseVersion: DatasetVersion | undefined;
  public isLogin: boolean = this.userService.isLogin();

  public isLiked: boolean = false;
  public likeCount: number = 0;
  public currentUid: number | undefined;
  public viewCount: number = 0;
  public displayPreciseViewCount = false;

  userHasPendingChanges: boolean = false;
  public uploadProgress: MultipartUploadProgress | null = null;

  @Output() userMakeChanges = new EventEmitter<void>();

  constructor(
    private route: ActivatedRoute,
    private modalService: NzModalService,
    private datasetService: DatasetService,
    private notificationService: NotificationService,
    private downloadService: DownloadService,
    private userService: UserService,
    private hubService: HubService
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
      .getLikeCount(this.did, "dataset")
      .pipe(untilDestroyed(this))
      .subscribe(count => {
        this.likeCount = count;
      });

    this.hubService
      .postView(this.did, this.currentUid ? this.currentUid : 0, "dataset")
      .pipe(throttleTime(THROTTLE_TIME_MS))
      .pipe(untilDestroyed(this))
      .subscribe(count => {
        this.viewCount = count;
      });

    if (!isDefined(this.currentUid)) {
      return;
    }

    this.hubService
      .isLiked(this.did, this.currentUid, "dataset")
      .pipe(untilDestroyed(this))
      .subscribe((isLiked: boolean) => {
        this.isLiked = isLiked;
      });
  }

  public onClickOpenVersionCreator() {
    if (this.did) {
      const modal = this.modalService.create({
        nzTitle: "Create New Dataset Version",
        nzContent: UserDatasetVersionCreatorComponent,
        nzFooter: null,
        nzData: {
          isCreatingVersion: true,
          did: this.did,
        },
        nzBodyStyle: {
          resize: "both",
          overflow: "auto",
          minHeight: "200px",
          minWidth: "550px",
          maxWidth: "90vw",
          maxHeight: "80vh",
        },
        nzWidth: "fit-content",
      });
      modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
        if (result != null) {
          this.retrieveDatasetVersionList();
          this.userMakeChanges.emit();
        }
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
          if (typeof dataset.creationTime === "number") {
            this.datasetCreationTime = new Date(dataset.creationTime).toString();
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

    this.downloadService.downloadSingleFile(this.currentDisplayedFileName).pipe(untilDestroyed(this)).subscribe();
  };

  onClickScaleTheView() {
    this.isMaximized = !this.isMaximized;
  }

  onClickHideRightBar() {
    this.isRightBarCollapsed = !this.isRightBarCollapsed;
  }

  onStagedObjectsUpdated(stagedObjects: DatasetStagedObject[]) {
    this.userHasPendingChanges = stagedObjects.length > 0;
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

  onNewUploadFilesChanged(files: FileUploadItem[]) {
    if (this.did) {
      const did = this.did;
      files.forEach(file => {
        this.datasetService
          .multipartUpload(this.datasetName, file.name, file.file)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: res => {
              this.uploadProgress = res; // Update the progress UI
            },
            error: () => {
              this.uploadProgress = {
                filePath: file.name,
                percentage: 100,
                status: "aborted",
                physicalAddress: "",
                uploadId: "",
              };
              setTimeout(() => (this.uploadProgress = null), 3000); // Auto-hide after 3s
            },
            complete: () => {
              this.uploadProgress = {
                filePath: file.name,
                percentage: 100,
                status: "finished",
                uploadId: "",
                physicalAddress: "",
              };
              this.userMakeChanges.emit();
              setTimeout(() => (this.uploadProgress = null), 3000); // Auto-hide after 3s
            },
          });
      });
    }
  }

  onClickAbortUploadProgress() {
    if (this.uploadProgress) {
      this.datasetService
        .finalizeMultipartUpload(
          this.datasetName,
          this.uploadProgress.filePath,
          this.uploadProgress.uploadId,
          [],
          this.uploadProgress.physicalAddress,
          true
        )
        .pipe(untilDestroyed(this))
        .subscribe(res => {
          this.notificationService.info(`${this.uploadProgress?.filePath} uploading has been terminated`);
        });
    }
    this.uploadProgress = null;
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

  toggleLike(): void {
    const userId = this.currentUid;
    if (!isDefined(userId) || !isDefined(this.did)) {
      return;
    }

    if (this.isLiked) {
      this.hubService
        .postUnlike(this.did, userId, "dataset")
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = false;
            this.hubService
              .getLikeCount(this.did!, "dataset")
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
          }
        });
    } else {
      this.hubService
        .postLike(this.did, userId, "dataset")
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = true;
            this.hubService
              .getLikeCount(this.did!, "dataset")
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
          }
        });
    }
  }

  changeViewDisplayStyle() {
    this.displayPreciseViewCount = !this.displayPreciseViewCount;
  }
}
