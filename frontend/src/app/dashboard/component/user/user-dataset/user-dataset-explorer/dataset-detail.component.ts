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

import { Component, OnInit, ViewChild } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { USER_DATASET } from "../../../../../app-routing.constant";
import { extractErrorMessage } from "../../../../../common/util/error";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetService, validateDatasetName } from "../../../../service/user/dataset/dataset.service";
import { NzResizeEvent, NzResizableDirective, NzResizeHandleComponent } from "ng-zorro-antd/resizable";
import {
  DatasetFileNode,
  getFullPathFromDatasetFileNode,
  getRelativePathFromDatasetFileNode,
} from "../../../../../common/type/datasetVersionFileTree";
import { Contributor, DatasetVersion } from "../../../../../common/type/dataset";
import { switchMap, throttleTime } from "rxjs/operators";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { UserService } from "../../../../../common/service/user/user.service";
import { isDefined } from "../../../../../common/util/predicate";
import { ActionType, EntityType, HubService, LikedStatus } from "../../../../../hub/service/hub.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { HttpErrorResponse } from "@angular/common/http";
import { EMPTY, Observable, Subscription } from "rxjs";
import { formatCount, formatSpeed, formatTime } from "src/app/common/util/format.util";
import { replaceOneImmutable } from "src/app/common/util/array-utils";
import { format } from "date-fns";
import { NgIf, NgClass, NgFor } from "@angular/common";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { UserDatasetContributorEditorComponent } from "./user-dataset-contributor-editor/user-dataset-contributor-editor.component";
import { NzCardComponent, NzCardMetaComponent } from "ng-zorro-antd/card";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { FormsModule } from "@angular/forms";
import { MarkdownDescriptionComponent } from "../../markdown-description/markdown-description.component";
import { NzLayoutComponent, NzContentComponent, NzSiderComponent } from "ng-zorro-antd/layout";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";
import { UserDatasetFileRendererComponent } from "./user-dataset-file-renderer/user-dataset-file-renderer.component";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { UserDatasetVersionFiletreeComponent } from "./user-dataset-version-filetree/user-dataset-version-filetree.component";
import { NzDividerComponent } from "ng-zorro-antd/divider";
import { VersionUploaderComponent } from "../../version-uploader/version-uploader.component";
import { DATASET_FILE_RESOURCE_ENDPOINT } from "../../../../service/user/file-resource/file-resource-endpoint";
import { NzInputDirective } from "ng-zorro-antd/input";

export const THROTTLE_TIME_MS = 1000;

@UntilDestroy()
@Component({
  templateUrl: "./dataset-detail.component.html",
  styleUrls: ["./dataset-detail.component.scss"],
  imports: [
    NgIf,
    NzCardComponent,
    NzCardMetaComponent,
    NzTooltipDirective,
    NzTagComponent,
    NgClass,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzPopconfirmDirective,
    NzSwitchComponent,
    FormsModule,
    MarkdownDescriptionComponent,
    NzLayoutComponent,
    NzContentComponent,
    NzWaveDirective,
    NzEmptyComponent,
    NzTabsComponent,
    NzTabComponent,
    UserDatasetFileRendererComponent,
    NzSiderComponent,
    NzResizableDirective,
    NzResizeHandleComponent,
    NzCollapseComponent,
    NzCollapsePanelComponent,
    NzSelectComponent,
    NgFor,
    NzOptionComponent,
    UserDatasetVersionFiletreeComponent,
    NzDividerComponent,
    VersionUploaderComponent,
    NzInputDirective,
    NzDropdownDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NzMenuItemComponent,
  ],
})
export class DatasetDetailComponent implements OnInit {
  public did: number | undefined;
  public datasetName: string = "";
  public editedDatasetName: string = "";
  public datasetDescription: string = "";
  public datasetCreationTime: string = "";
  public datasetCreationTimeTooltip: string = "";
  public datasetIsPublic: boolean = false;
  public coverImageUrl: string | null = null;
  public datasetIsDownloadable: boolean = true;
  public userDatasetAccessLevel: "READ" | "WRITE" | "NONE" = "NONE";
  public ownerEmail: string = "";
  public isOwner: boolean = false;
  public datasetContributors: ReadonlyArray<Contributor> = [];

  public currentDisplayedFileName: string = "";
  public currentFileSize: number | undefined;
  public currentDatasetVersionSize: number | undefined;

  public isRightBarCollapsed = false;
  public isMaximized = false;

  public versions: ReadonlyArray<DatasetVersion> = [];
  public selectedVersion: DatasetVersion | undefined;
  public fileTreeNodeList: DatasetFileNode[] = [];
  public selectedVersionCreationTime: string = "";
  // The following three fields describe the latest version for the Data Card, all
  // sourced from the single retrieveDatasetLatestVersion response so they stay
  // mutually consistent and independent of the version selected in Versions & Files.
  public latestVersionCreationTime: string = "";
  public latestVersionFileName: string = "";
  public latestVersionSize: number | undefined;
  // Holds the in-flight latest-version fetch so a later call can supersede it.
  private latestVersionFileSubscription: Subscription | undefined;

  public versionCreatorBaseVersion: DatasetVersion | undefined;
  public isLogin: boolean = this.userService.isLogin();

  public isLiked: boolean = false;
  public likeCount: number = 0;
  public currentUid: number | undefined;
  public viewCount: number = 0;
  public displayPreciseViewCount = false;

  readonly datasetEndpoint = DATASET_FILE_RESOURCE_ENDPOINT;

  @ViewChild(VersionUploaderComponent) private versionUploader?: VersionUploaderComponent;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
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
          this.retrieveLatestVersionFile();
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
  }

  /** Commits the staged files; the panel owns the rest of the version flow. */
  createDatasetVersion = (versionName: string): Observable<unknown> =>
    this.datasetService.createDatasetVersion(this.did!, versionName);

  onVersionCreated(): void {
    this.retrieveDatasetVersionList();
    this.retrieveLatestVersionFile();
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
      const did = this.did;
      this.datasetService
        .getDataset(did, this.isLogin)
        .pipe(untilDestroyed(this))
        .subscribe(dashboardDataset => {
          const dataset = dashboardDataset.dataset;
          this.datasetName = dataset.name;
          this.editedDatasetName = dataset.name;
          this.datasetDescription = dataset.description;
          this.userDatasetAccessLevel = dashboardDataset.accessPrivilege;
          this.datasetIsPublic = dataset.isPublic;
          this.datasetIsDownloadable = dataset.isDownloadable;
          this.ownerEmail = dashboardDataset.ownerEmail;
          this.isOwner = dashboardDataset.isOwner;
          if (dataset.coverImage) {
            this.datasetService
              .getDatasetCoverUrl(did)
              .pipe(untilDestroyed(this))
              .subscribe({
                next: ({ url }) => (this.coverImageUrl = url),
                error: () => (this.coverImageUrl = null),
              });
          } else {
            this.coverImageUrl = null;
          }
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
          this.datasetContributors = dashboardDataset.contributors || [];
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

  // Fetches the latest version independently of the current selection and derives
  // the Data Card's latest-version facts from that single response: the file name
  // and created date directly, and the total size via a follow-up file-tree fetch
  // for the latest version's dvid (mirroring onVersionSelected's size lookup).
  retrieveLatestVersionFile() {
    if (this.did) {
      const did = this.did;
      // Both fetches live in one subscription (chained with switchMap rather than
      // nested subscribes) so dropping it cancels whichever is still in flight:
      // a call started here supersedes any earlier one, and a slow response from
      // the superseded call can no longer overwrite fresher facts out of order.
      this.latestVersionFileSubscription?.unsubscribe();
      this.latestVersionFileSubscription = this.datasetService
        .retrieveDatasetLatestVersion(did)
        .pipe(
          switchMap(version => {
            const firstFile = this.getFirstFileNode(version.fileNodes ?? []);
            this.latestVersionFileName = firstFile ? getFullPathFromDatasetFileNode(firstFile) : "";
            this.latestVersionCreationTime =
              typeof version.creationTime === "number"
                ? format(new Date(version.creationTime), "MM/dd/yyyy HH:mm:ss")
                : "";
            if (!version.dvid) {
              // Nothing to size: clear rather than keep a previous call's size.
              this.latestVersionSize = undefined;
              return EMPTY;
            }
            return this.datasetService.retrieveDatasetVersionFileTree(did, version.dvid, this.isLogin);
          }),
          untilDestroyed(this)
        )
        .subscribe(data => {
          this.latestVersionSize = data.size;
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
          const currentNode = this.getFirstFileNode(this.fileTreeNodeList);
          if (currentNode) {
            this.loadFileContent(currentNode);
          }
        });
  }

  // Walk from the first node into directories until reaching a file, returning a
  // representative leaf file node (or undefined if the tree has no files).
  private getFirstFileNode(nodes: DatasetFileNode[]): DatasetFileNode | undefined {
    let currentNode: DatasetFileNode | undefined = nodes[0];
    while (currentNode && currentNode.type === "directory" && currentNode.children) {
      currentNode = currentNode.children[0];
    }
    return currentNode;
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

  onPreviouslyUploadedFileDeleted(node: DatasetFileNode) {
    if (this.did) {
      const relativePath = getRelativePathFromDatasetFileNode(node);
      this.datasetService
        .deleteDatasetFile(this.did, relativePath)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.notificationService.success(
              `File ${node.name} is successfully deleted. You may finalize it or revert it at the "Create Version" panel`
            );
            this.versionUploader?.notePathStaged(relativePath);
          },
          error: (err: unknown) => {
            this.notificationService.error("Failed to delete the file");
          },
        });
    }
  }

  // alias for formatSize
  formatSize = formatSize;

  formatCount = formatCount;
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

  onSetCoverImage(filePath: string): void {
    if (!this.did || !this.selectedVersion) {
      return;
    }
    const did = this.did;

    const newCoverPath = `${this.selectedVersion.name}/${filePath}`;
    this.datasetService
      .updateDatasetCoverImage(did, newCoverPath)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.datasetService
            .getDatasetCoverUrl(did)
            .pipe(untilDestroyed(this))
            .subscribe({
              next: ({ url }) => (this.coverImageUrl = url),
              error: () => (this.coverImageUrl = null),
            });
          this.notificationService.success("Cover image updated.");
        },
        error: (err: unknown) => {
          this.notificationService.error(
            err instanceof HttpErrorResponse
              ? err.error?.message || "Failed to set cover image"
              : "Failed to set cover image"
          );
        },
      });
  }

  onDatasetDescriptionChange(description: string): void {
    const updatedDescription = description ?? "";
    const previousDescription = this.datasetDescription;

    if (!this.did || this.datasetDescription === updatedDescription) {
      return;
    }

    this.datasetDescription = updatedDescription;

    this.datasetService
      .updateDatasetDescription(this.did, updatedDescription)
      .pipe(untilDestroyed(this))
      .subscribe({
        error: () => {
          this.datasetDescription = previousDescription;
          this.notificationService.error("Failed to update dataset description");
        },
      });
  }

  onSaveDatasetName(): void {
    if (!this.did) {
      return;
    }
    // Reject invalid names outright instead of silently rewriting them, matching
    // the shared validation used by the other rename entry points (PR #6426).
    const name = this.editedDatasetName;
    const nameError = validateDatasetName(name);
    if (nameError) {
      this.notificationService.error(nameError);
      return;
    }

    this.datasetService
      .updateDatasetName(this.did, name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.datasetName = name;
          this.editedDatasetName = name;
          this.notificationService.success(`Dataset name updated to '${name}'`);
        },
        error: (err: unknown) => {
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  onDeleteDataset(): void {
    if (!this.did) {
      return;
    }
    this.datasetService
      .deleteDatasets(this.did)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success(`Dataset ${this.datasetName} was deleted`);
          this.router.navigate([USER_DATASET]);
        },
        error: (err: unknown) => {
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  async copyCurrentFilePath(): Promise<void> {
    if (!this.currentDisplayedFileName) {
      return;
    }

    try {
      await navigator.clipboard.writeText(this.currentDisplayedFileName);
      this.notificationService.success("File path copied to clipboard");
    } catch (error) {
      this.notificationService.error("Failed to copy file path");
    }
  }

  onAddContributor(): void {
    this.openContributorEditor("Add Contributor", null, newContributor => [
      ...this.datasetContributors,
      newContributor,
    ]);
  }

  onEditContributor(contributor: Contributor): void {
    this.openContributorEditor("Edit Contributor", contributor, updated =>
      replaceOneImmutable(this.datasetContributors, c => c === contributor, updated)
    );
  }

  onDeleteContributor(contributor: Contributor): void {
    this.saveContributors(this.datasetContributors.filter(c => c !== contributor));
  }

  private openContributorEditor(
    title: string,
    data: Contributor | null,
    apply: (result: Contributor) => ReadonlyArray<Contributor>
  ): void {
    const modal = this.modalService.create({
      nzTitle: title,
      nzContent: UserDatasetContributorEditorComponent,
      nzFooter: null,
      nzData: data,
    });
    modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
      if (result) {
        this.saveContributors(apply(result));
      }
    });
  }

  private saveContributors(next: ReadonlyArray<Contributor>): void {
    if (!this.did) {
      return;
    }
    const previous = this.datasetContributors;
    this.datasetContributors = next;
    this.datasetService
      .updateDatasetContributors(this.did, next)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Contributors updated"),
        error: () => {
          this.datasetContributors = previous;
          this.notificationService.error("Failed to update contributors");
        },
      });
  }
}
