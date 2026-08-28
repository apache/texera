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

import { Component, OnInit } from "@angular/core";
import { ActivatedRoute } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { switchMap } from "rxjs/operators";
import { format } from "date-fns";
import { NgIf, NgClass, NgFor } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzResizeEvent, NzResizableDirective, NzResizeHandleComponent } from "ng-zorro-antd/resizable";
import { NzCardComponent, NzCardMetaComponent } from "ng-zorro-antd/card";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzLayoutComponent, NzContentComponent, NzSiderComponent } from "ng-zorro-antd/layout";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";

import { ModelService } from "../../../../service/user/model/model.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { EntityType } from "../../../../../hub/service/hub.service";
import { extractErrorMessage } from "../../../../../common/util/error";
import { formatCount } from "src/app/common/util/format.util";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { ModelVersion } from "../../../../../common/type/model";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { MarkdownDescriptionComponent } from "../../markdown-description/markdown-description.component";
import { UserDatasetFileRendererComponent } from "../../user-dataset/user-dataset-explorer/user-dataset-file-renderer/user-dataset-file-renderer.component";
import { UserDatasetVersionFiletreeComponent } from "../../user-dataset/user-dataset-explorer/user-dataset-version-filetree/user-dataset-version-filetree.component";

@UntilDestroy()
@Component({
  templateUrl: "./model-detail.component.html",
  styleUrls: ["./model-detail.component.scss"],
  imports: [
    NgIf,
    NgFor,
    NgClass,
    FormsModule,
    NzCardComponent,
    NzCardMetaComponent,
    NzTooltipDirective,
    NzTagComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzButtonComponent,
    NzWaveDirective,
    NzLayoutComponent,
    NzContentComponent,
    NzSiderComponent,
    NzResizableDirective,
    NzResizeHandleComponent,
    NzEmptyComponent,
    NzCollapseComponent,
    NzCollapsePanelComponent,
    NzSelectComponent,
    NzOptionComponent,
    NzTabsComponent,
    NzTabComponent,
    MarkdownDescriptionComponent,
    UserDatasetFileRendererComponent,
    UserDatasetVersionFiletreeComponent,
  ],
})
export class ModelDetailComponent implements OnInit {
  public mid: number | undefined;
  public modelName: string = "";
  public modelDescription: string = "";
  public modelCreationTime: string = "";
  public modelCreationTimeTooltip: string = "";
  public modelIsPublic: boolean = false;
  public modelIsDownloadable: boolean = true;
  public modelFramework: string | undefined;
  public modelFormat: string | undefined;
  public userModelAccessLevel: "READ" | "WRITE" | "NONE" = "NONE";
  public ownerEmail: string = "";
  public isOwner: boolean = false;
  public coverImageUrl: string | null = null;

  public versions: ReadonlyArray<ModelVersion> = [];
  public selectedVersion: ModelVersion | undefined;
  public selectedVersionCreationTime: string = "";
  public fileTreeNodeList: DatasetFileNode[] = [];
  public currentModelVersionSize: number | undefined;

  // The Model Card's latest-version facts, read off the head of the version list.
  public latestVersionCreationTime: string = "";
  public latestVersionFileName: string = "";
  public latestVersionSize: number | undefined;

  public currentDisplayedFileName: string = "";
  public currentFileSize: number | undefined;

  // Placeholders until models reach the hub. The hub backend has no model entity type
  // (`hub/EntityType.scala` is Workflow and Dataset only), so nothing can populate these yet.
  public readonly viewCount: number = 0;
  public readonly likeCount: number = 0;

  public isRightBarCollapsed = false;
  public isMaximized = false;

  public isLogin: boolean = this.userService.isLogin();
  public currentUid: number | undefined = this.userService.getCurrentUser()?.uid;

  public readonly modelEntityType = EntityType.Model;

  formatSize = formatSize;
  formatCount = formatCount;

  constructor(
    private route: ActivatedRoute,
    private modelService: ModelService,
    private downloadService: DownloadService,
    private notificationService: NotificationService,
    private userService: UserService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  // Resizable sider holding the version picker and the file tree.
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
          // Route params are strings, and the segment is whatever the URL carried: reject
          // anything that is not a positive integer once, here, rather than at each use.
          const mid = Number(params["mid"]);
          this.mid = Number.isInteger(mid) && mid > 0 ? mid : undefined;
          if (this.mid === undefined) {
            this.notificationService.error("This is not a valid model id");
            return this.route.data;
          }
          this.retrieveModelInfo();
          this.retrieveModelVersionList();
          return this.route.data;
        }),
        untilDestroyed(this)
      )
      .subscribe();
  }

  retrieveModelInfo(): void {
    if (!this.mid) {
      return;
    }
    const mid = this.mid;
    this.modelService
      .getModel(mid, this.isLogin)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: dashboardModel => {
          const model = dashboardModel.model;
          this.modelName = model.name;
          this.modelDescription = model.description;
          this.modelIsPublic = model.isPublic;
          this.modelIsDownloadable = model.isDownloadable;
          this.modelFramework = model.framework;
          this.modelFormat = model.format;
          this.userModelAccessLevel = dashboardModel.accessPrivilege;
          this.ownerEmail = dashboardModel.ownerEmail;
          this.isOwner = dashboardModel.isOwner;
          if (model.coverImage) {
            this.loadCoverImageUrl(mid);
          } else {
            this.coverImageUrl = null;
          }
          if (typeof model.creationTime === "number") {
            const date = new Date(model.creationTime);
            this.modelCreationTime = format(date, "MM/dd/yyyy HH:mm:ss");
            const timeZoneName =
              new Intl.DateTimeFormat("en-US", { timeZoneName: "long" }).format(date).split(", ").pop() || "";
            this.modelCreationTimeTooltip = `${format(date, "zzzz")} (${timeZoneName})`;
          }
        },
        error: (err: unknown) => this.notificationService.error(extractErrorMessage(err)),
      });
  }

  private loadCoverImageUrl(mid: number): void {
    this.modelService
      .getModelCoverUrl(mid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ url }) => (this.coverImageUrl = url),
        error: () => (this.coverImageUrl = null),
      });
  }

  retrieveModelVersionList(): void {
    if (!this.mid) {
      return;
    }
    this.modelService
      .retrieveModelVersionList(this.mid, this.isLogin)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: versions => {
          this.versions = versions;
          if (versions.length === 0) {
            return;
          }
          const latest = versions[0];
          this.latestVersionCreationTime = this.formatCreationTime(latest);
          this.onVersionSelected(latest);
        },
        error: (err: unknown) => this.notificationService.error(extractErrorMessage(err)),
      });
  }

  onVersionSelected(version: ModelVersion | undefined): void {
    this.selectedVersion = version;
    if (!this.mid || !version?.mvid) {
      return;
    }
    this.modelService
      .retrieveModelVersionFileTree(this.mid, version.mvid, this.isLogin)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: data => {
          this.fileTreeNodeList = data.fileNodes;
          this.currentModelVersionSize = data.size;
          this.selectedVersionCreationTime = this.formatCreationTime(version);

          const firstFile = this.getFirstFileNode(this.fileTreeNodeList);
          if (version === this.versions[0]) {
            this.latestVersionFileName = firstFile ? getFullPathFromDatasetFileNode(firstFile) : "";
            this.latestVersionSize = data.size;
          }
          if (!firstFile) {
            this.currentDisplayedFileName = "";
            this.currentFileSize = undefined;
            return;
          }
          this.loadFileContent(firstFile);
        },
        error: (err: unknown) => this.notificationService.error(extractErrorMessage(err)),
      });
  }

  onVersionFileTreeNodeSelected(node: DatasetFileNode): void {
    this.loadFileContent(node);
  }

  loadFileContent(node: DatasetFileNode): void {
    this.currentDisplayedFileName = getFullPathFromDatasetFileNode(node);
    this.currentFileSize = node.size;
  }

  // Walk from the first node into directories until reaching a file.
  private getFirstFileNode(nodes: DatasetFileNode[]): DatasetFileNode | undefined {
    let currentNode: DatasetFileNode | undefined = nodes[0];
    while (currentNode && currentNode.type === "directory" && currentNode.children) {
      currentNode = currentNode.children[0];
    }
    return currentNode;
  }

  private formatCreationTime(version: ModelVersion): string {
    return typeof version.creationTime === "number"
      ? format(new Date(version.creationTime), "MM/dd/yyyy HH:mm:ss")
      : "";
  }

  onClickDownloadCurrentFile = (): void => {
    if (!this.mid || !this.selectedVersion?.mvid) {
      return;
    }
    const shouldUsePublicEndpoint = this.modelIsPublic && !this.isOwner;
    this.downloadService
      .downloadModelSingleFile(this.currentDisplayedFileName, !shouldUsePublicEndpoint)
      .pipe(untilDestroyed(this))
      .subscribe();
  };

  onClickDownloadVersionAsZip(): void {
    if (!this.mid || !this.selectedVersion?.mvid) {
      return;
    }
    this.downloadService
      .downloadModelVersion(this.mid, this.selectedVersion.mvid, this.modelName, this.selectedVersion.name)
      .pipe(untilDestroyed(this))
      .subscribe();
  }

  async copyCurrentFilePath(): Promise<void> {
    if (!this.currentDisplayedFileName) {
      return;
    }
    try {
      await navigator.clipboard.writeText(this.currentDisplayedFileName);
      this.notificationService.success("File path copied to clipboard");
    } catch {
      this.notificationService.error("Failed to copy file path");
    }
  }

  onClickScaleTheView(): void {
    this.isMaximized = !this.isMaximized;
  }

  onClickHideRightBar(): void {
    this.isRightBarCollapsed = !this.isRightBarCollapsed;
  }

  isDownloadAllowed(): boolean {
    if (this.isOwner) {
      return true;
    }
    return this.modelIsDownloadable && (this.modelIsPublic || this.userModelAccessLevel !== "NONE");
  }
}
