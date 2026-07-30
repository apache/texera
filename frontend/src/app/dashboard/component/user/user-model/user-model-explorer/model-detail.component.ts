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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, OnInit } from "@angular/core";
import { ActivatedRoute } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { switchMap } from "rxjs/operators";
import { format } from "date-fns";
import { NgIf, NgClass, NgFor } from "@angular/common";
import { NzResizeEvent, NzResizableDirective, NzResizeHandleComponent } from "ng-zorro-antd/resizable";
import { NzCardComponent, NzCardMetaComponent } from "ng-zorro-antd/card";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzPopoverDirective } from "ng-zorro-antd/popover";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { FormsModule } from "@angular/forms";
import { NzLayoutComponent, NzContentComponent, NzSiderComponent } from "ng-zorro-antd/layout";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";

import { ModelService } from "../../../../service/user/model/model.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { ModelVersion } from "../../../../../common/type/model";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { formatSize } from "src/app/common/util/size-formatter.util";
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
    NzCardComponent,
    NzCardMetaComponent,
    NzTooltipDirective,
    NzTagComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzButtonComponent,
    NzPopoverDirective,
    NzSwitchComponent,
    FormsModule,
    MarkdownDescriptionComponent,
    NzLayoutComponent,
    NzContentComponent,
    NzWaveDirective,
    NzEmptyComponent,
    UserDatasetFileRendererComponent,
    NzSiderComponent,
    NzResizableDirective,
    NzResizeHandleComponent,
    NzCollapseComponent,
    NzCollapsePanelComponent,
    NzSelectComponent,
    NzOptionComponent,
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

  // Relative to the version root, e.g. "weights/model.pt" — see loadFileContent.
  public currentDisplayedFileName: string = "";
  public currentFileSize: number | undefined;
  public currentModelVersionSize: number | undefined;

  public isRightBarCollapsed = false;
  public isMaximized = false;

  public versions: ReadonlyArray<ModelVersion> = [];
  public selectedVersion: ModelVersion | undefined;
  public fileTreeNodeList: DatasetFileNode[] = [];
  public selectedVersionCreationTime: string = "";

  public isLogin: boolean = this.userService.isLogin();

  constructor(
    private route: ActivatedRoute,
    private modelService: ModelService,
    private notificationService: NotificationService,
    private downloadService: DownloadService,
    private userService: UserService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
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
          // Route params are strings; mid is interpolated into URLs and compared numerically.
          this.mid = Number(params["mid"]);
          this.retrieveModelInfo();
          this.retrieveModelVersionList();
          return this.route.data;
        }),
        untilDestroyed(this)
      )
      .subscribe();
  }

  retrieveModelInfo(): void {
    if (this.mid) {
      this.modelService
        .getModel(this.mid, this.isLogin)
        .pipe(untilDestroyed(this))
        .subscribe(dashboardModel => {
          const model = dashboardModel.model;
          this.modelName = model.name;
          this.modelDescription = model.description;
          this.userModelAccessLevel = dashboardModel.accessPrivilege;
          this.modelIsPublic = model.isPublic;
          this.modelIsDownloadable = model.isDownloadable;
          this.modelFramework = model.framework;
          this.modelFormat = model.format;
          this.ownerEmail = dashboardModel.ownerEmail;
          this.isOwner = dashboardModel.isOwner;
          if (typeof model.creationTime === "number") {
            const date = new Date(model.creationTime);
            this.modelCreationTime = format(date, "MM/dd/yyyy HH:mm:ss");
            const timeZoneName =
              new Intl.DateTimeFormat("en-US", {
                timeZoneName: "long",
              })
                .format(date)
                .split(", ")
                .pop() || "";
            this.modelCreationTimeTooltip = `${format(date, "zzzz")} (${timeZoneName})`;
          }
        });
    }
  }

  retrieveModelVersionList(): void {
    if (this.mid) {
      this.modelService
        .retrieveModelVersionList(this.mid)
        .pipe(untilDestroyed(this))
        .subscribe(versions => {
          this.versions = versions;
          // The backend orders newest first, so the head is the latest version.
          if (this.versions.length > 0) {
            this.selectedVersion = this.versions[0];
            this.onVersionSelected(this.selectedVersion);
          }
        });
    }
  }

  onVersionSelected(version: ModelVersion): void {
    this.selectedVersion = version;
    if (this.mid && this.selectedVersion.mvid) {
      this.modelService
        .retrieveModelVersionFileTree(this.mid, this.selectedVersion.mvid)
        .pipe(untilDestroyed(this))
        .subscribe(data => {
          this.fileTreeNodeList = data.fileNodes;
          this.currentModelVersionSize = data.size;
          if (typeof version.creationTime === "number") {
            this.selectedVersionCreationTime = format(new Date(version.creationTime), "MM/dd/yyyy HH:mm:ss");
          }
          if (this.fileTreeNodeList.length === 0) {
            this.currentDisplayedFileName = "";
            this.currentFileSize = undefined;
            return;
          }
          let currentNode = this.fileTreeNodeList[0];
          while (currentNode.type === "directory" && currentNode.children && currentNode.children.length > 0) {
            currentNode = currentNode.children[0];
          }
          this.loadFileContent(currentNode);
        });
    }
  }

  onVersionFileTreeNodeSelected(node: DatasetFileNode): void {
    this.loadFileContent(node);
  }

  loadFileContent(node: DatasetFileNode): void {
    this.currentDisplayedFileName = getFullPathFromDatasetFileNode(node);
    this.currentFileSize = node.size;
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
    if (this.mid && this.selectedVersion && this.selectedVersion.mvid) {
      this.downloadService
        .downloadModelVersion(this.mid, this.selectedVersion.mvid, this.modelName, this.selectedVersion.name)
        .pipe(untilDestroyed(this))
        .subscribe();
    }
  }

  onPublicStatusChange(checked: boolean): void {
    if (this.mid) {
      this.modelService
        .updateModelPublicity(this.mid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.modelIsPublic = checked;
            const state = this.modelIsPublic ? "public" : "private";
            this.notificationService.success(`Model ${this.modelName} is now ${state}`);
          },
          error: () => {
            this.notificationService.error("Fail to change the model publicity");
          },
        });
    }
  }

  onDownloadableStatusChange(checked: boolean): void {
    if (this.mid) {
      this.modelService
        .updateModelDownloadable(this.mid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.modelIsDownloadable = checked;
            const state = this.modelIsDownloadable ? "allowed" : "not allowed";
            this.notificationService.success(`Model downloads are now ${state}`);
          },
          error: () => {
            this.notificationService.error("Failed to change the model download permission");
          },
        });
    }
  }

  onModelDescriptionChange(description: string): void {
    const updatedDescription = description ?? "";
    const previousDescription = this.modelDescription;

    if (!this.mid || this.modelDescription === updatedDescription) {
      return;
    }

    this.modelDescription = updatedDescription;

    this.modelService
      .updateModelDescription(this.mid, updatedDescription)
      .pipe(untilDestroyed(this))
      .subscribe({
        error: () => {
          this.modelDescription = previousDescription;
          this.notificationService.error("Failed to update model description");
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

  onClickScaleTheView(): void {
    this.isMaximized = !this.isMaximized;
  }

  onClickHideRightBar(): void {
    this.isRightBarCollapsed = !this.isRightBarCollapsed;
  }

  userHasWriteAccess(): boolean {
    return this.userModelAccessLevel == "WRITE";
  }

  isDownloadAllowed(): boolean {
    if (this.isOwner) {
      return true;
    }
    return this.modelIsDownloadable && (this.modelIsPublic || this.userModelAccessLevel !== "NONE");
  }

  formatSize = formatSize;
}
