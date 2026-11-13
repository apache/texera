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

import { Component, EventEmitter, Input, Output, ViewChild } from "@angular/core";
import { Observable } from "rxjs";
import { NgIf, NgFor } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzResizeEvent, NzResizableDirective, NzResizeHandleComponent } from "ng-zorro-antd/resizable";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzLayoutComponent, NzContentComponent, NzSiderComponent } from "ng-zorro-antd/layout";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { NzDividerComponent } from "ng-zorro-antd/divider";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { EntityType } from "../../../../hub/service/hub.service";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import {
  FileResourceEndpoint,
  DATASET_FILE_RESOURCE_ENDPOINT,
} from "../../../service/user/file-resource/file-resource-endpoint";
import { UserDatasetFileRendererComponent } from "../user-dataset/user-dataset-explorer/user-dataset-file-renderer/user-dataset-file-renderer.component";
import { UserDatasetVersionFiletreeComponent } from "../user-dataset/user-dataset-explorer/user-dataset-version-filetree/user-dataset-version-filetree.component";
import { VersionUploaderComponent } from "../version-uploader/version-uploader.component";

/** The only field the version picker needs from a dataset/model version. */
export interface VersionListEntry {
  readonly name: string;
}

/**
 * Presentational "Versions & Files" tab shared by the dataset and model detail pages: a file
 * header/toolbar, the file renderer, and a resizable sider holding the version picker, file tree,
 * and version uploader. It owns only its local toolbar view state; every server call and the
 * version-selection flow stay in the parent, reached through the inputs and outputs below.
 */
@Component({
  selector: "texera-versions-files-browser",
  templateUrl: "./versions-files-browser.component.html",
  styleUrls: ["./versions-files-browser.component.scss"],
  imports: [
    NgIf,
    NgFor,
    FormsModule,
    NzCardComponent,
    NzTooltipDirective,
    NzIconDirective,
    NzButtonComponent,
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
    NzDividerComponent,
    UserDatasetFileRendererComponent,
    UserDatasetVersionFiletreeComponent,
    VersionUploaderComponent,
  ],
})
export class VersionsFilesBrowserComponent<V extends VersionListEntry = VersionListEntry> {
  // File currently shown in the renderer (resolved by the parent).
  @Input() currentDisplayedFileName: string = "";
  @Input() currentFileSize: number | undefined;
  @Input() currentVersionSize: number | undefined;
  @Input() selectedVersionCreationTime: string = "";

  // Version picker.
  @Input() versions: ReadonlyArray<V> = [];
  @Input() selectedVersion: V | undefined;
  @Output() versionSelected = new EventEmitter<V | undefined>();

  // File tree.
  @Input() fileTreeNodeList: DatasetFileNode[] = [];
  @Output() fileTreeNodeSelected = new EventEmitter<DatasetFileNode>();
  @Output() fileDeleted = new EventEmitter<DatasetFileNode>();
  @Output() setCoverImage = new EventEmitter<string>();

  // File renderer. resourceType defaults to Dataset to match the renderer's own default.
  @Input() resourceId: number | undefined;
  @Input() resourceType: EntityType = EntityType.Dataset;
  @Input() versionId: number | undefined;

  // Version uploader.
  @Input() ownerEmail: string = "";
  @Input() resourceName: string = "";
  @Input() endpoint: FileResourceEndpoint = DATASET_FILE_RESOURCE_ENDPOINT;
  @Input() createVersion!: (versionName: string) => Observable<unknown>;
  @Output() versionCreated = new EventEmitter<void>();
  @Output() uploadsInFlightChange = new EventEmitter<boolean>();

  // Flags and per-resource labels.
  @Input() isLogin: boolean = false;
  @Input() writeAccess: boolean = false;
  @Input() downloadAllowed: boolean = false;
  @Input() emptyMessage: string = "";
  @Input() downloadZipTooltip: string = "";

  // Toolbar actions handled by the parent (per-resource download endpoints).
  @Output() downloadCurrentFile = new EventEmitter<void>();
  @Output() downloadVersionAsZip = new EventEmitter<void>();

  // The parent owns the two toolbar view flags (maximizing also hides the parent page
  // header); the browser reads them and reports toggles back through the two-way bindings.
  @Input() isMaximized = false;
  @Output() isMaximizedChange = new EventEmitter<boolean>();
  @Input() isRightBarCollapsed = false;
  @Output() isRightBarCollapsedChange = new EventEmitter<boolean>();

  // Local sider width state.
  readonly MAX_SIDER_WIDTH = 600;
  readonly MIN_SIDER_WIDTH = 150;
  siderWidth = 400;
  private rafId = -1;

  formatSize = formatSize;

  @ViewChild(VersionUploaderComponent) private versionUploader?: VersionUploaderComponent;

  constructor(private notificationService: NotificationService) {}

  onSideResize({ width }: NzResizeEvent): void {
    cancelAnimationFrame(this.rafId);
    this.rafId = requestAnimationFrame(() => {
      this.siderWidth = width!;
    });
  }

  onClickScaleTheView(): void {
    this.isMaximizedChange.emit(!this.isMaximized);
  }

  onClickHideRightBar(): void {
    this.isRightBarCollapsedChange.emit(!this.isRightBarCollapsed);
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

  /** Lets the parent stage a path on the embedded uploader after it deletes the file. */
  notePathStaged(relativePath: string): void {
    this.versionUploader?.notePathStaged(relativePath);
  }
}
