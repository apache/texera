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

import { Component, EventEmitter, Host, Input, Optional, Output } from "@angular/core";
import { firstValueFrom } from "rxjs";
import { NgxFileDropEntry } from "ngx-file-drop";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { DatasetDetailComponent } from "../user-dataset/user-dataset-explorer/dataset-detail.component";
import { formatSize } from "../../../../common/util/size-formatter.util";
import {
  ConflictingFileModalContentComponent,
  ConflictingFileModalData,
} from "./conflicting-file-modal-content/conflicting-file-modal-content.component";

@UntilDestroy()
@Component({
  selector: "texera-user-files-uploader",
  templateUrl: "./files-uploader.component.html",
  styleUrls: ["./files-uploader.component.scss"],
})
export class FilesUploaderComponent {
  @Input() showUploadAlert: boolean = false;

  @Output() uploadedFiles = new EventEmitter<FileUploadItem[]>();

  newUploadFileTreeNodes: DatasetFileNode[] = [];

  fileUploadingFinished: boolean = false;
  fileUploadBannerType: "error" | "success" | "info" | "warning" = "success";
  fileUploadBannerMessage: string = "";
  singleFileUploadMaxSizeMiB: number = 20;

  constructor(
    private notificationService: NotificationService,
    private adminSettingsService: AdminSettingsService,
    private datasetService: DatasetService,
    @Optional() @Host() private parent: DatasetDetailComponent,
    private modal: NzModalService
  ) {
    this.adminSettingsService
      .getSetting("single_file_upload_max_size_mib")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.singleFileUploadMaxSizeMiB = parseInt(value)));
  }

  private markForceRestart(item: FileUploadItem): void {
    // uploader should call backend init with type=forceRestart when this is set
    item.restart = true;
  }

  private askResumeOrSkip(
    item: FileUploadItem,
    showForAll: boolean
  ): Promise<"resume" | "resumeAll" | "restart" | "restartAll"> {
    return new Promise(resolve => {
      const fileName = item.name.split("/").pop() || item.name;
      const sizeStr = formatSize(item.file.size);

      const ref: NzModalRef = this.modal.create<ConflictingFileModalContentComponent, ConflictingFileModalData>({
        nzTitle: "Conflicting File",
        nzMaskClosable: false,
        nzClosable: false,
        nzContent: ConflictingFileModalContentComponent,
        nzData: {
          fileName,
          path: item.name,
          size: sizeStr,
        },
        nzFooter: [
          ...(showForAll
            ? [
                {
                  label: "Restart For All",
                  onClick: () => {
                    resolve("restartAll");
                    ref.destroy();
                  },
                },
                {
                  label: "Resume For All",
                  onClick: () => {
                    resolve("resumeAll");
                    ref.destroy();
                  },
                },
              ]
            : []),
          {
            label: "Restart",
            onClick: () => {
              resolve("restart");
              ref.destroy();
            },
          },
          {
            label: "Resume",
            type: "primary",
            onClick: () => {
              resolve("resume");
              ref.destroy();
            },
          },
        ],
      });
    });
  }

  private async resolveConflicts(items: FileUploadItem[], activePaths: string[]): Promise<FileUploadItem[]> {
    const active = new Set(activePaths ?? []);
    const isConflict = (p: string) => active.has(p) || active.has(encodeURIComponent(p));

    const showForAll = items.length > 1;

    let mode: "ask" | "resumeAll" | "restartAll" = "ask";
    const out: FileUploadItem[] = [];

    await items.reduce<Promise<void>>(async (chain, item) => {
      await chain;

      if (!isConflict(item.name)) {
        out.push(item);
        return;
      }

      if (mode === "resumeAll") {
        out.push(item);
        return;
      }

      if (mode === "restartAll") {
        this.markForceRestart(item);
        out.push(item);
        return;
      }

      const choice = await this.askResumeOrSkip(item, showForAll);

      if (choice === "resume") out.push(item);

      if (choice === "resumeAll") {
        mode = "resumeAll";
        out.push(item);
      }

      if (choice === "restart") {
        this.markForceRestart(item);
        out.push(item);
      }

      if (choice === "restartAll") {
        mode = "restartAll";
        this.markForceRestart(item);
        out.push(item);
      }
    }, Promise.resolve());

    return out;
  }

  hideBanner(): void {
    this.fileUploadingFinished = false;
  }

  showFileUploadBanner(bannerType: "error" | "success" | "info" | "warning", bannerMessage: string): void {
    this.fileUploadingFinished = true;
    this.fileUploadBannerType = bannerType;
    this.fileUploadBannerMessage = bannerMessage;
  }

  private getOwnerAndName(): { ownerEmail: string; datasetName: string } {
    return {
      ownerEmail: this.parent?.ownerEmail ?? "",
      datasetName: this.parent?.datasetName ?? "",
    };
  }

  public fileDropped(files: NgxFileDropEntry[]): void {
    const filePromises = files.map(droppedFile => {
      return new Promise<FileUploadItem | null>((resolve, reject) => {
        if (droppedFile.fileEntry.isFile) {
          const fileEntry = droppedFile.fileEntry as FileSystemFileEntry;
          fileEntry.file(
            file => {
              if (file.size > this.singleFileUploadMaxSizeMiB * 1024 * 1024) {
                this.notificationService.error(
                  `File ${file.name}'s size exceeds the maximum limit of ${this.singleFileUploadMaxSizeMiB}MiB.`
                );
                reject(null);
                return;
              }

              resolve({
                file,
                name: droppedFile.relativePath,
                description: "",
                uploadProgress: 0,
                isUploadingFlag: false,
                restart: false,
              });
            },
            err => reject(err)
          );
        } else {
          resolve(null);
        }
      });
    });

    Promise.allSettled(filePromises)
      .then(async results => {
        const { ownerEmail, datasetName } = this.getOwnerAndName();

        const activePathsPromise =
          ownerEmail && datasetName
            ? firstValueFrom(this.datasetService.listMultipartUploads(ownerEmail, datasetName)).catch(() => [])
            : [];

        const activePaths = await activePathsPromise;
        const successfulUploads = results
          .filter((r): r is PromiseFulfilledResult<FileUploadItem | null> => r.status === "fulfilled")
          .map(r_1 => r_1.value)
          .filter((item): item is FileUploadItem => item !== null);
        const filteredUploads = await this.resolveConflicts(successfulUploads, activePaths);
        if (filteredUploads.length > 0) {
          const msg = `${filteredUploads.length} file${filteredUploads.length > 1 ? "s" : ""} selected successfully!`;
          this.showFileUploadBanner("success", msg);
        }
        const failedCount = results.length - successfulUploads.length;
        if (failedCount > 0) {
          const errorMessage = `${failedCount} file${failedCount > 1 ? "s" : ""} failed to be selected.`;
          this.showFileUploadBanner("error", errorMessage);
        }
        this.uploadedFiles.emit(filteredUploads);
      })
      .catch(error => {
        this.showFileUploadBanner("error", `Unexpected error: ${error?.message ?? error}`);
      });
  }
}
