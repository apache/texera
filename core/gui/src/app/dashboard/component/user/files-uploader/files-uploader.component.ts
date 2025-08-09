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

import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { NgxFileDropEntry } from "ngx-file-drop";
import {
  DatasetVersionFileTreeManager,
  DatasetFileNode,
  getPathsUnderOrEqualDatasetFileNode,
} from "../../../../common/type/datasetVersionFileTree";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-user-files-uploader",
  templateUrl: "./files-uploader.component.html",
  styleUrls: ["./files-uploader.component.scss"],
})
export class FilesUploaderComponent {
  @Input()
  showUploadAlert: boolean = false;

  @Output()
  uploadedFiles = new EventEmitter<FileUploadItem[]>();

  newUploadFileTreeNodes: DatasetFileNode[] = [];

  fileUploadingFinished: boolean = false;
  // four types: "success", "info", "warning" and "error"
  fileUploadBannerType: "error" | "success" | "info" | "warning" = "success";
  fileUploadBannerMessage: string = "";
  singleFileUploadMaxSizeMB: number = 20;

  constructor(
    private notificationService: NotificationService,
    private adminSettingsService: AdminSettingsService
  ) {
    this.adminSettingsService
      .getSetting("single_file_upload_max_size_mb")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.singleFileUploadMaxSizeMB = parseInt(value)));
  }

  hideBanner() {
    this.fileUploadingFinished = false;
  }

  showFileUploadBanner(bannerType: "error" | "success" | "info" | "warning", bannerMessage: string) {
    this.fileUploadingFinished = true;
    this.fileUploadBannerType = bannerType;
    this.fileUploadBannerMessage = bannerMessage;
  }

  public fileDropped(files: NgxFileDropEntry[]) {
    // this promise create the FileEntry from each of the NgxFileDropEntry
    // this filePromises is used to ensure the atomicity of file upload
    const filePromises = files.map(droppedFile => {
      return new Promise<FileUploadItem | null>((resolve, reject) => {
        if (droppedFile.fileEntry.isFile) {
          const fileEntry = droppedFile.fileEntry as FileSystemFileEntry;
          fileEntry.file(file => {
            // Check the file size here
            if (file.size > this.singleFileUploadMaxSizeMB * 1024 * 1024) {
              // If the file is too large, reject the promise
              this.notificationService.error(
                `File ${file.name}'s size exceeds the maximum limit of ${this.singleFileUploadMaxSizeMB}MB.`
              );
              reject(null);
            } else {
              // If the file size is within the limit, proceed
              resolve({
                file: file,
                name: droppedFile.relativePath,
                description: "",
                uploadProgress: 0,
                isUploadingFlag: false,
              });
            }
          }, reject);
        } else {
          resolve(null);
        }
      });
    });

    Promise.allSettled(filePromises)
      .then(results => {
        // once all FileUploadItems are created/some of them are rejected, enter this block
        const successfulUploads = results
          .filter((result): result is PromiseFulfilledResult<FileUploadItem | null> => result.status === "fulfilled")
          .map(result => result.value)
          .filter((item): item is FileUploadItem => item !== null);

        if (successfulUploads.length > 0) {
          // successfulUploads.forEach(fileUploadItem => {
          //   this.addFileToNewUploadsFileTree(fileUploadItem.name, fileUploadItem);
          // });
          const successMessage = `${successfulUploads.length} file${successfulUploads.length > 1 ? "s" : ""} selected successfully!`;
          this.showFileUploadBanner("success", successMessage);
        }

        const failedCount = results.length - successfulUploads.length;
        if (failedCount > 0) {
          const errorMessage = `${failedCount} file${failedCount > 1 ? "s" : ""} failed to be selected.`;
          this.showFileUploadBanner("error", errorMessage);
        }

        this.uploadedFiles.emit(successfulUploads);
      })
      .catch(error => {
        this.showFileUploadBanner("error", `Unexpected error: ${error.message}`);
      });
  }
}
