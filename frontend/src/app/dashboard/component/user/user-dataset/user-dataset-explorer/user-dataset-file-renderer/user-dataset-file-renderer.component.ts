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

import { Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from "@angular/core";
import { ResourceRegistryService } from "../../../../../service/user/resource-registry/resource-registry.service";
import { EntityType } from "../../../../../../hub/service/hub.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Subscription } from "rxjs";
import * as Papa from "papaparse";
import { DomSanitizer, SafeUrl } from "@angular/platform-browser";
import readXlsxFile from "read-excel-file";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { NgStyle, NgIf, NgFor } from "@angular/common";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import {
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzTbodyComponent,
} from "ng-zorro-antd/table";
import { MarkdownComponent } from "ngx-markdown";
import { NgxJsonViewerModule } from "ngx-json-viewer";

export const MIME_TYPES = {
  JPEG: "image/jpeg",
  JPG: "image/jpeg",
  PNG: "image/png",
  WEBP: "image/webp",
  GIF: "image/gif",
  CSV: "text/csv",
  TXT: "text/plain",
  MD: "text/markdown",
  JSON: "application/json",
  MSEXCEL: "application/vnd.ms-excel",
  XLSX: "application/vnd.ms-excel",
  MP4: "video/mp4",
  MP3: "audio/mpeg",
  OCTET_STREAM: "application/octet-stream", // Default binary format
};

export function getMimeType(filename: string): string {
  const extension = filename.split(".").pop()?.toUpperCase();
  return extension && MIME_TYPES[extension as keyof typeof MIME_TYPES]
    ? MIME_TYPES[extension as keyof typeof MIME_TYPES]
    : MIME_TYPES.OCTET_STREAM;
}

// the size limits for all preview-supported types
export const MIME_TYPE_SIZE_LIMITS_MB = {
  [MIME_TYPES.JPEG]: 5 * 1024 * 1024, // 5 MB
  [MIME_TYPES.PNG]: 5 * 1024 * 1024, // 5 MB
  [MIME_TYPES.WEBP]: 5 * 1024 * 1024, // 5 MB
  [MIME_TYPES.GIF]: 10 * 1024 * 1024, // 10 MB
  [MIME_TYPES.CSV]: 2 * 1024 * 1024, // 2 MB for text-based data files
  [MIME_TYPES.TXT]: 1 * 1024 * 1024, // 1 MB for plain text files
  [MIME_TYPES.MD]: 1 * 1024 * 1024, // 1 MB for MD files
  [MIME_TYPES.JSON]: 1 * 1024 * 1024, // 1 MB for JSON files
  [MIME_TYPES.MSEXCEL]: 10 * 1024 * 1024, // 10 MB for Excel spreadsheets
  [MIME_TYPES.MP4]: 50 * 1024 * 1024, // 50 MB for MP4 videos
  [MIME_TYPES.MP3]: 10 * 1024 * 1024, // 10 MB for MP3 audio files
};

const SAFE_URL_VIEWERS: Partial<Record<string, "displayImage" | "displayMP4" | "displayMP3">> = {
  [MIME_TYPES.PNG]: "displayImage",
  [MIME_TYPES.JPEG]: "displayImage",
  [MIME_TYPES.WEBP]: "displayImage",
  [MIME_TYPES.GIF]: "displayImage",
  [MIME_TYPES.MP4]: "displayMP4",
  [MIME_TYPES.MP3]: "displayMP3",
};

const TEXT_VIEWERS: Partial<Record<string, "displayMarkdown" | "displayJson" | "displayPlainText">> = {
  [MIME_TYPES.MD]: "displayMarkdown",
  [MIME_TYPES.JSON]: "displayJson",
  [MIME_TYPES.TXT]: "displayPlainText",
};

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-file-renderer",
  templateUrl: "./user-dataset-file-renderer.component.html",
  styleUrls: ["./user-dataset-file-renderer.component.scss"],
  imports: [
    NgStyle,
    NgIf,
    NzSpinComponent,
    NzAlertComponent,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NgFor,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzTbodyComponent,
    MarkdownComponent,
    NgxJsonViewerModule,
  ],
})
export class UserDatasetFileRendererComponent implements OnInit, OnChanges, OnDestroy {
  public fileURL: string | undefined;
  // safe url is used to display some formats including image
  public safeFileURL: SafeUrl | undefined;

  // table related control
  public displayCSV: boolean = false;
  public displayXlsx: boolean = false;
  public tableDataHeader: any[] = [];
  public tableContent: any[][] = [];

  // image related control
  public displayImage: boolean = false;

  // markdown control
  public displayMarkdown: boolean = false;

  // json control
  public displayJson: boolean = false;

  // video
  public displayMP4: boolean = false;

  // audio
  public displayMP3: boolean = false;

  // plain text & octet stream related control
  public displayPlainText: boolean = false;
  public textContent: string = "";

  // control flags
  public isLoading: boolean = false;
  private fileLoad?: Subscription;
  private loadId = 0;
  public isFileSizeUnloadable = false;
  public isFileLoadingError: boolean = false;
  public isFileTypePreviewUnsupported: boolean = false;

  public currentFile: File | undefined = undefined;
  @Input()
  isMaximized: boolean = false;

  // Which kind of resource the file belongs to; the registry turns it into the fetch.
  @Input()
  resourceType: EntityType = EntityType.Dataset;

  @Input()
  resourceId: number | undefined;

  @Input()
  versionId: number | undefined;

  @Input()
  filePath: string = "";

  @Input()
  fileSize?: number;

  @Input()
  isLogin: boolean = false;

  constructor(
    private resourceRegistry: ResourceRegistryService,
    private sanitizer: DomSanitizer,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.reloadFileContent();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if ((changes.resourceId && changes.versionId) || changes.filePath) {
      this.reloadFileContent();
    }
  }

  ngOnDestroy(): void {
    if (this.fileURL) {
      URL.revokeObjectURL(this.fileURL);
    }
  }

  showImageModal = false;

  toggleImageModal() {
    this.showImageModal = !this.showImageModal;
  }

  reloadFileContent() {
    this.fileLoad?.unsubscribe();
    // Parsing finishes after the download, so its callbacks check they still belong to the shown file.
    const loadId = ++this.loadId;
    const isCurrent = () => loadId === this.loadId;
    this.turnOffAllDisplay();

    // Pre-check - file size
    if (this.rejectInvalidPreview(getMimeType(this.filePath), this.fileSize)) {
      return;
    }

    // Load file
    this.isLoading = true;
    const retrieveSingleFile = this.resourceRegistry.get(this.resourceType).retrieveSingleFile;
    if (retrieveSingleFile && this.resourceId && this.versionId && this.filePath != "") {
      this.fileLoad = retrieveSingleFile(this.filePath, this.isLogin)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: blob => {
            this.isLoading = false;
            const blobMimeType = getMimeType(this.filePath);
            if (this.rejectInvalidPreview(blobMimeType, blob.size, true)) {
              return;
            }
            this.currentFile = new File([blob], this.filePath, { type: blob.type });
            this.activateViewer(blobMimeType, blob, this.currentFile, isCurrent);
          },
          error: () => this.onFileLoadingError(),
        });
    }
  }

  private rejectInvalidPreview(mimeType: string, size?: number, notifyOversize = false): boolean {
    if (!this.isPreviewSupported(mimeType)) {
      this.onFileTypePreviewUnsupported();
      return true;
    }
    const maxSize = MIME_TYPE_SIZE_LIMITS_MB[mimeType];
    if (size != null && size > maxSize) {
      this.onFileSizeNotLoadable();
      if (notifyOversize) {
        this.notificationService.warning(`File ${this.filePath} is too large to be previewed`);
      }
      return true;
    }
    return false;
  }

  private activateViewer(blobMimeType: string, blob: Blob, file: File, isCurrent: () => boolean): void {
    const mediaFlag = SAFE_URL_VIEWERS[blobMimeType];
    if (mediaFlag) {
      this[mediaFlag] = true;
      this.loadSafeURL(blob);
      return;
    }
    if (blobMimeType === MIME_TYPES.MSEXCEL) {
      readXlsxFile(blob).then(rows => {
        if (!isCurrent()) return;
        const parsedData = rows.map(row => row.map(cell => (cell ? cell.toString() : "")));
        if (parsedData.length > 0) {
          this.loadTabularFile(parsedData);
          this.displayXlsx = true;
        }
      });
      return;
    }
    if (blobMimeType === MIME_TYPES.CSV) {
      this.displayCSV = true;
      Papa.parse(file, {
        complete: (results: Papa.ParseResult<any>) => {
          if (isCurrent() && results.data.length > 0) {
            this.loadTabularFile(results.data);
          }
        },
        error: error => {
          if (!isCurrent()) return;
          console.error("Error parsing file:", error);
          this.onFileLoadingError();
        },
      });
      return;
    }
    this[TEXT_VIEWERS[blobMimeType] ?? "displayPlainText"] = true;
    this.readFileAsText(blob, isCurrent);
  }

  turnOffAllDisplay() {
    this.displayCSV = false;
    this.displayXlsx = false;
    this.displayImage = false;
    this.displayPlainText = false;
    this.displayMarkdown = false;
    this.displayJson = false;
    this.displayMP4 = false;
    this.displayMP3 = false;
    this.isLoading = false;
    this.isFileLoadingError = false;
    this.isFileSizeUnloadable = false;
    this.isFileTypePreviewUnsupported = false;
    // garbage collection
    if (this.fileURL) {
      URL.revokeObjectURL(this.fileURL);
    }
    if (this.safeFileURL) {
      URL.revokeObjectURL(this.safeFileURL.toString());
    }
  }

  onFileLoadingError() {
    this.turnOffAllDisplay();
    this.isFileLoadingError = true;
  }

  onFileSizeNotLoadable() {
    this.turnOffAllDisplay();
    this.isFileSizeUnloadable = true;
  }

  onFileTypePreviewUnsupported() {
    this.turnOffAllDisplay();
    this.isFileTypePreviewUnsupported = true;
  }

  isPreviewSupported(mimeType: string) {
    return Object.hasOwnProperty.call(MIME_TYPE_SIZE_LIMITS_MB, mimeType);
  }

  private readFileAsText(blob: Blob, isCurrent: () => boolean = () => true) {
    const txtReader = new FileReader();
    txtReader.onload = (event: any) => {
      if (isCurrent()) {
        this.textContent = event.target.result;
      }
    };
    txtReader.readAsText(blob);
  }

  private loadSafeURL(blob: Blob) {
    this.fileURL = URL.createObjectURL(blob);
    this.safeFileURL = this.sanitizer.bypassSecurityTrustUrl(this.fileURL);
  }

  private loadTabularFile(data: any[][]) {
    if (data.length > 0) {
      // Extract the header (first row)
      this.tableDataHeader = data[0];

      // Process the rest of the rows
      this.tableContent = data.slice(1).map(row => {
        // Normalize the row length to match the header length
        while (row.length < this.tableDataHeader.length) {
          row.push("");
        }
        return row;
      });
    }
  }
}
