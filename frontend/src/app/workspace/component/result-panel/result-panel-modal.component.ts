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

import { Component, inject, OnChanges, OnDestroy } from "@angular/core";
import { CommonModule } from "@angular/common";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { HttpClient } from "@angular/common/http";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { isAudioUrl, isVideoUrl, isImageUrl } from "src/app/common/util/media-type.util";
import { AppSettings } from "../../../common/app-setting";

/**
 *
 * The pop-up window that will be
 *  displayed when the user clicks on a specific row
 *  to show the displays of that row.
 *
 * User can exit the pop-up window by
 *  1. Clicking the dismiss button on the top-right hand corner
 *      of the Modal
 *  2. Clicking the `Close` button at the bottom-right
 *  3. Clicking any shaded area that is not the pop-up window
 *  4. Pressing `Esc` button on the keyboard
 */
@UntilDestroy()
@Component({
  selector: "texera-row-modal-content",
  templateUrl: "./result-panel-modal.component.html",
  styleUrls: ["./result-panel-model.component.scss"],
  imports: [CommonModule, NzButtonModule, NzIconModule],
})
export class RowModalComponent implements OnChanges, OnDestroy {
  rowEntries: { key: string; value: string; mediaSrc: string; isVideo: boolean; isImage: boolean; isAudio: boolean }[] =
    [];
  private readonly allocatedBlobUrls: string[] = [];
  // Index of current displayed row in currentResult
  private readonly modalData: { operatorId: string; rowIndex: number; rowData?: Record<string, unknown> } =
    inject(NZ_MODAL_DATA);
  readonly operatorId: string = this.modalData.operatorId;
  rowIndex: number = this.modalData.rowIndex;
  currentDisplayRowData: Record<string, unknown> = {};

  constructor(
    public modal: NzModalRef<any, number>,
    private http: HttpClient,
    private workflowResultService: WorkflowResultService,
    private resizeService: PanelResizeService,
    private notificationService: NotificationService
  ) {
    if (this.modalData.rowData) {
      this.currentDisplayRowData = this.modalData.rowData;
      this.rowEntries = this.buildRowEntries(this.currentDisplayRowData);
    }
    this.ngOnChanges();
  }

  get prettyRowJson(): string {
    return JSON.stringify(this.currentDisplayRowData, null, 2);
  }

  copyText(text: string): void {
    navigator.clipboard.writeText(text).then(
      () => this.notificationService.success("Copied to clipboard"),
      () => this.notificationService.error("Failed to copy")
    );
  }

  ngOnChanges(): void {
    this.workflowResultService
      .getPaginatedResultService(this.operatorId)
      ?.selectTuple(this.rowIndex, this.resizeService.pageSize)
      .pipe(untilDestroyed(this))
      .subscribe(res => {
        if (res?.tuple) {
          this.currentDisplayRowData = res.tuple;
          this.rowEntries = this.buildRowEntries(this.currentDisplayRowData);
        }
      });
  }

  trackByEntryKey(_index: number, entry: { key: string }): string {
    return entry.key;
  }

  ngOnDestroy(): void {
    for (const url of this.allocatedBlobUrls) {
      URL.revokeObjectURL(url);
    }
  }

  private fetchBlobSrc(
    entry: { mediaSrc: string; isVideo: boolean; isImage: boolean; isAudio: boolean },
    remoteUrl: string
  ): void {
    const proxyUrl = `${AppSettings.getApiEndpoint()}/huggingface/media-proxy?url=${encodeURIComponent(remoteUrl)}`;
    this.http
      .get(proxyUrl, { responseType: "blob" })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: blob => {
          const blobUrl = URL.createObjectURL(blob);
          this.allocatedBlobUrls.push(blobUrl);
          entry.mediaSrc = blobUrl;
        },
        error: () => {
          // The proxy rejected this URL (e.g. the SSRF allowlist blocked it). Don't fall
          // back to loading the raw remote URL directly in the browser — fall back to the
          // text view instead so the entry never bypasses the proxy's allowlist.
          entry.isVideo = false;
          entry.isImage = false;
          entry.isAudio = false;
        },
      });
  }

  private buildRowEntries(
    rowData: Record<string, unknown>
  ): { key: string; value: string; mediaSrc: string; isVideo: boolean; isImage: boolean; isAudio: boolean }[] {
    return Object.entries(rowData).map(([key, val]) => {
      const value = typeof val === "string" ? val : JSON.stringify(val) ?? String(val);
      const isRemote = value.startsWith("http://") || value.startsWith("https://");
      const entry = {
        key,
        value,
        mediaSrc: isRemote ? "" : value,
        isVideo: typeof val === "string" && isVideoUrl(val),
        isImage: typeof val === "string" && isImageUrl(val),
        isAudio: typeof val === "string" && isAudioUrl(val),
      };
      if (isRemote && (entry.isVideo || entry.isImage || entry.isAudio)) {
        this.fetchBlobSrc(entry, value);
      }
      return entry;
    });
  }
}
