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

import { Component } from "@angular/core";
import { NgIf } from "@angular/common";
import { ICellRendererAngularComp } from "ag-grid-angular";
import { ICellRendererParams } from "ag-grid-community";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";

/**
 * Identifies a base64-encoded image data URL so the cell can render an inline
 * thumbnail instead of the raw string. Kept inline to avoid a fan-out import
 * for a one-line check.
 */
function isImageDataUrl(value: unknown): value is string {
  return typeof value === "string" && /^data:image\/(?:png|jpeg|gif|webp);base64,/i.test(value);
}

export interface ResultCellRendererParams extends ICellRendererParams {
  onDownload: (rowIndex: number, columnName: string) => void;
}

@Component({
  selector: "texera-result-cell-renderer",
  template: `
    <div class="cell-wrapper">
      <img
        *ngIf="isImage; else textCell"
        class="image-thumbnail"
        [src]="displayValue"
        [alt]="columnName" />
      <ng-template #textCell>
        <span class="cell-content">{{ displayValue }}</span>
      </ng-template>
      <button
        nz-button
        nzType="link"
        class="download-button"
        title="Download data"
        (click)="onDownloadClick($event)">
        <i
          nz-icon
          nzType="cloud-download"></i>
      </button>
    </div>
  `,
  styles: [
    `
      .cell-wrapper {
        position: relative;
        width: 100%;
        height: 100%;
        display: flex;
        align-items: center;
        padding-right: 28px;
      }
      .cell-content {
        display: block;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        width: 100%;
      }
      .image-thumbnail {
        display: block;
        width: 32px;
        height: 32px;
        object-fit: cover;
      }
      .download-button {
        position: absolute;
        right: 0;
        top: 50%;
        transform: translateY(-50%);
        opacity: 0;
        transition: opacity 0.2s ease-in-out;
        padding: 4px;
      }
      .download-button i {
        font-size: 14px;
        color: #1890ff;
      }
      :host:hover .download-button {
        opacity: 0.7;
      }
      :host:hover .download-button:hover {
        opacity: 1;
      }
    `,
  ],
  imports: [NgIf, NzButtonComponent, NzIconDirective],
})
export class ResultCellRendererComponent implements ICellRendererAngularComp {
  displayValue = "";
  isImage = false;
  columnName = "";
  private rowIndex = 0;
  private onDownload?: (rowIndex: number, columnName: string) => void;

  agInit(params: ResultCellRendererParams): void {
    this.update(params);
  }

  refresh(params: ResultCellRendererParams): boolean {
    this.update(params);
    return true;
  }

  private update(params: ResultCellRendererParams): void {
    const raw = params.value;
    this.columnName = params.colDef?.field ?? "";
    this.rowIndex = params.node.rowIndex ?? 0;
    this.onDownload = params.onDownload;
    this.isImage = isImageDataUrl(raw);
    this.displayValue = raw === null || raw === undefined ? "" : raw.toString();
  }

  onDownloadClick(event: Event): void {
    event.stopPropagation();
    this.onDownload?.(this.rowIndex, this.columnName);
  }
}
