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

import { Component, ElementRef, ViewChild } from "@angular/core";
import { NgIf } from "@angular/common";
import { IHeaderAngularComp } from "ag-grid-angular";
import { IHeaderParams } from "ag-grid-community";
import { NzIconDirective } from "ng-zorro-antd/icon";

export type HeaderStats = {
  min?: number;
  max?: number;
  not_null_count?: number;
  firstCat?: string;
  firstPercent?: number;
  secondCat?: string;
  secondPercent?: number;
  other?: number;
  reachedLimit?: number;
};

export interface ResultHeaderParams extends IHeaderParams {
  stats?: HeaderStats;
}

/**
 * Custom ag-grid header that shows the column name on top and an inline stats
 * block (Min / Max / Non-Null / Category %) below — restoring the per-column
 * summary that existed in the old nz-table result pane. Clicking the title
 * progresses sort just like the default header; the filter menu opens via the
 * funnel icon on the right of the title row.
 */
@Component({
  selector: "texera-result-header",
  template: `
    <div class="texera-header">
      <div
        class="texera-header-title-row"
        (click)="onSortClick($event)">
        <span class="texera-header-title">{{ displayName }}</span>
        <span
          *ngIf="sort"
          class="texera-header-sort">
          <i
            nz-icon
            [nzType]="sort === 'asc' ? 'caret-up' : 'caret-down'"></i>
        </span>
        <span
          class="texera-header-menu"
          (click)="onMenuClick($event)">
          <i
            nz-icon
            nzType="filter"
            [class.active]="filterActive"></i>
        </span>
      </div>
      <div
        *ngIf="hasAnyStat()"
        class="texera-header-stats">
        <div
          *ngIf="stats?.min !== undefined"
          class="stat-row">
          <span class="stat-label">Min</span>
          <span class="stat-value">{{ format(stats?.min) }}</span>
        </div>
        <div
          *ngIf="stats?.max !== undefined"
          class="stat-row">
          <span class="stat-label">Max</span>
          <span class="stat-value">{{ format(stats?.max) }}</span>
        </div>
        <div
          *ngIf="stats?.not_null_count !== undefined"
          class="stat-row">
          <span class="stat-label">Non-Null</span>
          <span class="stat-value">{{ formatInt(stats?.not_null_count) }}</span>
        </div>
        <div
          *ngIf="stats?.firstPercent !== undefined"
          class="stat-row">
          <span class="stat-label">
            {{ stats?.firstCat }}
            <span
              *ngIf="stats?.reachedLimit === 1"
              class="stat-approx"
              >~</span
            >
          </span>
          <span class="stat-value">{{ formatPercent(stats?.firstPercent) }}</span>
        </div>
        <div
          *ngIf="stats?.secondPercent !== undefined"
          class="stat-row">
          <span class="stat-label">
            {{ stats?.secondCat }}
            <span
              *ngIf="stats?.reachedLimit === 1"
              class="stat-approx"
              >~</span
            >
          </span>
          <span class="stat-value">{{ formatPercent(stats?.secondPercent) }}</span>
        </div>
        <div
          *ngIf="stats?.other !== undefined"
          class="stat-row">
          <span class="stat-label">Other</span>
          <span class="stat-value">{{ formatPercent(stats?.other) }}</span>
        </div>
      </div>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
        width: 100%;
        height: 100%;
      }
      .texera-header {
        display: flex;
        flex-direction: column;
        height: 100%;
        padding: 4px 0;
      }
      .texera-header-title-row {
        display: flex;
        align-items: center;
        gap: 4px;
        cursor: pointer;
        user-select: none;
        padding: 0 4px 2px;
      }
      .texera-header-title {
        flex: 1 1 auto;
        font-weight: 600;
        font-size: 13px;
        color: rgba(0, 0, 0, 0.85);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .texera-header-sort i {
        font-size: 10px;
        color: #1890ff;
      }
      .texera-header-menu {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 18px;
        height: 18px;
        border-radius: 3px;
        cursor: pointer;
        opacity: 0.6;
      }
      .texera-header-menu:hover {
        opacity: 1;
        background: rgba(0, 0, 0, 0.04);
      }
      .texera-header-menu i {
        font-size: 11px;
      }
      .texera-header-menu i.active {
        color: #1890ff;
      }
      .texera-header-stats {
        flex: 1 1 auto;
        display: flex;
        flex-direction: column;
        gap: 1px;
        padding: 2px 4px 0;
        font-size: 11px;
        color: rgba(0, 0, 0, 0.55);
        font-weight: 400;
        line-height: 1.4;
      }
      .stat-row {
        display: flex;
        justify-content: space-between;
        gap: 6px;
      }
      .stat-label {
        opacity: 0.7;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .stat-value {
        font-variant-numeric: tabular-nums;
        color: rgba(0, 0, 0, 0.75);
      }
      .stat-approx {
        opacity: 0.6;
      }
    `,
  ],
  imports: [NgIf, NzIconDirective],
})
export class ResultHeaderComponent implements IHeaderAngularComp {
  @ViewChild("menuButton", { read: ElementRef }) menuButton?: ElementRef<HTMLElement>;

  displayName = "";
  stats: HeaderStats | null = null;
  sort: "asc" | "desc" | null = null;
  filterActive = false;

  private params!: ResultHeaderParams;

  agInit(params: ResultHeaderParams): void {
    this.update(params);
    params.column.addEventListener("sortChanged", () => this.refreshLocalState());
    params.column.addEventListener("filterChanged", () => this.refreshLocalState());
  }

  refresh(params: ResultHeaderParams): boolean {
    this.update(params);
    return true;
  }

  private update(params: ResultHeaderParams): void {
    this.params = params;
    this.displayName = params.displayName;
    this.stats = params.stats ?? null;
    this.refreshLocalState();
  }

  private refreshLocalState(): void {
    this.sort = (this.params.column.getSort() as "asc" | "desc" | null) ?? null;
    this.filterActive = this.params.column.isFilterActive();
  }

  hasAnyStat(): boolean {
    if (!this.stats) return false;
    return (
      this.stats.min !== undefined ||
      this.stats.max !== undefined ||
      this.stats.not_null_count !== undefined ||
      this.stats.firstPercent !== undefined ||
      this.stats.secondPercent !== undefined ||
      this.stats.other !== undefined
    );
  }

  onSortClick(event: MouseEvent): void {
    if (!this.params.enableSorting) return;
    this.params.progressSort(event.shiftKey);
  }

  onMenuClick(event: MouseEvent): void {
    event.stopPropagation();
    this.params.showColumnMenu(event.target as HTMLElement);
  }

  format(value: number | undefined): string {
    if (value === undefined || value === null) return "";
    if (typeof value !== "number") return String(value);
    if (Number.isInteger(value)) return value.toString();
    return value.toFixed(2);
  }

  formatInt(value: number | undefined): string {
    if (value === undefined || value === null) return "";
    return Math.round(value).toLocaleString();
  }

  formatPercent(value: number | undefined): string {
    if (value === undefined || value === null) return "";
    return `${value.toFixed(1)}%`;
  }
}
