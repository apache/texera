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
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { Observable } from "rxjs";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzSpinModule } from "ng-zorro-antd/spin";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetBankService } from "../../../service/user/dataset-bank/dataset-bank.service";
import {
  BANK_CATEGORY_LABELS,
  BANK_CATEGORY_ORDER,
  BANK_SOURCE_COLORS,
  BANK_SOURCE_LABELS,
  BankCategory,
  BankDataset,
} from "../../../service/user/dataset-bank/dataset-bank.types";

interface CategoryChip {
  key: BankCategory | "all";
  label: string;
}

const CHIPS: CategoryChip[] = [
  { key: "all", label: "All" },
  ...BANK_CATEGORY_ORDER.map(k => ({ key: k, label: BANK_CATEGORY_LABELS[k] })),
];

@UntilDestroy()
@Component({
  selector: "texera-dataset-bank",
  templateUrl: "./dataset-bank.component.html",
  styleUrls: ["./dataset-bank.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzButtonModule,
    NzCardModule,
    NzIconModule,
    NzInputModule,
    NzTagModule,
    NzEmptyModule,
    NzSpinModule,
    NzTooltipModule,
  ],
})
export class DatasetBankComponent implements OnInit {
  readonly chips: CategoryChip[] = CHIPS;
  readonly sourceLabel = BANK_SOURCE_LABELS;
  readonly sourceColor = BANK_SOURCE_COLORS;

  searchQuery = "";
  activeCategory: BankCategory | "all" = "all";

  datasets$!: Observable<BankDataset[]>;
  isLoading$!: Observable<boolean>;

  constructor(
    private bank: DatasetBankService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.datasets$ = this.bank.filteredDatasets$;
    this.isLoading$ = this.bank.isLoading$;

    this.bank.searchQuery$.pipe(untilDestroyed(this)).subscribe(q => (this.searchQuery = q));
    this.bank.category$.pipe(untilDestroyed(this)).subscribe(c => (this.activeCategory = c));

    // Best-effort live refresh on first visit. Failures are silent — the seed
    // list stays visible regardless.
    this.bank.refreshFromApis(this.searchQuery);
  }

  onSearchChange(value: string): void {
    this.bank.setSearchQuery(value);
  }

  onCategoryClick(category: BankCategory | "all"): void {
    this.bank.setCategory(category);
  }

  refresh(): void {
    this.bank.refreshFromApis(this.searchQuery);
  }

  trackById(_: number, d: BankDataset): string {
    return d.id;
  }

  formatNumber(n?: number): string {
    if (n === undefined || n === null) return "—";
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, "") + "M";
    if (n >= 1_000) return (n / 1_000).toFixed(1).replace(/\.0$/, "") + "k";
    return String(n);
  }

  /**
   * Per-card import state. "idle" → not yet imported; "importing" → in flight;
   * "imported" → success (button stays disabled); "failed" → error (button is
   * clickable again to retry).
   */
  importState: Record<string, "idle" | "importing" | "imported" | "failed"> = {};

  /** Resolved URL for the Download anchor — prefers direct file link, falls back to source. */
  downloadHref(d: BankDataset): string {
    return d.downloadUrl || d.url || "#";
  }

  importStatus(d: BankDataset): "idle" | "importing" | "imported" | "failed" {
    return this.importState[d.id] ?? "idle";
  }

  import(d: BankDataset): void {
    const status = this.importStatus(d);
    if (status === "importing" || status === "imported") return;

    this.importState[d.id] = "importing";
    this.bank
      .importToTexera(d)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ datasetName }) => {
          this.importState[d.id] = "imported";
          this.notificationService.success(`Imported "${datasetName}" to your datasets.`);
        },
        error: err => {
          this.importState[d.id] = "failed";
          const msg = err?.message ?? String(err);
          this.notificationService.error(`Import failed: ${msg}`);
        },
      });
  }
}
