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

import { Component, inject, OnInit } from "@angular/core";
import { NgFor, NgIf, DatePipe } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { DashboardDataset } from "../../../../../type/dashboard-dataset.interface";
import { addDatasetsToProject, getProjectDatasetIds } from "../../project-dataset.util";

interface AddProjectDatasetModalData {
  projectId: number;
}

@UntilDestroy()
@Component({
  selector: "texera-add-project-dataset-modal",
  templateUrl: "./ngbd-modal-add-project-dataset.component.html",
  styleUrls: ["./ngbd-modal-add-project-dataset.component.scss"],
  imports: [NgFor, NgIf, FormsModule, DatePipe, NzButtonComponent, NzIconDirective],
})
export class NgbdModalAddProjectDatasetComponent implements OnInit {
  readonly nzModalData: AddProjectDatasetModalData = inject(NZ_MODAL_DATA);
  readonly projectId: number = this.nzModalData.projectId;

  public unaddedDatasets: DashboardDataset[] = [];
  public checked: boolean[] = [];
  public searchTerm = "";
  public loading = false;

  constructor(
    private datasetService: DatasetService,
    private modalRef: NzModalRef
  ) {}

  ngOnInit(): void {
    this.refresh();
  }

  get filteredIndices(): number[] {
    const q = this.searchTerm.trim().toLowerCase();
    if (!q) return this.unaddedDatasets.map((_, i) => i);
    return this.unaddedDatasets
      .map((d, i) => ({ d, i }))
      .filter(({ d }) => {
        const name = (d.dataset.name ?? "").toLowerCase();
        const desc = (d.dataset.description ?? "").toLowerCase();
        return name.includes(q) || desc.includes(q);
      })
      .map(({ i }) => i);
  }

  public selectedCount(): number {
    return this.checked.filter(Boolean).length;
  }

  public isAllChecked(): boolean {
    const indices = this.filteredIndices;
    return indices.length > 0 && indices.every(i => this.checked[i]);
  }

  public toggleAll(): void {
    const fill = !this.isAllChecked();
    this.filteredIndices.forEach(i => (this.checked[i] = fill));
  }

  public anyChecked(): boolean {
    return this.checked.some(Boolean);
  }

  public confirm(): void {
    const selectedIds = this.unaddedDatasets
      .filter((_, i) => this.checked[i])
      .map(entry => entry.dataset.did)
      .filter((did): did is number => typeof did === "number");
    if (selectedIds.length === 0) {
      this.modalRef.close([]);
      return;
    }
    addDatasetsToProject(this.projectId, selectedIds);
    this.modalRef.close(selectedIds);
  }

  public cancel(): void {
    this.modalRef.close(null);
  }

  public formatSize(bytes: number): string {
    if (!bytes || bytes < 0) return "—";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let i = 0;
    let size = bytes;
    while (size >= 1024 && i < units.length - 1) {
      size /= 1024;
      i++;
    }
    return `${size.toFixed(size >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
  }

  private refresh(): void {
    this.loading = true;
    const alreadyInProject = new Set(getProjectDatasetIds(this.projectId));
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: datasets => {
          this.unaddedDatasets = datasets.filter(
            d => typeof d.dataset.did === "number" && !alreadyInProject.has(d.dataset.did)
          );
          this.checked = new Array(this.unaddedDatasets.length).fill(false);
          this.loading = false;
        },
        error: () => {
          this.unaddedDatasets = [];
          this.checked = [];
          this.loading = false;
        },
      });
  }
}
