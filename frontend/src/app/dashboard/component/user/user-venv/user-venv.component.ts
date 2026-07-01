/*
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
import { FormsModule } from "@angular/forms";
import { NgFor, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzModalComponent, NzModalContentDirective, NzModalService } from "ng-zorro-antd/modal";
import { NzOptionComponent, NzSelectComponent } from "ng-zorro-antd/select";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";

import { NotificationService } from "../../../../common/service/notification/notification.service";
import {
  UserPveRecord,
  WorkflowPveService,
} from "../../../../workspace/service/virtual-environment/virtual-environment.service";

type PveUserPackageRow = {
  name: string;
  versionOp: "==" | ">=" | "<=";
  version: string;
  deleteToggle?: boolean;
};

type PveDraft = {
  veid?: number;
  name: string;
  newPackages: PveUserPackageRow[];
};

@UntilDestroy()
@Component({
  selector: "texera-user-venv",
  templateUrl: "./user-venv.component.html",
  styleUrls: ["./user-venv.component.scss"],
  imports: [
    NgIf,
    NgFor,
    FormsModule,
    NzButtonComponent,
    NzCardComponent,
    NzIconDirective,
    NzInputDirective,
    NzModalComponent,
    NzModalContentDirective,
    NzSelectComponent,
    NzOptionComponent,
    NzTooltipDirective,
  ],
})
export class UserVenvComponent implements OnInit {
  // The user's PVEs (fetched from the DB), rendered as the page list.
  pves: PveDraft[] = [];

  // The single PVE currently being edited in the modal. Null when modal is closed.
  currentDraft: PveDraft | null = null;

  pveModalVisible = false;
  saving = false;

  constructor(
    private workflowPveService: WorkflowPveService,
    private notificationService: NotificationService,
    private modalService: NzModalService
  ) {}

  ngOnInit(): void {
    this.refreshPves();
  }

  confirmDeletePve(index: number): void {
    const target = this.pves[index];
    if (!target) return;
    const name = target.name || "(unnamed)";
    this.modalService.confirm({
      nzTitle: `Delete environment "${name}"?`,
      nzContent: "This permanently removes the environment from the database.",
      nzOkText: "Delete",
      nzOkDanger: true,
      nzOnOk: () => this.deletePve(index),
    });
  }

  private refreshPves(): void {
    this.workflowPveService
      .listUserPves()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: records => {
          this.pves = records.map(record => this.recordToDraft(record));
        },
        error: (err: unknown) => {
          console.error("Failed to fetch Python environments", err);
          this.notificationService.error("Failed to fetch Python environments.");
        },
      });
  }

  private recordToDraft(record: UserPveRecord): PveDraft {
    const newPackages: PveUserPackageRow[] = Object.entries(record.packages ?? {}).map(([name, raw]) => {
      const match = raw?.match?.(/^(==|>=|<=)(.*)$/);
      return {
        name,
        versionOp: (match ? match[1] : "==") as "==" | ">=" | "<=",
        version: match ? match[2] : raw ?? "",
      };
    });
    return {
      veid: record.veid,
      name: record.name,
      newPackages,
    };
  }

  showPveModal(): void {
    this.currentDraft = {
      name: "",
      newPackages: [],
    };
    this.pveModalVisible = true;
  }

  openExistingPve(index: number): void {
    const source = this.pves[index];
    if (!source) return;
    this.currentDraft = {
      veid: source.veid,
      name: source.name,
      newPackages: source.newPackages.map(p => ({ ...p })),
    };
    this.pveModalVisible = true;
  }

  closePveModal(): void {
    this.pveModalVisible = false;
    this.currentDraft = null;
  }

  addPackage(): void {
    this.currentDraft?.newPackages.push({ name: "", versionOp: "==", version: "" });
  }

  togglePackageDelete(pkg: PveUserPackageRow): void {
    pkg.deleteToggle = !pkg.deleteToggle;
  }

  saveEnvironment(): void {
    const draft = this.currentDraft;
    if (!draft) return;

    const trimmedName = draft.name.trim();
    if (!trimmedName) {
      this.notificationService.error("Environment name is required.");
      return;
    }

    if (!/^[a-zA-Z0-9]+$/.test(trimmedName)) {
      this.notificationService.error("Environment name must contain only letters and numbers.");
      return;
    }

    const conflict = this.pves.find(p => p.name.trim() === trimmedName && p.veid !== draft.veid);
    if (conflict) {
      this.notificationService.error(`An environment named "${trimmedName}" already exists.`);
      return;
    }

    const packages: Record<string, string> = {};
    for (const row of draft.newPackages) {
      if (row.deleteToggle) continue;
      const pkgName = row.name.trim();
      if (!pkgName) continue;
      const pkgVersion = (row.version ?? "").trim();
      if (packages[pkgName] !== undefined) {
        this.notificationService.error(`Duplicate package "${pkgName}".`);
        return;
      }
      packages[pkgName] = pkgVersion ? `${row.versionOp}${pkgVersion}` : "";
    }

    this.saving = true;
    const request$ =
      draft.veid === undefined
        ? this.workflowPveService.savePve(trimmedName, packages)
        : this.workflowPveService.updateUserPve(draft.veid, trimmedName, packages);

    request$.pipe(untilDestroyed(this)).subscribe({
      next: () => {
        this.saving = false;
        this.notificationService.success(`Saved environment "${trimmedName}".`);
        this.closePveModal();
        this.refreshPves();
      },
      error: (err: unknown) => {
        this.saving = false;
        console.error("Failed to save PVE", err);
        this.notificationService.error("Failed to save Python environment.");
      },
    });
  }

  trackByVeid(_: number, pve: PveDraft): number | undefined {
    return pve.veid;
  }

  deletePve(index: number): void {
    const target = this.pves[index];
    if (!target || target.veid === undefined) return;

    const veid = target.veid;
    const name = target.name || "(unnamed)";

    this.workflowPveService
      .deleteUserPve(veid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success(`Deleted environment "${name}".`);
          this.refreshPves();
        },
        error: (err: unknown) => {
          console.error("Failed to delete PVE", err);
          this.notificationService.error("Failed to delete Python environment.");
        },
      });
  }
}
