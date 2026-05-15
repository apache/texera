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
import { FormsModule } from "@angular/forms";
import { NgFor, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzModalService, NzModalModule } from "ng-zorro-antd/modal";
import { NzTableModule } from "ng-zorro-antd/table";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzMessageService } from "ng-zorro-antd/message";
import { MachineService } from "../../../../common/service/machine/machine.service";
import { Machine, MachineRequest } from "../../../../common/type/machine";

@UntilDestroy()
@Component({
  selector: "texera-user-machine",
  standalone: true,
  imports: [
    FormsModule,
    NgFor,
    NgIf,
    NzCardComponent,
    NzButtonComponent,
    NzIconDirective,
    NzInputDirective,
    NzModalModule,
    NzTableModule,
    NzFormModule,
  ],
  templateUrl: "./user-machine.component.html",
  styleUrls: ["./user-machine.component.scss"],
})
export class UserMachineComponent implements OnInit {
  machines: Machine[] = [];
  loading = false;

  showAddModal = false;
  editing: Machine | null = null;
  form: MachineRequest = { name: "", url: "http://localhost:5555", token: "" };

  constructor(
    private machineService: MachineService,
    private modal: NzModalService,
    private message: NzMessageService
  ) {}

  ngOnInit() {
    this.refresh();
  }

  refresh() {
    this.loading = true;
    this.machineService
      .list()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: machines => {
          this.machines = machines;
          this.loading = false;
        },
        error: err => {
          this.message.error("Failed to load machines: " + (err?.error?.message ?? err?.message ?? err));
          this.loading = false;
        },
      });
  }

  openAdd() {
    this.editing = null;
    this.form = { name: "", url: "http://localhost:5555", token: "" };
    this.showAddModal = true;
  }

  openEdit(m: Machine) {
    this.editing = m;
    this.form = { name: m.name, url: m.url, token: m.token ?? "" };
    this.showAddModal = true;
  }

  cancel() {
    this.showAddModal = false;
  }

  save() {
    const req: MachineRequest = {
      name: this.form.name.trim(),
      url: this.form.url.trim(),
      token: this.form.token?.trim() || null,
    };
    if (!req.name || !req.url) {
      this.message.warning("Name and URL are required");
      return;
    }
    const op$ = this.editing
      ? this.machineService.update(this.editing.mid, req)
      : this.machineService.create(req);
    op$.pipe(untilDestroyed(this)).subscribe({
      next: () => {
        this.showAddModal = false;
        this.refresh();
      },
      error: err => {
        this.message.error("Failed to save: " + (err?.error?.message ?? err?.message ?? err));
      },
    });
  }

  remove(m: Machine) {
    this.modal.confirm({
      nzTitle: `Delete machine "${m.name}"?`,
      nzOkText: "Delete",
      nzOkDanger: true,
      nzOnOk: () =>
        this.machineService
          .delete(m.mid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: () => this.refresh(),
            error: err =>
              this.message.error("Failed to delete: " + (err?.error?.message ?? err?.message ?? err)),
          }),
    });
  }
}
