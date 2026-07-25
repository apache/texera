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
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgFor, NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import {
  MountedDatasetInfo,
  WorkflowComputingUnitManagingService,
} from "../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";

interface DatasetVariableRow {
  variableName: string;
  datasetPath: string;
}

/**
 * Property-editor widget for the Python UDF "Mounted dataset variables" property. It edits
 * a list of {variableName, datasetPath} bindings: each row maps a Python variable to a
 * dataset mounted on the active computing unit. The dataset dropdown is populated from the
 * datasets currently mounted on that CU (see the "Mount datasets into computing unit"
 * action). At runtime each variable holds the dataset's local mount path.
 */
@UntilDestroy()
@Component({
  templateUrl: "dataset-variables-editor.component.html",
  styleUrls: ["dataset-variables-editor.component.scss"],
  imports: [
    NgFor,
    NgIf,
    FormsModule,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzInputDirective,
    NzIconDirective,
    NzSelectComponent,
    NzOptionComponent,
    NzTooltipDirective,
  ],
})
export class DatasetVariablesEditorComponent extends FieldType<FieldTypeConfig> implements OnInit {
  rows: DatasetVariableRow[] = [];
  mountedDatasetPaths: string[] = [];
  loading = false;
  activeCuid?: number;
  activeCuIsKubernetes = false;

  constructor(
    private computingUnitStatusService: ComputingUnitStatusService,
    private computingUnitService: WorkflowComputingUnitManagingService
  ) {
    super();
  }

  ngOnInit(): void {
    const value = this.formControl.value;
    this.rows = Array.isArray(value)
      ? value.map((row: Partial<DatasetVariableRow>) => ({
          variableName: row?.variableName ?? "",
          datasetPath: row?.datasetPath ?? "",
        }))
      : [];

    this.computingUnitStatusService
      .getSelectedComputingUnit()
      .pipe(untilDestroyed(this))
      .subscribe(unit => {
        this.activeCuid = unit?.computingUnit?.cuid;
        this.activeCuIsKubernetes = unit?.computingUnit?.type === "kubernetes";
        this.loadMountedDatasets();
      });
  }

  private loadMountedDatasets(): void {
    this.mountedDatasetPaths = [];
    if (this.activeCuid == null || !this.activeCuIsKubernetes) {
      return;
    }
    this.loading = true;
    this.computingUnitService
      .listMountedDatasets(this.activeCuid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (mounts: MountedDatasetInfo[]) => {
          this.mountedDatasetPaths = mounts.map(mount => mount.datasetPath).filter(path => !!path);
          this.loading = false;
        },
        error: () => {
          this.loading = false;
        },
      });
  }

  /** Options for a row's dropdown, always including the row's own saved value. */
  optionsForRow(row: DatasetVariableRow): string[] {
    if (row.datasetPath && !this.mountedDatasetPaths.includes(row.datasetPath)) {
      return [row.datasetPath, ...this.mountedDatasetPaths];
    }
    return this.mountedDatasetPaths;
  }

  addRow(): void {
    this.rows = [...this.rows, { variableName: "", datasetPath: "" }];
    this.sync();
  }

  removeRow(index: number): void {
    this.rows = this.rows.filter((_, rowIndex) => rowIndex !== index);
    this.sync();
  }

  onRowChange(): void {
    this.sync();
  }

  trackByIndex(index: number): number {
    return index;
  }

  private sync(): void {
    this.formControl.setValue(this.rows.map(row => ({ ...row })));
    this.formControl.markAsDirty();
    this.formControl.markAsTouched();
  }
}
