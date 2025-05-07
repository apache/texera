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
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { FileSelectionComponent } from "../file-selection/file-selection.component";
import { environment } from "../../../../environments/environment";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";

@UntilDestroy()
@Component({
  selector: "texera-input-autocomplete-template",
  templateUrl: "./input-autocomplete.component.html",
  styleUrls: ["input-autocomplete.component.scss"],
})
export class InputAutoCompleteComponent extends FieldType<FieldTypeConfig> {
  constructor(
    private modalService: NzModalService,
    public workflowActionService: WorkflowActionService,
    public datasetService: DatasetService
  ) {
    super();
  }

  onClickOpenFileSelectionModal(): void {
    const modal = this.modalService.create({
      nzTitle: "Please select one file from datasets",
      nzContent: FileSelectionComponent,
      nzFooter: null,
      nzData: {
        selectedFilePath: this.formControl.getRawValue(),
      },
      nzBodyStyle: {
        // Enables the file selection window to be resizable
        resize: "both",
        overflow: "auto",
        minHeight: "200px",
        minWidth: "550px",
        maxWidth: "90vw",
        maxHeight: "80vh",
      },
      nzWidth: "fit-content",
    });
    // Handle the selection from the modal
    modal.afterClose.pipe(untilDestroyed(this)).subscribe(fileNode => {
      const node: DatasetFileNode = fileNode as DatasetFileNode;
      this.formControl.setValue(getFullPathFromDatasetFileNode(node));
    });
  }

  get isFileSelectionEnabled(): boolean {
    return environment.userSystemEnabled && environment.selectingFilesFromDatasetsEnabled;
  }

  get selectedFilePath(): string | null {
    return this.formControl.value;
  }
}
