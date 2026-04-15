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
import { DatasetSelectionModalComponent } from "../dataset-selection-modal/dataset-selection-modal.component";
import { GuiConfigService } from "../../../common/service/gui-config.service";

@UntilDestroy()
@Component({
  templateUrl: "dataset-file-selector.component.html",
})
export class DatasetFileSelectorComponent extends FieldType<FieldTypeConfig> {
  constructor(
    private modalService: NzModalService,
    public workflowActionService: WorkflowActionService,
    private config: GuiConfigService
  ) {
    super();
  }

  onClickOpenFileSelectionModal(): void {
    const modal = this.modalService.create({
      nzContent: DatasetSelectionModalComponent,
      nzFooter: null,
      nzData: {
        fileMode: true,
        selectedPath: this.formControl.getRawValue(),
      },
      nzBodyStyle: {
        resize: "both",
        overflow: "auto",
        minHeight: "200px",
        minWidth: "550px",
        maxWidth: "90vw",
        maxHeight: "80vh",
      },
      nzWidth: "fit-content",
    });
    modal.afterClose.pipe(untilDestroyed(this)).subscribe(selectedPath => {
      if (selectedPath) {
        this.formControl.setValue(selectedPath);
      }
    });
  }

  get isFileSelectionEnabled(): boolean {
    return this.config.env.selectingFilesFromDatasetsEnabled;
  }
}
