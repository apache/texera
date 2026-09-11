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

import { Component, Type } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { ModelSelectionModalComponent } from "../model-selection-modal/model-selection-modal.component";
import { DatasetSelectionModalComponent } from "../dataset-selection-modal/dataset-selection-modal.component";
import { DATASET_INPUT_TYPE, MODEL_INPUT_TYPE } from "../../service/code-editor/ui-udf-parameters-parser.service";

type ResourceBrowser = Readonly<{
  title: string;
  icon: string;
  emptyLabel: string;
  component: Type<unknown>;
  data?: Readonly<Record<string, unknown>>;
}>;

const BROWSERS: Readonly<Record<string, ResourceBrowser>> = {
  [MODEL_INPUT_TYPE]: {
    title: "Select a model version",
    icon: "experiment",
    emptyLabel: "Select model",
    component: ModelSelectionModalComponent,
  },
  [DATASET_INPUT_TYPE]: {
    title: "Select a dataset version",
    icon: "database",
    emptyLabel: "Select dataset",
    component: DatasetSelectionModalComponent,
    // A mount exposes a whole version, so the value is the version's path, not a file in it.
    data: { fileMode: false },
  },
};

/**
 * Value editor for a UDF parameter declared with `value=Resource.MODEL` or `Resource.DATASET`.
 * The value is still a string — the chosen version's path, picked from that resource's browser
 * rather than typed.
 */
@UntilDestroy()
@Component({
  selector: "texera-resource-value-selector",
  templateUrl: "./resource-value-selector.component.html",
  styleUrls: ["./resource-value-selector.component.scss"],
  imports: [NzButtonComponent, NzWaveDirective, ɵNzTransitionPatchDirective, NzIconDirective, NzTooltipDirective],
})
export class ResourceValueSelectorComponent extends FieldType<FieldTypeConfig> {
  constructor(private modalService: NzModalService) {
    super();
  }

  private get browser(): ResourceBrowser {
    const resource = (this.props as { resource?: string } | undefined)?.resource ?? "";
    return BROWSERS[resource] ?? BROWSERS[DATASET_INPUT_TYPE];
  }

  get icon(): string {
    return this.browser.icon;
  }

  get selectedPath(): string {
    return (this.formControl?.value as string) ?? "";
  }

  get tooltip(): string {
    return this.selectedPath || this.browser.title;
  }

  /** Name and version only — the cell is narrow, and the full path is the tooltip. */
  get label(): string {
    const parts = this.selectedPath.split("/").filter(part => part.length > 0);
    return parts.length >= 4 ? `${parts[2]} · ${parts[3]}` : this.selectedPath || this.browser.emptyLabel;
  }

  openPicker(): void {
    if (this.formControl?.disabled) return;
    const browser = this.browser;
    const modal = this.modalService.create({
      nzTitle: browser.title,
      nzContent: browser.component,
      nzFooter: null,
      nzData: { ...(browser.data ?? {}), selectedPath: this.selectedPath || null },
      // Explicit: the browsers size to their contents, so "fit-content" collapses the dialog.
      nzWidth: 720,
      nzBodyStyle: { overflow: "auto", minHeight: "200px", maxHeight: "70vh" },
    });
    modal.afterClose.pipe(untilDestroyed(this)).subscribe((selectedPath?: string) => {
      if (!selectedPath) return;
      this.formControl.setValue(selectedPath);
      this.formControl.markAsDirty();
      this.formControl.markAsTouched();
    });
  }
}
