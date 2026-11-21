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
import { NgFor, NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { MacroService } from "../../../service/macro/macro.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { MacroDefinition, macroType, macroRevisionLabel } from "../../../../common/util/workflow-macro";

@Component({
  selector: "texera-macro-panel",
  imports: [NgIf, NgFor, FormsModule, NzButtonModule, NzInputModule, NzCollapseModule, OperatorLabelComponent],
  templateUrl: "./macro-panel.component.html",
  styleUrls: ["./macro.component.scss"],
})
export class MacroPanelComponent {
  public name = "";
  public busy = false;
  public readonly type = macroType;
  public readonly revisionLabel = macroRevisionLabel;
  public readonly trackDefinition = (_: number, d: MacroDefinition) => macroType(d);
  public get canModify(): boolean {
    return this.actions.checkWorkflowModificationEnabled();
  }
  public get selected(): readonly string[] {
    return this.actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
  }
  public get selectionHint(): string {
    if (!this.canModify) return "Read-only workflow";
    const count = this.selected.length;
    return count ? `${count} operator${count === 1 ? "" : "s"} selected` : "Select operators on the canvas";
  }
  constructor(
    public macros: MacroService,
    public actions: WorkflowActionService,
    public metadata: OperatorMetadataService,
    private notification: NotificationService
  ) {}
  public async refresh(): Promise<void> {
    await this.perform(() => this.macros.refresh());
  }
  public async create(): Promise<void> {
    await this.perform(async () => {
      await this.macros.createFromSelection(this.name);
      this.name = "";
      this.notification.success("Macro created");
    });
  }
  private async perform(action: () => Promise<unknown>): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    try {
      await action();
    } catch (e) {
      this.notification.error(e instanceof Error ? e.message : String(e));
    } finally {
      this.busy = false;
    }
  }
}
