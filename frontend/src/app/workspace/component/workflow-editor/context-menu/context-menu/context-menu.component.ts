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
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowResultService } from "src/app/workspace/service/workflow-result/workflow-result.service";
import { WorkflowResultExportService } from "src/app/workspace/service/workflow-result-export/workflow-result-export.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { ResultExportationComponent } from "../../../result-exportation/result-exportation.component";
import { ValidationWorkflowService } from "src/app/workspace/service/validation/validation-workflow.service";
import { GuiConfigService } from "../../../../../common/service/gui-config.service";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NgIf } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { MacroService } from "src/app/workspace/service/macro/macro.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";

@UntilDestroy()
@Component({
  selector: "texera-context-menu",
  templateUrl: "./context-menu.component.html",
  styleUrls: ["./context-menu.component.scss"],
  imports: [NzMenuDirective, NgIf, NzMenuItemComponent, ɵNzTransitionPatchDirective, NzIconDirective],
})
export class ContextMenuComponent {
  public isWorkflowModifiable: boolean = false;
  public highlightedOperatorIds: readonly string[] = [];
  public highlightedCommentBoxIds: readonly string[] = [];

  constructor(
    public workflowActionService: WorkflowActionService,
    public operatorMenuService: OperatorMenuService,
    public workflowResultExportService: WorkflowResultExportService,
    protected config: GuiConfigService,
    private workflowResultService: WorkflowResultService,
    private modalService: NzModalService,
    private validationWorkflowService: ValidationWorkflowService,
    private macroService: MacroService,
    private notificationService: NotificationService
  ) {
    this.registerWorkflowModifiableChangedHandler();
    this.operatorMenuService.highlightedOperators$
      .pipe(untilDestroyed(this))
      .subscribe(ids => (this.highlightedOperatorIds = ids));
    this.operatorMenuService.highlightedCommentBoxes$
      .pipe(untilDestroyed(this))
      .subscribe(ids => (this.highlightedCommentBoxIds = ids));
  }

  public canExecuteOperator(): boolean {
    if (!this.hasExactlyOneOperatorSelected() || !this.isWorkflowModifiable) {
      return false;
    }

    const operatorID = this.getSelectedOperatorID();
    return this.isOperatorExecutable(operatorID);
  }

  private hasExactlyOneOperatorSelected(): boolean {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length === 1;
  }

  private getSelectedOperatorID(): string {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
  }

  private isOperatorExecutable(operatorID: string): boolean {
    return (
      this.validationWorkflowService.validateOperator(operatorID).isValid &&
      !this.workflowActionService.getTexeraGraph().isOperatorDisabled(operatorID)
    );
  }

  public hasHighlightedLinks(): boolean {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs().length > 0;
  }

  public onCopy(): void {
    this.operatorMenuService.saveHighlightedElements();
  }

  public onPaste(): void {
    this.operatorMenuService.performPasteOperation();
  }

  public onCut(): void {
    this.onCopy();
    this.onDelete();
  }

  public onDelete(): void {
    // Capture all highlighted IDs before starting deletion to avoid modification during iteration
    const highlightedOperatorIDs = Array.from(
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()
    );
    const highlightedCommentBoxIDs = Array.from(
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedCommentBoxIDs()
    );
    const highlightedLinkIDs = Array.from(
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs()
    );

    // Bundle all deletions together for proper undo/redo support
    this.workflowActionService.getTexeraGraph().bundleActions(() => {
      // Delete operators and their connected links
      this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs);

      // Delete standalone selected links
      highlightedLinkIDs.forEach(highlightedLinkID => {
        // Only delete if the link still exists (might have been deleted with operators)
        if (this.workflowActionService.getTexeraGraph().hasLinkWithID(highlightedLinkID)) {
          this.workflowActionService.deleteLinkWithID(highlightedLinkID);
        }
      });

      // Delete comment boxes
      highlightedCommentBoxIDs.forEach(highlightedCommentBoxID =>
        this.workflowActionService.deleteCommentBox(highlightedCommentBoxID)
      );
    });
  }

  private registerWorkflowModifiableChangedHandler() {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(modifiable => (this.isWorkflowModifiable = modifiable));
  }

  /**
   * This is the handler for the execution result export button for only highlighted operators.
   *
   */
  /**
   * Bundles the highlighted operators into a new macro definition on the
   * backend. The current selection is left on the canvas — the follow-up
   * step will replace it with a MacroOpDesc node and rewire boundary links.
   */
  public onCreateMacro(): void {
    const selected = Array.from(this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs());
    if (selected.length < 2) {
      return;
    }
    const name = window.prompt("Macro name", `macro-${Date.now()}`);
    if (!name) {
      return;
    }
    const req = this.macroService.buildMacroCreateRequestFromSelection(this.workflowActionService, selected, name);
    this.macroService
      .createMacro(req)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: detail =>
          this.notificationService.success(`Macro "${detail.name}" created (wid=${detail.wid})`),
        error: err => this.notificationService.error(`Failed to create macro: ${err?.message ?? err}`),
      });
  }

  public onClickExportHighlightedExecutionResult(): void {
    this.modalService.create({
      nzTitle: "Export Highlighted Operators Result",
      nzContent: ResultExportationComponent,
      nzData: {
        workflowName: this.workflowActionService.getWorkflowMetadata()?.name,
        sourceTriggered: "context-menu",
      },
      nzFooter: null,
    });
  }
}
