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

import { ChangeDetectorRef, Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from "@angular/core";
import { NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective, NzAutosizeDirective } from "ng-zorro-antd/input";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { descriptionText, MAX_OPERATOR_DESCRIPTION_LENGTH } from "../../../util/operator-description.util";
import { renderDescriptionMarkdown } from "./description-markdown";

/**
 * The per-operator description: free-form markdown notes on what this operator does in this workflow,
 * why it is set up the way it is, or context left for collaborators and agents.
 *
 * Unlike the operator type's documentation (schema <code>operatorDescription</code>), this belongs to
 * the operator instance, is stored on the OperatorPredicate, and is shared, undoable and saved with the
 * workflow. It never reaches the engine.
 *
 * Edits are kept in a local draft and written on save (button, Ctrl/Cmd+Enter), on switching to another
 * operator, or when the panel closes. A draft is only ever written into the same operator instance it
 * was started on (a reload or version preview that re-creates an operator under the same ID gets
 * nothing), and nothing is written while editing is locked.
 */
@UntilDestroy()
@Component({
  selector: "texera-operator-description",
  templateUrl: "./operator-description.component.html",
  styleUrls: ["./operator-description.component.scss"],
  imports: [
    NgIf,
    FormsModule,
    NzButtonComponent,
    NzIconDirective,
    NzInputDirective,
    NzAutosizeDirective,
    NzTooltipDirective,
  ],
})
export class OperatorDescriptionComponent implements OnInit, OnChanges, OnDestroy {
  @Input() operatorId?: string;
  /** False for viewers and while the workflow cannot be modified: the description is shown, not editable. */
  @Input() editable = true;

  readonly maxLength = MAX_OPERATOR_DESCRIPTION_LENGTH;

  description = "";
  renderedHtml = "";
  draft = "";
  editing = false;
  /** Set when someone else changes the description while this user is editing it. */
  changedElsewhere = false;

  /** The operator and its shared type that the current draft belongs to. */
  private editOperatorId?: string;
  private editTarget?: unknown;
  /** The description when editing started, to tell a co-editor's change from this user's own save. */
  private editBase = "";

  constructor(
    private workflowActionService: WorkflowActionService,
    private changeDetectorRef: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDescriptionChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, newDescription }) => {
        if (operatorID !== this.operatorId) return;
        this.setDescription(newDescription);
        // the draft is left alone so a co-editor's change never overwrites what is being typed
        if (this.editing && newDescription !== this.editBase) this.changedElsewhere = true;
        this.changeDetectorRef.markForCheck();
      });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.operatorId) {
      if (!changes.operatorId.firstChange && this.editing) this.commit({ onlyIfEdited: true });
      this.stopEditing();
      this.setDescription(this.readDescription(this.operatorId));
    }
  }

  ngOnDestroy(): void {
    if (this.editing) this.commit({ onlyIfEdited: true });
  }

  get hasDescription(): boolean {
    return this.description.trim().length > 0;
  }

  startEditing(): void {
    if (!this.editable || !this.operatorId) return;
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasOperator(this.operatorId)) return;
    this.editOperatorId = this.operatorId;
    this.editTarget = graph.getSharedOperatorType(this.operatorId);
    this.editBase = this.description;
    this.draft = this.description;
    this.changedElsewhere = false;
    this.editing = true;
  }

  save(): void {
    if (!this.editable) return;
    this.commit();
    this.stopEditing();
  }

  cancel(): void {
    this.stopEditing();
  }

  /** Replaces the draft with the description as a co-editor just saved it. */
  loadLatest(): void {
    this.draft = this.description;
    this.editBase = this.description;
    this.changedElsewhere = false;
  }

  onKeydown(event: KeyboardEvent): void {
    const mod = event.metaKey || event.ctrlKey;
    if (event.key === "Enter" && mod) {
      event.preventDefault();
      this.save();
    } else if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      this.cancel();
    } else if (mod && ["z", "y"].includes(event.key.toLowerCase())) {
      // keep undo/redo inside the text box; otherwise the canvas would also undo a workflow edit
      event.stopPropagation();
    }
  }

  /**
   * Writes the draft to the operator it was started on, if that is still the same operator instance
   * and editing is allowed. A reload or version preview re-creates operators under the same IDs, so
   * checking the ID alone could write the draft into a different version of the workflow.
   *
   * Leaving the editor (switching operators, closing the panel) passes onlyIfEdited: a draft the user never
   * changed is not a decision to keep the old text, and writing it back would undo a co-editor's newer save.
   * The Save button still writes, as the conflict banner says it will.
   */
  private commit({ onlyIfEdited = false }: { onlyIfEdited?: boolean } = {}): void {
    const operatorId = this.editOperatorId;
    if (!this.editable || !operatorId) return;
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasOperator(operatorId) || graph.getSharedOperatorType(operatorId) !== this.editTarget) return;
    const text = this.draft.trim().length === 0 ? "" : this.draft;
    if (onlyIfEdited && this.draft === this.editBase) return;
    if (text === this.readDescription(operatorId)) return;
    this.workflowActionService.setOperatorDescription(operatorId, text);
    if (operatorId === this.operatorId) this.setDescription(this.readDescription(operatorId));
  }

  private stopEditing(): void {
    this.editing = false;
    this.changedElsewhere = false;
    this.editOperatorId = undefined;
    this.editTarget = undefined;
  }

  private setDescription(text: string): void {
    this.description = text;
    this.renderedHtml = renderDescriptionMarkdown(text);
  }

  private readDescription(operatorId: string | undefined): string {
    const graph = this.workflowActionService.getTexeraGraph();
    if (!operatorId || !graph.hasOperator(operatorId)) return "";
    return descriptionText(graph.getOperator(operatorId).description);
  }
}
