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

import { ChangeDetectorRef, Component, ComponentRef, OnDestroy, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { CodeEditorComponent } from "../code-editor-dialog/code-editor.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { CodeEditorService } from "../../service/code-editor/code-editor.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NgClass, NgIf } from "@angular/common";

/**
 * CodeareaCustomTemplateComponent is the custom template for 'codearea' type of formly field.
 *
 * When the formly field type is 'codearea', it overrides the default one line string input template
 * with this component.
 */
@UntilDestroy()
@Component({
  selector: "texera-codearea-custom-template",
  templateUrl: "codearea-custom-template.component.html",
  styleUrls: ["codearea-custom-template.component.scss"],
  imports: [
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NgClass,
    NgIf,
  ],
})
export class CodeareaCustomTemplateComponent extends FieldType<FieldTypeConfig> implements OnInit, OnDestroy {
  componentRef: ComponentRef<CodeEditorComponent> | undefined;
  public isEditorOpen: boolean = false;
  private operatorID: string = "";

  /**
   * Whether the button offers a look at the code rather than an edit: the control is read-only and
   * the button still works, as on the canvas for a workflow one cannot write. Not on the Form View's
   * read-only inspection of a step, where the whole panel is inert and nothing in it can be pressed:
   * there the button keeps its usual face, "Edit code content", greyed out with the rest of the
   * panel, rather than promising a view it cannot open (#8760).
   */
  get viewOnly(): boolean {
    return this.field.formControl.disabled && !this.inspect;
  }

  /**
   * A read-only inspection mount (see viewOnly). The button is greyed out on it whatever its
   * control's state: the inspecting frame disables its form group as it is built, and formly adds
   * the controls to that group afterwards, so whether this one ends up disabled depends on the
   * order -- the button's face should not.
   */
  get inspect(): boolean {
    return this.field.props?.["inspect"] === true;
  }

  constructor(
    private coeditorPresenceService: CoeditorPresenceService,
    private codeEditorService: CodeEditorService,
    private changeDetectorRef: ChangeDetectorRef,
    private workflowActionService: WorkflowActionService
  ) {
    super();
    // A shadowed co-editor opening their editor opens the same operator's here, not whatever
    // operator this codearea happens to be on: only the codearea on that operator answers (#8760).
    this.coeditorPresenceService
      .getCoeditorOpenedCodeEditorSubject()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorId }) => {
        if (operatorId === this.operatorID) {
          this.openEditor();
        }
      });
    this.coeditorPresenceService
      .getCoeditorClosedCodeEditorSubject()
      .pipe(untilDestroyed(this))
      .subscribe(_ => this.componentRef?.destroy());
  }

  ngOnInit() {
    this.operatorID = this.getOperatorID();
    this.codeEditorService
      .getEditorState(this.operatorID)
      .pipe(untilDestroyed(this))
      .subscribe(isOpen => {
        this.isEditorOpen = isOpen;
        this.changeDetectorRef.detectChanges();
      });
  }

  openEditor(): void {
    // Without a host there is nothing to open into. Every view that mounts a codearea hosts the
    // editor today (the operator canvas and the Form View), so this is a guard, not a state the
    // button shows: a co-editor's open reaching a codearea with no host must not throw or mark
    // this operator's editor open.
    const host = this.codeEditorService.host;
    if (!host) {
      return;
    }
    this.componentRef = host.createComponent(CodeEditorComponent);
    // The editor is told its operator by its opener, rather than reading the highlight itself.
    this.componentRef.setInput("currentOperatorId", this.operatorID);
    this.componentRef.instance.componentRef = this.componentRef;
    this.componentRef.instance.formControl = this.field.formControl;
    this.isEditorOpen = true;

    this.codeEditorService.setEditorState(this.operatorID, true);

    this.componentRef.onDestroy(() => {
      this.isEditorOpen = false;
      this.codeEditorService.setEditorState(this.operatorID, false);
      this.changeDetectorRef.detectChanges();
    });
  }

  ngOnDestroy() {
    this.codeEditorService.setEditorState(this.operatorID, this.isEditorOpen);
  }

  private getOperatorID(): string {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
  }
}
