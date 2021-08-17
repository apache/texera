import { Component, Inject, OnDestroy, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { merge, Subscription } from 'rxjs';
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPredicate } from "../../types/workflow-common.interface";


export const CODE_SAVE_ON_EDITION_DEBOUNCE_TIME_MS = 100;

/**
 * CodeEditorDialogComponent is the content of the dialogue invoked by CodeareaCustomTemplateComponent.
 *
 * It contains a Monaco editor which is inside a mat-dialog-content. When the dialogue is invoked by
 * the button in CodeareaCustomTemplateComponent, the data of the custom field (or empty String if no data)
 * will be sent to the Monaco editor as its text. The dialogue can be closed with ESC key or by clicking on areas outside
 * the dialogue. Closing the dialogue will send the edited contend back to the custom template field.
 * @author Xiaozhen Liu
 */
@Component({
  selector: 'texera-code-editor-dialog',
  templateUrl: './code-editor-dialog.component.html',
  styleUrls: ['./code-editor-dialog.component.scss']
})
export class CodeEditorDialogComponent implements OnInit, OnDestroy {

  editorOptions = { theme: 'vs-dark', language: 'python', fontSize: '11', automaticLayout: true};
  code: string;
  subscriptions: Subscription = new Subscription();

  constructor(
    private dialogRef: MatDialogRef<CodeEditorDialogComponent>,
    @Inject(MAT_DIALOG_DATA) code: any,
    private workflowActionService: WorkflowActionService
  ) {
    this.code = code;
  }

  ngOnInit() {
    this.registerAutoSaveUponClosure();
    this.registerAutoSaveUponEdition();
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  private registerAutoSaveUponClosure() {
    this.subscriptions.add(merge(
      this.dialogRef.keydownEvents().filter(event => event.key === 'Escape'),
      this.dialogRef.backdropClick()
    ).subscribe(_ => this.dialogRef.close(this.code)));
  }

  private registerAutoSaveUponEdition() {
    this.subscriptions.add(this.dialogRef.keydownEvents().debounceTime(CODE_SAVE_ON_EDITION_DEBOUNCE_TIME_MS).subscribe(_ => {

      // here the assumption is the operator being edited must be highlighted
      const currentOperatorId: string = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
      const currentOperatorPredicate: OperatorPredicate = this.workflowActionService.getTexeraGraph().getOperator(currentOperatorId);
      this.workflowActionService.setOperatorProperty(currentOperatorId, {
        ...currentOperatorPredicate.operatorProperties,
        code: this.code
      });
    }));
  }
}
