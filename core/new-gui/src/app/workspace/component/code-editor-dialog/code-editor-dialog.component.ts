import { Component, Inject } from "@angular/core";
import { MAT_DIALOG_DATA, MatDialogRef } from "@angular/material/dialog";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowCollabService } from "../../service/workflow-collab/workflow-collab.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OperatorPredicate } from "../../types/workflow-common.interface";

/**
 * CodeEditorDialogComponent is the content of the dialogue invoked by CodeareaCustomTemplateComponent.
 *
 * It contains a Monaco editor which is inside a mat-dialog-content. When the dialogue is invoked by
 * the button in CodeareaCustomTemplateComponent, the data of the custom field (or empty String if no data)
 * will be sent to the Monaco editor as its text. The dialogue can be closed with ESC key or by clicking on areas outside
 * the dialogue. Closing the dialogue will send the edited contend back to the custom template field.
 */
@UntilDestroy()
@Component({
  selector: "texera-code-editor-dialog",
  templateUrl: "./code-editor-dialog.component.html",
  styleUrls: ["./code-editor-dialog.component.scss"],
})
export class CodeEditorDialogComponent {
  editorOptions = {
    theme: "vs-dark",
    language: "python",
    fontSize: "11",
    automaticLayout: true,
    readOnly: true,
  };
  code: string;

  public lockGranted: boolean = false;

  constructor(
    private dialogRef: MatDialogRef<CodeEditorDialogComponent>,
    @Inject(MAT_DIALOG_DATA) code: any,
    private workflowActionService: WorkflowActionService,
    private workflowCollabService: WorkflowCollabService
  ) {
    this.code = code;
    this.handleLockChange();
  }

  private handleLockChange(): void {
    this.workflowCollabService
      .getLockStatusStream()
      .pipe(untilDestroyed(this))
      .subscribe((lockGranted: boolean) => {
        this.lockGranted = lockGranted;
        this.editorOptions.readOnly = !this.lockGranted;
      });
  }

  onCodeChange(code: string): void {
    this.code = code;
    // here the assumption is the operator being edited must be highlighted
    const currentOperatorId: string = this.workflowActionService
      .getJointGraphWrapper()
      .getCurrentHighlightedOperatorIDs()[0];
    const currentOperatorPredicate: OperatorPredicate = this.workflowActionService
      .getTexeraGraph()
      .getOperator(currentOperatorId);
    this.workflowActionService.setOperatorProperty(currentOperatorId, {
      ...currentOperatorPredicate.operatorProperties,
      code,
    });
  }
}
