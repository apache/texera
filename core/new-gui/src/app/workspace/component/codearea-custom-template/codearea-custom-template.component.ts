import { Component } from "@angular/core";
import { FieldType } from "@ngx-formly/core";
import { MatDialog, MatDialogRef } from "@angular/material/dialog";
import { CodeEditorDialogComponent } from "../code-editor-dialog/code-editor-dialog.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";

/**
 * CodeareaCustomTemplateComponent is the custom template for 'codearea' type of formly field.
 *
 * When the formly field type is 'codearea', it overrides the default one line string input template
 * with this component.
 *
 * Clicking on the 'Edit code content' button will create a new dialogue with CodeEditorComponent
 * as its content. The data of this field will be sent to this dialogue, which contains a Monaco editor.
 */
@UntilDestroy()
@Component({
  selector: "texera-codearea-custom-template",
  templateUrl: "./codearea-custom-template.component.html",
  styleUrls: ["./codearea-custom-template.component.scss"],
})
export class CodeareaCustomTemplateComponent extends FieldType {
  dialogRef: MatDialogRef<CodeEditorDialogComponent> | undefined;

  constructor(public dialog: MatDialog, public workflowActionService: WorkflowActionService) {
    super();
  }

  /**
   * Opens the code editor.
   */
  onClickEditor(): void {
    this.dialogRef = this.dialog.open(CodeEditorDialogComponent, {
      data: this.formControl?.value || "",
    });
  }
}
