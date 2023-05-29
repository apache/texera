import { AfterViewInit, Component } from "@angular/core";
import { FieldType } from "@ngx-formly/core";
import { MatDialog, MatDialogRef } from "@angular/material/dialog";
import { CodeEditorDialogComponent } from "../code-editor-dialog/code-editor-dialog.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";

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
export class CodeareaCustomTemplateComponent extends FieldType<any> implements AfterViewInit {
  dialogRef: MatDialogRef<CodeEditorDialogComponent> | undefined;
  readonly: boolean = false;

  constructor(public dialog: MatDialog, private coeditorPresenceService: CoeditorPresenceService) {
    super();
    this.handleShadowingMode();
  }

  ngAfterViewInit() {
    this.handleReadonlyStatusChange();
  }

  /**
   * Syncs the disabled status of the button with formControl.
   * Used to fit the unit test since undefined might occur.
   * TODO: Using <code>formControl</code> here instead of
   *  <code>WorkflowActionService.checkWorkflowModificationEnabled()</code>
   *  since the readonly status of operator properties also locally depend on whether "Unlock for Logic Change"
   *  is enabled, which can only be accessed via <code>formControl</code>. Might need a more unified solution.
   */
  handleReadonlyStatusChange(): void {
    if (this.field === undefined) return;
    this.field.formControl.statusChanges.pipe(untilDestroyed(this)).subscribe(() => {
      this.readonly = this.field.formControl.disabled;
    });
  }

  /**
   * Opens the code editor.
   */
  onClickEditor(): void {
    this.dialogRef = this.dialog.open(CodeEditorDialogComponent, {
      id: "mat-dialog-udf",
      maxWidth: "95vw",
      maxHeight: "95vh",
      data: this.field.formControl,
    });
  }

  /**
   * When shadowing a co-editor, this method handles the opening/closing of the code editor based on the co-editor.
   * @private
   */
  private handleShadowingMode(): void {
    this.coeditorPresenceService
      .getCoeditorOpenedCodeEditorSubject()
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.onClickEditor();
      });
    this.coeditorPresenceService
      .getCoeditorClosedCodeEditorSubject()
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.dialogRef?.close();
      });
  }
}
