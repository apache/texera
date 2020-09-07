import { Component, ChangeDetectorRef } from '@angular/core';
import { FieldType } from '@ngx-formly/core';
import { MatDialog, MatDialogConfig } from '@angular/material/dialog';
import { CodeEditorDialogComponent } from '../code-editor-dialog/code-editor-dialog.component';

/**
 * CodeareaCustomTemplateComponent is the custom template for 'codearea' type of formly field.
 *
 * When the formly field type is 'codearea', it overrides the default one line string input template
 * with this component.
 *
 * Clicking on the 'Edit code content' button will create a new dialogue with CodeEditorComponent
 * as its content. The data of this field will be sent to this dialogue, which contains a Ace editor.
 * After the editor is closed, the text content of the editor will be sent back and override the data
 * of the field. If the field initially contains no data, then an empty string input will be sent instead.
 * @author Xiaozhen Liu
 */
@Component({
  selector: 'texera-codearea-custom-template',
  templateUrl: './codearea-custom-template.component.html',
  styleUrls: ['./codearea-custom-template.component.scss']
})
export class CodeareaCustomTemplateComponent extends FieldType {

  constructor(public dialog: MatDialog) {
    super();
  }

  onClickEditor(): void {
    const dialogConfig = new MatDialogConfig();
    dialogConfig.minWidth = '1000px';
    dialogConfig.data = this?.formControl?.value || '';

    const dialogRef = this.dialog.open(CodeEditorDialogComponent, dialogConfig);

    dialogRef.afterClosed().subscribe(
      data => {
        this.formControl.setValue(data);
       }
    );
  }

}
