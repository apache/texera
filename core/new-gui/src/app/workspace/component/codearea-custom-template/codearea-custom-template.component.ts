import { Component, ChangeDetectorRef } from '@angular/core';
import { FieldType } from '@ngx-formly/core';
import { MatDialog, MatDialogConfig } from '@angular/material/dialog';
import { CodeEditorDialogComponent } from '../code-editor-dialog/code-editor-dialog.component';

/**
 * CodeareaCustomTemplateComponent is the custom template for 'codearea' type of formly field.
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
    dialogConfig.minHeight = '600px';
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
