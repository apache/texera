import { Component, ChangeDetectorRef } from '@angular/core';
import { FieldType } from '@ngx-formly/core';
import { MatDialog, MatDialogConfig } from '@angular/material/dialog';
import { CodeEditorDialogComponent } from '../code-editor-dialog/code-editor-dialog.component';

@Component({
  selector: 'texera-code-edit-panel',
  templateUrl: './code-edit-panel.component.html',
  styleUrls: ['./code-edit-panel.component.scss']
})
export class CodeEditPanelComponent extends FieldType {
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
