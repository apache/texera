import { Component, OnInit, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

@Component({
  selector: 'texera-code-editor-dialog',
  templateUrl: './code-editor-dialog.component.html',
  styleUrls: ['./code-editor-dialog.component.scss']
})
export class CodeEditorDialogComponent implements OnInit {

  text: string;

  constructor(
    private dialogRef: MatDialogRef<CodeEditorDialogComponent>,
    @Inject(MAT_DIALOG_DATA) data: any) {
      this.text = data;
    }

  ngOnInit() {
    this.dialogRef.keydownEvents().subscribe(event => {
      if (event.key === 'Escape') {
          this.onCancel();
      }
  });

  this.dialogRef.backdropClick().subscribe(event => {
      this.onCancel();
  });
  }

  onCancel(): void {
    this.dialogRef.close(this.text);
}

}
