import { Component, OnInit, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

/**
 * CodeEditorDialogComponent is the content of the dialogue invoked by CodeareaCustomTemplateComponent.
 *
 * It contains an Ace editor which is inside a resizable mat-dialog-content. When the dialogue is invoked by
 * the button in CodeareaCustomTemplateComponent, the data of the custom field (or empty String if no data)
 * will be sent to the Ace editor as its text. The dialogue can be closed with ESC key or by clicking on areas outside
 * the dialogue. Closing the dialogue will send the eidted contend back to the custom template field.
 * @author Xiaozhen Liu
 */
@Component({
  selector: 'texera-code-editor-dialog',
  templateUrl: './code-editor-dialog.component.html',
  styleUrls: ['./code-editor-dialog.component.scss']
})
export class CodeEditorDialogComponent implements OnInit {

  aclOptions = {
    enableBasicAutocompletion: true,
    enableSnippets: true,
    enableLiveAutocompletion: true,
    maxLines: 80,
    minLines: 20,
    autoScrollEditorIntoView: false,
    highlightActiveLine: true,
    highlightSelectedWord: true,
    highlightGutterLine: true,
    animatedScroll: true
  };

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
