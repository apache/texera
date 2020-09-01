import { Component } from '@angular/core';
import { FieldType } from '@ngx-formly/core';

@Component({
  selector: 'texera-code-edit-panel',
  templateUrl: './code-edit-panel.component.html',
  styleUrls: ['./code-edit-panel.component.scss']
})
export class CodeEditPanelComponent extends FieldType {
  text: string = '';
}
