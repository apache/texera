import { Component, OnInit } from '@angular/core';

import { NavigationComponent } from './navigation/navigation.component';
import { OperatorPanelComponent } from './operator-panel/operator-panel.component';
import { PropertyEditorComponent } from './property-editor/property-editor.component';
import { WorkflowEditorComponent } from './workflow-editor/workflow-editor.component';
import { ResultPanelComponent } from './result-panel/result-panel.component';



@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss']
})
export class WorkspaceComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }

}
