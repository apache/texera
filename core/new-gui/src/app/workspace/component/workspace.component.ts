import { Component, OnInit } from '@angular/core';

import { NavigationComponent } from './navigation/navigation.component';
import { OperatorPanelComponent } from './operator-panel/operator-panel.component';
import { PropertyEditorComponent } from './property-editor/property-editor.component';
import { WorkflowEditorComponent } from './workflow-editor/workflow-editor.component';
import { ResultPanelComponent } from './result-panel/result-panel.component';
import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';



@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    OperatorMetadataService,
  ]
})
export class WorkspaceComponent implements OnInit {

  constructor(
    private operatorMetadataService: OperatorMetadataService,
  ) { }

  ngOnInit() {
    // do the following things at app initialization time:

    // fetch the operator metadata from the backend
    this.operatorMetadataService.fetchAllOperatorMetadata();
  }

}
