import { WorkflowUtilService } from './../service/workflow-graph/util/workflow-util.service';
import { WorkflowModelActionService } from './../service/workflow-graph/model/workflow-model-action.service';
import { JointModelService } from './../service/workflow-graph/model/jointjs-model.service';
import { Component, OnInit } from '@angular/core';

import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { JointUIService } from '../service/joint-ui/joint-ui.service';
import { StubOperatorMetadataService } from '../service/operator-metadata/stub-operator-metadata.service';
import { TexeraModelService } from '../service/workflow-graph/model/texera-model.service';


@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    // OperatorMetadataService,
    // StubOperatorMetadataService can be used for debugging without start the backend server
    { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },

    JointUIService,
    JointModelService,
    TexeraModelService,
    WorkflowModelActionService,
    WorkflowUtilService
  ]
})
export class WorkspaceComponent implements OnInit {

  constructor(
    texeraModelService: TexeraModelService
  ) { }

  ngOnInit() {
  }

}
