import { Component, OnInit } from '@angular/core';

import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { JointUIService } from '../service/joint-ui/joint-ui.service';
import { StubOperatorMetadataService } from '../service/operator-metadata/stub-operator-metadata.service';


@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    OperatorMetadataService,
    // StubOperatorMetadataService can be used for debugging without start the backend server
    // { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },

    JointUIService
  ]
})
export class WorkspaceComponent implements OnInit {

  constructor(
  ) { }

  ngOnInit() {
  }

}
