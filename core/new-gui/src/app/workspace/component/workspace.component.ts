import { Component, OnInit } from '@angular/core';

import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { OperatorViewElementService } from '../service/operator-view-element/operator-view-element.service';
import { StubOperatorMetadataService } from '../service/operator-metadata/stub-operator-metadata.service';


@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    // OperatorMetadataService,
    { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },

    OperatorViewElementService
  ]
})
export class WorkspaceComponent implements OnInit {

  constructor(
  ) { }

  ngOnInit() {
  }

}
