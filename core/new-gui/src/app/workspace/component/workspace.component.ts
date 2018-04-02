import { Component, OnInit } from '@angular/core';

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
  }

}
