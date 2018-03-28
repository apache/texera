import { Component, OnInit } from '@angular/core';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';

@Component({
  selector: 'texera-operator-panel',
  templateUrl: './operator-panel.component.html',
  styleUrls: ['./operator-panel.component.scss']
})
export class OperatorPanelComponent implements OnInit {

  public operatorSchemaList: OperatorSchema[] = [];
  public groupNamesOrdered: string[] = [];
  public operatorGroupMap = new Map<string, OperatorSchema[]>();


  constructor(
    private operatorMetadataService: OperatorMetadataService
  ) {
    this.operatorMetadataService.metadataChanged$.subscribe(
      value => this.processOperatorMetadata(value)
    );

  }

  ngOnInit() {
  }

  private processOperatorMetadata(operatorMetadata: OperatorMetadata): void {

    this.operatorSchemaList = operatorMetadata.operators;

    this.groupNamesOrdered = operatorMetadata.groups.slice()
      .sort((a, b) => (a.groupOrder - b.groupOrder))
      .map(groupOrder => groupOrder.groupName);

    this.operatorGroupMap = new Map(
      this.groupNamesOrdered.map(groupName =>
        <[string, OperatorSchema[]]>[groupName,
          operatorMetadata.operators.filter(x => x.additionalMetadata.operatorGroupName === groupName)]
      )
    );
  }

}
