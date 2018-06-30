import { Component, OnInit } from '@angular/core';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';

import { OperatorSchema, OperatorMetadata, GroupInfo } from '../../types/operator-schema.interface';

/**
 * OperatorViewComponent is the left-side panel that shows the operators.
 *
 * This component gets all the operator metadata from OperatorMetaDataService,
 *  and then displays the operators, which are grouped using their group name from the metadata.
 *
 * Clicking a group name reveals the operators in the group, each operator is a sub-component: OperatorLabelComponent,
 *  this is implemented using Angular Material's expansion panel component: https://material.angular.io/components/expansion/overview
 *
 *
 * @author Bolin Chen
 * @author Zuozhi Wang
 *
 */
@Component({
  selector: 'texera-operator-panel',
  templateUrl: './operator-panel.component.html',
  styleUrls: ['./operator-panel.component.scss'],
  providers: [
    // uncomment this line for manual testing without opening backend server
    // { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
  ]
})
export class OperatorPanelComponent implements OnInit {

  // a list of all operator's schema
  public operatorSchemaList: ReadonlyArray<OperatorSchema> = [];
  // a list of group names, sorted based on the groupOrder from OperatorMetadata
  public groupNamesOrdered: ReadonlyArray<string> = [];
  // a map of group name to a list of operator schema of this group
  public operatorGroupMap = new Map<string, ReadonlyArray<OperatorSchema>>();


  constructor(
    private operatorMetadataService: OperatorMetadataService
  ) {
  }

  ngOnInit() {
    // subscribe to the operator metadata changed observable and process it
    // the operator metadata will be fetched asynchronously on application init
    //   after the data is fetched, it will be passed through this observable
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => this.processOperatorMetadata(value)
    );
  }

  /**
   * populate the class variables based on the operator metadata fetched from the backend:
   *  - sort the group names based on the group order
   *  - put the operators into the hashmap of group names
   *
   * @param operatorMetadata metadata of all operators
   */
  private processOperatorMetadata(operatorMetadata: OperatorMetadata): void {
    this.operatorSchemaList = operatorMetadata.operators;
    this.groupNamesOrdered = getGroupNamesSorted(operatorMetadata.groups);
    this.operatorGroupMap = getOperatorGroupMap(operatorMetadata);
  }

}

// generates a list of group names sorted by the orde
// slice() will make a copy of the list, because we don't want to sort the orignal list
export function getGroupNamesSorted(groupInfoList: ReadonlyArray<GroupInfo>): string[] {

  return groupInfoList.slice()
    .sort((a, b) => (a.groupOrder - b.groupOrder))
    .map(groupInfo => groupInfo.groupName);
}

// returns a new empty map from the group name to a list of OperatorSchema
export function getOperatorGroupMap(
  operatorMetadata: OperatorMetadata): Map<string, OperatorSchema[]> {

  const groups = operatorMetadata.groups.map(groupInfo => groupInfo.groupName);
  const operatorGroupMap = new Map<string, OperatorSchema[]>();
  groups.forEach(
    groupName => {
      const operators = operatorMetadata.operators.filter(x => x.additionalMetadata.operatorGroupName === groupName);
      operatorGroupMap.set(groupName, operators);
    }
  );
  return operatorGroupMap;
}
