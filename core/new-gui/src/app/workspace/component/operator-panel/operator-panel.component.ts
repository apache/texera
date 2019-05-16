import { Component, OnInit } from '@angular/core';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';

import { OperatorSchema, OperatorMetadata, GroupInfo } from '../../types/operator-schema.interface';

import {FormControl} from '@angular/forms';
import {Observable} from 'rxjs';

import { Point } from "../../types/workflow-common.interface";

import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";

// Requires --esModuleInterop compiler flag!
// import * as Fuse with '--allowSyntheticDefaultImport'
// or import Fuse = require('fuse.js') with neither
import * as Fuse from 'fuse.js';
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


  myControl = new FormControl();
  options: string[] = []
  filteredOptions: Observable<string[]> | undefined;


  // a list of all operator's schema
  public operatorSchemaList: ReadonlyArray<OperatorSchema> = [];
  // a list of group names, sorted based on the groupOrder from OperatorMetadata
  public groupNamesOrdered: ReadonlyArray<string> = [];
  // a map of group name to a list of operator schema of this group
  public operatorGroupMap = new Map<string, ReadonlyArray<OperatorSchema>>();


  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService
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



    this.filteredOptions = this.myControl.valueChanges
      .map(value => this._filter(value));
    }

  private _filter(value: string): string[] {

    type userFriendlyNamesType = {
      title: string;
    };

    const userFriendlyNames = this.operatorSchemaList.map(value => value.additionalMetadata.userFriendlyName);

    const new_userFriendlyNames : userFriendlyNamesType[] = []
    for (let entry of userFriendlyNames) {
      // console.log(entry); 
      new_userFriendlyNames.push({'title':entry})
    }

    const filterValue = value.toLowerCase();

    const options: Fuse.FuseOptions<userFriendlyNamesType> = {
      location:0,
      distance:1000,
      threshold: 0.3,
      keys: ['title']
    };
    const fuse = new Fuse(new_userFriendlyNames,options);
    //userFriendlyNames.filter(option => option.toLowerCase().includes(filterValue));
    console.log('fuse');
    console.log(fuse);
    const result = fuse.search(filterValue);
    console.log('filterValue');
    console.log(filterValue);
    console.log('result');
    console.log(result);
    //userFriendlyNames.filter(option => option.toLowerCase().includes(filterValue));

    const final_result = []
    for (let entry of result) {
       console.log(entry); 
       final_result.push(entry.title);
    }

    return final_result;
  }

  public OnHumanSelected(option: string) {
    // display the operator on the workflow
    console.log(option);
    const currentType = this.operatorSchemaList.filter(
      schema => {
        return schema.additionalMetadata.userFriendlyName === option;
      }
    ).map(schema => schema.operatorType)[0];
    console.log(currentType);
    const selectedOperatorPredicate = this.workflowUtilService.getNewOperatorPredicate(currentType);
    console.log(selectedOperatorPredicate);
    this.workflowActionService.addOperator(selectedOperatorPredicate, {x:600, y:399});
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
