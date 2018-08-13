import { Component, OnInit, EventEmitter, Output } from '@angular/core';

import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';

import { OperatorLabelComponent } from './operator-label/operator-label.component';
import { FormControl } from '@angular/forms';
import { Observable } from 'rxjs/Observable';
import { startWith } from 'rxjs/operators/startWith';
import { map } from 'rxjs/operators/map';

import * as Fuse from 'fuse.js';
import { MatAutocompleteSelectedEvent } from '@angular/material';
import { WorkflowModelActionService } from '../../service/workflow-graph/model-action/workflow-model-action.service';
import { WorkflowGraphUtilsService } from '../../service/workflow-graph/utils/workflow-graph-utils.service';
import { WorkflowViewEventService } from '../../service/workflow-graph/view-event/workflow-view-event.service';
import { SmartOperatorLocation } from './smart-operator-location';
import { WorkflowTexeraGraphService } from '../../service/workflow-graph/model/workflow-texera-graph.service';
import { WorkflowJointGraphService } from '../../service/workflow-graph/model/workflow-joint-graph.service';

/**
 * OperatorViewComponent is the left-side panel that shows the operators.
 *
 * The operators are grouped using their group name from the operator metadata,
 *  clicking a group name reveals the operators in the group, each operator is a sub-component: OperatorLabelComponent.
 *
 * The component has an input box to search the operators.
 * Search is full text search and fuzzy search, implemented using fuse.js http://fusejs.io/
 * Search results are shown in the autocomplete dropdown list,
 *  pressing "Enter" or clicking a suggestion will creates an operator in the main workflow editor panel.
 *
 *
 * @author Bolin Chen
 * @author Zuozhi Wang
 *
 */
@Component({
  selector: 'texera-operator-view',
  templateUrl: './operator-view.component.html',
  styleUrls: ['./operator-view.component.scss']
})
export class OperatorViewComponent implements OnInit {

  searchOperatorForm: FormControl = new FormControl();

  filteredOptions = this.searchOperatorForm.valueChanges.pipe(
    startWith<string | OperatorSchema>(''),
    map(value => typeof value === 'string' ? value : value.additionalMetadata.userFriendlyName),
    map(name => this.findOperatorName(name))
  );

  optionId = '';

  currentExpand: string;

  inputMonitor = '';

  public operatorSchemaList: OperatorSchema[] = [];
  public groupNamesOrdered: string[] = [];
  public operatorGroupMap = new Map<string, OperatorSchema[]>();

  private smartOperatorLocation: SmartOperatorLocation;

  private readonly fuseSearchOptions: Fuse.FuseOptions = {
    tokenize: true,
    matchAllTokens: true,
    threshold: 0.2,
    keys: ['additionalMetadata.userFriendlyName'],
    minMatchCharLength: 2,
    location: 0,
  };
  public fuseSearch = new Fuse(this.operatorSchemaList, this.fuseSearchOptions);

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowModelActionService: WorkflowModelActionService,
    private workflowGraphUtilsService: WorkflowGraphUtilsService,
    private workflowViewEventService: WorkflowViewEventService,
    private workflowTexeraGraphService: WorkflowTexeraGraphService,
    private workflowJointGraphService: WorkflowJointGraphService
  ) {
    operatorMetadataService.metadataChanged$.subscribe(value => this.processOperatorMetadata(value));
    this.smartOperatorLocation = new SmartOperatorLocation(this.workflowTexeraGraphService, this.workflowJointGraphService);
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
        <[string, OperatorSchema[]]> [groupName,
          operatorMetadata.operators.filter(x => x.additionalMetadata.operatorGroupName === groupName)]
      )
    );

    this.fuseSearch = new Fuse(this.operatorSchemaList, this.fuseSearchOptions);
  }

  findOperatorName(query: string): OperatorSchema[] {
    // console.log(query);
    const searchResult: OperatorSchema[] = this.fuseSearch.search(query);
    // console.log(searchResult);
    return searchResult;
  }

  onAutocompleteOptionSelected(event: MatAutocompleteSelectedEvent) {
    this.inputMonitor = '';
    const operator = this.workflowGraphUtilsService.getNewOperatorPredicate(event.option.value.operatorType);
    const smartLocation = this.smartOperatorLocation.suggestNextLocation(operator.operatorType);
    this.workflowModelActionService.addOperator(
      operator, smartLocation.x, smartLocation.y);
    this.workflowViewEventService.operatorSelectedInEditor.next({operatorID: operator.operatorID});
  }


  HighlightSelection(option) {
    const operatorLabelID = 'texera-operator-label-'  + option.operatorType;
    if (this.optionId) {
      document.getElementById(this.optionId).style.backgroundColor = '';
    }
    document.getElementById(operatorLabelID).style.backgroundColor = '#ed5281';

    this.optionId = operatorLabelID;
    this.currentExpand = option.additionalMetadata.operatorGroupName.toLowerCase();

  }

  removeSelection() {
    if (this.inputMonitor.length === 0 && this.optionId !== '') {
      document.getElementById(this.optionId).style.backgroundColor = '';
    }
  }

  displayOperatorName(value: OperatorSchema): string {
    if (! value) {
      return '';
    }
    return value.additionalMetadata.userFriendlyName;
  }

}

