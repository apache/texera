import { Component, OnInit, ChangeDetectorRef } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { WorkflowTexeraGraphService } from '../../service/workflow-graph/model/workflow-texera-graph.service';
import { WorkflowViewEventService } from '../../service/workflow-graph/view-event/workflow-view-event.service';
import { WorkflowModelActionService } from '../../service/workflow-graph/model-action/workflow-model-action.service';
import { OperatorDragDropService } from '../../service/operator-drag-drop/operator-drag-drop.service';


@Component({
  selector: 'texera-property-editor',
  templateUrl: './property-editor.component.html',
  styleUrls: ['./property-editor.component.scss']
})
export class PropertyEditorComponent implements OnInit {

  /*
    Disable two-way data binding of currentPredicate
    to prevent "onChanges" event fired continously.
    currentPredicate won't change as the form value changes
  */
  operatorID: string = null;
  initialData: Object = null;
  currentSchema: OperatorSchema = null;
  formLayout: object = this.generateFormLayout();

  operatorSchemaList: OperatorSchema[] = [];

  displayForm = false;

  formChangeTimes = 0;


  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowTexeraGraphService: WorkflowTexeraGraphService,
    private workflowViewEventService: WorkflowViewEventService,
    private operatorDragDropService: OperatorDragDropService,
    private workflowModelActionService: WorkflowModelActionService
  ) {
    this.operatorMetadataService.metadataChanged$.subscribe(
      value => { this.operatorSchemaList = value.operators; }
    );

    this.workflowViewEventService.operatorSelectedInEditor
      .map(data => this.workflowTexeraGraphService.texeraWorkflowGraph.getOperator(data.operatorID))
      .merge(
        this.operatorDragDropService.operatorDroppedInEditor.map(data => data.operator))
      .distinctUntilKeyChanged('operatorID')
      .subscribe(data => this.changePropertyEditor(data));

    this.workflowTexeraGraphService.operatorDeletedObservable
      .filter(data => data.operator.operatorID === this.operatorID)
      .subscribe(data => this.clearPropertyEditor());

  }

  ngOnInit() {
  }

  clearPropertyEditor() {
    // set displayForm to false in the very beginning
    // hide the view first and then make everything null
    this.displayForm = false;

    this.operatorID = null;
    this.initialData = null;
    this.currentSchema = null;

  }

  changePropertyEditor(operator: OperatorPredicate) {
    console.log('changePropertyEditor called');
    console.log('operatorID: ' + operator.operatorID);
    this.operatorID = operator.operatorID;
    this.currentSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    // make a copy of the property data
    this.initialData = Object.assign({}, operator.operatorProperties);

    // set displayForm to true in the very end
    // initialize all the data first then show the view
    this.displayForm = true;
  }

  // layout for the form
  generateFormLayout(): Object {
    // hide submit button
    return [
      '*',
      { type: 'submit', condition: 'false' }
    ];
  }

  onFormChanges(event: Object) {
    // this.formChangeTimes++;
    // console.log('onform changes called');
    // console.log(event);
    // console.log('called ' + this.formChangeTimes.toString() + ' times');
    this.workflowModelActionService.changeOperatorProperty(this.operatorID, event);
  }

}
