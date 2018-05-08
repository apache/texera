import { JointModelService } from './../../service/workflow-graph/model/joint-model.service';
import { OperatorSchema } from './../../types/operator-schema';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorPredicate } from './../../types/workflow-graph';
import { TexeraModelService } from './../../service/workflow-graph/model/texera-model.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { Component, OnInit } from '@angular/core';

import {
  JsonSchemaFormModule, MaterialDesignFrameworkModule
} from 'angular2-json-schema-form';

import cloneDeep from 'lodash-es/cloneDeep';


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
  operatorID: string | undefined;
  initialData: Object | undefined;
  currentSchema: OperatorSchema | undefined;
  formLayout: object = PropertyEditorComponent.generateFormLayout();

  operatorSchemaList: OperatorSchema[] = [];

  displayForm = false;

  formChangeTimes = 0;


  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private jointModelService: JointModelService,
    private texeraModelService: TexeraModelService,
    private workflowActionService: WorkflowActionService
  ) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => { this.operatorSchemaList = value.operators; }
    );

    this.jointModelService.onJointCellHighlight()
      .filter(value => value.operatorID !== this.operatorID)
      .map(value => this.texeraModelService.getTexeraGraph().getOperator(value.operatorID))
      .subscribe(
        operator => this.changePropertyEditor(operator)
      );

    this.jointModelService.onJointCellUnhighlight()
      .filter(value => value.operatorID === this.operatorID)
      .subscribe(value => this.clearPropertyEditor());


  }

  ngOnInit() {
  }

  clearPropertyEditor() {
    // set displayForm to false in the very beginning
    // hide the view first and then make everything null
    this.displayForm = false;

    this.operatorID = undefined;
    this.initialData = undefined;
    this.currentSchema = undefined;

  }

  changePropertyEditor(operator: OperatorPredicate) {
    console.log('changePropertyEditor called');
    console.log('operatorID: ' + operator.operatorID);
    this.operatorID = operator.operatorID;
    this.currentSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);
    // make a copy of the property data
    this.initialData = cloneDeep(operator.operatorProperties);

    // set displayForm to true in the end
    //  because we need to initialize all the data first then show the view
    this.displayForm = true;
  }


  onFormChanges(formData: Object) {
    this.formChangeTimes++;
    console.log('onform changes called');
    console.log(formData);
    console.log('called ' + this.formChangeTimes.toString() + ' times');
    if (!this.operatorID) {
      throw new Error(`Property Editor component not binded with the current operatorID`);
    }
    this.workflowActionService.changeOperatorProperty(this.operatorID, formData);
  }

  // layout for the form
  private static generateFormLayout(): Object {
    // hide submit button
    return [
      '*',
      { type: 'submit', condition: 'false' }
    ];
  }

}
