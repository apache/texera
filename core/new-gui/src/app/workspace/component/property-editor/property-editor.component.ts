import { OperatorSchema } from './../../types/operator-schema.interface';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { Component, OnInit } from '@angular/core';

import {
  JsonSchemaFormModule, MaterialDesignFrameworkModule
} from 'angular2-json-schema-form';

import cloneDeep from 'lodash-es/cloneDeep';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';
import { OperatorPredicate } from '../../types/workflow-common.interface';

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
  public operatorID: string | undefined;
  public initialData: object | undefined;
  public currentSchema: OperatorSchema | undefined;
  public operatorSchemaList: ReadonlyArray<OperatorSchema> = [];
  public displayForm = false;


  private formLayout: object = PropertyEditorComponent.generateFormLayout();
  private formChangeTimes = 0;
  private jsonSchemaOnFormChangeStream = new Subject<object>();

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService
  ) {
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => { this.operatorSchemaList = value.operators; }
    );

    this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream()
      .filter(value => value.operatorID !== this.operatorID)
      .map(value => this.workflowActionService.getTexeraGraph().getOperator(value.operatorID))
      .subscribe(
        operator => this.changePropertyEditor(operator)
      );

    this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
      .filter(value => value.operatorID === this.operatorID)
      .subscribe(value => this.clearPropertyEditor());


  }

  ngOnInit() {
    this.jsonSchemaOnFormChangeStream.subscribe(
      formData => {
        this.handleFormChange(formData);
      }
    );
  }

  public clearPropertyEditor(): void {
    // set displayForm to false in the very beginning
    // hide the view first and then make everything null
    this.displayForm = false;

    this.operatorID = undefined;
    this.initialData = undefined;
    this.currentSchema = undefined;

  }

  public changePropertyEditor(operator: OperatorPredicate | undefined): void {
    if (! operator) {
      throw new Error(`change property editor: operator is undefined`);
    }

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


  public onFormChanges(formData: object): void {
    this.jsonSchemaOnFormChangeStream.next(formData);
  }

  private handleFormChange(formData: object): void {
    this.formChangeTimes++;
    console.log('onform changes called');
    console.log(formData);
    console.log('called ' + this.formChangeTimes.toString() + ' times');
    if (!this.operatorID) {
      throw new Error(`Property Editor component not binded with the current operatorID`);
    }
    this.workflowActionService.setOperatorProperty(this.operatorID, formData);
  }

  // layout for the form
  private static generateFormLayout(): object {
    // hide submit button
    return [
      '*',
      { type: 'submit', condition: 'false' }
    ];
  }

}
