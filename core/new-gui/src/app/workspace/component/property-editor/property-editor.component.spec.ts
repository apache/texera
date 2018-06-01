import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PropertyEditorComponent } from './property-editor.component';

import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';

import { getMockOperatorSchemaList } from './../../service/operator-metadata/mock-operator-metadata.data';

import { marbles } from 'rxjs-marbles';


import { mockScanPredicate, mockPoint } from '../../service/workflow-graph/model/mock-workflow-data';
import { OperatorPredicate } from '../../types/workflow-common.interface';


describe('PropertyEditorComponent', () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PropertyEditorComponent ],
      providers : [
        JointUIService,
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PropertyEditorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should change or clear the content of property editor correctly', () => {
    const predicate = mockScanPredicate;
    const currentSchema = component.operatorSchemaList.find(schema => schema.operatorType === predicate.operatorType);

    // check if the changePropertyEditor called after the operator
    //  is highlighted has correctly update the variables
    component.changePropertyEditor(predicate);

    expect(component.currentOperatorID).toEqual(predicate.operatorID);
    expect(component.currentOperatorSchema).toEqual(currentSchema);
    expect(component.currentOperatorInitialData).toEqual(predicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    // check if the clearPropertyEditor called after the operator
    //  is unhighlighted has correctly update the variables
    component.clearPropertyEditor();

    expect(component.currentOperatorID).toBeFalsy();
    expect(component.currentOperatorSchema).toBeFalsy();
    expect(component.currentOperatorInitialData).toBeFalsy();
    expect(component.displayForm).toBeFalsy();

  });

  it('should receive operator metadata from the service', () => {
    expect(component.operatorSchemaList.length).toEqual(getMockOperatorSchemaList().length);
  });

  it('should capture highlight and unhighlight event from JointJS paper', marbles(() => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointUIService: JointUIService = TestBed.get(JointUIService);
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // check if the highlight function successfully updated the
    //  variables inside property editor component

    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    expect(component.currentOperatorID).toEqual(mockScanPredicate.operatorID);
    expect(component.currentOperatorSchema).toEqual(getMockOperatorSchemaList()[0]);
    expect(component.currentOperatorInitialData).toEqual(mockScanPredicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    // check if the unhighlight function successfully updated the
    //  variables inside property editor component
    jointGraphWrapper.unhighlightCurrent();

    expect(component.currentOperatorID).toBeFalsy();
    expect(component.currentOperatorSchema).toBeFalsy();
    expect(component.currentOperatorInitialData).toBeFalsy();
    expect(component.displayForm).toBeFalsy();

  }));


  it('should change Texera graph property when the form is edited by the user', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointUIService: JointUIService = TestBed.get(JointUIService);
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highligh the operator so that the
    //  variables in property editor component is setted correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    // stimulate a form change by the user
    const sampleFormChange = {tableName: 'twitter_sample'};
    component.onFormChanges(sampleFormChange);

    const operator: OperatorPredicate | undefined =
      workflowActionService.getTexeraGraph().getOperator(mockScanPredicate.operatorID);

    // check if the operator exist (might be undefined if operator
    //  is not added or is deleted previously)
    expect(operator).toBeTruthy();

    // check if the updated property of the operator equals to the
    //  changed user made in the form
    if (operator !== undefined) {
      const operatorProperty = operator.operatorProperties;
      expect(operatorProperty).toEqual(sampleFormChange);
    }

  }));


});
