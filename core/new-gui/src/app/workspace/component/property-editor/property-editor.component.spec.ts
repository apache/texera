import { Observable } from 'rxjs/Observable';
import { JsonSchemaFormModule, MaterialDesignFrameworkModule } from 'angular2-json-schema-form';
import { async, ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';

import { PropertyEditorComponent } from './property-editor.component';

import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';

import { getMockOperatorSchemaList } from './../../service/operator-metadata/mock-operator-metadata.data';

import { marbles } from 'rxjs-marbles';


import { mockScanPredicate, mockPoint } from '../../service/workflow-graph/model/mock-workflow-data';
import { OperatorPredicate } from '../../types/workflow-common.interface';
import { HotObservable } from 'rxjs/testing/HotObservable';
import { TestScheduler } from 'rxjs/testing/TestScheduler';


describe('PropertyEditorComponent', () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;
  let workflowActionService: WorkflowActionService;
  let jointUIService: JointUIService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [PropertyEditorComponent],
      providers: [
        JointUIService,
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ],
      imports: [
        MaterialDesignFrameworkModule,
        JsonSchemaFormModule
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(PropertyEditorComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.get(WorkflowActionService);
    jointUIService = TestBed.get(JointUIService);

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should receive operator json schema from operator metadata service', () => {
    expect(component.operatorSchemaList.length).toEqual(getMockOperatorSchemaList().length);
  });

  /**
   * check if the changePropertyEditor called after the operator
   *  is highlighted has correctly update the variables
   */
  it('should change the content of property editor from an empty panel correctly', () => {
    const predicate = mockScanPredicate;
    const currentSchema = component.operatorSchemaList.find(schema => schema.operatorType === predicate.operatorType);

    // check if the changePropertyEditor called after the operator
    //  is highlighted has correctly update the variables
    component.changePropertyEditor(predicate);

    expect(component.currentOperatorID).toEqual(predicate.operatorID);
    expect(component.currentOperatorSchema).toEqual(currentSchema);
    expect(component.currentOperatorInitialData).toEqual(predicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

  });

  it('should clear the property editor panel correctly', () => {
    const predicate = mockScanPredicate;
    const currentSchema = component.operatorSchemaList.find(schema => schema.operatorType === predicate.operatorType);

    component.changePropertyEditor(predicate);

    // check if the clearPropertyEditor called after the operator
    //  is unhighlighted has correctly update the variables
    component.clearPropertyEditor();

    expect(component.currentOperatorID).toBeFalsy();
    expect(component.currentOperatorSchema).toBeFalsy();
    expect(component.currentOperatorInitialData).toBeFalsy();
    expect(component.displayForm).toBeFalsy();
  })

  it('should capture highlight and unhighlight event from JointJS paper', () => {

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

  });


  it('should change Texera graph property when the form is edited by the user', fakeAsync(() => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highligh the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    // stimulate a form change by the user
    const formChangeValue = { tableName: 'twitter_sample' };
    component.onFormChanges(formChangeValue)

    // tick first, wait for the set property to be done
    tick(PropertyEditorComponent.formInputDebounceTime + 10);

    // then get the opeator, because operator is immutable, the operator before the tick
    //   is a different object reference from the operator after the tick
    const operator = workflowActionService.getTexeraGraph().getOperator(mockScanPredicate.operatorID);
    if (!operator) {
      throw new Error(`operator ${mockScanPredicate.operatorID} is undefined`);
    }
    expect(operator.operatorProperties).toEqual(formChangeValue);
  }));

  it('should debounce the user form input to avoid emitting event too frequently', marbles(m => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add an operator and highligh the operator so that the
    //  variables in property editor component is set correctly
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    // prepare the form user input event stream
    // simulate user types in `table` character by character
    const formUserInputMarbleString = '-a-b-c-d-e';
    const formUserInputMarbleValue = {
      a: { tableName: 't' },
      b: { tableName: 'ta' },
      c: { tableName: 'tab' },
      d: { tableName: 'tabl' },
      e: { tableName: 'table' },
    }
    const formUserInputEventStream = m.hot(formUserInputMarbleString, formUserInputMarbleValue);

    // prepare the expected output stream after debounce time
    const formChangeEventMarbleStrig =
      // wait for the time of last marble string starting to emit
      '-'.repeat(formUserInputMarbleString.length - 1) +
      // then wait for debounce time (each tick represents 10 ms)
      '-'.repeat(PropertyEditorComponent.formInputDebounceTime / 10) +
      'e-';
    const formChangeEventMarbleValue = {
      e: { tableName: 'table' } as object
    };
    const expectedFormChangeEventStream = m.hot(formChangeEventMarbleStrig, formChangeEventMarbleValue);


    m.bind();

    const actualFormChangeEventStream = component.createOutputFormChangeEventStream(formUserInputEventStream);
    formUserInputEventStream.subscribe();

    m.expect(actualFormChangeEventStream).toBeObservable(expectedFormChangeEventStream);

  }));

});
