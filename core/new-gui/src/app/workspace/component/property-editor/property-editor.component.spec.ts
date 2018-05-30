import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PropertyEditorComponent } from './property-editor.component';


import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';

import { getMockOperatorSchemaList } from './../../service/operator-metadata/mock-operator-metadata.data';

import { marbles } from 'rxjs-marbles';


import {
  JsonSchemaFormModule, MaterialDesignFrameworkModule
} from 'angular2-json-schema-form';
import { mockScanPredicate, mockPoint } from '../../service/workflow-graph/model/mock-workflow-data';


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
      ],
      imports: [
        MaterialDesignFrameworkModule,
        JsonSchemaFormModule.forRoot(MaterialDesignFrameworkModule)
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


    component.changePropertyEditor(predicate);

    expect(component.operatorID).toEqual(predicate.operatorID);
    expect(component.currentSchema).toEqual(currentSchema);
    expect(component.initialData).toEqual(predicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    component.clearPropertyEditor();

    expect(component.operatorID).toBeFalsy();
    expect(component.currentSchema).toBeFalsy();
    expect(component.initialData).toBeFalsy();
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
    jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    expect(component.operatorID).toEqual(mockScanPredicate.operatorID);
    expect(component.currentSchema).toEqual(getMockOperatorSchemaList()[0]);
    expect(component.initialData).toEqual(mockScanPredicate.operatorProperties);
    expect(component.displayForm).toBeTruthy();

    jointGraphWrapper.unhighlightCurrent();

    expect(component.operatorID).toBeFalsy();
    expect(component.currentSchema).toBeFalsy();
    expect(component.initialData).toBeFalsy();
    expect(component.displayForm).toBeFalsy();

  }));

});
