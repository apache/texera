import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { PropertyEditorComponent } from './property-editor.component';

import { OperatorPredicate } from './../../types/workflow-graph';

import { JointModelService } from './../../service/workflow-graph/model/joint-model.service';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { TexeraModelService } from './../../service/workflow-graph/model/texera-model.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';
import {
  getMockScanPredicate, getMockPoint
} from './../../service/workflow-graph/model/mock-workflow-data';

import { getMockOperatorSchemaList } from './../../service/operator-metadata/mock-operator-metadata.data';

import { marbles } from 'rxjs-marbles';


import {
  JsonSchemaFormModule, MaterialDesignFrameworkModule
} from 'angular2-json-schema-form';


describe('PropertyEditorComponent', () => {
  let component: PropertyEditorComponent;
  let fixture: ComponentFixture<PropertyEditorComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ PropertyEditorComponent ],
      providers : [
        JointModelService,
        JointUIService,
        WorkflowActionService,
        TexeraModelService,
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
    const predicate = getMockScanPredicate();
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
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());
    jointModelService.highlightOperator(getMockScanPredicate().operatorID);

    expect(component.operatorID).toEqual(getMockScanPredicate().operatorID);
    expect(component.currentSchema).toEqual(getMockOperatorSchemaList()[0]);
    expect(component.initialData).toEqual(getMockScanPredicate().operatorProperties);
    expect(component.displayForm).toBeTruthy();

    jointModelService.unhighlightCurrent();

    expect(component.operatorID).toBeFalsy();
    expect(component.currentSchema).toBeFalsy();
    expect(component.initialData).toBeFalsy();
    expect(component.displayForm).toBeFalsy();

  }));

});
