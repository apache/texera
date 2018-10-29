import { WorkflowUtilService } from './../../../service/workflow-graph/util/workflow-util.service';
import { JointUIService } from './../../../service/joint-ui/joint-ui.service';
import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorLabelComponent } from './operator-label.component';
import { OperatorMetadataService } from '../../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../../service/operator-metadata/stub-operator-metadata.service';

import { CustomNgMaterialModule } from '../../../../common/custom-ng-material.module';
import { mockOperatorSchemaList } from '../../../service/operator-metadata/mock-operator-metadata.data';
import { By } from '@angular/platform-browser';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { NgbModule, NgbTooltip} from '@ng-bootstrap/ng-bootstrap';
import { DebugElement } from '../../../../../../node_modules/@angular/core';
import { detectChanges } from '../../../../../../node_modules/@angular/core/src/render3';
import { by } from '../../../../../../node_modules/protractor';

describe('OperatorLabelComponent', () => {
  const mockOperatorData = mockOperatorSchemaList[0];
  let component: OperatorLabelComponent;
  let fixture: ComponentFixture<OperatorLabelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorLabelComponent],
      imports: [CustomNgMaterialModule, NgbModule],
      providers: [
        DragDropService,
        JointUIService,
        WorkflowUtilService,
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorLabelComponent);
    component = fixture.componentInstance;

    // use one mock operator schema as input to construct the operator label
    component.operator = mockOperatorData;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should generate an ID for the component DOM element', () => {
    expect(component.operatorLabelID).toContain('texera-operator-label-');
  });

  it('should display operator user friendly name on the UI', () => {
    const element = <HTMLElement>(fixture.debugElement.query(By.css('.texera-operator-label-body')).nativeElement);
    expect(element.innerHTML.trim()).toEqual(mockOperatorData.additionalMetadata.userFriendlyName);
  });

  it('should register itself as a draggable element', () => {
    const jqueryElement = jQuery(`#${component.operatorLabelID}`);
    expect(jqueryElement.data('uiDraggable')).toBeTruthy();
  });

  fit('should display operator description shortly after hovering on a operator label', () => {
    expect(component.t._ngbTooltip).toBe(mockOperatorData.additionalMetadata.operatorDescription);

    const spy = spyOn<any>(component, 'displayDescription');
    const operatorLabelElement = fixture.debugElement.query(By.css('#' + component.operatorLabelID));
    operatorLabelElement.triggerEventHandler('mouseenter', component.t);


    const tpWindow = fixture.debugElement.query(By.css('#ngb-tooltip-window'));
    console.log(tpWindow); // unable to find tpWindow
    expect(spy).toHaveBeenCalled();

    expect(mockOperatorData.additionalMetadata.operatorDescription).toContain(operatorLabelElement.attributes['ng-reflect-ngb-tooltip']);
  });

  fit('should hide operator descritption once the cursor leaves a operator label', () => {
    const spy = spyOn<any>(component, 'hideDescription');
    const operatorLabelElement = fixture.debugElement.query(By.css('#' + component.operatorLabelID));
    operatorLabelElement.triggerEventHandler('mouseleave', component.t);
    expect(spy).toHaveBeenCalled();
  });

//   // TODO: simulate drag and drop in tests, possibly using jQueryUI Simulate plugin
//   //  https://github.com/j-ulrich/jquery-simulate-ext/blob/master/doc/drag-n-drop.md

});
