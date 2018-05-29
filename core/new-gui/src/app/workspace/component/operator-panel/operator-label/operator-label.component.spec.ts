import { Observable } from 'rxjs/Observable';
import { WorkflowUtilService } from './../../../service/workflow-graph/util/workflow-util.service';
import { JointUIService } from './../../../service/joint-ui/joint-ui.service';
import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { JointModelService } from '../../../service/workflow-graph/model/joint-model.service';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorLabelComponent } from './operator-label.component';
import { OperatorMetadataService } from '../../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../../service/operator-metadata/stub-operator-metadata.service';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { CustomNgMaterialModule } from '../../../../common/custom-ng-material.module';
import { getMockOperatorSchemaList } from '../../../service/operator-metadata/mock-operator-metadata.data';
import { By } from '@angular/platform-browser';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { marbles } from 'rxjs-marbles';

describe('OperatorLabelComponent', () => {
  const mockOperatorData = getMockOperatorSchemaList()[0];
  let component: OperatorLabelComponent;
  let fixture: ComponentFixture<OperatorLabelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [OperatorLabelComponent],
      imports: [CustomNgMaterialModule],
      providers: [
        DragDropService,
        JointModelService,
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

  it('should be able to drag the label into an operator', marbles((m) => {
    fixture.detectChanges();

    const mouseDown = m.hot('-s-|');

    const mouseMove = m.hot('---abcdefg-|', {
      a: { x: 10, y: 10 },
      b: { x: 10, y: 30 },
      c: { x: 10, y: 50 },
      d: { x: 10, y: 70 },
      e: { x: 10, y: 90 },
      f: { x: 10, y: 110 },
      g: { x: 10, y: 130 },
    });

    mouseDown
      .subscribe(
        value => {
          const el: HTMLElement = fixture.nativeElement;
          el.dispatchEvent(new MouseEvent('mousedown',
            { clientX: 10 + el.getBoundingClientRect().left, clientY: 10 + el.getBoundingClientRect().top }));
          fixture.detectChanges();
          console.log('drag test: mouse down!');
        }
      );

    mouseMove
      .subscribe(
        value => {
          const el: HTMLElement = fixture.nativeElement;
          el.dispatchEvent(new MouseEvent('mousemove', {
            clientX: value.x + el.getBoundingClientRect().left, clientY: value.y + el.getBoundingClientRect().top
          }));
          el.dispatchEvent(new MouseEvent('drag',
            { clientX: value.x + el.getBoundingClientRect().left, clientY: value.y + el.getBoundingClientRect().top }));
          fixture.detectChanges();
          console.log('drag test: mouse move!');
        }
      );

    Observable.fromEvent(fixture.debugElement.nativeElement, 'mousedown').subscribe(
      value => console.log(value)
    );

    Observable.fromEvent(fixture.debugElement.nativeElement, 'mousemove').subscribe(
      value => console.log(value)
    );

    const dragDropService: DragDropService = TestBed.get(DragDropService);

    dragDropService.getOperatorStartDragStream().subscribe(
      v => console.log('drag test: start drag!')
    );

    // mouseDown.subscribe(
    //   value => {
    //     fixture.debugElement.triggerEventHandler(
    //       'mousedown', null
    //     );
    //   }
    // );

    // mouseMove.subscribe(
    //   value => {
    //     fixture.debugElement.triggerEventHandler(

    //     )
    //   }
    // );

  }));

});
