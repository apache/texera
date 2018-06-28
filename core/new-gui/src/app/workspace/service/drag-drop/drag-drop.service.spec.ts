import { JointUIService } from './../joint-ui/joint-ui.service';
import { TestBed, inject } from '@angular/core/testing';

import { DragDropService } from './drag-drop.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../workflow-graph/util/workflow-util.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { mockOperatorMetaData } from '../operator-metadata/mock-operator-metadata.data';

import { marbles } from 'rxjs-marbles';

describe('DragDropService', () => {

  let dragDropService: DragDropService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        DragDropService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ]
    });

    dragDropService = TestBed.get(DragDropService);
  });

  it('should be created', inject([DragDropService], (injectedService: DragDropService) => {
    expect(injectedService).toBeTruthy();
  }));


  it('should successfully register the element as draggable', () => {

    const dragElementID = 'testing-draggable-1';
    jQuery('body').append(`<div id="${dragElementID}"></div>`);

    const operatorType = mockOperatorMetaData.operators[0].operatorType;
    dragDropService.registerOperatorLabelDrag(dragElementID, operatorType);

    expect(jQuery('#' + dragElementID).is('.ui-draggable')).toBeTruthy();

  });


  it('should successfully register the element as droppable', () => {

    const dropElement = 'testing-droppable-1';
    jQuery('body').append(`<div id="${dropElement}"></div>`);

    dragDropService.registerWorkflowEditorDrop(dropElement);

    expect(jQuery('#' + dropElement).is('.ui-droppable')).toBeTruthy();

  });

  it('should add an operator when the element is dropped', marbles((m) => {

    const operatorType = mockOperatorMetaData.operators[0].operatorType;

    const marbleString = '-a-|';
    const marbleValues = {
      a : {operatorType: operatorType, offset: {x: 100, y: 100}}
    };

    spyOn(dragDropService, 'getOperatorDropStream').and.returnValue(
      m.hot(marbleString, marbleValues)
    );

    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

    dragDropService.handleOperatorDropEvent();

    const addOperatorStream = workflowActionService.getTexeraGraph().getOperatorAddStream().map(() => 'a');

    const expectedStream = m.hot('-a-');
    m.expect(addOperatorStream).toBeObservable(expectedStream);


  }));

});
