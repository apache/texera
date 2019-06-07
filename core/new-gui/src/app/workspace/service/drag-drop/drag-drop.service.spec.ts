import { JointUIService } from './../joint-ui/joint-ui.service';
import { TestBed, inject } from '@angular/core/testing';

import { DragDropService } from './drag-drop.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../workflow-graph/util/workflow-util.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { mockOperatorMetaData } from '../operator-metadata/mock-operator-metadata.data';

import { marbles } from 'rxjs-marbles';
import { mockScanPredicate, mockResultPredicate, mockScanResultLink } from '../workflow-graph/model/mock-workflow-data';
import { OperatorLink } from '../../types/workflow-common.interface';

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

  it('should change the add an operator at correct position when the element is dropped', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

    workflowActionService.getJointGraphWrapper().setPanningOffset({x: 100, y: 100});
    workflowActionService.getJointGraphWrapper().setZoomProperty(0.1);

    const operatorType = mockOperatorMetaData.operators[0].operatorType;
    const marbleString = '-e-|';
    const marbleValues = {
      e : {operatorType: operatorType, offset: {x: 200, y: 200}}
    };

    spyOn(dragDropService, 'getOperatorDropStream').and.returnValue(
      m.hot(marbleString, marbleValues)
    );


    dragDropService.handleOperatorDropEvent();

    workflowActionService.getTexeraGraph().getOperatorAddStream().subscribe(
      value => {
        const jointGraph: joint.dia.Graph = (workflowActionService as any).jointGraph;
        const currenOperatorPosition = jointGraph.getCell(value.operatorID).attributes.position;
        expect(currenOperatorPosition.x).toEqual(1000);
        expect(currenOperatorPosition.y).toEqual(1000);
      });
  }));

  it('should successfully create a new operator link given 2 operator predicates', () => {
    const createdLink: OperatorLink = (dragDropService as any).getNewOperatorLink(mockScanPredicate, mockResultPredicate);

    expect(createdLink.source).toEqual(mockScanResultLink.source);
    expect(createdLink.target).toEqual(mockScanResultLink.target);
  });

  it('should trigger the highlight event if it found a closest operator', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    workflowActionService.addOperator(mockScanPredicate, {x: 0, y: 0});
    workflowActionService.addOperator(mockResultPredicate, {x: 100, y: 100});

    (dragDropService as any).currentOperatorType = 'NlpSentiment';

    m.hot('-e-').do(() => (dragDropService as any).findClosestOperator({x:  10, y: 10})).subscribe();

    const suggestionHighlightStream = dragDropService.getOperatorSuggestionHighlightStream().map(value => 'e');
    const expectedStream = '-e-';
    m.expect(suggestionHighlightStream).toBeObservable(expectedStream);
  }));

  it('should find the closest operator and highlight it when calling "findClosestOperator()"', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const suggestionSpy = spyOn(dragDropService, 'getOperatorSuggestionHighlightStream').and.callThrough();
    workflowActionService.addOperator(mockScanPredicate, {x: 0, y: 0});
    workflowActionService.addOperator(mockResultPredicate, {x: 100, y: 100});

    (dragDropService as any).currentOperatorType = 'NlpSentiment';

    m.hot('-e-').do(() => (dragDropService as any).findClosestOperator({x:  10, y: 10})).subscribe();
    dragDropService.getOperatorSuggestionHighlightStream().subscribe(
      operatorID => {
        expect(operatorID).toEqual(mockScanPredicate.operatorID);
      }
    );

    expect(suggestionSpy).toHaveBeenCalledTimes(1);
  }));

  it('should not find any operator when the mouse coordinate is greater than the threshold defined', () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const suggestionSpy = spyOn(dragDropService, 'getOperatorSuggestionHighlightStream').and.callThrough();
    workflowActionService.addOperator(mockScanPredicate, {x: 0, y: 0});
    (dragDropService as any).currentOperatorType = 'NlpSentiment';
    (dragDropService as any).findClosestOperator(
      {x:  DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10, y: DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10}
    );

    expect(suggestionSpy).toHaveBeenCalledTimes(0);

  });
});
