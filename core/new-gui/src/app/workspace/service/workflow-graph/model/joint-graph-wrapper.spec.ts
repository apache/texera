import { WorkflowActionService } from './workflow-action.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { JointUIService } from '../../joint-ui/joint-ui.service';
import { JointGraphWrapper } from './joint-graph-wrapper';
import { TestBed } from '@angular/core/testing';
import { marbles } from 'rxjs-marbles';

import {
  mockScanPredicate, mockResultPredicate, mockScanResultLink,
  mockSentimentPredicate, mockScanSentimentLink, mockSentimentResultLink,
  mockPoint
} from './mock-workflow-data';

import * as joint from 'jointjs';
import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';

describe('JointGraphWrapperService', () => {

  let jointGraph: joint.dia.Graph;
  let jointGraphWrapper: JointGraphWrapper;
  let jointUIService: JointUIService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ]
    });
    jointGraph = new joint.dia.Graph();
    jointGraphWrapper = new JointGraphWrapper(jointGraph);
    jointUIService = TestBed.get(JointUIService);
  });


  it('should emit operator delete event correctly when operator is deleted by JointJS', marbles((m) => {

    jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));

    m.hot('-e-').do(() => jointGraph.getCell(mockScanPredicate.operatorID).remove()).subscribe();

    const jointOperatorDeleteStream = jointGraphWrapper.getJointOperatorCellDeleteStream().map(() => 'e');
    const expectedStream = m.hot('-e-');

    m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);

  }));


  it('should emit link add event correctly when a link is connected by JointJS', marbles((m) => {

    jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
    jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

    const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);

    m.hot('-e-').do(() => jointGraph.addCell(mockScanResultLinkCell)).subscribe();

    const jointLinkAddStream = jointGraphWrapper.getJointLinkCellAddStream().map(() => 'e');
    const expectedStream = m.hot('-e-');

    m.expect(jointLinkAddStream).toBeObservable(expectedStream);

  }));


  it('should emit link delete event correctly when a link is deleted by JointJS', marbles((m) => {

    jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
    jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

    const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);
    jointGraph.addCell(mockScanResultLinkCell);

    m.hot('---e-').do(() => jointGraph.getCell(mockScanResultLink.linkID).remove()).subscribe();

    const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().map(() => 'e');
    const expectedStream = m.hot('---e-');

    m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);

  }));

  /**
   * When the user deletes an operator in the UI, jointJS will delete the connected links automatically.
   *
   * This test verfies that when an operator is deleted, causing the one connected link to be deleted,
   *   the JointJS event Observalbe streams are emitted correctly.
   * It should emit one operator delete event and one link delete event at the same time.
   */
  it(`should emit operator delete event and link delete event correctly
          when an operator along with one connected link are deleted by JonitJS`
    , marbles((m) => {

      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);
      jointGraph.addCell(mockScanResultLinkCell);

      m.hot('-e-').do(() => jointGraph.getCell(mockScanPredicate.operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointOperatorCellDeleteStream().map(() => 'e');
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().map(() => 'e');

      const expectedStream = '-e-';

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);

    }));

  /**
   *
   * This test verfies that when an operator is deleted, causing *multiple* connected links to be deleted,
   *   the JointJS event Observalbe streams are emitted correctly.
   * It should emit one operator delete event and one link delete event at the same time.
   */
  it(`should emit operator delete event and link delete event correctly when
        an operator along with multiple links are deleted by JointJS`, marbles((m) => {

      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockSentimentPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanSentimentLinkCell = JointUIService.getJointLinkCell(mockScanSentimentLink);
      const mockSentimentResultLinkCell = JointUIService.getJointLinkCell(mockSentimentResultLink);
      jointGraph.addCell(mockScanSentimentLinkCell);
      jointGraph.addCell(mockSentimentResultLinkCell);

      m.hot('-e--').do(() => jointGraph.getCell(mockSentimentPredicate.operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointOperatorCellDeleteStream().map(() => 'e');
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().map(() => 'e');

      const expectedStream = '-e--';
      const expectedMultiStream = '-(ee)--';

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
      m.expect(jointLinkDeleteStream).toBeObservable(expectedMultiStream);

    }));

  it('should emit a highlight event correctly when an operator is highlighted', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add one operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // prepare marble operation for highlighting an operator
    const highlightActionMarbleEvent = m.hot(
      '-a-|',
      { a: mockScanPredicate.operatorID }
    ).share();

    // highlight that operator at events
    highlightActionMarbleEvent.subscribe(
      value => localJointGraphWrapper.highlightOperator(value)
    );

    // prepare expected output highlight event stream
    const expectedHighlightEventStream = m.hot('-a-', {
      a: { operatorID: mockScanPredicate.operatorID }
    });

    // expect the output event stream is correct
    m.expect(localJointGraphWrapper.getJointCellHighlightStream()).toBeObservable(expectedHighlightEventStream);

    // expect the current highlighted operator is correct
    highlightActionMarbleEvent.subscribe({
      complete: () => {
        expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toEqual(mockScanPredicate.operatorID);
      }
    });

  }));

  it('should emit an unhighlight event correctly when an operator is unhighlighted', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add one operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    // highlight the operator
    localJointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    // prepare marble operation for unhighlighting an operator
    const unhighlightActionMarbleEvent = m.hot('-a-|').share();

    // unhighlight that operator at events
    unhighlightActionMarbleEvent.subscribe(
      () => localJointGraphWrapper.unhighlightCurrent()
    );

    // prepare expected output highlight event stream
    const expectedUnhighlightEventStream = m.hot('-a-', {
      a: { operatorID: mockScanPredicate.operatorID }
    });

    // expect the output event stream is correct
    m.expect(localJointGraphWrapper.getJointCellUnhighlightStream()).toBeObservable(expectedUnhighlightEventStream);

    // expect the current highlighted operator is correct
    unhighlightActionMarbleEvent.subscribe({
      complete: () => {
        expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toBeFalsy();
      }
    });

  }));

  it('should unhighlight previous highlighted operator if a new operator is highlighted', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    // prepare marble operation for highlighting one operator, then highlight another
    const highlightActionMarbleEvent = m.hot(
      '-a-b-|',
      { a: mockScanPredicate.operatorID, b: mockResultPredicate.operatorID }
    ).share();


    // highlight that operator at events
    highlightActionMarbleEvent.subscribe(
      value => localJointGraphWrapper.highlightOperator(value)
    );

    // prepare expected output highlight event stream
    const expectedHighlightEventStream = m.hot('-a-b-', {
      a: { operatorID: mockScanPredicate.operatorID },
      b: { operatorID: mockResultPredicate.operatorID },
    });

    // expect the output event stream is correct
    m.expect(localJointGraphWrapper.getJointCellHighlightStream()).toBeObservable(expectedHighlightEventStream);

    // expect the current highlighted operator is correct
    highlightActionMarbleEvent.subscribe({
      complete: () => {
        expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toEqual(mockResultPredicate.operatorID);
      }
    });

  }));

  it('should ignore the action if tring to highlight the same currently highlighted operator', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    // prepare marble operation for highlighting the same operator twice
    const highlightActionMarbleEvent = m.hot(
      '-a-b-|',
      { a: mockScanPredicate.operatorID, b: mockScanPredicate.operatorID }
    ).share();


    // highlight that operator at events
    highlightActionMarbleEvent.subscribe(
      value => localJointGraphWrapper.highlightOperator(value)
    );

    // prepare expected output highlight event stream: the second highlight is ignored
    const expectedHighlightEventStream = m.hot('-a---', {
      a: { operatorID: mockScanPredicate.operatorID },
    });

    // expect the output event stream is correct
    m.expect(localJointGraphWrapper.getJointCellHighlightStream()).toBeObservable(expectedHighlightEventStream);

  }));

  it('should unhighlight the currently highlighted operator if it is deleted', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight the operator
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    localJointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toEqual(mockScanPredicate.operatorID);

    // prepare the delete operator action marble test
    const deleteOperatorActionMarble = m.hot('-a-').share();
    deleteOperatorActionMarble.subscribe(
      () => workflowActionService.deleteOperator(mockScanPredicate.operatorID)
    );

    // expect that the unhighlight event stream is triggered
    const expectedEventStream = m.hot('-a-', { a: { operatorID: mockScanPredicate.operatorID }});
    m.expect(localJointGraphWrapper.getJointCellUnhighlightStream()).toBeObservable(expectedEventStream);

    // expect that the current highlighted operator is undefined
    deleteOperatorActionMarble.subscribe({
      complete: () => expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toBeFalsy()
    });

  }));

});

