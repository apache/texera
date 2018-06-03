import { WorkflowActionService } from './workflow-action.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { JointUIService } from '../../joint-ui/joint-ui.service';
import { JointGraphWrapper } from './joint-graph-wrapper';
import { TestBed, inject } from '@angular/core/testing';
import { marbles } from 'rxjs-marbles';
import { isEqual } from 'lodash';

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

    m.hot('-e-').do(v => jointGraph.getCell(mockScanPredicate.operatorID).remove()).subscribe();

    const jointOperatorDeleteStream = jointGraphWrapper.getJointOperatorCellDeleteStream().map(value => 'e');
    const expectedStream = m.hot('-e-');

    m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);

  }));


  it('should emit link add event correctly when a link is connected by JointJS', marbles((m) => {

    jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
    jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

    const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);

    m.hot('-e-').do(event => jointGraph.addCell(mockScanResultLinkCell)).subscribe();

    const jointLinkAddStream = jointGraphWrapper.getJointLinkCellAddStream().map(value => 'e');
    const expectedStream = m.hot('-e-');

    m.expect(jointLinkAddStream).toBeObservable(expectedStream);

  }));


  it('should emit link delete event correctly when a link is deleted by JointJS', marbles((m) => {

    jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
    jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

    const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);
    jointGraph.addCell(mockScanResultLinkCell);

    m.hot('---e-').do(event => jointGraph.getCell(mockScanResultLink.linkID).remove()).subscribe();

    const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().map(value => 'e');
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

      m.hot('-e-').do(event => jointGraph.getCell(mockScanPredicate.operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointOperatorCellDeleteStream().map(value => 'e');
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().map(value => 'e');

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

      m.hot('-e--').do(event => jointGraph.getCell(mockSentimentPredicate.operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointOperatorCellDeleteStream().map(value => 'e');
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().map(value => 'e');

      const expectedStream = '-e--';
      const expectedMultiStream = '-(ee)--';

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
      m.expect(jointLinkDeleteStream).toBeObservable(expectedMultiStream);

    }));

  it('should handle the event when an operator is highlighted or unhighlighted in the JointJS paper', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    localJointGraphWrapper.getJointCellHighlightStream().subscribe(
      operator => {
        expect(operator.operatorID).toEqual(mockScanPredicate.operatorID);
      }
    );

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    localJointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

    expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toEqual(mockScanPredicate.operatorID);


    localJointGraphWrapper.getJointCellUnhighlightStream().subscribe(
      operator => {
        expect(operator.operatorID).toEqual(mockScanPredicate.operatorID);
      }
    );

    localJointGraphWrapper.unhighlightCurrent();
    expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toBeFalsy();
  }));

  it('should unhighlight previous highlighted operator if a new operator is highlighted', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    localJointGraphWrapper.getJointCellUnhighlightStream().subscribe(
      operator => {
        expect(operator.operatorID).toEqual(mockScanPredicate.operatorID);
      }
    );

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);

    localJointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);
    localJointGraphWrapper.highlightOperator(mockResultPredicate.operatorID);

    expect(localJointGraphWrapper.getCurrentHighlightedOpeartorID()).toEqual(mockResultPredicate.operatorID);

  }));

});

