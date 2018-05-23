import { WorkflowGraph } from './../../../types/workflow-graph';
import {
  getMockScanPredicate, getMockResultPredicate, getMockSentimentPredicate, getMockScanResultLink,
  getMockScanSentimentLink, getMockSentimentResultLink, getMockFalseResultSentimentLink, getMockFalseSentimentScanLink,
  getMockPoint
} from './mock-workflow-data';
import { TestBed, inject } from '@angular/core/testing';

import { WorkflowActionService } from './workflow-action.service';
import { marbles } from 'rxjs-marbles';

describe('WorkflowActionService', () => {

  let service: WorkflowActionService;
  let texeraGraph: WorkflowGraph;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowActionService]
    });
    service = TestBed.get(WorkflowActionService);
    // we don't want to expose the texeraGraph to be public accessible,
    //   but we need to access it in the test cases,
    //   therefore we cast it to <any> type to bypass the private constraint
    texeraGraph = (service as any).texeraGraph;
  });

  it('should be created', inject([WorkflowActionService], (injectedService: WorkflowActionService) => {
    expect(injectedService).toBeTruthy();
  }));


  it('should emit event when addOperator is called', marbles((m) => {
    // at the time specified by event stream
    const eventStream = '-e-';
    // subscribe to that event time and call service.addOperator()
    m.hot(eventStream).subscribe(
      event => service.addOperator(getMockScanPredicate(), getMockPoint())
    );

    // set the added value to be 'e' since we only
    // want to test if the addOperation is called
    const outputStream = service._onAddOperatorAction().map(value => 'e');

    // This checks if the addOperation(now gives a value 'e') happens at the same
    //  time as the eventStream.
    m.expect(outputStream).toBeObservable(eventStream);
  }));

  it('should throw an error when adding an existed operator', () => {
    texeraGraph.addOperator(getMockScanPredicate());
    expect(() => {
      service.addOperator(getMockScanPredicate(), getMockPoint());
    }).toThrowError(new RegExp('already exists'));
  });

  it('should emit event when deleteOperator is called', marbles((m) => {
    texeraGraph.addOperator(getMockScanPredicate());

    const eventStream = '-e-';
    m.hot(eventStream).subscribe(
      event => service.deleteOperator(getMockScanPredicate().operatorID)
    );

    const outputStream = service._onDeleteOperatorAction().map(value => 'e');
    m.expect(outputStream).toBeObservable(eventStream);
  }));

  it('should throw an error when trying to delete an non-existing operator', () => {
    expect(() => {
      service.deleteOperator(getMockScanPredicate().operatorID);
    }).toThrowError(new RegExp(`doesn't exist`));
  });


  it('should emit event when addLink is called', marbles((m) => {
    texeraGraph.addOperator(getMockScanPredicate());
    texeraGraph.addOperator(getMockResultPredicate());

    const eventStream = '-e-';
    m.hot(eventStream).subscribe(
      event => service.addLink(getMockScanResultLink())
    );
    const outputStream = service._onAddLinkAction().map(value => 'e');
    m.expect(outputStream).toBeObservable(eventStream);

  }));


  it('should throw an appropriate error when adding incorrect links', () => {
    texeraGraph.addOperator(getMockScanPredicate());
    texeraGraph.addOperator(getMockResultPredicate());
    texeraGraph.addLink(getMockScanResultLink());

    // link already exist
    expect(() => {
      service.addLink(getMockScanResultLink());
    }).toThrowError(new RegExp('already exists'));

    const sameLinkDifferentID = getMockScanResultLink();
    sameLinkDifferentID.linkID = 'link-2';


    // same link but different id already exist
    expect(() => {
      service.addLink(sameLinkDifferentID);
    }).toThrowError(new RegExp('link from'));

    // link's target doesn't exist
    expect(() => {
      service.addLink(getMockScanSentimentLink());
    }).toThrowError(new RegExp(`doesn't exist`));

    // link's source doesn't exist
    expect(() => {
      service.addLink(getMockSentimentResultLink());
    }).toThrowError(new RegExp(`doesn't exist`));


    texeraGraph.addOperator(getMockSentimentPredicate());

    // link source portID doesn't exist (no output port for source operator)
    expect(() => {
      service.addLink(getMockFalseResultSentimentLink());
    }).toThrowError(new RegExp(`on output ports of the source operator`));

    // link target portID doesn't exist (no input port for target operator)

    expect(() => {
      service.addLink(getMockFalseSentimentScanLink());
    }).toThrowError(new RegExp(`on input ports of the target operator`));

  });

  it('should emit event when deleteLinkWithID is called', marbles((m) => {
    texeraGraph.addOperator(getMockScanPredicate());
    texeraGraph.addOperator(getMockResultPredicate());
    texeraGraph.addLink(getMockScanResultLink());

    const eventStream = '-e-';

    m.hot(eventStream).subscribe(
      event => service.deleteLinkWithID(getMockScanResultLink().linkID)
    );

    const outputStream = service._onDeleteLinkAction().map(value => 'e');
    m.expect(outputStream).toBeObservable(eventStream);

  }));



  it('should throw an error when trying to delete non-existing link', () => {
    texeraGraph.addOperator(getMockScanPredicate());
    texeraGraph.addOperator(getMockResultPredicate());
    expect(() => {
      service.deleteLinkWithID(getMockScanResultLink().linkID);
    }).toThrowError(new RegExp(`doesn't exist`));
  });


});
