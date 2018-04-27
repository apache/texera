import { Point } from './../../../types/common.interface';
import { OperatorPredicate, OperatorLink } from './../../../types/workflow-graph';
import {
  mockScanSourcePredicate, mockViewResultPredicate, mockSentimentAnalysisPredicate,
  mockLinkSourceViewResult, mockLinkSourceSentiment, mockLinkSentimentViewResult
} from './mock-workflow-data';
import { Observable } from 'rxjs/Observable';
import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { JointModelService } from './joint-model.service';
import { WorkflowActionService } from './workflow-action.service';
import { TestBed, inject } from '@angular/core/testing';
import { marbles, Context } from 'rxjs-marbles';

import { TexeraModelService } from './texera-model.service';

import '../../../../common/rxjs-operators';


class StubJointModelService {

  public onJointOperatorCellDelete(): any {
    return Observable.empty();
  }

  public onJointLinkCellAdd(): any {
    return Observable.empty();
  }

  public onJointLinkCellDelete(): any {
    return Observable.empty();
  }

  public onJointLinkCellChange(): any {
    return Observable.empty();
  }

}


describe('TexeraModelService', () => {

  const mockPoint: Point = { x: 100, y: 100 };

  function getAddOperatorValue(operator: OperatorPredicate) {
    return {
      operator: operator,
      point: mockPoint
    };
  }

  /**
   * Returns a mock JointJS operator Element object (joint.dia.Element)
   * The implementation code only uses the id attribute of the object.
   *
   * @param operatorID
   */
  function getJointOperatorValue(operatorID: string) {
    return {
      id: operatorID
    };
  }

  /**
   * Returns a mock JointJS Link object (joint.dia.Link)
   * It includes the attributes and functions same as JointJS
   *  and are used by the implementation code.
   * @param link
   */
  function getJointLinkValue(link: OperatorLink) {
    // getSourceElement, getTargetElement, and get all returns a function
    //  that returns the corresponding value
    return {
      id: link.linkID,
      getSourceElement: () => ({ id: link.source.operatorID }),
      getTargetElement: () => ({ id: link.target.operatorID }),
      get: (port) => {
        if (port === 'source') {
          return { port: link.source.portID };
        } else if (port === 'target') {
          return { port: link.target.portID };
        } else {
          throw new Error('getJointLinkValue: mock is inconsistent with implementation');
        }
      }
    };
  }

  /**
   * This helper function returns a mock JointJS link object (joint.dia.Link)
   *  that is only connected to a source port, but detached from the target port.
   *
   * This scenario happens when the user is still moving the link
   *  and it is not connected to a target port.
   *
   * @param link an operator link, but the target operator and target link is ignored
   */
  function getIncompleteJointLink(link: OperatorLink) {
    // getSourceElement, getTargetElement, and get all returns a function
    //  that returns the corresponding value
    return {
      id: link.linkID,
      getSourceElement: () => ({ id: link.source.operatorID }),
      getTargetElement: () => null,
      get: (port) => {
        if (port === 'source') {
          return { port: link.source.portID };
        } else if (port === 'target') {
          return null;
        } else {
          throw new Error('getJointLinkValue: mock is inconsistent with implementation');
        }
      }
    };
  }

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        TexeraModelService,
        WorkflowActionService,
        { provide: JointModelService, useClass: StubJointModelService },
      ]
    });
  });

  /**
   * The service should be created succesfully with provided dependencies
   */
  it('should be created', inject([TexeraModelService], (injectedService: TexeraModelService) => {
    expect(injectedService).toBeTruthy();
  }));

  /**
   * Add one operator
   * addOperatorObs: -a-|
   *
   * Expected:
   * The workflow graph should contain the added operator.
   * Texera Operator Add Stream should emit one event with the operator data.
   */
  it('should add an operator when create operator event happens from workflow action', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

    // prepare add operator
    const marbleString = '-a-|';
    const marbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(marbleString, marbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    // assert workflow graph
    workflowActionService._onAddOperatorAction().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().hasOperator(mockScanSourcePredicate.operatorID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(1);
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

    // assert operator add stream
    const operatorAddStream = texeraModelService.onOperatorAdd();
    const expectedAddStream = m.hot('-a-', { a: mockScanSourcePredicate });

    m.expect(operatorAddStream).toBeObservable(expectedAddStream);

  }));

  /**
   * Add two operators one by one.
   * addOperator: -a-b-|
   *
   * Expected:
   * The workflow graph should contain all two operators.
   * Texera Operator Add Stream should emit 2 events
   */
  it('should add two operators when two create operator events happen from workflow action', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

    // prepare add operator
    const marbleString = '-a-b-|';
    const marbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(marbleString, marbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    // assert workflow graph
    workflowActionService._onAddOperatorAction().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().hasOperator(mockScanSourcePredicate.operatorID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().hasOperator(mockViewResultPredicate.operatorID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(2);
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

    // assert operator add stream
    const operatorAddStream = texeraModelService.onOperatorAdd();
    const expectedAddStream = m.hot('-a-b-', { a: mockScanSourcePredicate, b: mockViewResultPredicate });

    m.expect(operatorAddStream).toBeObservable(expectedAddStream);

  }));

  /**
   * Add one operator
   * Delete one operator
   *
   * addOperator:         -a-|
   * jointDeleteOperator: ---d-|
   *
   * Expected:
   * The workflow graph should not have the added operator
   * The workflow graph should have 0 operators
   * Texera Delete Operator Stream should emit one event
   */
  it('should delete an operator when the delete operator event happens from JointJS', marbles((m) => {

    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare delete operator
    const deleteOpMarbleString = '---d-|';
    const deleteOpMarbleValues = {
      d: getJointOperatorValue(mockScanSourcePredicate.operatorID)
    };
    spyOn(jointModelService, 'onJointOperatorCellDelete').and.returnValue(
      m.hot(deleteOpMarbleString, deleteOpMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    // assert workflow graph
    jointModelService.onJointOperatorCellDelete().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().hasOperator(mockScanSourcePredicate.operatorID)).toBeFalsy();
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(0);
      }
    });

    // assert operator delete stream
    const operatorDeleteStream = texeraModelService.onOperatorDelete();
    const expectedStream = m.hot('---d-', { d: mockScanSourcePredicate });

    m.expect(operatorDeleteStream).toBeObservable(expectedStream);

  }));

  /**
   * Add two operators
   * Then delete one operator
   *
   * addOperator:         -a-b-|
   * jointDeleteOperator: -----d-|
   *
   * Expected:
   * Only the deleted operator should be removed.
   * The graph should have 1 operators and 0 links.
   * Texera Operator Delete Stream should emit one event
   */
  it('should delete an operator and not touch other operators when the delete operator event happens from JointJS', marbles((m) => {

    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare delete operator
    const deleteOpMarbleString = '-----d-|';
    const deleteOpMarbleValues = {
      d: getJointOperatorValue(mockScanSourcePredicate.operatorID)
    };
    spyOn(jointModelService, 'onJointOperatorCellDelete').and.returnValue(
      m.hot(deleteOpMarbleString, deleteOpMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointOperatorCellDelete().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().hasOperator(mockScanSourcePredicate.operatorID)).toBeFalsy();
        expect(texeraModelService.getTexeraGraph().hasOperator(mockViewResultPredicate.operatorID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(1);
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

    // assert operator delete stream
    const operatorDeleteStream = texeraModelService.onOperatorDelete();
    const expectedStream = m.hot('-----d-', { d: mockScanSourcePredicate });

    m.expect(operatorDeleteStream).toBeObservable(expectedStream);

  }));


  /**
   * Add two operators
   * Then add a link of these two operators
   *
   * addOperator:   -a-b-|
   * jointAddLink:  -----p-|
   *
   * Expected:
   * The graph should have two operators and a link between the operators
   * Texera Link Add Stream should emit one event
   */
  it('should add a link when link add event happen from JointJS', marbles((m) => {
    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link
    const addLinkMarbleString = '-----p-|';
    const addLinkMarbleValues = {
      p: getJointLinkValue(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellAdd().subscribe({
      complete: () => {
        console.log(texeraModelService.getTexeraGraph());
        console.log(texeraModelService.getTexeraGraph().hasLink(mockLinkSourceViewResult.source, mockLinkSourceViewResult.target));

        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(2);
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(1);
        expect(texeraModelService.getTexeraGraph().hasLinkWithID(mockLinkSourceViewResult.linkID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getLinkWithID(mockLinkSourceViewResult.linkID)).toEqual(mockLinkSourceViewResult);
        expect(texeraModelService.getTexeraGraph().hasLink(
          mockLinkSourceViewResult.source, mockLinkSourceViewResult.target
        )).toBeTruthy();
      }
    });

    // assert link add stream
    const linkAddStream = texeraModelService.onLinkAdd();
    const expectedStream = m.hot('-----p-', { p: mockLinkSourceViewResult });

    m.expect(linkAddStream).toBeObservable(expectedStream);

  }));

  /**
   * Add two operators
   * Then add a link of these two operators
   * Then delete the link
   *
   * addOperator:     -a-b-|
   * jointAddLink:    -----p-|
   * jointDeleteLink: -------r-|
   *
   * Expected:
   * The link should be deleted
   * Texera Link Delete Stream should emit one event
   */
  it('should delete a link when link delete event happen from JointJS', marbles((m) => {
    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link
    const addLinkMarbleString = '-----p-|';
    const addLinkMarbleValues = {
      p: getJointLinkValue(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // prepare delete link
    const deleteLinkMarbleString = '-------r-|';
    const deleteLinkMarbleValues = {
      r: getJointLinkValue(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellDelete').and.returnValue(
      m.hot(deleteLinkMarbleString, deleteLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellDelete().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

    // assert link delete stream
    const linkDeleteStream = texeraModelService.onLinkDelete();
    const expectedStream = m.hot('-------r-', { r: mockLinkSourceViewResult });

    m.expect(linkDeleteStream).toBeObservable(expectedStream);

  }));

  /**
   * Add two operators
   * Then a user drags a link from a source port,
   *  the link is visually added,
   *  but the link is not yet connected to a target port.
   * This link is considered invalid and should not appear in the graph
   *
   * addOperator:   -a-b-|
   * jointAddLink:  -----q-| (q is an incomplete Joint link)
   *
   * Expected:
   * The graph doesn't contain the incomplete link
   * Texera Link Add Stream should not emit anything
   */
  it('should not create a link when an incomplete link is added in JointJS', marbles((m) => {
    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link (incomplete link)
    const addLinkMarbleString = '-----q-|';
    const addLinkMarbleValues = {
      q: getIncompleteJointLink(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellDelete().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

    // assert link add stream
    const linkAddStream = texeraModelService.onLinkAdd();
    const expectedStream = m.hot('-------');

    m.expect(linkAddStream).toBeObservable(expectedStream);

  }));

  /**
   * Add two operators
   * Then a user drags a link from a source port,
   *  the link is visually added,
   *  but the link is not yet connected to a target port.
   *
   * Then the user release the mouse and the link is visually deleted,
   *  JointJS emits Link Delete event,
   *  the workflow graph should ignore it.
   *
   * addOperator:     -a-b-|
   * jointAddLink:    -----q-| (q is an incomplete Joint link)
   * jointDeleteLink: -------r-| (the visual deletion of the incomplete link)
   *
   * Expected:
   * The graph doesn't contain the link
   * Texera link add stream should not emit any event
   * Texera link delete stream should not emit any event
   */
  it('should ignore JointJS link delete event of an incomplete link', marbles((m) => {
    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link (incomplete link)
    const addLinkMarbleString = '-----q-|';
    const addLinkMarbleValues = {
      q: getIncompleteJointLink(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // prepare delete link (incomplete link)
    const deleteLinkMarbleString = '-------r-|';
    const deleteLinkMarbleValues = {
      r: getIncompleteJointLink(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellDelete').and.returnValue(
      m.hot(deleteLinkMarbleString, deleteLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellAdd().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

    // assert link add and delete stream
    const linkAddStream = texeraModelService.onLinkAdd();
    const linkDeleteStream = texeraModelService.onLinkDelete();
    const expectedStream = m.hot('---------');

    m.expect(linkAddStream).toBeObservable(expectedStream);
    m.expect(linkDeleteStream).toBeObservable(expectedStream);

  }));

  /**
   * Add two operators
   * Then add a link of these operators
   * Then the user drags the target port of the connected link,
   *   the link is detached from the target port.
   * This link is now considered invalid and should be deleted from the graph
   *
   * addOperator: -a-b-|
   * addLink:     -----p-|
   * changeLink:  -------q-| (link changes: detached from the target)
   *
   * The detatched link should be deleted from the graph.
   * Texera Link Delete Stream should emit one event when it's detached
   */
  it('should delete the link when a link is detached from the target port', marbles((m) => {
    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link
    const addLinkMarbleString = '-----p-|';
    const addLinkMarbleValues = {
      p: getJointLinkValue(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // prepare change link (link detached from target port)
    const changeLinkMarbleString = '-------q-|';
    const changeLinkMarbleValues = {
      q: getIncompleteJointLink(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellChange').and.returnValue(
      m.hot(changeLinkMarbleString, changeLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellChange().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

    // assert link delete stream
    const linkDeleteStream = texeraModelService.onLinkDelete();
    const expectedStream = m.hot('-------q-', { q: mockLinkSourceViewResult });

    m.expect(linkDeleteStream).toBeObservable(expectedStream);

  }));

  /**
   * Add three operators
   * Then add a link from operator 1 to operator 2
   * Then the user directly drags the target port from operator 2's input operator
   *  to operator 3's input port. The link automatically attach to operator3's target port,
   *  and JointJS only emits one link change event,
   *
   * addOperator: -a-b-c-|
   * addLink:     -------p-|
   * changeLink:  ---------t-| (link changes: target operator/port changed)
   *
   * Expected:
   * the link should be changed to the new target
   * Texera Link Delete Stream should emit one event,
   *  then immediately (at the same time frame) Link Add Stream should emit an event.
   *
   */
  it('should delete and then re-add the link if link target is changed from one port to another', marbles((m) => {
    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-c-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockSentimentAnalysisPredicate),
      c: getAddOperatorValue(mockViewResultPredicate),
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link
    const addLinkMarbleString = '-------p-|';
    const addLinkMarbleValues = {
      p: getJointLinkValue(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // create a mock changed link using another link's source/target
    // but the link ID remains the same
    const mockChangedLink = mockLinkSourceSentiment;
    mockChangedLink.linkID = mockLinkSourceViewResult.linkID;

    // prepare change link (link detached from target port)
    const changeLinkMarbleString = '---------t-|';
    const changeLinkMarbleValues = {
      t: getJointLinkValue(mockChangedLink)
    };
    spyOn(jointModelService, 'onJointLinkCellChange').and.returnValue(
      m.hot(changeLinkMarbleString, changeLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellChange().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(1);
        expect(texeraModelService.getTexeraGraph().hasLinkWithID(mockChangedLink.linkID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getLinkWithID(mockChangedLink.linkID)).toEqual(mockChangedLink);
        expect(texeraModelService.getTexeraGraph().hasLink(
          mockLinkSourceViewResult.source, mockLinkSourceViewResult.target
        )).toBeFalsy();
        expect(texeraModelService.getTexeraGraph().hasLink(
          mockChangedLink.source, mockChangedLink.target
        )).toBeTruthy();
      }
    });

    // assert link delete stream: delete original link
    const linkDeleteStream = texeraModelService.onLinkDelete();
    const expectedDeleteStream = m.hot('---------r-', { r: mockLinkSourceViewResult });
    m.expect(linkDeleteStream).toBeObservable(expectedDeleteStream);

    // assert link add stream: add original link, then add changed link
    const linkAddStream = texeraModelService.onLinkAdd();
    const expectedAddStream = m.hot('-------p-t-', { p: mockLinkSourceViewResult, t: mockChangedLink });
    m.expect(linkAddStream).toBeObservable(expectedAddStream);

  }));

  /**
   * Add three operators
   * Then add a link from operator 1 to operator 2
   * Then the user *gradually* drags the target port from operator 2's input port
   *  to operator 3's input port.
   * The link is detached, then move around the paper for a while, then re-attached to another port
   *
   * addOperator: -a-b-c-|
   * addLink:     -------p-|
   * changeLink:  ---------q-r-s-t-| (q: link detached with target being a point, r: target moved to another point,
   *    s: target moved to another point, t: target re-attached to another port)
   *
   * Expected:
   * the link should be changed to the new target.
   * Texera Link Delete stream should emit event when the link is detached.
   * Texera Link Add Stream should emit event when the link is re-attached
   */
  it('should remove then add link if link target port is detached then dragged around then re-attached', marbles((m) => {
    // prepare the dependency services
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-c-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockSentimentAnalysisPredicate),
      c: getAddOperatorValue(mockViewResultPredicate),
    };
    spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link
    const addLinkMarbleString = '-------p-|';
    const addLinkMarbleValues = {
      p: getJointLinkValue(mockLinkSourceViewResult)
    };
    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // create a mock changed link using another link's source/target
    // but the link ID remains the same
    const mockChangedLink = mockLinkSourceSentiment;
    mockChangedLink.linkID = mockLinkSourceViewResult.linkID;

    // prepare change link (link detached from target port)
    const changeLinkMarbleString = '---------q-r-s-t-|';
    const changeLinkMarbleValues = {
      q: getIncompleteJointLink(mockLinkSourceViewResult),
      r: getIncompleteJointLink(mockLinkSourceViewResult),
      s: getIncompleteJointLink(mockLinkSourceViewResult),
      t: getJointLinkValue(mockChangedLink)
    };
    spyOn(jointModelService, 'onJointLinkCellChange').and.returnValue(
      m.hot(changeLinkMarbleString, changeLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellChange().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(1);
        expect(texeraModelService.getTexeraGraph().hasLink(
          mockChangedLink.source, mockChangedLink.target
        )).toBeTruthy();
      }
    });

    // assert link delete stream: delete original link
    const linkDeleteStream = texeraModelService.onLinkDelete();
    const expectedDeleteStream = m.hot('---------q---', { q: mockLinkSourceViewResult });
    m.expect(linkDeleteStream).toBeObservable(expectedDeleteStream);

    // assert link add stream: add original link, then add changed link after its re-attached
    const linkAddStream = texeraModelService.onLinkAdd();
    const expectedAddStream = m.hot('-------p-------t-', { p: mockLinkSourceViewResult, t: mockChangedLink });
    m.expect(linkAddStream).toBeObservable(expectedAddStream);

  }));


});
