import { Point } from './../../../types/common.interface';
import { OperatorPredicate, OperatorLink } from './../../../types/workflow-graph';
import { mockScanSourcePredicate, mockViewResultPredicate } from './mock-workflow-data';
import { Observable } from 'rxjs/Observable';
import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { JointModelService } from './jointjs-model.service';
import { WorkflowModelActionService } from './workflow-model-action.service';
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
      getSourceElement: () => ({ id: link.sourceOperator }),
      getTargetElement: () => ({ id: link.targetOperator }),
      get: (port) => {
        if (port === 'source') {
          return { port: link.sourcePort };
        } else if (port === 'target') {
          return { port: link.targetPort };
        } else {
          throw new Error('getJointLinkValue: mock is inconsistent with implementation');
        }
      }
    };
  }

  /**
   * This helper function returns a mock JointJS link object (joint.dia.Link)
   *  that is invalid in the context of Texera graph,
   *  where the link is only connected to a source port,
   *  but the user is still moving the link and it is not connected to a target port.
   *
   * @param sourceOperator
   * @param sourcePort
   * @param targetPoint
   */
  function getInvalidJointLink(linkID: string, sourceOperator: string, sourcePort: string) {
    // getSourceElement, getTargetElement, and get all returns a function
    //  that returns the corresponding value
    return {
      id: linkID,
      getSourceElement: () => ({ id: sourceOperator }),
      getTargetElement: () => null,
      get: (port) => {
        if (port === 'source') {
          return { port: sourcePort };
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
        WorkflowModelActionService,
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
   * The workflow graph should contain the added operator
   */
  it('should create an operator when create operator event happens from workflow action test 1', marbles((m) => {
    const workflowModelActionService: WorkflowModelActionService = TestBed.get(WorkflowModelActionService);

    // prepare add operator
    const marbleString = '-a-|';
    const marbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate)
    };
    spyOn(workflowModelActionService, 'onAddOperatorAction').and.returnValue(
      m.hot(marbleString, marbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);
    workflowModelActionService.onAddOperatorAction().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().hasOperator(mockScanSourcePredicate.operatorID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(1);
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

  }));

  /**
   * Add two operators one by one.
   * addOperator: -a-b-|
   *
   * The workflow graph should contain all two operators.
   */
  it('should create two operators when create operator event happens from workflow action test 2', marbles((m) => {
    const workflowModelActionService: WorkflowModelActionService = TestBed.get(WorkflowModelActionService);

    // prepare add operator
    const marbleString = '-a-b-|';
    const marbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowModelActionService, 'onAddOperatorAction').and.returnValue(
      m.hot(marbleString, marbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);
    workflowModelActionService.onAddOperatorAction().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().hasOperator(mockScanSourcePredicate.operatorID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().hasOperator(mockViewResultPredicate.operatorID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(2);
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

  }));

  /**
   * Add one operator
   * Delete one operator
   *
   * addOperator:    -a-|
   * deleteOperator: ---d-|
   *
   * The workflow graph should not have the added operator
   * The workflow graph should have 0 operators
   */
  it('should delete an operator when the delete operator event happens from JointJS test 1', marbles((m) => {

    // prepare the dependency services
    const workflowModelActionService: WorkflowModelActionService = TestBed.get(WorkflowModelActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate)
    };
    spyOn(workflowModelActionService, 'onAddOperatorAction').and.returnValue(
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

    jointModelService.onJointOperatorCellDelete().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().hasOperator(mockScanSourcePredicate.operatorID)).toBeFalsy();
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(0);
      }
    });

  }));

  /**
   * Add two operators
   * Then delete one operator
   *
   * addOperator:    -a-b-|
   * deleteOperator: -----d-|
   *
   * Only the deleted operator should be removed.
   * The graph should have 1 operators and 0 links.
   *
   */
  it('should delete an operator when the delete operator event happens from JointJS test 2', marbles((m) => {

    // prepare the dependency services
    const workflowModelActionService: WorkflowModelActionService = TestBed.get(WorkflowModelActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowModelActionService, 'onAddOperatorAction').and.returnValue(
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

  }));


  /**
   * Add two operators
   * Then a link of these two operators
   *
   * addOperator: -a-b-|
   * addLink:     -----p-|
   *
   * The graph should have two operators and a link between the operators
   *
   */
  it('should create a link when link add event happen from JointJS test 1', marbles((m) => {
    // prepare the dependency services
    const workflowModelActionService: WorkflowModelActionService = TestBed.get(WorkflowModelActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowModelActionService, 'onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link
    const mockLink1: OperatorLink = {
      linkID: 'link-1',
      sourceOperator: mockScanSourcePredicate.operatorID,
      sourcePort: mockScanSourcePredicate.outputPorts[0],
      targetOperator: mockViewResultPredicate.operatorID,
      targetPort: mockViewResultPredicate.inputPorts[0]
    };
    const addLinkMarbleString = '-----p-|';
    const addLinkMarbleValues = {
      p: getJointLinkValue(mockLink1)
    };

    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellAdd().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getOperators().length).toEqual(2);
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(1);
        expect(texeraModelService.getTexeraGraph().hasLinkWithID(mockLink1.linkID)).toBeTruthy();
        expect(texeraModelService.getTexeraGraph().getLink(mockLink1.linkID)).toEqual(mockLink1);
        expect(texeraModelService.getTexeraGraph().hasLink(
          mockLink1.sourceOperator, mockLink1.sourcePort, mockLink1.targetOperator, mockLink1.targetPort
        )).toBeTruthy();
      }
    });

  }));

  /**
   * Add two operators
   * Then a user drags a link from a source port,
   *   but the link is not connected to a target port yet.
   * This link is considered invalid and should not appear in the graph
   *
   * addOperator: -a-b-|
   * addLink:     -----p-| (p is an invalid Joint link)
   *
   * The graph doesn't contain the invalid link
   *
   */
  it('should not create a link when an incomplete link is added in JointJS', marbles((m) => {
    // prepare the dependency services
    const workflowModelActionService: WorkflowModelActionService = TestBed.get(WorkflowModelActionService);
    const jointModelService: JointModelService = TestBed.get(JointModelService);

    // prepare add operator
    const addOpMarbleString = '-a-b-|';
    const addOpMarbleValues = {
      a: getAddOperatorValue(mockScanSourcePredicate),
      b: getAddOperatorValue(mockViewResultPredicate)
    };
    spyOn(workflowModelActionService, 'onAddOperatorAction').and.returnValue(
      m.hot(addOpMarbleString, addOpMarbleValues)
    );

    // prepare add link
    const addLinkMarbleString = '-----p-|';
    const addLinkMarbleValues = {
      p: getInvalidJointLink('link-1', mockScanSourcePredicate.operatorID, mockScanSourcePredicate.outputPorts[0])
    };

    spyOn(jointModelService, 'onJointLinkCellAdd').and.returnValue(
      m.hot(addLinkMarbleString, addLinkMarbleValues)
    );

    // construct the texera model service with spied dependencies
    const texeraModelService: TexeraModelService = TestBed.get(TexeraModelService);

    jointModelService.onJointLinkCellAdd().subscribe({
      complete: () => {
        expect(texeraModelService.getTexeraGraph().getLinks().length).toEqual(0);
      }
    });

  }));




});
