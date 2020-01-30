import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { WorkflowGraph } from './workflow-graph';
import { UndoRedoService } from './../../undo-redo/undo-redo.service';
import {
  mockScanPredicate, mockResultPredicate, mockSentimentPredicate, mockScanResultLink,
  mockScanSentimentLink, mockSentimentResultLink, mockFalseResultSentimentLink, mockFalseSentimentScanLink,
  mockPoint
} from './mock-workflow-data';
import { TestBed, inject } from '@angular/core/testing';

import { WorkflowActionService } from './workflow-action.service';
import { OperatorPredicate, Point } from '../../../types/workflow-common.interface';
import { g } from 'jointjs';
import { environment } from './../../../../../environments/environment';

describe('WorkflowActionService', () => {

  let service: WorkflowActionService;
  let texeraGraph: WorkflowGraph;
  let jointGraph: joint.dia.Graph;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        JointUIService,
        UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ],
      imports: []
    });
    service = TestBed.get(WorkflowActionService);
    texeraGraph = (service as any).texeraGraph;
    jointGraph = (service as any).jointGraph;
  });

  it('should be created', inject([WorkflowActionService], (injectedService: WorkflowActionService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should add an operator to both jointjs and texera graph correctly', () => {
    service.addOperator(mockScanPredicate, mockPoint);

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
  });

  it('should throw an error when adding an existed operator', () => {
    service.addOperator(mockScanPredicate, mockPoint);

    expect(() => {
      service.addOperator(mockScanPredicate, mockPoint);
    }).toThrowError(new RegExp(`exists`));
  });

  it('should throw an error when adding an operator with invalid operator type', () => {
    const invalidOperator: OperatorPredicate = {
      ...mockScanPredicate,
      operatorType: 'invalidOperatorTypeForTesting'
    };

    expect(() => {
      service.addOperator(invalidOperator, mockPoint);
    }).toThrowError(new RegExp(`invalid`));
  });

  it('should delete an operator to both jointjs and texera graph correctly', () => {
    service.addOperator(mockScanPredicate, mockPoint);

    service.deleteOperator(mockScanPredicate.operatorID);

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
    expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeFalsy();
  });

  it('should throw an error when trying to delete an non-existing operator', () => {
    expect(() => {
      service.deleteOperator(mockScanPredicate.operatorID);
    }).toThrowError(new RegExp(`does not exist`));
  });


  it('should add a link to both jointjs and texera graph correctly', () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);

    service.addLink(mockScanResultLink);

    expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeTruthy();
    expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeTruthy();
    expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeTruthy();
  });

  it('should throw appropriate errors when adding various types of incorrect links', () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanResultLink);

    // link already exist
    expect(() => {
      service.addLink(mockScanResultLink);
    }).toThrowError(new RegExp('already exists'));

    const sameLinkDifferentID = {
      ...mockScanResultLink,
      linkID: 'link-2'
    };

    // same link but different id already exist
    expect(() => {
      service.addLink(sameLinkDifferentID);
    }).toThrowError(new RegExp('exists'));

    // link's target operator or port doesn't exist
    expect(() => {
      service.addLink(mockScanSentimentLink);
    }).toThrowError(new RegExp(`does not exist`));

    // link's source operator or port doesn't exist
    expect(() => {
      service.addLink(mockSentimentResultLink);
    }).toThrowError(new RegExp(`does not exist`));

    // add another operator for tests below
    texeraGraph.addOperator(mockSentimentPredicate);

    // link source portID doesn't exist (no output port for source operator)
    expect(() => {
      service.addLink(mockFalseResultSentimentLink);
    }).toThrowError(new RegExp(`on output ports of the source operator`));

    // link target portID doesn't exist (no input port for target operator)

    expect(() => {
      service.addLink(mockFalseSentimentScanLink);
    }).toThrowError(new RegExp(`on input ports of the target operator`));

  });

  it('should delete a link by link ID from both jointjs and texera graph correctly', () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanResultLink);

    // test delete by link ID
    service.deleteLinkWithID(mockScanResultLink.linkID);

    expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeFalsy();
    expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeFalsy();
    expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeFalsy();
  });

  it('should delete a link by source and target from both jointjs and texera graph correctly', () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanResultLink);

    // test delete by link source and target
    service.deleteLink(mockScanResultLink.source, mockScanResultLink.target);

    expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeFalsy();
    expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeFalsy();
    expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeFalsy();
  });

  it('should throw an error when trying to delete non-existing link', () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);

    expect(() => {
      service.deleteLinkWithID(mockScanResultLink.linkID);
    }).toThrowError(new RegExp(`does not exist`));

    expect(() => {
      service.deleteLinkWithID(mockScanResultLink.linkID);
    }).toThrowError(new RegExp(`does not exist`));
  });

  it('should set operator property to texera graph correctly', () => {
    service.addOperator(mockScanPredicate, mockPoint);

    const newProperty = { table: 'test-table' };
    service.setOperatorProperty(mockScanPredicate.operatorID, newProperty);

    const operator = texeraGraph.getOperator(mockScanPredicate.operatorID);
    if (! operator) {
      throw new Error(`operator ${mockScanPredicate.operatorID} doesn't exist`);
    }
    expect(operator.operatorProperties).toEqual(newProperty);
  });

  it('should throw an error when trying to set operator property of an nonexist operator', () => {
    expect(() => {
      const newProperty = { table: 'test-table' };
      service.setOperatorProperty(mockScanPredicate.operatorID, newProperty);
    }).toThrowError(new RegExp(`does not exist`));
  });

  it('should handle delete an operator causing connected links to be deleted correctly', () => {
    // add operator scan, sentiment, and result
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockSentimentPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    // add link scan -> result, and sentiment -> result
    service.addLink(mockScanResultLink);
    service.addLink(mockSentimentResultLink);

    // delete result operator, should cause two links to be deleted as well
    service.deleteOperator(mockResultPredicate.operatorID);

    expect(texeraGraph.getAllOperators().length).toEqual(2);
    expect(texeraGraph.getAllLinks().length).toEqual(0);

  });



  describe('when executionStatus is enabled', () => {
    beforeEach(() => {
      environment.executionStatusEnabled = true;
    });

    it('should handle delete an operator causeing corresponding operator status tooltip element to be deleted correctly', () => {
      service.addOperator(mockScanPredicate, mockPoint);

      const opStatusTooltipID = JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
      expect(jointGraph.getCell(opStatusTooltipID)).toBeTruthy();

      expect(jointGraph.getElements().length).toEqual(2);
      service.deleteOperator(mockScanPredicate.operatorID);
      expect(jointGraph.getElements().length).toEqual(0);

      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeFalsy();
      expect(jointGraph.getCell(opStatusTooltipID)).toBeFalsy();
    });

    it('should add a corresponding operator status tooltip element when adding a operator', () => {
      service.addOperator(mockScanPredicate, mockPoint);

      const opStatusTooltipID = JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
      expect(jointGraph.getCell(opStatusTooltipID)).toBeTruthy();
    });

    it('should move opStatusTootip when operator is moved', () => {
      service.addOperator(mockScanPredicate, mockPoint);
      const operatorElement = jointGraph.getElements()[0];
      const tooltipElement = jointGraph.getElements()[1];
      expect(operatorElement).toBeTruthy();
      expect(tooltipElement).toBeTruthy();

      const originalTooltipPosition = tooltipElement.position().toJSON();
      const expectedOperatorPosition = new g.Point(mockPoint.x + 50, mockPoint.y - 50);
      const expectedTooltipPosition = new g.Point(originalTooltipPosition['x'] + 50, originalTooltipPosition['y'] - 50);

      // only move operatorElement
      operatorElement.translate(50, -50);
      // tooltip should move with it
      expect(operatorElement.position()).toEqual(expectedOperatorPosition);
      expect(tooltipElement.position()).toEqual(expectedTooltipPosition);
    });

  });

});
