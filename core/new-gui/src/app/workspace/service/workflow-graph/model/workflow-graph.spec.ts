import {
  mockScanPredicate, mockSentimentPredicate, mockResultPredicate,
  mockScanSentimentLink, mockSentimentResultLink, mockScanResultLink
} from './mock-workflow-data';
import { WorkflowGraph } from './workflow-graph';

describe('WorkflowGraph', () => {

  let workflowGraph: WorkflowGraph;

  beforeEach(() => {
    workflowGraph = new WorkflowGraph();
  });

  it('should have an empty graph from the beginning', () => {
    expect(workflowGraph.getAllOperators().length).toEqual(0);
    expect(workflowGraph.getAllLinks().length).toEqual(0);
  });

  it('should load an existing graph properly', () => {
    workflowGraph = new WorkflowGraph(
      [mockScanPredicate, mockSentimentPredicate, mockResultPredicate],
      [mockScanSentimentLink, mockSentimentResultLink]
    );
    expect(workflowGraph.getAllOperators().length).toEqual(3);
    expect(workflowGraph.getAllLinks().length).toEqual(2);
  });

  it('should add an operator and get it properly', () => {
    workflowGraph.addOperator(mockScanPredicate);
    expect(workflowGraph.getOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(workflowGraph.getAllOperators().length).toEqual(1);
    expect(workflowGraph.getAllOperators()[0]).toEqual(mockScanPredicate);
  });

  it('should return undefined when get an operator with a nonexist operator ID', () => {
    expect(workflowGraph.getOperator('nonexist')).toBe(undefined);
  });

  it('should throw an error when trying to add an operator with an existing operator ID', () => {
    expect(() => {
      workflowGraph.addOperator(mockScanPredicate);
      workflowGraph.addOperator(mockScanPredicate);
    }).toThrowError(new RegExp('already exists'));
  });

  it('should delete an operator properly', () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.deleteOperator(mockScanPredicate.operatorID);
    expect(workflowGraph.getAllOperators().length).toBe(0);
  });

  it('should throw an error when tring to delete an operator that doesn\'t exist', () => {
    expect(() => {
      workflowGraph.deleteOperator('nonexist');
    }).toThrowError(new RegExp(`doesn't exist`));
  });

  it('should add and get a link properly', () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);

    expect(workflowGraph.getLinkWithID(mockScanResultLink.linkID)).toEqual(mockScanResultLink);
    expect(workflowGraph.getLink(
      mockScanResultLink.source, mockScanResultLink.target
    )).toEqual(mockScanResultLink);
    expect(workflowGraph.getAllLinks().length).toEqual(1);
  });

  it('should throw an error when try to add a link with an existingID', () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addOperator(mockSentimentPredicate);
    workflowGraph.addLink(mockScanResultLink);

    // create a mock link with modified target
    const mockLink = {
      ...mockScanResultLink,
      target: {
        operatorID: mockSentimentPredicate.operatorID,
        portID: mockSentimentPredicate.inputPorts[0]
      },
    };

    expect(() => {
      workflowGraph.addLink(mockLink);
    }).toThrowError(new RegExp('already exists'));
  });

  it('should throw an error when try to add a link with exising source and target but different ID', () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addOperator(mockSentimentPredicate);
    workflowGraph.addLink(mockScanResultLink);

    // create a mock link with modified ID
    const mockLink = {
      ...mockScanResultLink,
      linkID: 'new-link-id',
    };

    expect(() => {
      workflowGraph.addLink(mockLink);
    }).toThrowError(new RegExp('already exists'));
  });

  it('should return undefined when tring to get a nonexist link by link ID', () => {
    expect(workflowGraph.getLinkWithID('nonexist')).toBe(undefined);
  });

  it('should throw an error when tring to get a nonexist link by link source and target', () => {
    expect(workflowGraph.getLink(
        { operatorID: 'source', portID: 'source port' },
        { operatorID: 'target', portID: 'taret port' }
      )).toBe(undefined);
  });

  it('should delete a link by ID properly', () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    workflowGraph.deleteLinkWithID(mockScanResultLink.linkID);

    expect(workflowGraph.getAllLinks().length).toEqual(0);
  });

  it('should delete a link by source and target properly', () => {
    workflowGraph.addOperator(mockScanPredicate);
    workflowGraph.addOperator(mockResultPredicate);
    workflowGraph.addLink(mockScanResultLink);
    workflowGraph.deleteLink(mockScanResultLink.source, mockScanResultLink.target);

    expect(workflowGraph.getAllLinks().length).toEqual(0);
  });

  it('should throw an error when trying to delete a link (by ID) that doesn\'t exist', () => {
    expect(() => {
      workflowGraph.deleteLinkWithID(mockScanResultLink.linkID);
    }).toThrowError(new RegExp(`doesn't exist`));
  });

  it('should throw an error when trying to delete a link (by source and target) that doesn\'t exist', () => {
    expect(() => {
      workflowGraph.deleteLink(
        { operatorID: 'source', portID: 'source port' },
        { operatorID: 'target', portID: 'taret port' }
      );
    }).toThrowError(new RegExp(`doesn't exist`));
  });

  it('should set the operator property(attributes) properly', () => {
    workflowGraph.addOperator(mockScanPredicate);

    const testProperty = { 'tableName': 'testTable' };
    workflowGraph.setOperatorProperty(mockScanPredicate.operatorID, testProperty);

    const operator = workflowGraph.getOperator(mockScanPredicate.operatorID);
    if (!operator) {
      throw new Error('test fails: operator is undefined');
    }
    expect(operator.operatorProperties).toEqual(testProperty);
  });

  it('should throw an error when trying to set the property of an nonexist operator', () => {
    expect(() => {
      const testProperty = { 'tableName': 'testTable' };
      workflowGraph.setOperatorProperty(mockScanPredicate.operatorID, testProperty);
    }).toThrowError(new RegExp(`doesn't exist`));
  });

});
