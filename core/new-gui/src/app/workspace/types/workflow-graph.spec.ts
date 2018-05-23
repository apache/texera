import {
  getMockScanPredicate, getMockSentimentPredicate, getMockResultPredicate,
  getMockScanSentimentLink, getMockSentimentResultLink, getMockScanResultLink
} from './../service/workflow-graph/model/mock-workflow-data';
import { WorkflowGraph } from './workflow-graph';

describe('WorkflowGraph', () => {

  let workflowGraph: WorkflowGraph;

  beforeEach(() => {
    workflowGraph = new WorkflowGraph();
  });

  it('should have an empty graph from the beginning', () => {
    expect(workflowGraph.getOperators().length).toEqual(0);
    expect(workflowGraph.getLinks().length).toEqual(0);
  });

  it('should load an existing graph properly', () => {
    workflowGraph = new WorkflowGraph(
      [getMockScanPredicate(), getMockSentimentPredicate(), getMockResultPredicate()],
      [getMockScanSentimentLink(), getMockSentimentResultLink()]
    );
    expect(workflowGraph.getOperators().length).toEqual(3);
    expect(workflowGraph.getLinks().length).toEqual(2);
  });

  it('should add an operator and get it properly', () => {
    workflowGraph.addOperator(getMockScanPredicate());
    expect(workflowGraph.getOperator(getMockScanPredicate().operatorID)).toBeTruthy();
    expect(workflowGraph.getOperators().length).toEqual(1);
    expect(workflowGraph.getOperators()[0]).toEqual(getMockScanPredicate());
  });

  it('should throw an error when tring to get an operator with a nonexist operator ID', () => {
    expect(() => {
      workflowGraph.getOperator('nonexist');
    }).toThrowError();
  });

  it('should throw an error when trying to add an operator with an existing operator ID', () => {
    expect(() => {
      workflowGraph.addOperator(getMockScanPredicate());
      workflowGraph.addOperator(getMockScanPredicate());
    }).toThrowError(new RegExp('already exists'));
  });

  it('should delete an operator properly', () => {
    workflowGraph.addOperator(getMockScanPredicate());
    workflowGraph.deleteOperator(getMockScanPredicate().operatorID);
    expect(workflowGraph.getOperators().length).toBe(0);
  });

  it('should throw an error when tring to delete an operator that doesn\'t exist', () => {
    expect(() => {
      workflowGraph.deleteOperator('nonexist');
    }).toThrowError(new RegExp(`doesn't exist`));
  });

  it('should add and get a link properly', () => {
    workflowGraph.addOperator(getMockScanPredicate());
    workflowGraph.addOperator(getMockResultPredicate());
    workflowGraph.addLink(getMockScanResultLink());

    expect(workflowGraph.getLinkWithID(getMockScanResultLink().linkID)).toEqual(getMockScanResultLink());
    expect(workflowGraph.getLink(
      getMockScanResultLink().source, getMockScanResultLink().target
    )).toEqual(getMockScanResultLink());
    expect(workflowGraph.getLinks().length).toEqual(1);
  });

  it('should throw an error when try to add a link with an existingID', () => {
    workflowGraph.addOperator(getMockScanPredicate());
    workflowGraph.addOperator(getMockResultPredicate());
    workflowGraph.addOperator(getMockSentimentPredicate());
    workflowGraph.addLink(getMockScanResultLink());

    // modify the target, but don't modify the ID
    const mockLink = getMockScanResultLink();
    mockLink.target = {
      operatorID: getMockSentimentPredicate().operatorID,
      portID: getMockSentimentPredicate().inputPorts[0]
    };

    expect(() => {
      workflowGraph.addLink(mockLink);
    }).toThrowError(new RegExp('already exists'));
  });

  it('should throw an error when try to add a link with exising source and target but different ID', () => {
    workflowGraph.addOperator(getMockScanPredicate());
    workflowGraph.addOperator(getMockResultPredicate());
    workflowGraph.addOperator(getMockSentimentPredicate());
    workflowGraph.addLink(getMockScanResultLink());

    // modify the ID, but don't modify the source/target
    const mockLink = getMockScanResultLink();
    mockLink.linkID = 'new-link-id';

    expect(() => {
      workflowGraph.addLink(mockLink);
    }).toThrowError(new RegExp('already exists'));
  });

  it('should throw an error when tring to get a nonexist link by link ID', () => {
    expect(() => {
      workflowGraph.getLinkWithID('nonexist');
    }).toThrowError(new RegExp(`doesn't exist`));
  });

  it('should throw an error when tring to get a nonexist link by link source and target', () => {
    expect(() => {
      workflowGraph.getLink(
        { operatorID: 'source', portID: 'source port' },
        { operatorID: 'target', portID: 'taret port' }
      );
    }).toThrowError(new RegExp(`doesn't exist`));
  });

  it('should delete a link by ID properly', () => {
    workflowGraph.addOperator(getMockScanPredicate());
    workflowGraph.addOperator(getMockResultPredicate());
    workflowGraph.addLink(getMockScanResultLink());
    workflowGraph.deleteLinkWithID(getMockScanResultLink().linkID);

    expect(workflowGraph.getLinks().length).toEqual(0);
  });

  it('should delete a link by source and target properly', () => {
    workflowGraph.addOperator(getMockScanPredicate());
    workflowGraph.addOperator(getMockResultPredicate());
    workflowGraph.addLink(getMockScanResultLink());
    workflowGraph.deleteLink(getMockScanResultLink().source, getMockScanResultLink().target);

    expect(workflowGraph.getLinks().length).toEqual(0);
  });

  it('should throw an error when trying to delete a link (by ID) that doesn\'t exist', () => {
    expect(() => {
      workflowGraph.deleteLinkWithID(getMockScanResultLink().linkID);
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
    workflowGraph.addOperator(getMockScanPredicate());

    const testProperty = { 'tableName': 'testTable' };
    workflowGraph.changeOperatorProperty(getMockScanPredicate().operatorID, testProperty);

    expect(workflowGraph.getOperator(
      getMockScanPredicate().operatorID).operatorProperties
    ).toEqual(testProperty);
  });

  it('should throw an error when trying to set the property of an nonexist operator', () => {
    expect(() => {
      const testProperty = { 'tableName': 'testTable' };
      workflowGraph.changeOperatorProperty(getMockScanPredicate().operatorID, testProperty);
    }).toThrowError(new RegExp(`doesn't exist`));
  });

});
