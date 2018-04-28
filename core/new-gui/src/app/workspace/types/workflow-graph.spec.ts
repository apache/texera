import {
  getMockScanPredicate, getMockSentimentPredicate, getMockResultPredicate,
  getMockScanSentimentLink, getMockSentimentResultLink
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

  it('should throw an error when tring to get a link from a nonexist source or target', () => {
    expect(() => {
      workflowGraph.getLink(
        { operatorID: 'source', portID: 'source port' },
        { operatorID: 'target', portID: 'taret port' }
      );
    }).toThrowError();

  });

  it('should add a link and get it properly by link ID', () => {

  });

  it('should throw an error when tring to get a nonexist link ID', () => {
    expect(() => {
      workflowGraph.getLinkWithID('nonexist');
    }
    ).toThrowError(new RegExp(`doesn't exist`));
  });


  it('should throw an error when trying to delete a link that doesn\'t exist', () => {

  });

});
