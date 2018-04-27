import { WorkflowGraph } from './workflow-graph';


describe('WorkflowGraph', () => {

  let workflowGraph: WorkflowGraph;

  beforeEach(() => {
    workflowGraph = new WorkflowGraph();
  });

  it('should throw an error when tring to get a non exist link ID', () => {
    expect(
      workflowGraph.getLinkWithID('nonexist')
    ).toThrowError();
  });

  it('should throw an error when tring to get a link from a nonexist source or target', () => {
    expect(
      workflowGraph.getLink(
        { operatorID: 'source', portID: 'source port' },
        { operatorID: 'target', portID: 'taret port' }
      )
    ).toThrowError();
  });

  it('should throw an error when tring to get an operator with nonexist operator ID', () => {
    expect(
      workflowGraph.getOperator('nonexist')
    ).toThrowError();
  });

  it('should throw an error when tring to delete an operator that doesn\'t exist', () => {
    expect(
      workflowGraph.deleteOperator('nonexist')
    ).toThrowError();
  });

  it('should throw an error when trying to delete a link that doesn\'t exist', () => {

  });

});
