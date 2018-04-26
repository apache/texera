import { WorkflowGraph } from './workflow-graph';


describe('WorkflowGraph', () => {

  let workflowGraph: WorkflowGraph;

  beforeEach(() => {
    workflowGraph = new WorkflowGraph();
  });

  it('should throw an error when get a non exist link ID', () => {
    expect(
      workflowGraph.getLinkWithID('nonexist')
    ).toThrowError();
  });

  it('should throw an error when get a link from a nonexist source or target', () => {
    expect(
      workflowGraph.getLink('source', 'source port', 'target', 'taret port')
    ).toThrowError();
  });

  it('should throw an error when get an operator with nonexist operator ID', () => {
    expect(
      workflowGraph.getOperator('nonexist')
    ).toThrowError();

  });

});
