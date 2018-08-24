import { WorkflowGraph } from './../workflow-graph/model/workflow-graph';
import { mockScanPredicate, mockResultPredicate, mockScanResultLink } from './../workflow-graph/model/mock-workflow-data';
import { LogicalPlan } from '../../types/execute-workflow.interface';


// TODO: unify the port handling interface
export const mockWorkflowPlan: WorkflowGraph = new WorkflowGraph(
    [
        mockScanPredicate,
        mockResultPredicate
    ],
    [
        mockScanResultLink
    ]
);


export const mockLogicalPlan: LogicalPlan = {
  operators : [
    {
      ...mockScanPredicate.operatorProperties,
      operatorID: mockScanPredicate.operatorID,
      operatorType: mockScanPredicate.operatorType
    },
    {
      ...mockResultPredicate.operatorProperties,
      operatorID: mockResultPredicate.operatorID,
      operatorType: mockResultPredicate.operatorType
    }
  ],
  links : [
    {
      origin: mockScanPredicate.operatorID,
      destination: mockResultPredicate.operatorID
    }
  ]
};
