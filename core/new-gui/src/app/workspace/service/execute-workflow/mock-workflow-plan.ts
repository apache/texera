import { WorkflowGraph } from './../workflow-graph/model/workflow-graph';
import { mockScanPredicate, mockResultPredicate, mockScanResultLink } from './../workflow-graph/model/mock-workflow-data';
import { LogicalPlan, LogicalLink, LogicalOperator } from '../../types/workflow-execute.interface';


// TODO: unify the port handling interface
export const MOCK_WORKFLOW_PLAN: WorkflowGraph = new WorkflowGraph(
    [
        mockScanPredicate,
        mockResultPredicate
    ],
    [
        mockScanResultLink
    ]
);


export const MOCK_LOGICAL_PLAN: LogicalPlan = {
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
