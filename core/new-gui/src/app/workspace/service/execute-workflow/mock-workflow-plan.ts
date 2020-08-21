import { WorkflowGraph } from './../workflow-graph/model/workflow-graph';
import { mockScanPredicate, mockSentimentPredicate,
  mockResultPredicate, mockScanResultLink,
  mockScanSentimentLink, mockSentimentResultLink } from './../workflow-graph/model/mock-workflow-data';
import { LogicalPlan } from '../../types/execute-workflow.interface';


// TODO: unify the port handling interface
export const mockWorkflowPlan_scan_result: WorkflowGraph = new WorkflowGraph(
    [
        mockScanPredicate,
        mockResultPredicate
    ],
    [
        mockScanResultLink
    ]
);


export const mockLogicalPlan_scan_result: LogicalPlan = {
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
      destination: mockResultPredicate.operatorID,
    }
  ],
  breakpoints : []
};

export const mockWorkflowPlan_scan_sentiment_result: WorkflowGraph = new WorkflowGraph(
  [
    mockScanPredicate,
    mockSentimentPredicate,
    mockResultPredicate
  ],
  [
    mockScanSentimentLink,
    mockSentimentResultLink
  ]
);

export const mockLogicalPlan_scan_sentiment_result: LogicalPlan = {
  operators : [
    {
      ...mockScanPredicate.operatorProperties,
      operatorID: mockScanPredicate.operatorID,
      operatorType: mockScanPredicate.operatorType
    },
    {
      ...mockSentimentPredicate.operatorProperties,
      operatorID: mockSentimentPredicate.operatorID,
      operatorType: mockSentimentPredicate.operatorType
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
      destination: mockSentimentPredicate.operatorID,
    },
    {
      origin: mockSentimentPredicate.operatorID,
      destination: mockResultPredicate.operatorID,
    }
  ],
  breakpoints : []
};
