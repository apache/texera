import { WorkflowGraph } from "../workflow-graph/model/workflow-graph";
import {
  mockScanPredicate,
  mockSentimentPredicate,
  mockResultPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentResultLink,
} from "../workflow-graph/model/mock-workflow-data";
import { LogicalPlan } from "../../types/execute-workflow.interface";

export const mockWorkflowPlan_scan_result: WorkflowGraph = new WorkflowGraph(
  [mockScanPredicate, mockResultPredicate],
  [mockScanResultLink]
);

export const mockLogicalPlan_scan_result: LogicalPlan = {
  operators: [
    {
      ...mockScanPredicate.operatorProperties,
      operatorID: mockScanPredicate.operatorID,
      operatorType: mockScanPredicate.operatorType,
      inputPorts: mockScanPredicate.inputPorts,
      outputPorts: mockScanPredicate.outputPorts,
    },
    {
      ...mockResultPredicate.operatorProperties,
      operatorID: mockResultPredicate.operatorID,
      operatorType: mockResultPredicate.operatorType,
      inputPorts: mockResultPredicate.inputPorts,
      outputPorts: mockResultPredicate.outputPorts,
    },
  ],
  links: [
    {
      fromOpId: mockScanPredicate.operatorID,
      fromPortId: { id: 0, internal: false },
      toOpId: mockResultPredicate.operatorID,
      toPortId: { id: 0, internal: false },
    },
  ],
  breakpoints: [],
  opsToViewResult: [],
  opsToReuseResult: [],
};

export const mockWorkflowPlan_scan_sentiment_result: WorkflowGraph = new WorkflowGraph(
  [mockScanPredicate, mockSentimentPredicate, mockResultPredicate],
  [mockScanSentimentLink, mockSentimentResultLink]
);

export const mockLogicalPlan_scan_sentiment_result: LogicalPlan = {
  operators: [
    {
      ...mockScanPredicate.operatorProperties,
      operatorID: mockScanPredicate.operatorID,
      operatorType: mockScanPredicate.operatorType,
    },
    {
      ...mockSentimentPredicate.operatorProperties,
      operatorID: mockSentimentPredicate.operatorID,
      operatorType: mockSentimentPredicate.operatorType,
    },
    {
      ...mockResultPredicate.operatorProperties,
      operatorID: mockResultPredicate.operatorID,
      operatorType: mockResultPredicate.operatorType,
    },
  ],
  links: [
    {
      fromOpId: mockScanPredicate.operatorID,
      fromPortId: { id: 0, internal: false },
      toOpId: mockSentimentPredicate.operatorID,
      toPortId: { id: 0, internal: false },
    },
    {
      fromOpId: mockSentimentPredicate.operatorID,
      fromPortId: { id: 0, internal: false },
      toOpId: mockResultPredicate.operatorID,
      toPortId: { id: 0, internal: false },
    },
  ],
  breakpoints: [],
  opsToViewResult: [],
  opsToReuseResult: [],
};
