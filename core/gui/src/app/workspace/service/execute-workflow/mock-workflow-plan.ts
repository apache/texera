/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
      ...mockResultPredicate.operatorProperties,
      operatorID: mockResultPredicate.operatorID,
      operatorType: mockResultPredicate.operatorType,
      inputPorts: mockResultPredicate.inputPorts,
      outputPorts: mockResultPredicate.outputPorts,
    },
    {
      ...mockScanPredicate.operatorProperties,
      operatorID: mockScanPredicate.operatorID,
      operatorType: mockScanPredicate.operatorType,
      inputPorts: mockScanPredicate.inputPorts,
      outputPorts: mockScanPredicate.outputPorts,
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
  opsToViewResult: [],
  opsToReuseResult: [],
};
