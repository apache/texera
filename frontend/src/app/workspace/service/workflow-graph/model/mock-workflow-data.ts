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
import { testOperator } from "../../../../common/testing/workflow-graph";

import { CommentBox, OperatorLink, OperatorPredicate, Point } from "../../../types/workflow-common.interface";
import { VIEW_RESULT_OP_TYPE } from "./workflow-graph";

/**
 * Provides mock data related operators and links:
 *
 * Operators:
 *  - 1: ScanSource
 *  - 2: NlpSentiment
 *  - 3: ViewResults
 *  - 4: MultiInputOutputOperator
 *  - 5: PresetEnabledOperator
 *
 * Links:
 *  - link-1: ScanSource -> ViewResults
 *  - link-2: ScanSource -> NlpSentiment
 *  - link-3: NlpSentiment -> ScanSource
 *
 * Invalid links:
 *  - link-4: (no source port) -> NlpSentiment
 *  - link-5: (NlpSentiment) -> (no target port)
 *
 */

export const mockPoint: Point = {
  x: 100,
  y: 100,
};

export const mockScanPredicate = testOperator("1", {
  operatorType: "ScanSource",
  operatorVersion: "scan",
  inputPorts: [],
  showAdvanced: true,
  isDisabled: false,
});

export const mockSentimentPredicate = testOperator("2", {
  operatorType: "NlpSentiment",
  operatorVersion: "nlp1",
  showAdvanced: true,
  isDisabled: false,
});

export const mockResultPredicate = testOperator("3", {
  operatorType: VIEW_RESULT_OP_TYPE,
  operatorVersion: "view1",
  outputPorts: [],
  showAdvanced: true,
  isDisabled: false,
});

export const mockMultiInputOutputPredicate = testOperator("4", {
  operatorType: "MultiInputOutput",
  operatorVersion: "m1",
  inputPorts: [{ portID: "input-0" }, { portID: "input-1" }, { portID: "input-2" }],
  outputPorts: [{ portID: "output-0" }, { portID: "output-1" }, { portID: "output-2" }],
  showAdvanced: true,
  isDisabled: false,
});

export const mockPresetEnabledPredicate = testOperator("5", {
  operatorType: "PresetEnabledOp",
  operatorVersion: "p1",
  inputPorts: [],
  outputPorts: [],
  showAdvanced: true,
});

export const mockJavaUDFPredicate = testOperator("6", {
  operatorType: "JavaUDF",
  operatorVersion: "p1",
  isDisabled: false,
});

export const mockPythonUDFPredicate = testOperator("7", {
  operatorType: "PythonUDF",
  operatorVersion: "p1",
  isDisabled: false,
});

export const mockHuggingFacePredicate = testOperator("8", {
  operatorType: "HuggingFace",
  operatorVersion: "hf1",
  operatorProperties: { task: "text-generation", modelId: "" },
  showAdvanced: true,
  isDisabled: false,
});

/** An operator backed by a Scala executor, where a loop-variable reference binds; see mockScalaExecutorSchema. */
export const mockScalaExecutorPredicate: OperatorPredicate = {
  operatorID: "12",
  operatorType: "ScalaExecutorOp",
  operatorVersion: "scala1",
  operatorProperties: { limit: 5 },
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
  isDisabled: false,
};

/** The start of a control block that declares the loop variable K. */
export const mockLoopStartPredicate: OperatorPredicate = {
  operatorID: "10",
  operatorType: "LoopStart",
  operatorVersion: "loop1",
  operatorProperties: { initialization: "K = 2", output: "table.iloc[K]" },
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
  isDisabled: false,
};

export const mockLoopEndPredicate: OperatorPredicate = {
  operatorID: "11",
  operatorType: "LoopEnd",
  operatorVersion: "loop1",
  operatorProperties: { update: "K += 1", condition: "K < 10" },
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
  isDisabled: false,
};

export const mockLoopStartScalaExecutorLink: OperatorLink = {
  linkID: "link-loop-1",
  source: {
    operatorID: mockLoopStartPredicate.operatorID,
    portID: mockLoopStartPredicate.outputPorts[0].portID,
  },
  target: {
    operatorID: mockScalaExecutorPredicate.operatorID,
    portID: mockScalaExecutorPredicate.inputPorts[0].portID,
  },
};
export const mockScalaExecutorLoopEndLink: OperatorLink = {
  linkID: "link-loop-2",
  source: {
    operatorID: mockScalaExecutorPredicate.operatorID,
    portID: mockScalaExecutorPredicate.outputPorts[0].portID,
  },
  target: {
    operatorID: mockLoopEndPredicate.operatorID,
    portID: mockLoopEndPredicate.inputPorts[0].portID,
  },
};
// Leave missing ports undefined so the invalid-link fixtures exercise graph validation.
function firstPortLink(linkID: string, source: OperatorPredicate, target: OperatorPredicate): OperatorLink {
  return {
    linkID,
    source: { operatorID: source.operatorID, portID: source.outputPorts[0]?.portID },
    target: { operatorID: target.operatorID, portID: target.inputPorts[0]?.portID },
  };
}

export const mockScanResultLink = firstPortLink("link-1", mockScanPredicate, mockResultPredicate);
export const mockScanSentimentLink = firstPortLink("link-2", mockScanPredicate, mockSentimentPredicate);
export const mockSentimentResultLink = firstPortLink("link-3", mockSentimentPredicate, mockResultPredicate);
export const mockFalseResultSentimentLink = firstPortLink("link-4", mockResultPredicate, mockSentimentPredicate);
export const mockFalseSentimentScanLink = firstPortLink("link-5", mockSentimentPredicate, mockScanPredicate);

export const mockCommentBox: CommentBox = {
  commentBoxID: "comment-box-1",
  comments: [],
  commentBoxPosition: mockPoint,
};
