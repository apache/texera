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

//All times in test Workflows are in PST because our local machine's timezone is PST

import { ExecutionMode, Workflow, WorkflowContent } from "../../common/type/workflow";
import { DashboardEntry } from "../type/dashboard-entry";

//the Date class creates unix timestamp based on local timezone, therefore test workflow time needs to be in local timezone
const oneDay = 86400000;
//januaryFirst1970 is 1970-01-01 in PST; oneDay is the number of milliseconds in a day
const januaryFirst1970 = 28800000;
export const testWorkflowContent = (operatorTypes: string[]): WorkflowContent => ({
  operators: operatorTypes.map(t => ({
    operatorType: t,
    operatorID: t,
    operatorVersion: "1",
    operatorProperties: {},
    inputPorts: [],
    outputPorts: [],
    showAdvanced: false,
  })),
  commentBoxes: [],
  links: [],
  operatorPositions: {},
  settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
});

const workflowFixture = (
  wid: number,
  name: string,
  operatorTypes: string[],
  creationTime: number,
  lastModifiedTime: number
): Workflow => ({
  wid,
  name,
  description: "dummy description",
  content: testWorkflowContent(operatorTypes),
  creationTime,
  lastModifiedTime,
  isPublished: 0,
  readonly: false,
});

const entryFixture = (workflow: Workflow, ownerName: string, ownerId: number): DashboardEntry =>
  new DashboardEntry({
    workflow,
    isOwner: true,
    ownerName,
    accessLevel: "Write",
    ownerId,
    coverImage: null,
  });

export const testWorkflow1: Workflow = workflowFixture(
  1,
  "workflow 1",
  ["Aggregation", "NlpSentiment", "SimpleSink"],
  januaryFirst1970,
  januaryFirst1970 + 2
);

export const testWorkflow2: Workflow = workflowFixture(
  2,
  "workflow 2",
  ["Aggregation", "NlpSentiment", "SimpleSink"],
  januaryFirst1970 + (oneDay + 3),
  januaryFirst1970 + (oneDay + 3)
);

export const testWorkflow3: Workflow = workflowFixture(
  3,
  "workflow 3",
  ["Aggregation", "NlpSentiment"],
  januaryFirst1970 + oneDay,
  januaryFirst1970 + (oneDay + 4)
);

export const testWorkflow4: Workflow = workflowFixture(
  4,
  "workflow 4",
  [],
  januaryFirst1970 + (oneDay + 3) * 2,
  januaryFirst1970 + oneDay * 2 + 6
);

export const testWorkflow5: Workflow = workflowFixture(
  5,
  "workflow 5",
  [],
  januaryFirst1970 + oneDay * 2,
  januaryFirst1970 + oneDay * 2 + 8
);

export const testDownloadWorkflow1: Workflow = workflowFixture(
  6,
  "workflow",
  [],
  januaryFirst1970,
  januaryFirst1970 + 2
);

export const testDownloadWorkflow2: Workflow = workflowFixture(
  7,
  "workflow",
  [],
  januaryFirst1970 + (oneDay + 3),
  januaryFirst1970 + (oneDay + 3)
);

export const testDownloadWorkflow3: Workflow = workflowFixture(
  8,
  "workflow",
  [],
  januaryFirst1970 + oneDay,
  januaryFirst1970 + (oneDay + 4)
);

export const testWorkflowFileNameConflictEntries: DashboardEntry[] = [
  entryFixture(testDownloadWorkflow1, "Texera", 1),
  entryFixture(testDownloadWorkflow2, "Texera", 1),
  entryFixture(testDownloadWorkflow3, "Angular", 2),
];

export const testWorkflowEntries: DashboardEntry[] = [
  entryFixture(testWorkflow1, "Texera", 1),
  entryFixture(testWorkflow2, "Texera", 1),
  entryFixture(testWorkflow3, "Angular", 2),
  entryFixture(testWorkflow4, "Angular", 2),
  entryFixture(testWorkflow5, "UCI", 3),
];

export const mockUserInfo = {
  1: { userName: "Texera", avatar: "avatar_url_1" },
  2: { userName: "Angular", avatar: "avatar_url_2" },
  3: { userName: "UCI", avatar: "avatar_url_3" },
};
