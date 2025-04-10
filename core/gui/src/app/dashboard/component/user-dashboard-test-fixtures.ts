//All times in test Workflows are in PST because our local machine's timezone is PST

import { Workflow, WorkflowContent } from "../../common/type/workflow";
import { DashboardEntry } from "../type/dashboard-entry";
import { DashboardProject } from "../type/dashboard-project.interface";
import { environment } from "../../../environments/environment";

//the Date class creates unix timestamp based on local timezone, therefore test workflow time needs to be in local timezone
const oneDay = 86400000;
const januaryFirst1970 = 28800000; // 1970-01-01 in PST
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
  settings: { dataTransferBatchSize: environment.defaultDataTransferBatchSize },
});

export const testWorkflow1: Workflow = {
  wid: 1,
  name: "workflow 1",
  description: "dummy description",
  content: testWorkflowContent(["Aggregation", "NlpSentiment", "SimpleSink"]),
  creationTime: januaryFirst1970,
  lastModifiedTime: januaryFirst1970 + 2,
  isPublished: 0,
  readonly: false,
};

export const testWorkflow2: Workflow = {
  wid: 2,
  name: "workflow 2",
  description: "dummy description",
  content: testWorkflowContent(["Aggregation", "NlpSentiment", "SimpleSink"]),
  creationTime: januaryFirst1970 + (oneDay + 3),
  lastModifiedTime: januaryFirst1970 + (oneDay + 3),
  isPublished: 0,
  readonly: false,
};

export const testWorkflow3: Workflow = {
  wid: 3,
  name: "workflow 3",
  description: "dummy description",
  content: testWorkflowContent(["Aggregation", "NlpSentiment"]),
  creationTime: januaryFirst1970 + oneDay,
  lastModifiedTime: januaryFirst1970 + (oneDay + 4),
  isPublished: 0,
  readonly: false,
};

export const testWorkflow4: Workflow = {
  wid: 4,
  name: "workflow 4",
  description: "dummy description",
  content: testWorkflowContent([]),
  creationTime: januaryFirst1970 + (oneDay + 3) * 2,
  lastModifiedTime: januaryFirst1970 + oneDay * 2 + 6,
  isPublished: 0,
  readonly: false,
};

export const testWorkflow5: Workflow = {
  wid: 5,
  name: "workflow 5",
  description: "dummy description",
  content: testWorkflowContent([]),
  creationTime: januaryFirst1970 + oneDay * 2,
  lastModifiedTime: januaryFirst1970 + oneDay * 2 + 8,
  isPublished: 0,
  readonly: false,
};

export const testDownloadWorkflow1: Workflow = {
  wid: 6,
  name: "workflow",
  description: "dummy description",
  content: testWorkflowContent([]),
  creationTime: januaryFirst1970, //januaryFirst1970 is 1970-01-01 in PST
  lastModifiedTime: januaryFirst1970 + 2,
  isPublished: 0,
  readonly: false,
};

export const testDownloadWorkflow2: Workflow = {
  wid: 7,
  name: "workflow",
  description: "dummy description",
  content: testWorkflowContent([]),
  creationTime: januaryFirst1970 + (oneDay + 3), // oneDay is the number of milliseconds in a day
  lastModifiedTime: januaryFirst1970 + (oneDay + 3),
  isPublished: 0,
  readonly: false,
};

export const testDownloadWorkflow3: Workflow = {
  wid: 8,
  name: "workflow",
  description: "dummy description",
  content: testWorkflowContent([]),
  creationTime: januaryFirst1970 + oneDay,
  lastModifiedTime: januaryFirst1970 + (oneDay + 4),
  isPublished: 0,
  readonly: false,
};

export const testWorkflowFileNameConflictEntries: DashboardEntry[] = [
  new DashboardEntry({
    workflow: testDownloadWorkflow1,
    isOwner: true,
    ownerName: "Texera",
    accessLevel: "Write",
    projectIDs: [1],
    ownerId: 1,
  }),
  new DashboardEntry({
    workflow: testDownloadWorkflow2,
    isOwner: true,
    ownerName: "Texera",
    accessLevel: "Write",
    projectIDs: [1, 2],
    ownerId: 1,
  }),
  new DashboardEntry({
    workflow: testDownloadWorkflow3,
    isOwner: true,
    ownerName: "Angular",
    accessLevel: "Write",
    projectIDs: [1],
    ownerId: 2,
  }),
];

export const testWorkflowEntries: DashboardEntry[] = [
  new DashboardEntry({
    workflow: testWorkflow1,
    isOwner: true,
    ownerName: "Texera",
    accessLevel: "Write",
    projectIDs: [1],
    ownerId: 1,
  }),
  new DashboardEntry({
    workflow: testWorkflow2,
    isOwner: true,
    ownerName: "Texera",
    accessLevel: "Write",
    projectIDs: [1, 2],
    ownerId: 1,
  }),
  new DashboardEntry({
    workflow: testWorkflow3,
    isOwner: true,
    ownerName: "Angular",
    accessLevel: "Write",
    projectIDs: [1],
    ownerId: 2,
  }),
  new DashboardEntry({
    workflow: testWorkflow4,
    isOwner: true,
    ownerName: "Angular",
    accessLevel: "Write",
    projectIDs: [3],
    ownerId: 2,
  }),
  new DashboardEntry({
    workflow: testWorkflow5,
    isOwner: true,
    ownerName: "UCI",
    accessLevel: "Write",
    projectIDs: [3],
    ownerId: 3,
  }),
];

export const testUserProjects: DashboardProject[] = [
  { pid: 1, name: "Project1", description: "p1", ownerId: 1, color: "#ffffff", creationTime: 0, accessLevel: "WRITE" },
  { pid: 2, name: "Project2", description: "p1", ownerId: 1, color: "#ffffff", creationTime: 0, accessLevel: "WRITE" },
  { pid: 3, name: "Project3", description: "p1", ownerId: 1, color: "#ffffff", creationTime: 0, accessLevel: "WRITE" },
];

export const mockUserInfo = {
  1: { userName: "Texera", googleAvatar: "avatar_url_1" },
  2: { userName: "Angular", googleAvatar: "avatar_url_2" },
  3: { userName: "UCI", googleAvatar: "avatar_url_3" },
};
