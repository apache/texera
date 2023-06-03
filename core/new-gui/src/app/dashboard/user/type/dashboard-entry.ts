import { Workflow } from "../../../common/type/workflow";
import { DashboardWorkflowEntry } from "./dashboard-workflow-entry";

export class DashboardEntry {
  isOwner: boolean;
  accessLevel: string;
  ownerName: string | undefined;
  workflow: Workflow;
  projectIDs: number[];
  checked = false;

  constructor(value: DashboardWorkflowEntry) {
    this.isOwner = value.isOwner;
    this.accessLevel = value.accessLevel;
    this.ownerName = value.ownerName;
    this.workflow = value.workflow;
    this.projectIDs = value.projectIDs;
  }
}
