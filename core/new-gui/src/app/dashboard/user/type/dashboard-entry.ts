import { Workflow } from "../../../common/type/workflow";
import { DashboardWorkflow } from "./dashboard-workflow.interface";

export class DashboardEntry {
  isOwner: boolean;
  accessLevel: string;
  ownerName: string | undefined;
  workflow: Workflow;
  projectIDs: number[];
  checked = false;

  constructor(value: DashboardWorkflow) {
    this.isOwner = value.isOwner;
    this.accessLevel = value.accessLevel;
    this.ownerName = value.ownerName;
    this.workflow = value.workflow;
    this.projectIDs = value.projectIDs;
  }
}
