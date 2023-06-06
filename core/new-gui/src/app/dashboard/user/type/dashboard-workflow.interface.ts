import { Workflow } from "../../../common/type/workflow";

export interface DashboardWorkflow {
  isOwner: boolean;
  accessLevel: string;
  ownerName: string | undefined;
  workflow: Workflow;
  projectIDs: number[];
}
