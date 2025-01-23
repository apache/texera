import { Workflow } from "../../common/type/workflow";

export interface DashboardWorkflow {
  isOwner: boolean;
  ownerName: string | undefined;
  workflow: Workflow;
  projectIDs: number[];
  accessLevel: string;
  ownerId: number;
}
