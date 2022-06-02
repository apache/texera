import { Workflow } from "../../common/type/workflow";

export interface DashboardWorkflowEntry
  extends Readonly<{
    isOwner: boolean;
    accessLevel: string;
    ownerName: string | undefined;
    workflow: Workflow;
    projectIDs: number[];
  }> {}
