import { Workflow } from "./workflow";

export interface WorkflowInfo extends Readonly<{
  isOwner: boolean;
  accessLevel: string;
  ownerName: string | undefined;
  workflow: Workflow;
}> {
}
