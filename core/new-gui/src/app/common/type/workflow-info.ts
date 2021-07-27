import {Workflow} from "./workflow";

export interface WorkflowInfo extends Readonly<{
  isOwner: boolean;
  workflow: Workflow;
}> {
}
