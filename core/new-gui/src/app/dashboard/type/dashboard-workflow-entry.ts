import { Workflow } from "../../common/type/workflow";

export interface DashboardWorkflowEntry
  extends Readonly<{
    isOwner: boolean;
    accessLevel: string;
    ownerName: string | undefined;
    workflow: Workflow;
    projectIDs: number[];
  }> {}

/**
 * This enum type helps indicate the method in which DashboardWorkflowEntry[] is sorted
 */
export enum SortMethod {
  NameAsc,
  NameDesc,
  CreateTimeDesc,
  EditTimeDesc,
}
