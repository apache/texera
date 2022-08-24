import { Workflow } from "../../common/type/workflow";
import { WorkflowMetadata } from "./workflow-metadata.interface";

export type WorkflowAccessLevel = 'none' | 'read' | 'write' | 'execute';

export interface DashboardWorkflowEntry
  extends Readonly<{
    workflow: WorkflowMetadata;
    isOwner: boolean;
    accessLevel: WorkflowAccessLevel;
    ownerName: string | undefined;
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
