export interface WorkflowVersionEntry
  extends Readonly<{
    vId: number;
    creationTime: number;
    content: string;
    importance: boolean;
  }> {}

export interface WorkflowVersionCollapsableEntry extends WorkflowVersionEntry {
  expand: boolean; // for double binding with nzExpand
}
