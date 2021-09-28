export interface WorkflowVersionEntry
  extends Readonly<{
    vId: number;
    creationTime: number;
    content: string;
  }> {}
