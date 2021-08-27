export interface WorkflowVersionEntry extends Readonly<{
  vId: number|undefined;
  creationTime: number|undefined;
  content: string | undefined;
}> {}
