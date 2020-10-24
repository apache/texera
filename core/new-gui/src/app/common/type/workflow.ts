export interface Workflow extends Readonly<{
  wfId: number;
  userId: number;
  content: string;
}> {
}


export interface WorkflowWebResponse extends Readonly<{
  code: 0; // 0 represents success and 1 represents error
  message: string;
  workflow: Workflow;
}> {
}
