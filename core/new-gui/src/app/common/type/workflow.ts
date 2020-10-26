export interface Workflow extends Readonly<{
  name: string;
  wfId: number;
  content: string;
  creationTime: string;
  lastModifiedTime: string;
}> {
}


export interface WorkflowWebResponse extends Readonly<{
  code: 0; // 0 represents success and 1 represents error
  message: string;
  workflow: Workflow;
}> {
}
