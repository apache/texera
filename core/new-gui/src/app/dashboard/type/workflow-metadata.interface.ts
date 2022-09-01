export interface WorkflowMetadata extends Readonly<{
  name: string;
  wid: number | undefined;
  creationTime: number | undefined;
  lastModifiedTime: number | undefined;
}> {}
