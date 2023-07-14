export interface WorkflowMetadata {
  name: string;
  description: string | undefined;
  wid: number | undefined;
  creationTime: number | undefined;
  lastModifiedTime: number | undefined;
  readonly: boolean;
}
