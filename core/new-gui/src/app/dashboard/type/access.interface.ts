export interface AccessEntry
  extends Readonly<{
    userName: string;
    accessLevel: string;
  }> {}

export enum Privilege {
  NONE = "NONE",
  READ = "READ",
  WRITE = "WRITE",
}
export interface WorkflowAccessEntry
  extends Readonly<{
    email: string;
    name: string;
    privilege: Privilege;
  }> {}
