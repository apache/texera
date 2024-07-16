export enum Privilege {
  NONE = "NONE",
  READ = "READ",
  WRITE = "WRITE",
}

export interface ShareAccess
  extends Readonly<{
    email: string;
    name: string;
    privilege: Privilege;
  }> {}
