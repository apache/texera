export interface UserProject
  extends Readonly<{
    pid: number;
    name: string;
    description: string;
    ownerID: number;
    creationTime: number;
    color: string | null;
  }> {}
