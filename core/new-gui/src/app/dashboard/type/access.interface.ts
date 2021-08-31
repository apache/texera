export interface AccessEntry
  extends Readonly<{
    userName: string;
    accessLevel: string;
  }> {}
