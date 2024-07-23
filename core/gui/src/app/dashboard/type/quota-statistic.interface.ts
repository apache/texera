export interface DatasetQuota
  extends Readonly<{
    did: number;
    name: string;
    creationTime: number;
    size: number;
  }> {}
