export interface DashboardProject {
  pid: number;
  name: string;
  description: string;
  ownerID: number;
  creationTime: number;
  color: string | null;
  accessLevel: string;
}

export interface PublicProject {
  pid: number;
  name: string;
  owner: string;
  creationTime: number;
}
