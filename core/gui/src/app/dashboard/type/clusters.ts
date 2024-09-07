export interface Clusters {
  cid: number;
  name: string;
  owner_id: number;
  machineType: string;
  numberOfMachines: number;
  status: ClusterStatus;
  creationTime: string;
}

export enum ClusterStatus {
  LAUNCHED = "launched",
  FAILED = "failed",
  LAUNCHING = "launching",
  PAUSING = "pausing",
  PAUSED = "paused",
  RESUMING = "resuming",
  TERMINATING = "terminating",
  TERMINATED = "terminated",
}
