export interface WorkflowComputingUnit {
  cuid: number;
  uid: number;
  name: string;
  creationTime: number;
  terminateTime: number | undefined;
}

export interface DashboardWorkflowComputingUnit {
  computingUnit: WorkflowComputingUnit;
  uri: string;
  status: string;
}
