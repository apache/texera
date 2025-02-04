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
  metrics: WorkflowComputingUnitMetrics;
}

export interface WorkflowComputingUnitMetrics {
  cpuUsage: number;
  memoryUsage: number;
}
