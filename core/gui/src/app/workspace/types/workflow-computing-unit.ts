export interface WorkflowComputingUnit {
  cuid: number;
  uid: number;
  name: string;
  creationTime: number;
  terminateTime: number | undefined;
}

export interface WorkflowComputingUnitResourceLimit {
  cpuLimit: string;
  memoryLimit: string;
  gpuLimit: string;
}

export interface WorkflowComputingUnitMetrics {
  cpuUsage: string;
  memoryUsage: string;
}

export interface DashboardWorkflowComputingUnit {
  computingUnit: WorkflowComputingUnit;
  uri: string;
  status: string;
  metrics: WorkflowComputingUnitMetrics;
  resourceLimits: WorkflowComputingUnitResourceLimit;
}
