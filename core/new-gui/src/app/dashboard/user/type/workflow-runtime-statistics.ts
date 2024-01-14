export interface WorkflowRuntimeStatistics {
  operatorId: string;
  inputTupleCount: number;
  outputTupleCount: number;
  timestamp: number;
}
