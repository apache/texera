export interface OperatorRuntimeStatistics {
  [key: string]: any;
  operatorId: string;
  inputTupleCount: number;
  outputTupleCount: number;
  timestamp: number;
  totalDataProcessingTime: number;
  totalControlProcessingTime: number;
  totalIdleTime: number;
  numberOfWorkers: number;
}
