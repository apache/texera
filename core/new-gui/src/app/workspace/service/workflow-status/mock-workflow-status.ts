import { Statistics, ProcessStatus, OperatorStates } from '../../types/execute-workflow.interface';
import { OperatorPredicate } from '../../types/workflow-common.interface';

export const mockScanOperatorID = 'c5207d7d-e94f-4796-b9c5-aa7e63f81e0a';

export const mockScanPredicateForStatus: OperatorPredicate = {
  operatorID: 'operator-c5207d7d-e94f-4796-b9c5-aa7e63f81e0a',
  operatorType: 'ScanSource',

  operatorProperties: {
  },
  operatorGroupName: 'Source',
  inputPorts: [],
  outputPorts: ['output-0'],
  showAdvanced: true
};

export const mockScanStatistic1: Statistics = {
  inputCount: 1234,
  outputCount: 1234,
  speed: 10.4
};

export const mockScanStatistic2: Statistics = {
  inputCount: 1888,
  outputCount: 1880,
  speed: 8.6
};

// mockStatus1 for a workflow with only one mockScanOperator
// this mockScanOperator can be found in ./../../workflow-graph/model/mock-workflow-data.ts
export const mockStatus1: ProcessStatus = {
  code: 0,
  message: 'Processing',
  operatorStates: {
    'c5207d7d-e94f-4796-b9c5-aa7e63f81e0a': OperatorStates.Running,
  },
  operatorStatistics: {
    'c5207d7d-e94f-4796-b9c5-aa7e63f81e0a': mockScanStatistic1,
  }
};

// this mockStatus is the last status received from backend
export const mockStatus2: ProcessStatus = {
  code: 0,
  message: 'Process Completed',
  operatorStates: {
    'c5207d7d-e94f-4796-b9c5-aa7e63f81e0a': OperatorStates.Completed,
  },
  operatorStatistics: {
    'c5207d7d-e94f-4796-b9c5-aa7e63f81e0a': mockScanStatistic2,
  }
};
