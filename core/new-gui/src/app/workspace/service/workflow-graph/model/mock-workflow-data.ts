import { MOCK_OPERATOR_SCHEMA_LIST } from './../../operator-metadata/mock-operator-metadata.data';
import { OperatorPredicate } from './../../../types/workflow-graph';


export const mockScanSourcePredicate: OperatorPredicate = {
  operatorID: '1',
  operatorType: 'ScanSource',
  operatorProperties: {
  },
  inputPorts: [],
  outputPorts: ['output-0']
};

export const mockSentimentAnalysisPredicate: OperatorPredicate = {
  operatorID: '2',
  operatorType: 'NlpSentiment',
  operatorProperties: {
  },
  inputPorts: ['input-0'],
  outputPorts: ['output-0']
};

export const mockViewResultPredicate: OperatorPredicate = {
  operatorID: '3',
  operatorType: 'ViewResults',
  operatorProperties: {
  },
  inputPorts: ['input-0'],
  outputPorts: []
};
