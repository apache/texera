import { MOCK_OPERATOR_SCHEMA_LIST } from './../../operator-metadata/mock-operator-metadata.data';
import { OperatorPredicate, OperatorLink } from './../../../types/workflow-graph';


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

export const mockLinkSourceViewResult: OperatorLink = {
  linkID: 'link-1',
  sourceOperator: mockScanSourcePredicate.operatorID,
  sourcePort: mockScanSourcePredicate.outputPorts[0],
  targetOperator: mockViewResultPredicate.operatorID,
  targetPort: mockViewResultPredicate.inputPorts[0]
};

export const mockLinkSourceSentiment: OperatorLink = {
  linkID: 'link-2',
  sourceOperator: mockScanSourcePredicate.operatorID,
  sourcePort: mockScanSourcePredicate.outputPorts[0],
  targetOperator: mockSentimentAnalysisPredicate.operatorID,
  targetPort: mockSentimentAnalysisPredicate.inputPorts[0]
};

export const mockLinkSentimentViewResult: OperatorLink = {
  linkID: 'link-3',
  sourceOperator: mockSentimentAnalysisPredicate.operatorID,
  sourcePort: mockSentimentAnalysisPredicate.inputPorts[0],
  targetOperator: mockSentimentAnalysisPredicate.operatorID,
  targetPort: mockSentimentAnalysisPredicate.inputPorts[0]
};
