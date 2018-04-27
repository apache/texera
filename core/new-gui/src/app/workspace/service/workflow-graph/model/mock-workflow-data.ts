import { Point } from './../../../types/common.interface';
import { MOCK_OPERATOR_SCHEMA_LIST } from './../../operator-metadata/mock-operator-metadata.data';
import { OperatorPredicate, OperatorLink } from './../../../types/workflow-graph';

export const mockPoint: Point = {
  x: 100, y: 100
};

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
  source: {
    operatorID: mockScanSourcePredicate.operatorID,
    portID: mockScanSourcePredicate.outputPorts[0]
  },
  target: {
    operatorID: mockViewResultPredicate.operatorID,
    portID: mockViewResultPredicate.inputPorts[0]
  }
};

export const mockLinkSourceSentiment: OperatorLink = {
  linkID: 'link-2',
  source: {
    operatorID: mockScanSourcePredicate.operatorID,
    portID: mockScanSourcePredicate.outputPorts[0]
  },
  target: {
    operatorID: mockSentimentAnalysisPredicate.operatorID,
    portID:  mockSentimentAnalysisPredicate.inputPorts[0]
  }
};

export const mockLinkSentimentViewResult: OperatorLink = {
  linkID: 'link-3',
  source: {
    operatorID: mockSentimentAnalysisPredicate.operatorID,
    portID:  mockSentimentAnalysisPredicate.inputPorts[0]
  },
  target: {
    operatorID: mockViewResultPredicate.operatorID,
    portID: mockViewResultPredicate.inputPorts[0]
  }
};
