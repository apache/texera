import { Point, OperatorPredicate, OperatorLink } from './../../../types/workflow-common.interface';

/**
 * Provides mock data related operators and links:
 *
 * Operators:
 *  - 1: ScanSource
 *  - 2: NlpSentiment
 *  - 3: ViewResults
 *
 * Links:
 *  - link-1: ScanSource -> ViewResults
 *  - link-2: ScanSource -> NlpSentiment
 *  - link-3: NlpSentiment -> ScanSource
 *
 * Invalid links:
 *  - link-4: (no source port) -> NlpSentiment
 *  - link-5: (NlpSentiment) -> (no target port)
 *
 */

export const mockPoint: Point = {
  x: 100, y: 100
};

export const mockScanPredicate: OperatorPredicate = {
  operatorID: '1',
  operatorType: 'ScanSource',
  operatorProperties: {
  },
  inputPorts: [],
  outputPorts: ['output-0']
};

export const mockSentimentPredicate: OperatorPredicate = {
  operatorID: '2',
  operatorType: 'NlpSentiment',
  operatorProperties: {
  },
  inputPorts: ['input-0'],
  outputPorts: ['output-0']
};

export const mockResultPredicate: OperatorPredicate = {
  operatorID: '3',
  operatorType: 'ViewResults',
  operatorProperties: {
  },
  inputPorts: ['input-0'],
  outputPorts: []
};

export const mockScanResultLink: OperatorLink = {
  linkID: 'link-1',
  source: {
    operatorID: mockScanPredicate.operatorID,
    portID: mockScanPredicate.outputPorts[0]
  },
  target: {
    operatorID: mockResultPredicate.operatorID,
    portID: mockResultPredicate.inputPorts[0]
  }
};

export const mockScanSentimentLink: OperatorLink = {
  linkID: 'link-2',
  source: {
    operatorID: mockScanPredicate.operatorID,
    portID: mockScanPredicate.outputPorts[0]
  },
  target: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.inputPorts[0]
  }
};

export const mockSentimentResultLink: OperatorLink = {
  linkID: 'link-3',
  source: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.outputPorts[0]
  },
  target: {
    operatorID: mockResultPredicate.operatorID,
    portID: mockResultPredicate.inputPorts[0]
  }
};


export const mockFalseResultSentimentLink: OperatorLink = {
  linkID: 'link-4',
  source: {
    operatorID: mockResultPredicate.operatorID,
    portID: undefined as any
  },
  target: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.inputPorts[0]
  }
};

export const mockFalseSentimentScanLink: OperatorLink = {
  linkID: 'link-5',
  source: {
    operatorID: mockSentimentPredicate.operatorID,
    portID: mockSentimentPredicate.outputPorts[0]
  },
  target: {
    operatorID: mockScanPredicate.operatorID,
    portID: undefined as any
  }
};
