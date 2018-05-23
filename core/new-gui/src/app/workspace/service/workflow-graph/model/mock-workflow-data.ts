import { cloneDeep } from 'lodash-es';
import { Point, OperatorPredicate, OperatorLink } from './../../../types/common.interface';
import { getMockOperatorSchemaList } from './../../operator-metadata/mock-operator-metadata.data';

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

export const getMockPoint: () => Point = () => cloneDeep({
  x: 100, y: 100
});

export const getMockScanPredicate: () => OperatorPredicate = () => cloneDeep({
  operatorID: '1',
  operatorType: 'ScanSource',
  operatorProperties: {
  },
  inputPorts: [],
  outputPorts: ['output-0']
});

export const getMockSentimentPredicate: () => OperatorPredicate = () => cloneDeep({
  operatorID: '2',
  operatorType: 'NlpSentiment',
  operatorProperties: {
  },
  inputPorts: ['input-0'],
  outputPorts: ['output-0']
});

export const getMockResultPredicate: () => OperatorPredicate = () => cloneDeep({
  operatorID: '3',
  operatorType: 'ViewResults',
  operatorProperties: {
  },
  inputPorts: ['input-0'],
  outputPorts: []
});

export const getMockScanResultLink: () => OperatorLink = () => cloneDeep({
  linkID: 'link-1',
  source: {
    operatorID: getMockScanPredicate().operatorID,
    portID: getMockScanPredicate().outputPorts[0]
  },
  target: {
    operatorID: getMockResultPredicate().operatorID,
    portID: getMockResultPredicate().inputPorts[0]
  }
});

export const getMockScanSentimentLink: () => OperatorLink = () => cloneDeep({
  linkID: 'link-2',
  source: {
    operatorID: getMockScanPredicate().operatorID,
    portID: getMockScanPredicate().outputPorts[0]
  },
  target: {
    operatorID: getMockSentimentPredicate().operatorID,
    portID:  getMockSentimentPredicate().inputPorts[0]
  }
});

export const getMockSentimentResultLink: () => OperatorLink = () => cloneDeep({
  linkID: 'link-3',
  source: {
    operatorID: getMockSentimentPredicate().operatorID,
    portID:  getMockSentimentPredicate().outputPorts[0]
  },
  target: {
    operatorID: getMockResultPredicate().operatorID,
    portID: getMockResultPredicate().inputPorts[0]
  }
});


export const getMockFalseResultSentimentLink: () => OperatorLink = () => cloneDeep({
  linkID: 'link-4',
  source: {
    operatorID : getMockResultPredicate().operatorID,
    portID: undefined as any
  },
  target: {
    operatorID: getMockSentimentPredicate().operatorID,
    portID: getMockSentimentPredicate().inputPorts[0]
  }
});

export const getMockFalseSentimentScanLink: () => OperatorLink = () => cloneDeep({
  linkID: 'link-5',
  source: {
    operatorID : getMockSentimentPredicate().operatorID,
    portID : getMockSentimentPredicate().outputPorts[0]
  },
  target: {
    operatorID : getMockScanPredicate().operatorID,
    portID: undefined as any
  }
});
