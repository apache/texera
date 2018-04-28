import { cloneDeep } from 'lodash-es';
import { Point } from './../../../types/common.interface';
import { MOCK_OPERATOR_SCHEMA_LIST } from './../../operator-metadata/mock-operator-metadata.data';
import { OperatorPredicate, OperatorLink } from './../../../types/workflow-graph';

/**
 * Provides common constants related to operators and link.
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
    portID:  getMockSentimentPredicate().inputPorts[0]
  },
  target: {
    operatorID: getMockResultPredicate().operatorID,
    portID: getMockResultPredicate().inputPorts[0]
  }
});
