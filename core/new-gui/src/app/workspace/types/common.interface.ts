/**
 * This file contains multiple type declarations related to workflow-graph.
 * These type declarations should be identical to the backend API.
 */

export interface Point {
  readonly x: number;
  readonly y: number;
}

export interface OperatorPort {
  operatorID: string;
  portID: string;
}

export interface OperatorPredicate {
  operatorID: string;
  operatorType: string;
  operatorProperties: Object;
  inputPorts: string[];
  outputPorts: string[];
}

export interface OperatorLink {
  linkID: string;
  source: OperatorPort;
  target: OperatorPort;
}

