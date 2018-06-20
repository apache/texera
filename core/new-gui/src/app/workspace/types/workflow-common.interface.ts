/**
 * This file contains multiple type declarations related to workflow-graph.
 * These type declarations should be identical to the backend API.
 */

export interface Point extends Readonly<{
  x: number;
  y: number;
}> { }

export interface OperatorPort extends Readonly<{
  operatorID: string;
  portID: string;
}> { }

export interface OperatorPredicate extends Readonly<{
  operatorID: string;
  operatorType: string;
  operatorProperties: Readonly<object>;
  inputPorts: string[];
  outputPorts: string[];
}> { }

export interface OperatorLink extends Readonly<{
  linkID: string;
  source: OperatorPort;
  target: OperatorPort;
}> { }
