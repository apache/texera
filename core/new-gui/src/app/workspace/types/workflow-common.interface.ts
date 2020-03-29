import { JSONSchema4 } from 'json-schema';
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
  operatorProperties: Readonly<{[key: string]: any}>;
  inputPorts: string[];
  outputPorts: string[];
  showAdvanced: boolean;
}> { }

export interface OperatorLink extends Readonly<{
  linkID: string;
  source: OperatorPort;
  target: OperatorPort;
  breakpointProperties: Readonly<{[key: string]: any}>;
}> { }

export interface BreakpointSchema extends Readonly<{
  jsonSchema: Readonly<JSONSchema4>;
}> {}
