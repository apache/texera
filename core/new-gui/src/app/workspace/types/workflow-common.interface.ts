import { JSONSchema7 } from "json-schema";
import * as Y from "yjs";
import {types} from "sass";
import List = types.List;

/**
 * This file contains multiple type declarations related to workflow-graph.
 * These type declarations should be identical to the backend API.
 */

export interface Point
  extends Readonly<{
    x: number;
    y: number;
  }> {}

export interface OperatorPort
  extends Readonly<{
    operatorID: string;
    portID: string;
  }> {}

export interface OperatorPredicate
  extends Readonly<{
    operatorID: string;
    operatorType: string;
    operatorProperties: Readonly<{ [key: string]: any }>;
    inputPorts: { portID: string; displayName?: string }[];
    outputPorts: { portID: string; displayName?: string }[];
    showAdvanced: boolean;
    isDisabled?: boolean;
    isCached?: boolean;
    customDisplayName?: string;
  }> {}

export interface Comment
  extends Readonly<{
    content: string;
    creationTime: string;
    creatorName: string;
    creatorID: number;
  }> {}

export interface CommentBox {
  commentBoxID: string;
  comments: Comment[];
  commentBoxPosition: Point;
}

export interface OperatorLink
  extends Readonly<{
    linkID: string;
    source: OperatorPort;
    target: OperatorPort;
  }> {}

export interface BreakpointSchema
  extends Readonly<{
    jsonSchema: Readonly<JSONSchema7>;
  }> {}

type ConditionBreakpoint = Readonly<{
  column: number;
  condition: "=" | ">" | ">=" | "<" | "<=" | "!=" | "contains" | "does not contain";
  value: string;
}>;

type CountBreakpoint = Readonly<{
  count: number;
}>;

export type Breakpoint = ConditionBreakpoint | CountBreakpoint;

export type BreakpointRequest =
  | Readonly<{ type: "ConditionBreakpoint" } & ConditionBreakpoint>
  | Readonly<{ type: "CountBreakpoint" } & CountBreakpoint>;

export type BreakpointFaultedTuple = Readonly<{
  tuple: ReadonlyArray<string>;
  id: number;
  isInput: boolean;
}>;

export type BreakpointFault = Readonly<{
  actorPath: string;
  faultedTuple: BreakpointFaultedTuple;
  messages: ReadonlyArray<string>;
}>;

export type BreakpointTriggerInfo = Readonly<{
  report: ReadonlyArray<BreakpointFault>;
  operatorID: string;
}>;

export type PythonPrintTriggerInfo = Readonly<{
  message: Readonly<string>;
  operatorID: string;
}>;

export type YTextify<T> = T extends string ? Y.Text : T;
export type YArrayify<T> = T extends Array<any> ? Y.Array<any> : T;

export type YType<T> = Omit<Y.Map<any>, "get" | "set" | "has" | "toJSON"> & {
  get<TKey extends keyof T>(key: TKey): YArrayify<YTextify<T[TKey]>>;
  set<TKey extends keyof T>(key: TKey, value: YArrayify<YTextify<T[TKey]>>): void;
  has<TKey extends keyof T>(key: TKey): boolean;
  toJSON(): T
}

/**
 * TODO: Recursive?
 * @param obj
 */
export function createYTypeFromObject<T extends object>(obj: T): YType<T> {
  // return new
  const yMap = new Y.Map();
  Object.keys(obj).forEach((k: string) => {
    const value = obj[k as keyof T] as any;
    const type = value.constructor.name;
    if (type === "String") {
      yMap.set(k, new Y.Text(value));
    } else if (type === "Array") {
      const yArray = new Y.Array();
      yArray.push(value);
      yMap.set(k, yArray);
    } else {
      yMap.set(k, value);
    }
  });

  return yMap as any as YType<T>;
}
