/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * assert.ts maintains a set of useful assertion functions.
 * They are used to provide type hints to help Typescript analyze code.
 */

/**
 *
 * ex:\
 * `let foo = ????;`\
 * `assertType<number>(foo);`\
 * `bar += foo;`
 */
export function assertType<T>(val: T | any): asserts val is NonNullable<T> {
  if (val === undefined || val === null) {
    throw new TypeError(`Expected 'val' to be defined, but received ${val}`);
  }
}

export function assert(condition: boolean, message?: string): void {
  if (!condition) {
    throw new Error(message);
  }
}

interface Primitives {
  number: number;
  boolean: boolean;
  string: string;
}

type AnyType = { new (...args: any[]): any } | keyof Primitives;

type GuardedType<T extends AnyType> = T extends { new (...args: any[]): infer U }
  ? U
  : T extends keyof Primitives
    ? Primitives[T]
    : never;

export function isType<T extends AnyType>(val: any, type: T): val is GuardedType<T> {
  const interfaceType: AnyType = type;
  if (typeof interfaceType === "string") {
    return typeof val === interfaceType;
  }
  return val instanceof interfaceType;
}

export function asType<T extends AnyType>(val: any, type: T): GuardedType<T> {
  if (!isType(val, type)) {
    throw new TypeError(`Type Guard expected value ${val} to be of type ${type}, but received ${typeof val}`);
  }
  return val;
}

export function isNull<T>(val: T | null | undefined): val is null | undefined {
  return val === undefined || val === null;
}

export function isNotNull<T>(val: T): val is NonNullable<T> {
  return val !== undefined && val !== null;
}

export function nonNull<T>(val: T): NonNullable<T> {
  if (!isNotNull(val)) {
    throw new TypeError(`Type Guard expected value ${val} to not be null or undefined`);
  }
  return val;
}
