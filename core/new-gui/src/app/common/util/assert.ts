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

export function isNull<T>(val: T): val is NonNullable<T> {
  return val !== undefined && val !== null;
}

export function nonNull<T>(val: T): NonNullable<T> {
  if (!isNull(val)) {
    throw new TypeError(`Type Guard expected value ${val} to not be null or undefined`);
  }
  return val;
}
