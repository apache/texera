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
export function assertType<T>(val: T|any): asserts val is NonNullable<T> {
  if (val === undefined || val === null) {
    throw new TypeError(`Expected 'val' to be defined, but received ${val}`);
  }
}
