/**
 * checks if the given parameter is undefined or not.
 * @param val
 * @returns {boolean}
 */
export function isDefined<T>(val: T | undefined | null): val is T {
  return val !== undefined && val != null;
}
