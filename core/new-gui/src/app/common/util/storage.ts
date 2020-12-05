/**
 * storage.ts maintains a set of useful static storage-related functions.
 * They are used to provide easy access to localStorage and sessionStorage.
 */

/**
 * Saves an object into the localStorage, in its the JSON format.
 * @param {string} key - the identifier of the object
 * @param {any} object - will be JSON.stringify-ed into a string
 */
export function localSetObject<T>(key: string, object: T): void {
  localStorage.setItem(key, JSON.stringify(object));
}

/**
 * Retrieves an object from the localStorage, converted from the JSON format into its original type (provided).
 * @param {string} key - the identifier of the object
 * @returns {T|undefined} - the converted object (in type<t>) from the JSON string, or null if the key is not found.
 */
export function localGetObject<T>(key: string): T|undefined {
  const data: string|null = localStorage.getItem(key);
  if (data == null) {
    return undefined;
  }

  return jsonCast<T>(data);
}

/**
 * removes the object from the localStorage
 * @param {string} key - the identifier of the object
 */
export function localRemoveObject(key: string): void {
  localStorage.removeItem(key);
}

export function jsonCast<T>(data: string): T {
  return <T>JSON.parse(data);
}
