/**
 * storage.ts maintains a set of useful static storage-related functions.
 * They are used to provide easy access to localStorage and sessionStorage.
 */

/**
 * Saves an object into the localStorage, in its the JSON format.
 * @param key - the identifier of the object
 * @param object - any type, will be JSON.stringify-ed into a string
 */
export function localSetObject<T>(key: string, object: T): void {
  localStorage.setItem(key, JSON.stringify(object));
}


/**
 * Retrieves an object from the localStorage, converted from the JSON format into its original type (provided).
 * @param key - the identifier of the object
 * @returns T - the converted object (in type<t>) from the JSON string, or null if the key is not found.
 */
export function localGetObject<T>(key: string): T | null {
  const data: string | null = localStorage.getItem(key);
  if (!data) {
    return null;
  }

  return jsonCast<T>(data);
}


export function jsonCast<T>(data: string): T {
  return <T>JSON.parse(data);

}
