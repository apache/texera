/**
 * Converts ES6 Map object to TS Record object.
 * This method is used to stringify Map objects.
 * @param map
 */
export function mapToRecord(map: Map<string, any>): Record<string, any> {
  const record: Record<string, any> = {};
  map.forEach((value, key) => (record[key] = value));
  return record;
}

/**
 * Converts TS Record object to ES6 Map object.
 * This method is used to construct Map objects from JSON.
 * @param record
 */
export function recordToMap(record: Record<string, any>): Map<string, any> {
  const map = new Map<string, any>();
  for (const key of Object.keys(record)) {
    map.set(key, record[key]);
  }
  return map;
}
