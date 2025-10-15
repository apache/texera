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
