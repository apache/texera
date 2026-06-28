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

// Generic type-level utilities shared across the type modules.

/**
 * Builds a discriminated union from a `{ type -> payload }` map: each entry
 * becomes `{ type: <key> } & <payload>`, then they are unioned. The `type` tag
 * is derived from the map key rather than hand-written into each payload
 */
export type CustomUnionType<T> = { [K in keyof T]: { readonly type: K } & T[K] }[keyof T];
