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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Addresses the upload endpoints of one versioned resource kind. The two families differ only in
 * their base path and in the query-param name they use for the resource, so the engine is
 * parameterized rather than duplicated.
 */
export interface FileResourceEndpoint {
  /** Path segment under the API root, e.g. "dataset". */
  readonly baseUrl: string;
  /** Query-param name carrying the resource name, e.g. "datasetName". */
  readonly nameParamKey: string;
}

export const DATASET_FILE_RESOURCE_ENDPOINT: FileResourceEndpoint = {
  baseUrl: "dataset",
  nameParamKey: "datasetName",
};
