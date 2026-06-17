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

import { getServiceEndpoints } from "../config/endpoints";
import type { OperatorMetadata } from "../types/metadata";

export async function fetchOperatorMetadata(): Promise<OperatorMetadata> {
  const url = `${getServiceEndpoints().apiEndpoint}/api/resources/operator-metadata`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Failed to fetch operator metadata: ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as OperatorMetadata;
}
