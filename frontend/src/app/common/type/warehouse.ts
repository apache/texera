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
 * A per-user warehouse registration (#6870), as served by `GET /warehouse/status`.
 * Mirrors the backend's `WarehouseResource.DashboardWarehouse`.
 */
export interface DashboardWarehouse {
  whid: number;
  /** The user-facing name, unique per user. */
  name: string;
  /** The Lakekeeper catalog name (`user-<uid>-<name>`), globally unique. */
  warehouseName: string;
  flavor: string;
  createdAtMillis: number;
}

/**
 * Response of `GET /warehouse/status`. `enabled` reflects the deployment-wide
 * feature flag; with it off the warehouse UI stays hidden entirely.
 */
export interface WarehouseStatus {
  enabled: boolean;
  warehouses: ReadonlyArray<DashboardWarehouse>;
}
