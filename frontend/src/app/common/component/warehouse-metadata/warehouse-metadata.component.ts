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

import { Component, inject } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { DashboardWarehouse } from "../../type/warehouse";

/**
 * Read-only warehouse details dialog (#6933), mirroring
 * ComputingUnitMetadataComponent. Opened via NzModalService with the
 * warehouse as NZ_MODAL_DATA.
 *
 * The Lakekeeper catalog name is deliberately left out: it is an internal
 * catalog identifier that happens to double as the storage key prefix. What a
 * user could act on, once warehouses live in their own bucket (#6870, Phase 1),
 * is the storage location itself — a separate field.
 */
@Component({
  template: `
    <table class="ant-table">
      <tbody>
        <tr>
          <th style="width: 150px;">Name</th>
          <td>{{ warehouse.name }}</td>
        </tr>
        <tr>
          <th>Owner</th>
          <td>{{ warehouse.ownerName || "None" }}</td>
        </tr>
        <tr>
          <th>Flavor</th>
          <td>{{ warehouse.flavor }}</td>
        </tr>
        <tr>
          <th>Created</th>
          <td>{{ createdAt }}</td>
        </tr>
      </tbody>
    </table>
  `,
})
export class WarehouseMetadataComponent {
  readonly warehouse: DashboardWarehouse = inject(NZ_MODAL_DATA);
  readonly createdAt = new Date(this.warehouse.createdAtMillis).toLocaleString();
}
