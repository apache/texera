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

import { NzSelectItemInterface } from "ng-zorro-antd/select";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";

/**
 * Whether a dataset matches a type-to-search query in the dataset picker.
 *
 * Matches against both the dataset name and its id as displayed in the dropdown
 * (`#<id>`), case-insensitively, so typing `iris`, `17`, or `#17` finds `#17 iris`
 * (per the design decision on the proposal). An empty/whitespace query matches
 * everything.
 */
export function datasetMatchesQuery(
  name: string | null | undefined,
  did: number | null | undefined,
  query: string
): boolean {
  const q = query.trim().toLowerCase();
  if (q === "") {
    return true;
  }
  const nameMatches = (name ?? "").toLowerCase().includes(q);
  const idMatches = did !== null && did !== undefined && `#${did}`.includes(q);
  return nameMatches || idMatches;
}

/**
 * ng-zorro `nzFilterOption` adapter for the dataset dropdown: pulls the dataset off the
 * option's value and matches the typed query against its name and #id via
 * {@link datasetMatchesQuery}. Safe when the option has no value.
 */
export function filterDatasetOption(input: string, option: NzSelectItemInterface): boolean {
  const dataset = option.nzValue as DashboardDataset | undefined;
  return datasetMatchesQuery(dataset?.dataset?.name, dataset?.dataset?.did, input ?? "");
}
