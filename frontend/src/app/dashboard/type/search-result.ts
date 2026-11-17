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

import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";
import { DashboardModel } from "./dashboard-model.interface";
import { DashboardEntry } from "./dashboard-entry";

export interface SearchResultItem {
  resourceType: "workflow" | "dataset" | "model" | "computing-unit";
  workflow?: DashboardWorkflow;
  dataset?: DashboardDataset;
  model?: DashboardModel;
}

/**
 * A search row's payload, or undefined when it carries none this client reads (a kind with no field here, or a
 * null payload). SearchService.search keeps a row only when this returns one, and its hub enrichment and
 * SearchBarComponent.convertToName read the same payload, so the rows kept and the rows the search bar can name
 * cannot disagree.
 */
export function searchResultPayload(
  item: SearchResultItem
): DashboardWorkflow | DashboardDataset | DashboardModel | undefined {
  return item.workflow ?? item.dataset ?? item.model ?? undefined;
}

export interface SearchResult {
  results: SearchResultItem[];
  more: boolean;
  hasMismatch?: boolean; // Indicates whether there are mismatched datasets (added for dashboard notification)
}

export interface SearchResultBatch {
  entries: DashboardEntry[];
  more: boolean;
  hasMismatch?: boolean;
}
