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

import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DataCheckService, DataCheck } from "../../../service/data-check/data-check.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";

@UntilDestroy()
@Component({
  selector: "texera-data-check-list",
  templateUrl: "./data-check-list.component.html",
  styleUrls: ["./data-check-list.component.scss"],
})
export class DataCheckListComponent implements OnInit {
  dataChecks: DataCheck[] = [];

  constructor(
    private dataCheckService: DataCheckService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngOnInit(): void {
    // Subscribe to data check updates
    this.dataCheckService
      .getDataChecks()
      .pipe(untilDestroyed(this))
      .subscribe(dataChecks => {
        this.dataChecks = dataChecks;
      });
  }

  // Cleanup handled by @UntilDestroy decorator

  /**
   * Delete a data check
   */
  deleteDataCheck(id: string): void {
    this.dataCheckService.deleteDataCheck(id);
  }

  /**
   * Clear all data checks
   */
  clearAll(): void {
    this.dataCheckService.clearAll();
  }

  /**
   * Handle click on data check card to highlight the upstream path
   * Delegates to service which handles toggle behavior
   */
  onDataCheckClick(dataCheck: DataCheck): void {
    if (!dataCheck.operatorId) {
      return;
    }

    // Find all upstream operators and links leading to this operator
    const pathResult = this.workflowActionService.findUpstreamPath(dataCheck.operatorId);

    // Trigger highlight via service (which handles toggle logic)
    this.dataCheckService.toggleHighlight(dataCheck.id, pathResult.operators, pathResult.links);
  }
}
