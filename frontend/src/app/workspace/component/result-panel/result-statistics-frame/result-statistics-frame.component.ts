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

import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";

interface ColumnEntry {
  columnName: string;
  json: string;
}

@UntilDestroy()
@Component({
  selector: "texera-result-statistics-frame",
  templateUrl: "./result-statistics-frame.component.html",
  styleUrls: ["./result-statistics-frame.component.scss"],
})
export class ResultStatisticsFrameComponent implements OnInit, OnChanges {
  @Input() operatorId!: string;

  columns: ColumnEntry[] = [];
  hasStats = false;

  constructor(private workflowStatusService: WorkflowStatusService) {}

  ngOnInit(): void {
    this.loadStats();
    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.loadStats());
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["operatorId"]) {
      this.loadStats();
    }
  }

  private loadStats(): void {
    const status = this.workflowStatusService.getCurrentStatus();
    const resultStats = status[this.operatorId]?.operatorResultStats;

    if (!resultStats || Object.keys(resultStats).length === 0) {
      this.columns = [];
      this.hasStats = false;
      return;
    }

    this.hasStats = true;
    this.columns = Object.entries(resultStats).map(([colName, statsJson]) => {
      try {
        return { columnName: colName, json: JSON.stringify(JSON.parse(statsJson), null, 2) };
      } catch {
        return { columnName: colName, json: statsJson };
      }
    });
  }
}
