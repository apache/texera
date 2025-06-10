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

import { Component, inject, OnInit } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { WorkflowRuntimeStatistics } from "../../../../../type/workflow-runtime-statistics";
import * as Plotly from "plotly.js-basic-dist-min";
import { NzTabChangeEvent } from "ng-zorro-antd/tabs";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

const NANOSECONDS_TO_SECONDS = 1_000_000_000;

interface ChartData {
  x: number[];
  y: number[];
  mode: string;
  name: string;
}

@UntilDestroy()
@Component({
  selector: "texera-workflow-runtime-statistics",
  templateUrl: "./workflow-runtime-statistics.component.html",
  styleUrls: ["./workflow-runtime-statistics.component.scss"],
})
export class WorkflowRuntimeStatisticsComponent implements OnInit {
  readonly workflowRuntimeStatistics: WorkflowRuntimeStatistics[] = inject(NZ_MODAL_DATA).workflowRuntimeStatistics;

  private groupedStatistics?: Record<string, WorkflowRuntimeStatistics[]>;
  public metrics: string[] = [
    "Input Tuple Count",
    "Input Tuple Size (bytes)",
    "Output Tuple Count",
    "Output Tuple Size (bytes)",
    "Total Data Processing Time (s)",
    "Total Control Processing Time (s)",
    "Total Idle Time (s)",
    "Number of Workers",
  ];

  ngOnInit(): void {
    if (!this.workflowRuntimeStatistics) {
      console.warn("No workflow runtime statistics available.");
      return;
    }

    this.groupedStatistics = this.groupStatisticsByOperatorId();
    this.createChart(0);
  }

  /**
   * Create a new line chart corresponding to the change of a tab
   */
  onTabChanged(event: NzTabChangeEvent): void {
    this.createChart(event.index!);
  }

  /**
   * Groups statistics by operator ID, converting times from nanoseconds to seconds,
   * and adjusts timestamps relative to the initial timestamp.
   */
  private groupStatisticsByOperatorId(): Record<string, WorkflowRuntimeStatistics[]> {
    if (!this.workflowRuntimeStatistics || this.workflowRuntimeStatistics.length === 0) {
      console.warn("No workflow runtime statistics available.");
      return {};
    }

    const initialTimestamp = this.workflowRuntimeStatistics[0].timestamp;

    return this.workflowRuntimeStatistics.reduce(
      (accumulator, stat) => {
        if (!stat.operatorId) {
          console.warn("Missing operatorId in statistic:", stat);
          return accumulator;
        }

        const statsArray = accumulator[stat.operatorId] ?? [];

        const processedStat = {
          ...stat,
          dataProcessingTime: stat.dataProcessingTime / NANOSECONDS_TO_SECONDS,
          controlProcessingTime: stat.controlProcessingTime / NANOSECONDS_TO_SECONDS,
          idleTime: stat.idleTime / NANOSECONDS_TO_SECONDS,
          timestamp: stat.timestamp - initialTimestamp,
        };

        statsArray.push(processedStat);
        accumulator[stat.operatorId] = statsArray;
        return accumulator;
      },
      {} as Record<string, WorkflowRuntimeStatistics[]>
    );
  }

  /**
   * Preprocess the dataset which will be used as an input for a line chart
   * 1. Shorten the operator ID
   * 2. Remove sink operator
   * 3. Contain only a certain metric given a metric idx
   * @param metricIndex
   */
  private createDataset(metricIndex: number): ChartData[] {
    if (!this.groupedStatistics) {
      return [];
    }

    const metricKeys = [
      "inputTupleCount",
      "inputTupleSize",
      "outputTupleCount",
      "outputTupleSize",
      "dataProcessingTime",
      "controlProcessingTime",
      "idleTime",
      "numWorkers",
    ];

    const yValuesKey = metricKeys[metricIndex] || "numWorkers";

    return Object.entries(this.groupedStatistics)
      .map(([operatorId, stats]) => {
        const [operatorName] = operatorId.split("-");
        const uuidLast6Digits = operatorId.slice(-6);

        if (operatorName.startsWith("sink")) {
          return null;
        }

        return {
          x: stats.map(stat => stat.timestamp / 1000),
          y: stats.map(stat => stat[yValuesKey]),
          mode: "lines",
          name: `${operatorName}-${uuidLast6Digits}`,
        };
      })
      .filter((data): data is ChartData => data !== null);
  }

  /**
   * Create a line chart using plotly
   * @param metricIndex
   */
  private createChart(metricIndex: number): void {
    const dataset = this.createDataset(metricIndex);

    if (!dataset || dataset.length === 0) {
      console.warn("No data available for the chart.");
      return;
    }

    const layout = {
      title: this.metrics[metricIndex],
      xaxis: { title: "Time (s)" },
      yaxis: { title: this.metrics[metricIndex] },
    };

    Plotly.newPlot("chart", dataset, layout);
  }
}
