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

import { Component } from "@angular/core";
import { AsyncPipe, NgIf } from "@angular/common";
import { combineLatest, Observable } from "rxjs";
import { map } from "rxjs/operators";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { OperatorPerformanceMetrics } from "../../service/workflow-status/performance-metrics";
import {
  formatMetricForView,
  HeatmapView,
  heatmapViewTitle,
  rawMetricForView,
} from "../../service/heatmap/heatmap-scoring";

interface HeatmapLegendState {
  readonly view: HeatmapView;
  readonly title: string;
  readonly minLabel: string;
  readonly maxLabel: string;
}

/**
 * Presentational legend for the performance heat-map overlay. Shows the active view's name, the
 * cold -> hot color scale, and the actual min/max metric values behind that scale. Hidden when the
 * overlay is off (view is null).
 */
@Component({
  selector: "texera-heatmap-legend",
  templateUrl: "./heatmap-legend.component.html",
  styleUrls: ["./heatmap-legend.component.scss"],
  imports: [NgIf, AsyncPipe],
})
export class HeatmapLegendComponent {
  public readonly legend$: Observable<HeatmapLegendState | null>;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowStatusService: WorkflowStatusService
  ) {
    this.legend$ = combineLatest([
      this.workflowActionService.getJointGraphWrapper().getHeatmapViewStream(),
      this.workflowStatusService.getPerformanceMetricsStream(),
    ]).pipe(map(([view, metrics]) => (view === null ? null : this.buildState(view, metrics))));
  }

  private buildState(view: HeatmapView, metrics: Record<string, OperatorPerformanceMetrics>): HeatmapLegendState {
    const raws = Object.values(metrics)
      .map(m => rawMetricForView(m, view))
      .filter(v => Number.isFinite(v));
    const hasData = raws.length > 0;
    const min = hasData ? Math.min(...raws) : 0;
    const max = hasData ? Math.max(...raws) : 0;
    return {
      view,
      title: heatmapViewTitle(view),
      minLabel: hasData ? formatMetricForView(min, view) : "—",
      maxLabel: hasData ? formatMetricForView(max, view) : "—",
    };
  }
}
