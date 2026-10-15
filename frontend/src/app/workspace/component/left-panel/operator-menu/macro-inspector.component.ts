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
import { AfterViewInit, ChangeDetectorRef, Component, ElementRef, Inject, OnDestroy, ViewChild } from "@angular/core";
import { JsonPipe, NgFor, NgIf } from "@angular/common";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import * as joint from "jointjs";
import {
  expandMacros,
  isMacro,
  MacroDefinition,
  MacroExpansion,
  macroType,
  macroRuntimeID,
  rollupMacroStatus,
} from "../../../../common/util/workflow-macro";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { MacroService } from "../../../service/macro/macro.service";
import { workspaceCanvasUrl } from "../../../../app-routing.constant";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { OperatorRuntimeStatus, OperatorState } from "../../../types/execute-workflow.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { extractPerformanceMetrics } from "../../../service/workflow-status/performance-metrics";
import { formatMetricForView, HeatmapView, rawMetricForView } from "../../../service/heatmap/heatmap-scoring";

@UntilDestroy()
@Component({
  selector: "texera-macro-inspector",
  imports: [NgIf, NgFor, JsonPipe, NzButtonModule],
  templateUrl: "./macro-inspector.component.html",
  styleUrls: ["./macro.component.scss"],
})
export class MacroInspectorComponent implements AfterViewInit, OnDestroy {
  @ViewChild("canvas", { static: true }) canvas!: ElementRef<HTMLDivElement>;
  public definition: MacroDefinition;
  public stack: MacroDefinition[] = [];
  public copying = false;
  public statistics: Record<string, OperatorRuntimeStatus> = {};
  public runtimeHint = "";
  public runtime = (stats: OperatorRuntimeStatus) =>
    formatMetricForView(rawMetricForView(extractPerformanceMetrics(stats), HeatmapView.Runtime), HeatmapView.Runtime);
  private paper?: joint.dia.Paper;
  private path: string[];
  private expanded?: MacroExpansion;
  public get editURL(): string | undefined {
    const matches = this.data.library.filter(d => d.id === this.definition.id);
    const source = matches.length === 1 ? matches[0] : undefined;
    return source?.wid ? workspaceCanvasUrl(source.wid) : undefined;
  }
  constructor(
    @Inject(NZ_MODAL_DATA)
    private data: {
      definition: MacroDefinition;
      dependencies: MacroDefinition[];
      library: MacroDefinition[];
      runtime?: { operatorID: string; getExpansion: () => MacroExpansion | undefined };
    },
    private ui: JointUIService,
    private macros: MacroService,
    private notification: NotificationService,
    private status: WorkflowStatusService,
    private changeDetector: ChangeDetectorRef
  ) {
    this.definition = data.definition;
    this.path = data.runtime ? [data.runtime.operatorID] : [];
  }
  ngAfterViewInit(): void {
    this.render();
    this.changeDetector.detectChanges();
    this.status
      .getStatisticsUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.updateStatistics();
        this.changeDetector.markForCheck();
      });
  }
  ngOnDestroy(): void {
    this.paper?.remove();
  }
  public back(): void {
    this.definition = this.stack.pop()!;
    this.path.pop();
    this.render();
  }
  private render(): void {
    this.paper?.remove();
    const canvas = document.createElement("div");
    this.canvas.nativeElement.replaceChildren(canvas);
    const graph = new joint.dia.Graph();
    this.paper = new joint.dia.Paper({ el: canvas, model: graph, width: "100%", height: 380, interactive: false });
    const body = this.definition.content;
    this.statistics = {};
    this.expanded = this.path.length ? expandMacros({ ...body, macroDependencies: this.data.dependencies }) : undefined;
    body.operators.forEach(op => {
      const cell = this.ui.getJointOperatorElement(op, body.operatorPositions[op.operatorID] ?? { x: 0, y: 0 });
      cell.attr("rect.body/stroke", "#bfbfbf");
      graph.addCell(cell);
    });
    body.links.forEach(l => graph.addCell(JointUIService.getJointLinkCell(l)));
    if (body.operators.length) this.paper.scaleContentToFit({ padding: 30, maxScale: 1 });
    this.updateStatistics();
    this.paper.on("element:pointerdblclick", (view: joint.dia.ElementView) => {
      const op = body.operators.find(o => o.operatorID === view.model.id);
      const nested = op && isMacro(op) ? this.data.dependencies.find(d => macroType(d) === op.operatorType) : undefined;
      if (nested) {
        this.stack.push(this.definition);
        if (this.path.length) this.path.push(op!.operatorID);
        this.definition = nested;
        this.render();
      }
    });
    this.paper.on("blank:mousewheel", (_event: unknown, x: number, y: number, delta: number) => {
      const scale = Math.max(0.15, Math.min(2, this.paper!.scale().sx + delta * 0.1));
      this.paper!.scale(scale, scale, x, y);
    });
  }
  private updateStatistics(): void {
    const expanded = this.path.length ? this.data.runtime?.getExpansion() : undefined;
    const states = this.status.getCurrentState(),
      statistics = this.status.getCurrentStatistics();
    const mapped = Object.fromEntries(
      Object.entries(expanded ? this.expanded?.paths ?? {} : {}).flatMap(([local, path]) => {
        const id = macroRuntimeID([...this.path, ...path.slice(0, -1)], path[path.length - 1]);
        return expanded?.paths[id] && statistics[id]
          ? [[local, { ...statistics[id], operatorState: states[id] ?? OperatorState.Uninitialized }]]
          : [];
      })
    );
    const next = this.expanded && expanded ? rollupMacroStatus(this.definition.content, mapped, this.expanded) : {};
    const lost = Object.keys(this.statistics).some(id => !next[id]);
    this.statistics = next;
    this.runtimeHint = Object.keys(mapped).length
      ? "Statistics for this workflow instance"
      : "No execution statistics for this view";
    if (lost) {
      this.render();
      return;
    }
    for (const op of this.definition.content.operators) {
      const update = next[op.operatorID];
      if (!update) continue;
      this.ui.changeOperatorState(this.paper!, op.operatorID, update.operatorState);
      this.ui.changeOperatorStatistics(this.paper!, op.operatorID, update);
      this.ui.unfoldOperatorDetails(this.paper!, op.operatorID, false);
    }
  }
  public async copy(): Promise<void> {
    if (this.copying) return;
    this.copying = true;
    try {
      this.definition = await this.macros.copyDefinition(this.definition, this.data.dependencies);
      this.data.library = this.macros.library$.value;
      this.stack = [];
      this.path = [];
      this.render();
      this.notification.success("Macro copy saved. Open Edit definition to edit it.");
    } catch (e) {
      this.notification.error(e instanceof Error ? e.message : String(e));
    } finally {
      this.copying = false;
    }
  }
}
