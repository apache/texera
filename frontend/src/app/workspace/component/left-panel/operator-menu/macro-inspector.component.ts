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
import { AfterViewInit, Component, ElementRef, Inject, OnDestroy, ViewChild } from "@angular/core";
import { JsonPipe, NgFor, NgIf } from "@angular/common";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import * as joint from "jointjs";
import { isMacro, MacroDefinition, macroType } from "../../../../common/util/workflow-macro";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { MacroService } from "../../../service/macro/macro.service";
import { workspaceCanvasUrl } from "../../../../app-routing.constant";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@Component({
  selector: "texera-macro-inspector",
  imports: [NgIf, NgFor, JsonPipe],
  template: `<button
      type="button"
      *ngIf="stack.length"
      (click)="back()">
      Back
    </button>
    <strong>{{ definition.name }} · v{{ definition.version }}</strong>
    <a
      *ngIf="editURL"
      [href]="editURL"
      target="_blank"
      rel="noopener"
      >Edit definition</a
    >
    <button
      type="button"
      (click)="copy()"
      [disabled]="copying">
      Save editable copy
    </button>
    <p>
      Double-click a nested macro to inspect it. Editing a definition creates versions; existing instances stay pinned.
    </p>
    <div
      #canvas
      style="height: 380px; border: 1px solid #ddd; overflow: hidden"></div>
    <div *ngFor="let op of definition.content.operators">
      <strong>{{ op.customDisplayName || op.operatorType }}</strong>
      <pre>{{ op.operatorProperties | json }}</pre>
    </div>`,
  styles: [
    `
      strong,
      a,
      button {
        margin-right: 12px;
      }
      pre {
        max-height: 120px;
        overflow: auto;
      }
    `,
  ],
})
export class MacroInspectorComponent implements AfterViewInit, OnDestroy {
  @ViewChild("canvas", { static: true }) canvas!: ElementRef<HTMLDivElement>;
  public definition: MacroDefinition;
  public stack: MacroDefinition[] = [];
  public copying = false;
  private paper?: joint.dia.Paper;
  public get editURL(): string | undefined {
    const matches = this.data.library.filter(d => d.id === this.definition.id);
    const source = matches.length === 1 ? matches[0] : undefined;
    return source?.wid ? workspaceCanvasUrl(source.wid) : undefined;
  }
  constructor(
    @Inject(NZ_MODAL_DATA)
    private data: { definition: MacroDefinition; dependencies: MacroDefinition[]; library: MacroDefinition[] },
    private ui: JointUIService,
    private macros: MacroService,
    private notification: NotificationService
  ) {
    this.definition = data.definition;
  }
  ngAfterViewInit(): void {
    this.render();
  }
  ngOnDestroy(): void {
    this.paper?.remove();
  }
  public back(): void {
    this.definition = this.stack.pop()!;
    this.render();
  }
  private render(): void {
    this.paper?.remove();
    const canvas = document.createElement("div");
    this.canvas.nativeElement.replaceChildren(canvas);
    const graph = new joint.dia.Graph();
    this.paper = new joint.dia.Paper({ el: canvas, model: graph, width: "100%", height: 380, interactive: false });
    const body = this.definition.content;
    body.operators.forEach(op => {
      const cell = this.ui.getJointOperatorElement(op, body.operatorPositions[op.operatorID] ?? { x: 0, y: 0 });
      cell.attr("rect.body/stroke", "#bfbfbf");
      graph.addCell(cell);
    });
    body.links.forEach(l => graph.addCell(JointUIService.getJointLinkCell(l)));
    if (body.operators.length) this.paper.scaleContentToFit({ padding: 30, maxScale: 1 });
    this.paper.on("element:pointerdblclick", (view: joint.dia.ElementView) => {
      const op = body.operators.find(o => o.operatorID === view.model.id);
      const nested = op && isMacro(op) ? this.data.dependencies.find(d => macroType(d) === op.operatorType) : undefined;
      if (nested) {
        this.stack.push(this.definition);
        this.definition = nested;
        this.render();
      }
    });
    this.paper.on("blank:mousewheel", (_event: unknown, x: number, y: number, delta: number) => {
      const scale = Math.max(0.15, Math.min(2, this.paper!.scale().sx + delta * 0.1));
      this.paper!.scale(scale, scale, x, y);
    });
  }
  public async copy(): Promise<void> {
    if (this.copying) return;
    this.copying = true;
    try {
      this.definition = await this.macros.copyDefinition(this.definition, this.data.dependencies);
      this.data.library = this.macros.library$.value;
      this.stack = [];
      this.render();
      this.notification.success("Macro copy saved. Open Edit definition to edit it.");
    } catch (e) {
      this.notification.error(e instanceof Error ? e.message : String(e));
    } finally {
      this.copying = false;
    }
  }
}
