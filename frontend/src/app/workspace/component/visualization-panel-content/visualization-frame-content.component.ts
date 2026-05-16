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

import { AfterContentInit, Component, ElementRef, HostListener, Input, ViewChild } from "@angular/core";
import { DomSanitizer } from "@angular/platform-browser";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { auditTime, filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { VisualTraceService } from "../../service/visual-trace/visual-trace.service";
import {
  buildStructuralVisualTrace,
  buildVisualTraceBridgeScript,
  extractVisualTraceSelectionFromElement,
  findVisualTraceElement,
  parseVisualTraceMessage,
  parseVisualTracePayloadAttribute,
  parseVisualTraceSelectionMessage,
} from "../../service/visual-trace/visual-trace.utils";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";

@UntilDestroy()
@Component({
  selector: "texera-visualization-panel-content",
  templateUrl: "./visualization-frame-content.component.html",
  styleUrls: ["./visualization-frame-content.component.scss"],
})
export class VisualizationFrameContentComponent implements AfterContentInit {
  // operatorId: string = inject(NZ_MODAL_DATA).operatorId;
  @Input() operatorId?: string;
  @ViewChild("visualizationFrame") visualizationFrame?: ElementRef<HTMLIFrameElement>;
  // progressive visualization update and redraw interval in milliseconds
  public static readonly UPDATE_INTERVAL_MS = 2000;
  htmlData: any = "";
  private removeFrameClickListener?: () => void;

  constructor(
    private workflowResultService: WorkflowResultService,
    private sanitizer: DomSanitizer,
    private visualTraceService: VisualTraceService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngAfterContentInit() {
    // attempt to draw chart immediately
    this.drawChart();

    // setup an event lister that re-draws the chart content every (n) milliseconds
    // auditTime makes sure the first re-draw happens after (n) milliseconds has elapsed
    this.workflowResultService
      .getResultUpdateStream()
      .pipe(auditTime(VisualizationFrameContentComponent.UPDATE_INTERVAL_MS))
      .pipe(filter(rec => this.operatorId !== undefined && rec[this.operatorId] !== undefined))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.drawChart();
      });
  }
  drawChart() {
    if (!this.operatorId) {
      return;
    }
    const operatorResultService = this.workflowResultService.getResultService(this.operatorId);
    if (!operatorResultService) {
      return;
    }
    const data = operatorResultService.getCurrentResultSnapshot();
    if (!data) {
      return;
    }

    const parser = new DOMParser();
    const lastData = data[data.length - 1];
    const doc = parser.parseFromString(Object(lastData)["html-content"], "text/html");

    doc.documentElement.style.height = "100%";
    doc.body.style.height = "95%";

    const firstDiv = doc.body.querySelector("div");
    if (firstDiv) firstDiv.style.height = "100%";

    const bridgeScript = doc.createElement("script");
    bridgeScript.textContent = buildVisualTraceBridgeScript();
    doc.body.appendChild(bridgeScript);

    const serializer = new XMLSerializer();
    const newHtmlString = serializer.serializeToString(doc);

    this.htmlData = this.sanitizer.bypassSecurityTrustHtml(newHtmlString); // this line bypasses angular security
  }

  @HostListener("window:message", ["$event"])
  handleWindowMessage(event: MessageEvent): void {
    if (this.visualizationFrame?.nativeElement.contentWindow && event.source !== this.visualizationFrame.nativeElement.contentWindow) {
      return;
    }
    const trace = parseVisualTraceMessage(event.data);
    if (trace) {
      this.visualTraceService.openTrace(trace);
      return;
    }

    const selection = parseVisualTraceSelectionMessage(event.data);
    if (!selection || !this.operatorId) {
      return;
    }
    this.openStructuralTrace(selection);
  }

  onVisualizationFrameLoad(): void {
    this.removeFrameClickListener?.();

    const frameDocument = this.visualizationFrame?.nativeElement.contentDocument;
    if (!frameDocument) {
      return;
    }

    const handleClick = (event: MouseEvent): void => {
      const traceElement = findVisualTraceElement(event.target);
      if (!traceElement) {
        return;
      }

      const trace = parseVisualTracePayloadAttribute(traceElement.getAttribute("data-texera-trace"));
      if (trace) {
        this.visualTraceService.openTrace(trace);
        return;
      }

      const selection = extractVisualTraceSelectionFromElement(traceElement);
      if (selection && this.operatorId) {
        this.openStructuralTrace(selection);
      }
    };

    frameDocument.addEventListener("click", handleClick);
    this.removeFrameClickListener = () => frameDocument.removeEventListener("click", handleClick);
  }

  private openStructuralTrace(selection: { title?: string; image?: string; imageAlt?: string }): void {
    if (!this.operatorId) {
      return;
    }

    const graph = this.workflowActionService.getTexeraGraph();
    const structuralTrace = buildStructuralVisualTrace(selection, this.operatorId, {
      hasOperator: operatorId => graph.hasOperator(operatorId),
      getOperator: operatorId => graph.getOperator(operatorId),
      getInputOperatorIds: operatorId => graph.getInputLinksByOperatorId(operatorId).map(link => link.source.operatorID),
    });
    if (structuralTrace) {
      this.visualTraceService.openTrace(structuralTrace);
    }
  }
}
