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

import { AfterContentInit, Component, Input } from "@angular/core";
import { DomSanitizer } from "@angular/platform-browser";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { auditTime, filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-visualization-panel-content",
  templateUrl: "./visualization-frame-content.component.html",
  styleUrls: ["./visualization-frame-content.component.scss"],
})
export class VisualizationFrameContentComponent implements AfterContentInit {
  // operatorId: string = inject(NZ_MODAL_DATA).operatorId;
  @Input() operatorId?: string;
  // progressive visualization update and redraw interval in milliseconds
  public static readonly UPDATE_INTERVAL_MS = 2000;
  htmlData: any = "";

  constructor(
    private workflowResultService: WorkflowResultService,
    private sanitizer: DomSanitizer
  ) {}

  ngAfterContentInit() {
    // attempt to draw chart immediately
    this.drawChart();

    // Re-draw on this operator's result updates, at most once per UPDATE_INTERVAL_MS: auditTime
    // holds the first re-draw until the interval has elapsed. Filter BEFORE auditing: an update
    // event carries only the operators whose result changed, so with the audit first a window that
    // closed on another operator's event handed the filter that event, and this operator's pending
    // re-draw was lost -- the last update of a run showed the previous picture for good.
    this.workflowResultService
      .getResultUpdateStream()
      .pipe(filter(rec => this.operatorId !== undefined && rec[this.operatorId] !== undefined))
      .pipe(auditTime(VisualizationFrameContentComponent.UPDATE_INTERVAL_MS))
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

    const serializer = new XMLSerializer();
    const newHtmlString = serializer.serializeToString(doc);

    this.htmlData = this.sanitizer.bypassSecurityTrustHtml(newHtmlString); // this line bypasses angular security
  }
}
