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

import { Component, ElementRef, OnInit, ViewChild } from "@angular/core";
import { CommonModule } from "@angular/common";
import { Bankroll, BankrollPoint, BetPilotService, TEXERA_WORKFLOW_IDS, texeraWorkflowUrl } from "../bet-pilot.service";
import { BpWfPreviewComponent } from "./bp-wf-preview.component";

@Component({
  selector: "texera-bp-bankroll",
  standalone: true,
  imports: [CommonModule, BpWfPreviewComponent],
  templateUrl: "./bp-bankroll.component.html",
  styleUrls: ["./bp-screen.shared.scss"],
})
export class BpBankrollComponent implements OnInit {
  b!: Bankroll;
  linePath = "";
  areaPath = "";
  hoverX = 0;
  hoverY = 0;
  hoverVisible = false;
  hoverDate = "";
  hoverValue = "";
  tooltipLeftPct = 0;
  workflowUrl: string | null = null;
  wfId = TEXERA_WORKFLOW_IDS.wf5_clv_monitor;

  @ViewChild("svg", { static: false }) svgRef?: ElementRef<SVGElement>;

  constructor(private svc: BetPilotService) {}

  ngOnInit(): void {
    this.b = this.svc.getBankroll();
    const segs = this.b.series.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x},${p.y}`).join(" ");
    this.linePath = segs;
    this.areaPath = `${segs} L 800,260 L 0,260 Z`;
    this.workflowUrl = texeraWorkflowUrl("wf5_clv_monitor");
  }

  onMove(event: MouseEvent): void {
    const target = event.currentTarget as SVGElement;
    const rect = target.getBoundingClientRect();
    const xPx = event.clientX - rect.left;
    const svgX = (xPx / rect.width) * 800;
    const p = this.nearest(svgX);
    this.hoverX = p.x;
    this.hoverY = p.y;
    this.hoverVisible = true;
    this.hoverDate = p.date;
    this.hoverValue = "$" + p.value.toFixed(2);
    this.tooltipLeftPct = (p.x / 800) * 100;
  }

  onLeave(): void {
    this.hoverVisible = false;
  }

  private nearest(svgX: number): BankrollPoint {
    let best = this.b.series[0];
    let bestD = Infinity;
    for (const p of this.b.series) {
      const d = Math.abs(p.x - svgX);
      if (d < bestD) {
        bestD = d;
        best = p;
      }
    }
    return best;
  }
}
