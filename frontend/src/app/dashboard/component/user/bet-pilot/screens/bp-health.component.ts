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
import { CommonModule } from "@angular/common";
import { BetPilotService, ModelHealth, TEXERA_WORKFLOW_IDS, texeraWorkflowUrl } from "../bet-pilot.service";
import { BpWfPreviewComponent } from "./bp-wf-preview.component";

@Component({
  selector: "texera-bp-health",
  standalone: true,
  imports: [CommonModule, BpWfPreviewComponent],
  templateUrl: "./bp-health.component.html",
  styleUrls: ["./bp-screen.shared.scss"],
})
export class BpHealthComponent implements OnInit {
  h!: ModelHealth;
  polylinePoints = "";
  workflowUrl: string | null = null;
  wfId = TEXERA_WORKFLOW_IDS.wf3_backtest;
  constructor(private svc: BetPilotService) {}
  ngOnInit(): void {
    this.h = this.svc.getModelHealth();
    this.polylinePoints = this.h.rollingClvSeries.map(p => `${p.x},${p.y}`).join(" ");
    this.workflowUrl = texeraWorkflowUrl("wf3_backtest");
  }
}
