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

import { ComponentRef, Injectable, Injector } from "@angular/core";
import { Overlay } from "@angular/cdk/overlay";
import { ComponentPortal } from "@angular/cdk/portal";
import { firstValueFrom } from "rxjs";
import { GenesisBuildProgressComponent } from "../../../component/user/genesis/genesis-build-progress.component";
import { GenesisCardComponent, GenesisCardChoice } from "../../../component/user/genesis/genesis-card.component";
import { GenesisStepItem, GenesisStepState } from "../../../component/user/genesis/genesis-steps-panel.component";
import { AnalyzeResponse, GenesisService, UploadResponse } from "./genesis.service";

export interface GenesisPickOutcome {
  choice: GenesisCardChoice;
  upload?: UploadResponse;
  analyze?: AnalyzeResponse;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function advancePhase(labels: string[], phaseIndex: number): GenesisStepItem[] {
  return labels.map((label, j) => ({
    label,
    state: (j < phaseIndex ? "completed" : j === phaseIndex ? "current" : "pending") as GenesisStepState,
  }));
}

function allCompleted(labels: string[]): GenesisStepItem[] {
  return labels.map(label => ({ label, state: "completed" as GenesisStepState }));
}

@Injectable({
  providedIn: "root",
})
export class GenesisCardOverlayService {
  constructor(
    private overlay: Overlay,
    private injector: Injector
  ) {}

  private patchCardSteps(ref: ComponentRef<GenesisCardComponent>, steps: GenesisStepItem[]): void {
    ref.setInput(
      "analysisSteps",
      steps.map(s => ({ ...s }))
    );
    ref.changeDetectorRef.detectChanges();
  }

  /**
   * Upload + analyze with a phased step checklist, then reveal suggestion cards.
   */
  public runUploadAnalyzeWithProgress(
    file: File,
    jwt: string,
    genesis: GenesisService
  ): Promise<GenesisPickOutcome> {
    return new Promise((resolve, reject) => {
      const overlayRef = this.overlay.create({
        hasBackdrop: true,
        backdropClass: "genesis-card-overlay-backdrop",
        scrollStrategy: this.overlay.scrollStrategies.block(),
        positionStrategy: this.overlay.position().global().centerHorizontally().centerVertically(),
      });

      const portal = new ComponentPortal(GenesisCardComponent, null, this.injector);
      const ref = overlayRef.attach(portal);
      ref.setInput("data", null);
      this.patchCardSteps(ref, [
        { label: "Reading your data", state: "current" },
        { label: "Detecting rows and columns", state: "pending" },
        { label: "Resolving target column", state: "pending" },
        { label: "Generating analysis recommendations", state: "pending" },
        { label: "Ready", state: "pending" },
      ]);

      let uploadResp: UploadResponse | undefined;
      let analyzeResp: AnalyzeResponse | undefined;
      let userClosed = false;

      const sub = ref.instance.choice.subscribe((choice: GenesisCardChoice) => {
        userClosed = true;
        sub.unsubscribe();
        overlayRef.dispose();
        resolve({
          choice,
          upload: uploadResp,
          analyze: analyzeResp,
        });
      });

      const run = async (): Promise<void> => {
        try {
          uploadResp = await firstValueFrom(genesis.upload(file, jwt));
          if (userClosed) {
            return;
          }
          const rows = uploadResp.row_count ?? "?";
          const cols = uploadResp.columns?.length ?? "?";
          this.patchCardSteps(ref, [
            { label: "Reading your data", state: "completed" },
            { label: `${rows} rows × ${cols} columns detected`, state: "current" },
            { label: "Resolving target column", state: "pending" },
            { label: "Generating analysis recommendations", state: "pending" },
            { label: "Ready", state: "pending" },
          ]);

          await sleep(400);
          if (userClosed) {
            return;
          }

          this.patchCardSteps(ref, [
            { label: "Reading your data", state: "completed" },
            { label: `${rows} rows × ${cols} columns detected`, state: "completed" },
            { label: "Identifying target column", state: "current" },
            { label: "Generating analysis recommendations", state: "pending" },
            { label: "Ready", state: "pending" },
          ]);

          analyzeResp = await firstValueFrom(genesis.analyze(uploadResp));
          if (userClosed) {
            return;
          }

          const task =
            analyzeResp.suggestions?.[0]?.task_type ??
            analyzeResp.detected_scenario ??
            "analysis";
          const conf = Math.round((analyzeResp.confidence ?? 0) * 100);
          const tgt = analyzeResp.target_column || "—";
          this.patchCardSteps(ref, [
            { label: "Reading your data", state: "completed" },
            { label: `${rows} rows × ${cols} columns detected`, state: "completed" },
            {
              label: `Target column: ${tgt} (${task}, ${conf}% confidence)`,
              state: "completed",
            },
            { label: "Generating analysis recommendations", state: "current" },
            { label: "Ready", state: "pending" },
          ]);

          await sleep(450);
          if (userClosed) {
            return;
          }

          this.patchCardSteps(ref, [
            { label: "Reading your data", state: "completed" },
            { label: `${rows} rows × ${cols} columns detected`, state: "completed" },
            {
              label: `Target column: ${tgt} (${task}, ${conf}% confidence)`,
              state: "completed",
            },
            { label: "Generating analysis recommendations", state: "completed" },
            { label: "Ready", state: "current" },
          ]);

          await sleep(500);
          if (userClosed) {
            return;
          }

          this.patchCardSteps(ref, [
            { label: "Reading your data", state: "completed" },
            { label: `${rows} rows × ${cols} columns detected`, state: "completed" },
            {
              label: `Target column: ${tgt} (${task}, ${conf}% confidence)`,
              state: "completed",
            },
            { label: "Generating analysis recommendations", state: "completed" },
            { label: "Ready", state: "completed" },
          ]);

          await sleep(150);
          if (userClosed) {
            return;
          }

          ref.setInput("analysisSteps", []);
          ref.setInput("data", { upload: uploadResp, analyze: analyzeResp });
          ref.changeDetectorRef.detectChanges();
        } catch (e: unknown) {
          if (userClosed) {
            return;
          }
          sub.unsubscribe();
          overlayRef.dispose();
          reject(e);
        }
      };

      void run();
    });
  }

  public show(upload: UploadResponse, analyze: AnalyzeResponse): Promise<GenesisCardChoice> {
    return new Promise(resolve => {
      const overlayRef = this.overlay.create({
        hasBackdrop: true,
        backdropClass: "genesis-card-overlay-backdrop",
        scrollStrategy: this.overlay.scrollStrategies.block(),
        positionStrategy: this.overlay.position().global().centerHorizontally().centerVertically(),
      });

      const portal = new ComponentPortal(GenesisCardComponent, null, this.injector);
      const ref = overlayRef.attach(portal);
      ref.setInput("analysisSteps", []);
      ref.setInput("data", { upload, analyze });

      ref.instance.choice.subscribe((choice: GenesisCardChoice) => {
        overlayRef.dispose();
        resolve(choice);
      });
    });
  }

  /**
   * Paced build steps (runs in parallel with real `genesis.build` via Promise.all).
   */
  public async showBuildProgress(opts: { title: string; operatorCount: number }): Promise<void> {
    const overlayRef = this.overlay.create({
      hasBackdrop: true,
      backdropClass: "genesis-card-overlay-backdrop",
      scrollStrategy: this.overlay.scrollStrategies.block(),
      positionStrategy: this.overlay.position().global().centerHorizontally().centerVertically(),
    });
    const portal = new ComponentPortal(GenesisBuildProgressComponent, null, this.injector);
    const ref = overlayRef.attach(portal);
    const short = opts.title.length > 48 ? `${opts.title.slice(0, 45)}…` : opts.title;
    const labels = [
      `Designing ${short}`,
      `Configuring ${opts.operatorCount} operators`,
      "Connecting data flow",
      "Ready",
    ];

    for (let phase = 0; phase < labels.length; phase++) {
      ref.setInput("steps", advancePhase(labels, phase));
      ref.changeDetectorRef.detectChanges();
      await sleep(300);
    }
    ref.setInput("steps", allCompleted(labels));
    ref.changeDetectorRef.detectChanges();
    await sleep(120);
    overlayRef.dispose();
  }
}
