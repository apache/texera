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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DataCheckService, DataCheck } from "../../../service/data-check/data-check.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import {
  DataCheckEvaluationService,
  EvaluationResult,
  GroundTruth,
  GroundTruthCheck,
  CheckEvaluationResult,
} from "../../../service/copilot/eval/data-check-evaluation.service";
import { TexeraCopilotManagerService, ModelType } from "../../../service/copilot/texera-copilot-manager.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";

/**
 * Represents a ground truth check with its matching score
 */
interface MatchedGroundTruthCheck extends GroundTruthCheck {
  similarity: number;
  explanation: string;
}

@UntilDestroy()
@Component({
  selector: "texera-data-check-list",
  templateUrl: "./data-check-list.component.html",
  styleUrls: ["./data-check-list.component.scss"],
})
export class DataCheckListComponent implements OnInit {
  dataChecks: DataCheck[] = [];

  // Ground truth state
  groundTruth: GroundTruth | null = null;
  groundTruthFileName: string = "";

  // Model selection
  availableModels: ModelType[] = [];
  selectedModelId: string = "";

  // Evaluation state
  isEvaluating: boolean = false;
  evaluationResult: EvaluationResult | null = null;

  // Selection state for matching view
  selectedDataCheck: DataCheck | null = null;
  matchedGroundTruthChecks: MatchedGroundTruthCheck[] = [];

  constructor(
    private dataCheckService: DataCheckService,
    private workflowActionService: WorkflowActionService,
    private evaluationService: DataCheckEvaluationService,
    private copilotManagerService: TexeraCopilotManagerService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    // Subscribe to data check updates
    this.dataCheckService
      .getDataChecks()
      .pipe(untilDestroyed(this))
      .subscribe(dataChecks => {
        this.dataChecks = dataChecks;
        // Clear selection if selected check was deleted
        if (this.selectedDataCheck && !dataChecks.find(dc => dc.id === this.selectedDataCheck?.id)) {
          this.clearSelection();
        }
      });

    // Fetch available models
    this.copilotManagerService
      .fetchModelTypes()
      .pipe(untilDestroyed(this))
      .subscribe(models => {
        this.availableModels = models;
        if (models.length > 0 && !this.selectedModelId) {
          this.selectedModelId = models[0].id;
        }
      });
  }

  /**
   * Handle file upload for ground truth JSON
   */
  onGroundTruthFileChange(event: Event): void {
    const input = event.target as HTMLInputElement;
    if (!input.files || input.files.length === 0) {
      return;
    }

    const file = input.files[0];
    this.groundTruthFileName = file.name;

    const reader = new FileReader();
    reader.onload = () => {
      try {
        const content = reader.result as string;
        const parsed = JSON.parse(content) as GroundTruth;

        if (!parsed.checks || !Array.isArray(parsed.checks)) {
          this.notificationService.error("Invalid ground truth format: missing 'checks' array");
          this.groundTruth = null;
          this.groundTruthFileName = "";
          return;
        }

        this.groundTruth = parsed;
        this.evaluationResult = null;
        this.clearSelection();
        this.notificationService.success(`Loaded ${parsed.checks.length} ground truth checks`);
      } catch (e) {
        this.notificationService.error("Invalid JSON format: " + (e as Error).message);
        this.groundTruth = null;
        this.groundTruthFileName = "";
      }
    };

    reader.onerror = () => {
      this.notificationService.error("Failed to read file");
      this.groundTruth = null;
      this.groundTruthFileName = "";
    };

    reader.readAsText(file);
    // Reset input to allow selecting the same file again
    input.value = "";
  }

  /**
   * Clear the uploaded ground truth
   */
  clearGroundTruth(): void {
    this.groundTruth = null;
    this.groundTruthFileName = "";
    this.evaluationResult = null;
    this.clearSelection();
  }

  /**
   * Delete a data check
   */
  deleteDataCheck(id: string): void {
    this.dataCheckService.deleteDataCheck(id);
  }

  /**
   * Clear all data checks
   */
  clearAll(): void {
    this.dataCheckService.clearAll();
    this.clearSelection();
  }

  /**
   * Handle click on data check card to highlight the upstream path
   */
  onDataCheckClick(dataCheck: DataCheck): void {
    if (!dataCheck.operatorId) {
      return;
    }

    const pathResult = this.workflowActionService.findUpstreamPath(dataCheck.operatorId);
    this.dataCheckService.toggleHighlight(dataCheck.id, pathResult.operators, pathResult.links);
  }

  /**
   * Handle selection of a data check for matching view
   */
  onDataCheckSelect(dataCheck: DataCheck, event: Event): void {
    event.stopPropagation();

    if (this.selectedDataCheck?.id === dataCheck.id) {
      this.clearSelection();
      return;
    }

    this.selectedDataCheck = dataCheck;
    this.updateMatchedGroundTruthChecks();
  }

  /**
   * Update the list of matched ground truth checks based on evaluation results
   * Shows ALL ground truth checks with their similarity scores, sorted by similarity descending
   */
  private updateMatchedGroundTruthChecks(): void {
    if (!this.selectedDataCheck || !this.evaluationResult || !this.groundTruth) {
      this.matchedGroundTruthChecks = [];
      return;
    }

    // Find all evaluation results for this discovered check (includes all ground truth checks)
    const matchingResults = this.evaluationResult.checkResults.filter(
      r => r.discoveredCheckId === this.selectedDataCheck?.checkId
    );

    // Map to MatchedGroundTruthCheck with similarity scores, sorted by similarity descending
    this.matchedGroundTruthChecks = matchingResults
      .map(result => {
        const gtCheck = this.groundTruth?.checks.find(c => c.check_id === result.groundTruthCheckId);
        if (!gtCheck) return null;
        return {
          ...gtCheck,
          similarity: result.similarity,
          explanation: result.explanation,
        };
      })
      .filter((c): c is MatchedGroundTruthCheck => c !== null)
      .sort((a, b) => b.similarity - a.similarity);
  }

  /**
   * Clear the current selection
   */
  clearSelection(): void {
    this.selectedDataCheck = null;
    this.matchedGroundTruthChecks = [];
  }

  /**
   * Check if a data check is currently selected
   */
  isSelected(dataCheck: DataCheck): boolean {
    return this.selectedDataCheck?.id === dataCheck.id;
  }

  /**
   * Check if there is an active highlight
   */
  hasActiveHighlight(): boolean {
    return this.dataCheckService.getCurrentHighlightedDataCheckId() !== null;
  }

  /**
   * Clear the current highlight from the workflow graph
   */
  clearHighlights(): void {
    this.dataCheckService.clearHighlight();
  }

  /**
   * Run evaluation using LLM as a judge
   */
  evaluateWithLLM(): void {
    if (this.dataChecks.length === 0) {
      this.notificationService.warning("No data checks to evaluate");
      return;
    }

    if (!this.groundTruth) {
      this.notificationService.warning("Please upload a ground truth file first");
      return;
    }

    if (!this.selectedModelId) {
      this.notificationService.warning("Please select a model");
      return;
    }

    this.isEvaluating = true;
    this.clearSelection();

    // Set the model type on the evaluation service
    this.evaluationService.setModelType(this.selectedModelId);

    this.evaluationService
      .evaluate(this.dataChecks, this.groundTruth)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: result => {
          this.evaluationResult = result;
          this.isEvaluating = false;
          this.notificationService.success(
            `Evaluation complete: ${result.matchedChecks} matches, F1: ${(result.f1Score * 100).toFixed(1)}%`
          );
        },
        error: (error: unknown) => {
          this.isEvaluating = false;
          this.notificationService.error(
            "Evaluation failed: " + (error instanceof Error ? error.message : String(error))
          );
        },
      });
  }

  /**
   * Get similarity score for a discovered check (best match)
   */
  getBestSimilarity(dataCheck: DataCheck): number | null {
    if (!this.evaluationResult) return null;

    const matches = this.evaluationResult.checkResults.filter(r => r.discoveredCheckId === dataCheck.checkId);
    if (matches.length === 0) return null;

    return Math.max(...matches.map(m => m.similarity));
  }

  /**
   * Get color class based on similarity score
   */
  getSimilarityColor(similarity: number): string {
    if (similarity >= 0.7) return "high-similarity";
    if (similarity >= 0.5) return "medium-similarity";
    return "low-similarity";
  }

  /**
   * Format similarity as percentage
   */
  formatSimilarity(similarity: number): string {
    return `${(similarity * 100).toFixed(0)}%`;
  }
}
