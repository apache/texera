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

import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, from, of, throwError, firstValueFrom } from "rxjs";
import { map, catchError, switchMap, finalize, tap } from "rxjs/operators";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, tool } from "ai";
import { z } from "zod";
import { AppSettings } from "../../../../common/app-setting";
import { DataCheck } from "../../data-check/data-check.service";

/**
 * Ground truth check format (from ground_truth.json)
 */
export interface GroundTruthCheck {
  check_id: string;
  description: string;
  tables: string[];
}

/**
 * Ground truth format
 */
export interface GroundTruth {
  dataset_name: string;
  checks: GroundTruthCheck[];
  violations?: any[];
}

/**
 * Evaluation result for a single check pair
 */
export interface CheckEvaluationResult {
  discoveredCheckId: string;
  groundTruthCheckId: string;
  similarity: number; // 0-1 scale
  explanation: string;
}

/**
 * Overall evaluation result
 */
export interface EvaluationResult {
  timestamp: Date;
  totalDiscoveredChecks: number;
  totalGroundTruthChecks: number;
  matchedChecks: number;
  missedChecks: string[]; // Ground truth checks not discovered
  extraChecks: string[]; // Discovered checks with no matching ground truth
  precision: number;
  recall: number;
  f1Score: number;
  checkResults: CheckEvaluationResult[];
  averageSimilarity: number;
}

/**
 * Evaluation state
 */
export enum EvaluationState {
  IDLE = "Idle",
  EVALUATING = "Evaluating",
  COMPLETED = "Completed",
  ERROR = "Error",
}

/**
 * LLM Judge system prompt for evaluating a single check pair
 */
const LLM_JUDGE_SYSTEM_PROMPT = `You are an expert judge for evaluating data quality check discovery.

Your task is to compare a discovered data check against a ground truth check and determine their similarity.

## Evaluation Criteria

Consider:
1. Semantic equivalence of the descriptions - do they describe the same data quality rule?
2. Whether the checks would catch the same data issues
3. The specificity and completeness of the discovered check

## Similarity Scale
- 0.9-1.0: Semantically identical - same rule, same meaning
- 0.7-0.89: Very similar - minor wording differences but same intent
- 0.5-0.69: Partially similar - related but not identical rules
- 0.3-0.49: Weakly related - some overlap but different focus
- 0.0-0.29: Not related - different rules entirely

Use the evaluateCheckPair tool to provide your assessment.`;

@Injectable({
  providedIn: "root",
})
export class DataCheckEvaluationService {
  private model: any;
  private modelType: string = "gpt-4o";
  private stateSubject = new BehaviorSubject<EvaluationState>(EvaluationState.IDLE);
  public state$ = this.stateSubject.asObservable();

  private lastResultSubject = new BehaviorSubject<EvaluationResult | null>(null);
  public lastResult$ = this.lastResultSubject.asObservable();

  constructor() {}

  /**
   * Initialize the evaluation service with LLM model
   */
  public initialize(): Observable<void> {
    return of(undefined).pipe(
      tap(() => {
        this.model = createOpenAI({
          baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
          apiKey: "dummy",
        }).chat(this.modelType);
      }),
      catchError((error: unknown) => throwError(() => error))
    );
  }

  /**
   * Create the evaluation tool for LLM to call
   */
  private createEvaluationTool() {
    return tool({
      description:
        "Evaluate how similar a discovered data check is to a ground truth check. Call this tool with your assessment.",
      inputSchema: z.object({
        similarity: z
          .number()
          .min(0)
          .max(1)
          .describe("Similarity score from 0 to 1, where 1 means semantically identical"),
        explanation: z.string().describe("Brief explanation of your similarity assessment (1-2 sentences)"),
      }),
      execute: async (args: { similarity: number; explanation: string }) => {
        return args;
      },
    });
  }

  /**
   * Evaluate a single pair of discovered check and ground truth check using LLM
   */
  private evaluatePair(
    discoveredCheck: DataCheck,
    groundTruthCheck: GroundTruthCheck
  ): Observable<CheckEvaluationResult> {
    const userPrompt = `## Discovered Check
Check ID: ${discoveredCheck.checkId}
Description: ${discoveredCheck.description}
Tables: ${discoveredCheck.tables.join(", ")}

## Ground Truth Check
Check ID: ${groundTruthCheck.check_id}
Description: ${groundTruthCheck.description}
Tables: ${groundTruthCheck.tables.join(", ")}

Please evaluate how similar the discovered check is to the ground truth check using the evaluateCheckPair tool.`;

    return from(
      generateText({
        model: this.model,
        system: LLM_JUDGE_SYSTEM_PROMPT,
        messages: [{ role: "user", content: userPrompt }],
        tools: {
          evaluateCheckPair: this.createEvaluationTool(),
        },
        toolChoice: "required",
      })
    ).pipe(
      map(result => {
        // Extract the tool result - the execute function returns the args directly
        const toolResult = result.toolResults?.[0] as any;
        if (toolResult && toolResult.toolName === "evaluateCheckPair" && toolResult.result) {
          const evalResult = toolResult.result as { similarity: number; explanation: string };
          return {
            discoveredCheckId: discoveredCheck.checkId,
            groundTruthCheckId: groundTruthCheck.check_id,
            similarity: evalResult.similarity,
            explanation: evalResult.explanation,
          };
        }

        // Fallback if no tool result found
        return {
          discoveredCheckId: discoveredCheck.checkId,
          groundTruthCheckId: groundTruthCheck.check_id,
          similarity: 0,
          explanation: "Failed to get evaluation from LLM",
        };
      }),
      catchError((error: unknown) => {
        console.error("Error evaluating pair:", error);
        return of({
          discoveredCheckId: discoveredCheck.checkId,
          groundTruthCheckId: groundTruthCheck.check_id,
          similarity: 0,
          explanation: `Evaluation error: ${error instanceof Error ? error.message : String(error)}`,
        });
      })
    );
  }

  /**
   * Evaluate discovered checks against ground truth using LLM as judge
   * Loop through each discovered check, find matching ground truth by table, and evaluate pairs
   */
  public evaluate(discoveredChecks: DataCheck[], groundTruth: GroundTruth): Observable<EvaluationResult> {
    // Ensure model is initialized
    const initObservable = this.model ? of(undefined) : this.initialize();

    return initObservable.pipe(
      tap(() => this.stateSubject.next(EvaluationState.EVALUATING)),
      switchMap(() => this.performEvaluation(discoveredChecks, groundTruth)),
      tap(result => {
        this.lastResultSubject.next(result);
        this.stateSubject.next(EvaluationState.COMPLETED);
      }),
      catchError((error: unknown) => {
        this.stateSubject.next(EvaluationState.ERROR);
        return throwError(() => error);
      }),
      finalize(() => {
        if (this.stateSubject.value === EvaluationState.EVALUATING) {
          this.stateSubject.next(EvaluationState.IDLE);
        }
      })
    );
  }

  /**
   * Perform the actual evaluation logic
   */
  private performEvaluation(discoveredChecks: DataCheck[], groundTruth: GroundTruth): Observable<EvaluationResult> {
    return from(this.evaluateAllPairs(discoveredChecks, groundTruth));
  }

  /**
   * Evaluate all check pairs and compute metrics
   * Evaluates every discovered check against every ground truth check
   */
  private async evaluateAllPairs(discoveredChecks: DataCheck[], groundTruth: GroundTruth): Promise<EvaluationResult> {
    const checkResults: CheckEvaluationResult[] = [];
    const matchedGroundTruthIds = new Set<string>();
    const discoveredWithMatches = new Set<string>();

    // Evaluate every discovered check against every ground truth check
    for (const discoveredCheck of discoveredChecks) {
      for (const gtCheck of groundTruth.checks) {
        const result = await firstValueFrom(this.evaluatePair(discoveredCheck, gtCheck));
        if (result) {
          checkResults.push(result);

          // Track matches (similarity >= 0.5 considered a match)
          if (result.similarity >= 0.5) {
            matchedGroundTruthIds.add(result.groundTruthCheckId);
            discoveredWithMatches.add(result.discoveredCheckId);
          }
        }
      }
    }

    // Calculate missed checks (ground truth checks not matched)
    const missedChecks = groundTruth.checks
      .filter(gt => !matchedGroundTruthIds.has(gt.check_id))
      .map(gt => gt.check_id);

    // Calculate extra checks (discovered checks with no good match)
    const extraChecks = discoveredChecks.filter(dc => !discoveredWithMatches.has(dc.checkId)).map(dc => dc.checkId);

    // Calculate metrics
    const matchedChecks = matchedGroundTruthIds.size;
    const precision = discoveredChecks.length > 0 ? discoveredWithMatches.size / discoveredChecks.length : 0;
    const recall = groundTruth.checks.length > 0 ? matchedChecks / groundTruth.checks.length : 0;
    const f1Score = precision + recall > 0 ? (2 * precision * recall) / (precision + recall) : 0;

    // Calculate average similarity (only for matched pairs with similarity >= 0.5)
    const matchedResults = checkResults.filter(r => r.similarity >= 0.5);
    const averageSimilarity =
      matchedResults.length > 0 ? matchedResults.reduce((sum, r) => sum + r.similarity, 0) / matchedResults.length : 0;

    return {
      timestamp: new Date(),
      totalDiscoveredChecks: discoveredChecks.length,
      totalGroundTruthChecks: groundTruth.checks.length,
      matchedChecks,
      missedChecks,
      extraChecks,
      precision,
      recall,
      f1Score,
      checkResults,
      averageSimilarity,
    };
  }

  /**
   * Get current state
   */
  public getState(): EvaluationState {
    return this.stateSubject.value;
  }

  /**
   * Get last evaluation result
   */
  public getLastResult(): EvaluationResult | null {
    return this.lastResultSubject.value;
  }

  /**
   * Clear last result
   */
  public clearResult(): void {
    this.lastResultSubject.next(null);
    this.stateSubject.next(EvaluationState.IDLE);
  }

  /**
   * Set the model type for evaluation
   * This will reinitialize the model on the next evaluation
   */
  public setModelType(modelType: string): void {
    if (this.modelType !== modelType) {
      this.modelType = modelType;
      this.model = null; // Force reinitialization
    }
  }

  /**
   * Get the current model type
   */
  public getModelType(): string {
    return this.modelType;
  }
}
