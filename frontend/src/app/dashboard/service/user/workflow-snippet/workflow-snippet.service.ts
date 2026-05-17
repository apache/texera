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
import { BehaviorSubject, Observable } from "rxjs";
import {
  DEFAULT_SNIPPET_CATEGORY,
  SnippetLink,
  SnippetOperator,
  WorkflowSnippet,
} from "../../../type/workflow-snippet.interface";

const STORAGE_KEY = "texera.snippets.v1";
const SEEDED_FLAG_KEY = "texera.snippets.seeded.v3";

// Operator types from a prior buggy seed batch. Any seeded snippet that
// references one of these is replaced with the corrected built-ins.
const STALE_SEEDED_TYPES = new Set([
  "MissingValueHandler",
  "TrainTestSplit",
  "MachineLearningEvaluator",
  "ScatterChart",
  "Scatterplot", // earlier seed used Scatterplot; the user wants ScatterMatrixChart
]);

@Injectable({
  providedIn: "root",
})
export class WorkflowSnippetService {
  private readonly snippetsSubject = new BehaviorSubject<WorkflowSnippet[]>([]);

  constructor() {
    const initial = this.readAll();
    const hasStaleSeed = initial.some(
      s => s.seeded && s.operators.some(o => STALE_SEEDED_TYPES.has(o.operatorType))
    );
    if (initial.length === 0 && !localStorage.getItem(SEEDED_FLAG_KEY)) {
      const seeded = this.buildSeedSnippets();
      this.persist(seeded);
      localStorage.setItem(SEEDED_FLAG_KEY, "1");
    } else if (hasStaleSeed) {
      const userSnippets = initial.filter(s => !s.seeded);
      const refreshed = [...this.buildSeedSnippets(), ...userSnippets];
      this.persist(refreshed);
      localStorage.setItem(SEEDED_FLAG_KEY, "1");
    } else {
      this.snippetsSubject.next(initial);
    }
  }

  public list$(): Observable<WorkflowSnippet[]> {
    return this.snippetsSubject.asObservable();
  }

  public list(): WorkflowSnippet[] {
    return this.snippetsSubject.value;
  }

  public get(id: string): WorkflowSnippet | undefined {
    return this.snippetsSubject.value.find(s => s.id === id);
  }

  public create(input: Omit<WorkflowSnippet, "id" | "createdAt" | "updatedAt">): WorkflowSnippet {
    const now = new Date().toISOString();
    const snippet: WorkflowSnippet = {
      ...input,
      id: this.generateId(),
      createdAt: now,
      updatedAt: now,
    };
    this.persist([...this.snippetsSubject.value, snippet]);
    return snippet;
  }

  public update(
    id: string,
    patch: Partial<Omit<WorkflowSnippet, "id" | "createdAt">>
  ): WorkflowSnippet | undefined {
    const idx = this.snippetsSubject.value.findIndex(s => s.id === id);
    if (idx === -1) return undefined;
    const updated: WorkflowSnippet = {
      ...this.snippetsSubject.value[idx],
      ...patch,
      id,
      createdAt: this.snippetsSubject.value[idx].createdAt,
      updatedAt: new Date().toISOString(),
    };
    const next = [...this.snippetsSubject.value];
    next[idx] = updated;
    this.persist(next);
    return updated;
  }

  public delete(id: string): void {
    this.persist(this.snippetsSubject.value.filter(s => s.id !== id));
  }

  /**
   * Group snippets by category for the operator panel.
   */
  public groupByCategory(): Map<string, WorkflowSnippet[]> {
    const result = new Map<string, WorkflowSnippet[]>();
    for (const snippet of this.snippetsSubject.value) {
      const cat = snippet.category || DEFAULT_SNIPPET_CATEGORY;
      const list = result.get(cat) ?? [];
      list.push(snippet);
      result.set(cat, list);
    }
    return result;
  }

  private readAll(): WorkflowSnippet[] {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed as WorkflowSnippet[];
    } catch {
      return [];
    }
  }

  private persist(snippets: WorkflowSnippet[]): void {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(snippets));
    this.snippetsSubject.next(snippets);
  }

  private generateId(): string {
    return `snippet-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  }

  // Three pre-built snippets so the section is never empty on first load.
  // Operator types referenced here are the canonical Texera built-ins; if the
  // user's deployment is missing one, the panel will surface a friendly
  // notification rather than silently failing.
  private buildSeedSnippets(): WorkflowSnippet[] {
    const now = new Date().toISOString();
    const linearLinks = (ops: SnippetOperator[]): SnippetLink[] => {
      const links: SnippetLink[] = [];
      for (let i = 0; i < ops.length - 1; i++) {
        links.push({
          fromOperatorId: ops[i].operatorId,
          fromPortId: "output-0",
          toOperatorId: ops[i + 1].operatorId,
          toPortId: "input-0",
        });
      }
      return links;
    };
    const seed = (
      partial: Omit<WorkflowSnippet, "id" | "createdAt" | "updatedAt" | "links" | "seeded"> & {
        operators: SnippetOperator[];
      }
    ): WorkflowSnippet => ({
      ...partial,
      id: this.generateId(),
      createdAt: now,
      updatedAt: now,
      links: linearLinks(partial.operators),
      seeded: true,
    });

    const cleaning: SnippetOperator[] = [
      {
        operatorId: "filter",
        operatorType: "Filter",
        operatorProperties: {},
        customDisplayName: "Filter (drop nulls)",
        position: { x: 0, y: 0 },
      },
      {
        operatorId: "dedupe",
        operatorType: "Distinct",
        operatorProperties: {},
        customDisplayName: "Remove Duplicates",
        position: { x: 220, y: 0 },
      },
      {
        operatorId: "cast",
        operatorType: "TypeCasting",
        operatorProperties: {},
        customDisplayName: "Type Cast",
        position: { x: 440, y: 0 },
      },
    ];

    const classification: SnippetOperator[] = [
      {
        operatorId: "split",
        operatorType: "Split",
        operatorProperties: {},
        customDisplayName: "Train/Test Split",
        position: { x: 0, y: 0 },
      },
      {
        operatorId: "model",
        operatorType: "SklearnLogisticRegression",
        operatorProperties: {},
        customDisplayName: "Logistic Regression",
        position: { x: 220, y: 0 },
      },
      {
        operatorId: "eval",
        operatorType: "SklearnTesting",
        operatorProperties: {},
        customDisplayName: "Sklearn Testing",
        position: { x: 440, y: 0 },
      },
    ];

    const eda: SnippetOperator[] = [
      {
        operatorId: "scan",
        operatorType: "CSVFileScan",
        operatorProperties: {},
        customDisplayName: "CSV File Scan",
        position: { x: 0, y: 0 },
      },
      {
        operatorId: "stats",
        operatorType: "Aggregate",
        operatorProperties: {},
        customDisplayName: "Statistics Summary",
        position: { x: 220, y: 0 },
      },
      {
        operatorId: "scatter",
        operatorType: "ScatterMatrixChart",
        operatorProperties: {},
        customDisplayName: "Scatter Matrix",
        position: { x: 440, y: 0 },
      },
    ];

    return [
      seed({
        name: "Data Cleaning Kit",
        description: "Missing values → Remove duplicates → Type cast.",
        icon: "🧹",
        category: DEFAULT_SNIPPET_CATEGORY,
        operators: cleaning,
        author: "Texera",
        isPublic: true,
      }),
      seed({
        name: "Classification Bundle",
        description: "Train/Test split → Logistic Regression → Evaluation.",
        icon: "🧠",
        category: DEFAULT_SNIPPET_CATEGORY,
        operators: classification,
        author: "Texera",
        isPublic: true,
      }),
      seed({
        name: "EDA Starter",
        description: "CSV scan → Statistics summary → Scatter plot.",
        icon: "📊",
        category: DEFAULT_SNIPPET_CATEGORY,
        operators: eda,
        author: "Texera",
        isPublic: true,
      }),
    ];
  }
}
