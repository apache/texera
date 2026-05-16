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
import { QuickStep, QuickStepAction } from "../../../type/quick-step.interface";

const STORAGE_KEY = "texera.quickSteps.v1";
const SEEDED_FLAG_KEY = "texera.quickSteps.seeded.v2";

@Injectable({
  providedIn: "root",
})
export class QuickStepService {
  private readonly stepsSubject = new BehaviorSubject<QuickStep[]>([]);

  constructor() {
    const initial = this.readAll();
    if (initial.length === 0 && !localStorage.getItem(SEEDED_FLAG_KEY)) {
      const seeded = this.buildSeedQuickSteps();
      this.persist(seeded);
      localStorage.setItem(SEEDED_FLAG_KEY, "1");
    } else if (!localStorage.getItem(SEEDED_FLAG_KEY)) {
      // Prior seed batch used simulated actions; refresh built-ins while
      // preserving any user-created quick steps.
      const userSteps = initial.filter(s => !s.seeded);
      this.persist([...this.buildSeedQuickSteps(), ...userSteps]);
      localStorage.setItem(SEEDED_FLAG_KEY, "1");
    } else {
      this.stepsSubject.next(initial);
    }
  }

  public list$(): Observable<QuickStep[]> {
    return this.stepsSubject.asObservable();
  }

  public list(): QuickStep[] {
    return this.stepsSubject.value;
  }

  public get(id: string): QuickStep | undefined {
    return this.stepsSubject.value.find(s => s.id === id);
  }

  public create(input: Omit<QuickStep, "id" | "createdAt" | "updatedAt">): QuickStep {
    const now = new Date().toISOString();
    const quickStep: QuickStep = {
      ...input,
      id: this.generateId(),
      createdAt: now,
      updatedAt: now,
    };
    this.persist([...this.stepsSubject.value, quickStep]);
    return quickStep;
  }

  public update(id: string, patch: Partial<Omit<QuickStep, "id" | "createdAt">>): QuickStep | undefined {
    const idx = this.stepsSubject.value.findIndex(s => s.id === id);
    if (idx === -1) return undefined;
    const updated: QuickStep = {
      ...this.stepsSubject.value[idx],
      ...patch,
      id,
      createdAt: this.stepsSubject.value[idx].createdAt,
      updatedAt: new Date().toISOString(),
    };
    const next = [...this.stepsSubject.value];
    next[idx] = updated;
    this.persist(next);
    return updated;
  }

  public delete(id: string): void {
    this.persist(this.stepsSubject.value.filter(s => s.id !== id));
  }

  private readAll(): QuickStep[] {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed as QuickStep[];
    } catch {
      return [];
    }
  }

  private persist(quickSteps: QuickStep[]): void {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(quickSteps));
    this.stepsSubject.next(quickSteps);
  }

  private generateId(): string {
    return `qs-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  }

  private buildSeedQuickSteps(): QuickStep[] {
    const now = new Date().toISOString();
    const make = (
      partial: Omit<QuickStep, "id" | "createdAt" | "updatedAt" | "seeded"> & { steps: QuickStepAction[] }
    ): QuickStep => ({
      ...partial,
      id: this.generateId(),
      createdAt: now,
      updatedAt: now,
      seeded: true,
    });

    return [
      make({
        name: "Clean and Profile",
        description: "Profile the data, add cleaning operators, then re-profile.",
        icon: "🧹",
        author: "Texera",
        isPublic: true,
        steps: [
          { order: 1, action: "profile_data", label: "Profile data source" },
          {
            order: 2,
            action: "add_snippet",
            label: "Add Data Cleaning Kit",
            config: { snippetName: "Data Cleaning Kit" },
          },
          { order: 3, action: "profile_data", label: "Re-profile to verify" },
        ],
      }),
      make({
        name: "Run and Report",
        description: "Run the current workflow, wait for completion, then generate a report.",
        icon: "🚀",
        author: "Texera",
        isPublic: true,
        steps: [
          { order: 1, action: "run_workflow", label: "Run the current workflow" },
          { order: 2, action: "generate_report", label: "Generate Results Dashboard" },
          {
            order: 3,
            action: "notify",
            label: "Report ready",
            config: { message: "Report ready — check the downloaded HTML." },
          },
        ],
      }),
      make({
        name: "Publish to Hub",
        description: "Generate documentation, then open the Share/Publish dialog.",
        icon: "📤",
        author: "Texera",
        isPublic: true,
        steps: [
          { order: 1, action: "generate_report", label: "Generate workflow documentation" },
          { order: 2, action: "publish_hub", label: "Open Share dialog" },
        ],
      }),
      make({
        name: "Full ML Pipeline",
        description: "Profile, clean, split, train, evaluate — full classification scaffold.",
        icon: "🔬",
        author: "Texera",
        isPublic: true,
        steps: [
          { order: 1, action: "profile_data", label: "Profile data source" },
          {
            order: 2,
            action: "add_snippet",
            label: "Add Data Cleaning Kit",
            config: { snippetName: "Data Cleaning Kit" },
          },
          {
            order: 3,
            action: "add_snippet",
            label: "Add Classification Bundle",
            config: { snippetName: "Classification Bundle" },
          },
          {
            order: 4,
            action: "notify",
            label: "Pipeline ready",
            config: { message: "ML pipeline scaffolded — configure and run." },
          },
        ],
      }),
    ];
  }
}
