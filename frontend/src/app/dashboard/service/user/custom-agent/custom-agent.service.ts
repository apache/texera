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
  CustomAgent,
  DEFAULT_AGENT_MODEL,
  DEFAULT_GUARDRAILS,
  DEFAULT_OUTPUT_PREFERENCES,
} from "../../../type/custom-agent.interface";

const STORAGE_KEY = "texera.customAgents.v1";

@Injectable({
  providedIn: "root",
})
export class CustomAgentService {
  private readonly agentsSubject = new BehaviorSubject<CustomAgent[]>(this.readAll());

  public list$(): Observable<CustomAgent[]> {
    return this.agentsSubject.asObservable();
  }

  public list(): CustomAgent[] {
    return this.agentsSubject.value;
  }

  public get(id: string): CustomAgent | undefined {
    return this.agentsSubject.value.find(a => a.id === id);
  }

  public create(input: Omit<CustomAgent, "id" | "createdAt" | "updatedAt">): CustomAgent {
    const now = new Date().toISOString();
    const agent: CustomAgent = {
      ...input,
      id: this.generateId(),
      createdAt: now,
      updatedAt: now,
    };
    const next = [...this.agentsSubject.value, agent];
    this.persist(next);
    return agent;
  }

  public update(id: string, patch: Partial<Omit<CustomAgent, "id" | "createdAt">>): CustomAgent | undefined {
    const idx = this.agentsSubject.value.findIndex(a => a.id === id);
    if (idx === -1) return undefined;
    const updated: CustomAgent = {
      ...this.agentsSubject.value[idx],
      ...patch,
      id,
      createdAt: this.agentsSubject.value[idx].createdAt,
      updatedAt: new Date().toISOString(),
    };
    const next = [...this.agentsSubject.value];
    next[idx] = updated;
    this.persist(next);
    return updated;
  }

  public delete(id: string): void {
    const next = this.agentsSubject.value.filter(a => a.id !== id);
    this.persist(next);
  }

  public duplicate(id: string): CustomAgent | undefined {
    const source = this.get(id);
    if (!source) return undefined;
    const { id: _ignored, createdAt: _c, updatedAt: _u, ...rest } = source;
    return this.create({ ...rest, name: `${source.name} (Copy)` });
  }

  public emptyDraft(creator: string): Omit<CustomAgent, "id" | "createdAt" | "updatedAt"> {
    return {
      name: "",
      description: "",
      icon: "🤖",
      creator,
      model: DEFAULT_AGENT_MODEL,
      domain: "general",
      methodology: "crisp_dm",
      taskType: "classification",
      guardrails: { ...DEFAULT_GUARDRAILS },
      customRules: "",
      knowledgeFiles: [],
      exampleWorkflowIds: [],
      outputPreferences: { ...DEFAULT_OUTPUT_PREFERENCES },
      preferredOperators: [],
      isPublic: false,
    };
  }

  private readAll(): CustomAgent[] {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return (parsed as CustomAgent[]).map(a => ({
        ...a,
        model: a.model ?? DEFAULT_AGENT_MODEL,
        knowledgeFiles: a.knowledgeFiles ?? [],
        exampleWorkflowIds: a.exampleWorkflowIds ?? [],
        outputPreferences: a.outputPreferences ?? { ...DEFAULT_OUTPUT_PREFERENCES },
      }));
    } catch {
      return [];
    }
  }

  private persist(agents: CustomAgent[]): void {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(agents));
    this.agentsSubject.next(agents);
  }

  private generateId(): string {
    return `agent-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  }
}
