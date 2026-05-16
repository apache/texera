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
  CustomOperator,
  DEFAULT_CUSTOM_OPERATOR_CATEGORY,
  DEFAULT_CUSTOM_OPERATOR_CODE,
} from "../../../type/custom-operator.interface";

const STORAGE_KEY = "texera.customOperators.v1";

@Injectable({
  providedIn: "root",
})
export class CustomOperatorService {
  private readonly operatorsSubject = new BehaviorSubject<CustomOperator[]>(this.readAll());

  public list$(): Observable<CustomOperator[]> {
    return this.operatorsSubject.asObservable();
  }

  public list(): CustomOperator[] {
    return this.operatorsSubject.value;
  }

  public get(id: string): CustomOperator | undefined {
    return this.operatorsSubject.value.find(op => op.id === id);
  }

  public create(input: Omit<CustomOperator, "id" | "createdAt" | "updatedAt">): CustomOperator {
    const now = new Date().toISOString();
    const operator: CustomOperator = {
      ...input,
      id: this.generateId(),
      createdAt: now,
      updatedAt: now,
    };
    this.persist([...this.operatorsSubject.value, operator]);
    return operator;
  }

  public update(id: string, patch: Partial<Omit<CustomOperator, "id" | "createdAt">>): CustomOperator | undefined {
    const idx = this.operatorsSubject.value.findIndex(op => op.id === id);
    if (idx === -1) return undefined;
    const existing = this.operatorsSubject.value[idx];
    const updated: CustomOperator = {
      ...existing,
      ...patch,
      id,
      createdAt: existing.createdAt,
      updatedAt: new Date().toISOString(),
    };
    const next = [...this.operatorsSubject.value];
    next[idx] = updated;
    this.persist(next);
    return updated;
  }

  public delete(id: string): void {
    this.persist(this.operatorsSubject.value.filter(op => op.id !== id));
  }

  public emptyDraft(author: string): Omit<CustomOperator, "id" | "createdAt" | "updatedAt"> {
    return {
      name: "",
      description: "",
      icon: "🧩",
      category: DEFAULT_CUSTOM_OPERATOR_CATEGORY,
      author,
      code: DEFAULT_CUSTOM_OPERATOR_CODE,
      language: "python",
      inputPorts: [{ name: "input", type: "any" }],
      outputPorts: [{ name: "output", type: "any" }],
      properties: [],
      isPublic: false,
    };
  }

  private readAll(): CustomOperator[] {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return (parsed as CustomOperator[]).map(op => ({
        ...op,
        inputPorts: op.inputPorts ?? [],
        outputPorts: op.outputPorts ?? [],
        properties: op.properties ?? [],
        category: op.category ?? DEFAULT_CUSTOM_OPERATOR_CATEGORY,
      }));
    } catch {
      return [];
    }
  }

  private persist(operators: CustomOperator[]): void {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(operators));
    this.operatorsSubject.next(operators);
  }

  private generateId(): string {
    return `custom-op-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  }
}
