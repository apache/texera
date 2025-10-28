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

/**
 * Interface for a data inconsistency item
 */
export interface DataInconsistency {
  id: string;
  name: string;
  description: string;
  operatorId: string;
}

/**
 * Service to manage data inconsistencies found in workflows
 * Singleton service that maintains an in-memory list of inconsistencies
 */
@Injectable({
  providedIn: "root",
})
export class DataInconsistencyService {
  private inconsistencies: Map<string, DataInconsistency> = new Map();
  private inconsistenciesSubject = new BehaviorSubject<DataInconsistency[]>([]);

  constructor() {}

  /**
   * Get all inconsistencies as an observable
   */
  public getInconsistencies(): Observable<DataInconsistency[]> {
    return this.inconsistenciesSubject.asObservable();
  }

  /**
   * Get all inconsistencies as an array
   */
  public getAllInconsistencies(): DataInconsistency[] {
    return Array.from(this.inconsistencies.values());
  }

  /**
   * Get a specific inconsistency by ID
   */
  public getInconsistency(id: string): DataInconsistency | undefined {
    return this.inconsistencies.get(id);
  }

  /**
   * Add a new inconsistency
   * Returns the created inconsistency
   */
  public addInconsistency(name: string, description: string, operatorId: string): DataInconsistency {
    const id = this.generateId();
    const inconsistency: DataInconsistency = {
      id,
      name,
      description,
      operatorId,
    };

    this.inconsistencies.set(id, inconsistency);
    this.emitUpdate();

    console.log(`Added inconsistency: ${name} (ID: ${id})`);
    return inconsistency;
  }

  /**
   * Update an existing inconsistency
   * Returns the updated inconsistency or undefined if not found
   */
  public updateInconsistency(
    id: string,
    updates: Partial<Omit<DataInconsistency, "id">>
  ): DataInconsistency | undefined {
    const existing = this.inconsistencies.get(id);
    if (!existing) {
      console.warn(`Inconsistency not found: ${id}`);
      return undefined;
    }

    const updated: DataInconsistency = {
      ...existing,
      ...updates,
    };

    this.inconsistencies.set(id, updated);
    this.emitUpdate();

    console.log(`Updated inconsistency: ${id}`);
    return updated;
  }

  /**
   * Delete an inconsistency by ID
   * Returns true if deleted, false if not found
   */
  public deleteInconsistency(id: string): boolean {
    const existed = this.inconsistencies.delete(id);
    if (existed) {
      this.emitUpdate();
      console.log(`Deleted inconsistency: ${id}`);
    } else {
      console.warn(`Inconsistency not found: ${id}`);
    }
    return existed;
  }

  /**
   * Clear all inconsistencies
   */
  public clearAll(): void {
    this.inconsistencies.clear();
    this.emitUpdate();
    console.log("Cleared all inconsistencies");
  }

  /**
   * Get inconsistencies for a specific operator
   */
  public getInconsistenciesForOperator(operatorId: string): DataInconsistency[] {
    return Array.from(this.inconsistencies.values()).filter(inc => inc.operatorId === operatorId);
  }

  /**
   * Generate a unique ID for an inconsistency
   */
  private generateId(): string {
    return `inc-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Emit updated list to subscribers
   */
  private emitUpdate(): void {
    this.inconsistenciesSubject.next(this.getAllInconsistencies());
  }
}
