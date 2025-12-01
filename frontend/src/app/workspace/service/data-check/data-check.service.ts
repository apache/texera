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
import { BehaviorSubject, Observable, Subject } from "rxjs";

/**
 * Interface for a data check item
 */
export interface DataCheck {
  id: string;
  checkId: string; // Unique identifier for the check type (e.g., 'invalid_rating')
  description: string; // Human-readable description of the check
  tables: string[]; // Tables involved in the check
  operatorId: string; // ID of the operator that implements this check
}

/**
 * Interface for a data check highlight event
 */
export interface DataCheckHighlight {
  dataCheckId: string;
  operatorIds: string[];
  linkIds: string[];
}

/**
 * Service to manage data checks found in workflows
 * Singleton service that maintains an in-memory list of data checks
 */
@Injectable({
  providedIn: "root",
})
export class DataCheckService {
  private dataChecks: Map<string, DataCheck> = new Map();
  private dataChecksSubject = new BehaviorSubject<DataCheck[]>([]);
  private highlightSubject = new Subject<DataCheckHighlight>();
  private cleanupSubject = new Subject<void>();
  private currentHighlightedDataCheckId: string | null = null;

  constructor() {}

  /**
   * Get all data checks as an observable
   */
  public getDataChecks(): Observable<DataCheck[]> {
    return this.dataChecksSubject.asObservable();
  }

  /**
   * Get all data checks as an array
   */
  public getAllDataChecks(): DataCheck[] {
    return Array.from(this.dataChecks.values());
  }

  /**
   * Get a specific data check by ID
   */
  public getDataCheck(id: string): DataCheck | undefined {
    return this.dataChecks.get(id);
  }

  /**
   * Add a new data check
   * Returns the created data check
   */
  public addDataCheck(checkId: string, description: string, tables: string[], operatorId: string): DataCheck {
    const id = this.generateId();
    const dataCheck: DataCheck = {
      id,
      checkId,
      description,
      tables,
      operatorId,
    };

    this.dataChecks.set(id, dataCheck);
    this.emitUpdate();

    console.log(`Added data check: ${checkId} (ID: ${id})`);
    return dataCheck;
  }

  /**
   * Update an existing data check
   * Returns the updated data check or undefined if not found
   */
  public updateDataCheck(id: string, updates: Partial<Omit<DataCheck, "id">>): DataCheck | undefined {
    const existing = this.dataChecks.get(id);
    if (!existing) {
      console.warn(`Data check not found: ${id}`);
      return undefined;
    }

    const updated: DataCheck = {
      ...existing,
      ...updates,
    };

    this.dataChecks.set(id, updated);
    this.emitUpdate();

    console.log(`Updated data check: ${id}`);
    return updated;
  }

  /**
   * Delete a data check by ID
   * Returns true if deleted, false if not found
   */
  public deleteDataCheck(id: string): boolean {
    const existed = this.dataChecks.delete(id);
    if (existed) {
      this.emitUpdate();
      console.log(`Deleted data check: ${id}`);
    } else {
      console.warn(`Data check not found: ${id}`);
    }
    return existed;
  }

  /**
   * Clear all data checks
   */
  public clearAll(): void {
    this.dataChecks.clear();
    this.emitUpdate();
    console.log("Cleared all data checks");
  }

  /**
   * Get data checks for a specific operator
   */
  public getDataChecksForOperator(operatorId: string): DataCheck[] {
    return Array.from(this.dataChecks.values()).filter(dc => dc.operatorId === operatorId);
  }

  /**
   * Generate a unique ID for a data check
   */
  private generateId(): string {
    return `dc-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Emit updated list to subscribers
   */
  private emitUpdate(): void {
    this.dataChecksSubject.next(this.getAllDataChecks());
  }

  /**
   * Get data check highlight stream
   */
  public getHighlightStream(): Observable<DataCheckHighlight> {
    return this.highlightSubject.asObservable();
  }

  /**
   * Get cleanup stream - emits when highlight should be removed
   */
  public getCleanupStream(): Observable<void> {
    return this.cleanupSubject.asObservable();
  }

  /**
   * Trigger highlight for a data check (toggle behavior)
   * If clicking the same data check, triggers cleanup instead
   */
  public toggleHighlight(dataCheckId: string, operatorIds: string[], linkIds: string[]): void {
    // If clicking the same data check, cleanup and return
    if (this.currentHighlightedDataCheckId === dataCheckId) {
      this.cleanupSubject.next();
      this.currentHighlightedDataCheckId = null;
      return;
    }

    // Otherwise, highlight the new data check
    this.currentHighlightedDataCheckId = dataCheckId;
    this.highlightSubject.next({ dataCheckId, operatorIds, linkIds });
  }

  /**
   * Clear the current highlight
   */
  public clearHighlight(): void {
    if (this.currentHighlightedDataCheckId !== null) {
      this.cleanupSubject.next();
      this.currentHighlightedDataCheckId = null;
    }
  }

  /**
   * Get the currently highlighted data check ID
   */
  public getCurrentHighlightedDataCheckId(): string | null {
    return this.currentHighlightedDataCheckId;
  }
}
