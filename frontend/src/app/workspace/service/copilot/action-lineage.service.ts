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
import { AgentResponse } from "./texera-copilot";

/**
 * Represents an affected entity (operator or link) in the workflow
 */
export interface AffectedEntity {
  type: "operator" | "link";
  id: string;
}

/**
 * Service for managing the lineage between agent responses and workflow entities
 * Tracks which operators/links were affected by each agent response
 */
@Injectable({
  providedIn: "root",
})
export class ActionLineageService {
  // Map from AgentResponse id to list of affected operator/link ids
  private responseToEntities: Map<string, AffectedEntity[]> = new Map();

  // Map from operator/link id to list of AgentResponse ids
  private entityToResponses: Map<string, string[]> = new Map();

  // Map from AgentResponse id to the full AgentResponse object for retrieval
  private responseMap: Map<string, AgentResponse> = new Map();

  constructor() {}

  /**
   * Record that a response affected certain operators/links
   * @param response The agent response
   * @param affectedEntities List of operators/links that were affected
   */
  recordResponseAction(response: AgentResponse, affectedEntities: AffectedEntity[]): void {
    // Store the response object
    this.responseMap.set(response.id, response);

    // Store response -> entities mapping
    this.responseToEntities.set(response.id, affectedEntities);

    // Update entity -> responses mapping
    for (const entity of affectedEntities) {
      const entityKey = `${entity.type}:${entity.id}`;
      const existingResponses = this.entityToResponses.get(entityKey) || [];
      if (!existingResponses.includes(response.id)) {
        existingResponses.push(response.id);
        this.entityToResponses.set(entityKey, existingResponses);
      }
    }
  }

  /**
   * Get all entities affected by a specific response
   * @param responseId The agent response id
   * @returns List of affected entities or empty array if not found
   */
  getAffectedEntitiesByResponse(responseId: string): AffectedEntity[] {
    return this.responseToEntities.get(responseId) || [];
  }

  /**
   * Get all responses that affected a specific operator
   * @param operatorId The operator id
   * @returns List of agent response ids sorted by timestamp (oldest first)
   */
  getResponsesByOperator(operatorId: string): AgentResponse[] {
    const entityKey = `operator:${operatorId}`;
    const responseIds = this.entityToResponses.get(entityKey) || [];
    return this.getResponsesById(responseIds);
  }

  /**
   * Get all responses that affected a specific link
   * @param linkId The link id
   * @returns List of agent response ids sorted by timestamp (oldest first)
   */
  getResponsesByLink(linkId: string): AgentResponse[] {
    const entityKey = `link:${linkId}`;
    const responseIds = this.entityToResponses.get(entityKey) || [];
    return this.getResponsesById(responseIds);
  }

  /**
   * Get all responses that affected a specific entity (operator or link)
   * @param entityType Type of entity ("operator" or "link")
   * @param entityId The entity id
   * @returns List of agent responses sorted by timestamp (oldest first)
   */
  getResponsesByEntity(entityType: "operator" | "link", entityId: string): AgentResponse[] {
    const entityKey = `${entityType}:${entityId}`;
    const responseIds = this.entityToResponses.get(entityKey) || [];
    return this.getResponsesById(responseIds);
  }

  /**
   * Get response objects by their ids, sorted by timestamp
   * @param responseIds List of response ids
   * @returns List of agent responses sorted by timestamp (oldest first)
   */
  private getResponsesById(responseIds: string[]): AgentResponse[] {
    const responses: AgentResponse[] = [];
    for (const id of responseIds) {
      const response = this.responseMap.get(id);
      if (response) {
        responses.push(response);
      }
    }
    // Sort by timestamp (oldest first)
    return responses.sort((a, b) => a.timestamp - b.timestamp);
  }

  /**
   * Get a specific response by id
   * @param responseId The response id
   * @returns The agent response or undefined if not found
   */
  getResponseById(responseId: string): AgentResponse | undefined {
    return this.responseMap.get(responseId);
  }

  /**
   * Remove a response from the lineage tracking
   * @param responseId The response id to remove
   */
  removeResponse(responseId: string): void {
    // Get affected entities before removing
    const affectedEntities = this.responseToEntities.get(responseId) || [];

    // Remove from response -> entities mapping
    this.responseToEntities.delete(responseId);

    // Remove from response map
    this.responseMap.delete(responseId);

    // Remove from entity -> responses mapping
    for (const entity of affectedEntities) {
      const entityKey = `${entity.type}:${entity.id}`;
      const responses = this.entityToResponses.get(entityKey);
      if (responses) {
        const filtered = responses.filter(id => id !== responseId);
        if (filtered.length > 0) {
          this.entityToResponses.set(entityKey, filtered);
        } else {
          this.entityToResponses.delete(entityKey);
        }
      }
    }
  }

  /**
   * Remove all responses associated with a specific entity
   * @param entityType Type of entity ("operator" or "link")
   * @param entityId The entity id
   */
  removeEntity(entityType: "operator" | "link", entityId: string): void {
    const entityKey = `${entityType}:${entityId}`;
    const responseIds = this.entityToResponses.get(entityKey) || [];

    // Remove the entity from all its associated responses
    for (const responseId of responseIds) {
      const affectedEntities = this.responseToEntities.get(responseId);
      if (affectedEntities) {
        const filtered = affectedEntities.filter(e => !(e.type === entityType && e.id === entityId));
        this.responseToEntities.set(responseId, filtered);
      }
    }

    // Remove the entity key
    this.entityToResponses.delete(entityKey);
  }

  /**
   * Clear all lineage data
   */
  clearAll(): void {
    this.responseToEntities.clear();
    this.entityToResponses.clear();
    this.responseMap.clear();
  }

  /**
   * Get all responses (sorted by timestamp)
   * @returns List of all agent responses sorted by timestamp (oldest first)
   */
  getAllResponses(): AgentResponse[] {
    const responses = Array.from(this.responseMap.values());
    return responses.sort((a, b) => a.timestamp - b.timestamp);
  }
}
