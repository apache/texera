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
import { v4 as uuid } from "uuid";
import { Point } from "../../types/workflow-common.interface";
import { WORKFLOW_SNIPPET_SCHEMA_VERSION, WorkflowSnippet } from "../../types/workflow-snippet.interface";
import { InsertedWorkflowFragment } from "../../types/workflow-fragment.interface";
import { WorkflowFragmentService } from "../workflow-fragment/workflow-fragment.service";

@Injectable({ providedIn: "root" })
export class WorkflowSnippetService {
  public static readonly STORAGE_KEY = "texera.reusable-sections.v1";

  private readonly snippetsSubject: BehaviorSubject<readonly WorkflowSnippet[]>;
  public readonly snippets$: Observable<readonly WorkflowSnippet[]>;

  constructor(private readonly fragmentService: WorkflowFragmentService) {
    this.snippetsSubject = new BehaviorSubject<readonly WorkflowSnippet[]>(this.load());
    this.snippets$ = this.snippetsSubject.asObservable();
  }

  public getSnippets(): readonly WorkflowSnippet[] {
    return this.snippetsSubject.value;
  }

  public createFromSectionBox(sectionBoxID: string, description: string): WorkflowSnippet {
    const graph = this.fragmentService.captureSectionBox(sectionBoxID);
    const normalizedName = graph.sectionBox!.name.trim();
    const timestamp = new Date().toISOString();
    const snippet: WorkflowSnippet = {
      id: uuid(),
      schemaVersion: WORKFLOW_SNIPPET_SCHEMA_VERSION,
      name: normalizedName,
      description: description.trim(),
      createdAt: timestamp,
      updatedAt: timestamp,
      fragment: graph,
    };
    this.replaceSnippets([...this.snippetsSubject.value, snippet]);
    return snippet;
  }

  public insert(id: string, anchor: Point): InsertedWorkflowFragment {
    const snippet = this.snippetsSubject.value.find(candidate => candidate.id === id);
    if (!snippet) {
      throw new Error("The selected reusable section no longer exists.");
    }
    return this.fragmentService.insert(snippet.fragment, anchor);
  }

  public delete(id: string): void {
    this.replaceSnippets(this.snippetsSubject.value.filter(snippet => snippet.id !== id));
  }

  private replaceSnippets(snippets: readonly WorkflowSnippet[]): void {
    localStorage.setItem(WorkflowSnippetService.STORAGE_KEY, JSON.stringify(snippets));
    this.snippetsSubject.next(snippets);
  }

  private load(): readonly WorkflowSnippet[] {
    const stored = localStorage.getItem(WorkflowSnippetService.STORAGE_KEY);
    if (!stored) {
      return [];
    }
    try {
      const snippets = JSON.parse(stored) as unknown;
      if (!Array.isArray(snippets)) {
        return [];
      }
      return snippets.filter(this.isSupportedSnippet);
    } catch {
      return [];
    }
  }

  private isSupportedSnippet(value: unknown): value is WorkflowSnippet {
    if (!value || typeof value !== "object") {
      return false;
    }
    const snippet = value as Partial<WorkflowSnippet>;
    const fragment = snippet.fragment;
    const sectionBox = fragment?.sectionBox;
    return (
      snippet.schemaVersion === WORKFLOW_SNIPPET_SCHEMA_VERSION &&
      typeof snippet.id === "string" &&
      typeof snippet.name === "string" &&
      snippet.name.trim().length > 0 &&
      typeof snippet.description === "string" &&
      !!fragment &&
      !!sectionBox &&
      sectionBox.name === snippet.name &&
      typeof sectionBox.color === "string" &&
      /^#[0-9a-f]{6}$/i.test(sectionBox.color) &&
      Number.isFinite(sectionBox.width) &&
      sectionBox.width > 0 &&
      Number.isFinite(sectionBox.height) &&
      sectionBox.height > 0 &&
      !!fragment.operatorPositions &&
      Array.isArray(fragment.operators) &&
      fragment.operators.length > 0 &&
      fragment.operators.every(
        operator =>
          typeof operator.operatorID === "string" &&
          typeof operator.operatorType === "string" &&
          Number.isFinite(fragment.operatorPositions[operator.operatorID]?.x) &&
          Number.isFinite(fragment.operatorPositions[operator.operatorID]?.y)
      ) &&
      Array.isArray(fragment.links)
    );
  }
}
