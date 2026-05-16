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

import { Component, inject, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalRef, NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { NzCheckboxModule } from "ng-zorro-antd/checkbox";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { OperatorMetadataService } from "../../../../../workspace/service/operator-metadata/operator-metadata.service";
import { OperatorSchema } from "../../../../../workspace/types/operator-schema.interface";
import { WorkflowSnippetService } from "../../../../service/user/workflow-snippet/workflow-snippet.service";
import {
  DEFAULT_SNIPPET_CATEGORY,
  SNIPPET_ICON_CHOICES,
  SnippetLink,
  SnippetOperator,
  WorkflowSnippet,
} from "../../../../type/workflow-snippet.interface";

export interface SnippetBuilderData {
  author: string;
  // When provided, the dialog opens in edit mode: prefilled from this snippet
  // and saving will update it instead of creating a new one.
  editing?: WorkflowSnippet;
}

interface CategorySection {
  groupName: string;
  operators: OperatorSchema[];
}

interface PickedOperator {
  uid: string;
  schema: OperatorSchema;
  customDisplayName: string;
}

@UntilDestroy()
@Component({
  selector: "texera-snippet-builder-dialog",
  templateUrl: "./snippet-builder-dialog.component.html",
  styleUrls: ["./snippet-builder-dialog.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzInputModule,
    NzButtonModule,
    NzIconModule,
    NzSelectModule,
    NzCollapseModule,
    NzCheckboxModule,
    NzTagModule,
    NzEmptyModule,
  ],
})
export class SnippetBuilderDialogComponent implements OnInit {
  protected readonly modal = inject(NzModalRef<SnippetBuilderDialogComponent, void>);
  protected readonly data = inject<SnippetBuilderData>(NZ_MODAL_DATA);

  public readonly iconChoices = SNIPPET_ICON_CHOICES;

  public name = "";
  public description = "";
  public icon = "📦";
  public category = DEFAULT_SNIPPET_CATEGORY;
  public isPublic = false;

  public categories: CategorySection[] = [];
  public picked: PickedOperator[] = [];
  public filterText = "";

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowSnippetService: WorkflowSnippetService
  ) {}

  ngOnInit(): void {
    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(metadata => {
        const knownTypes = new Map<string, OperatorSchema>();
        const byGroup = new Map<string, OperatorSchema[]>();
        for (const op of metadata.operators) {
          knownTypes.set(op.operatorType, op);
          if (op.operatorType === "PythonUDF" || op.operatorType === "Dummy" || op.operatorType === "Sleep") {
            continue;
          }
          const group = op.additionalMetadata.operatorGroupName || "Other";
          const list = byGroup.get(group) ?? [];
          list.push(op);
          byGroup.set(group, list);
        }
        for (const list of byGroup.values()) {
          list.sort((a, b) =>
            a.additionalMetadata.userFriendlyName.localeCompare(b.additionalMetadata.userFriendlyName)
          );
        }
        this.categories = Array.from(byGroup.entries())
          .sort((a, b) => a[0].localeCompare(b[0]))
          .map(([groupName, operators]) => ({ groupName, operators }));

        // Pre-fill from the snippet being edited once we know the operator
        // catalog (so we can look up schemas by operatorType).
        if (this.data.editing && this.picked.length === 0) {
          this.prefillFromSnippet(this.data.editing, knownTypes);
        }
      });
  }

  private prefillFromSnippet(snippet: WorkflowSnippet, knownTypes: Map<string, OperatorSchema>): void {
    this.name = snippet.name;
    this.description = snippet.description ?? "";
    this.icon = snippet.icon || "📦";
    this.category = snippet.category || DEFAULT_SNIPPET_CATEGORY;
    this.isPublic = !!snippet.isPublic;
    const missingTypes: string[] = [];
    this.picked = snippet.operators
      .map(op => {
        const schema = knownTypes.get(op.operatorType);
        if (!schema) {
          missingTypes.push(op.operatorType);
          return null;
        }
        return {
          uid: op.operatorId,
          schema,
          customDisplayName: op.customDisplayName ?? schema.additionalMetadata.userFriendlyName,
        } satisfies PickedOperator;
      })
      .filter((p): p is PickedOperator => p !== null);
    if (missingTypes.length > 0) {
      // Surface missing types so the user knows we couldn't reconstruct them.
      this.description =
        (this.description ? this.description + "\n\n" : "") +
        `Note: ${[...new Set(missingTypes)].join(", ")} not found in this deployment's operator catalog.`;
    }
  }

  public get isEditing(): boolean {
    return !!this.data.editing;
  }

  public filteredOperators(operators: OperatorSchema[]): OperatorSchema[] {
    const needle = this.filterText.trim().toLowerCase();
    if (!needle) return operators;
    return operators.filter(
      op =>
        op.operatorType.toLowerCase().includes(needle) ||
        op.additionalMetadata.userFriendlyName.toLowerCase().includes(needle)
    );
  }

  public hasVisibleOperators(operators: OperatorSchema[]): boolean {
    return this.filteredOperators(operators).length > 0;
  }

  public add(schema: OperatorSchema): void {
    this.picked.push({
      uid: `picked-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      schema,
      customDisplayName: schema.additionalMetadata.userFriendlyName,
    });
  }

  public moveUp(index: number): void {
    if (index <= 0) return;
    const next = this.picked.slice();
    [next[index - 1], next[index]] = [next[index], next[index - 1]];
    this.picked = next;
  }

  public moveDown(index: number): void {
    if (index >= this.picked.length - 1) return;
    const next = this.picked.slice();
    [next[index + 1], next[index]] = [next[index], next[index + 1]];
    this.picked = next;
  }

  public remove(index: number): void {
    this.picked = this.picked.filter((_, i) => i !== index);
  }

  public get autoLinksDescription(): string {
    if (this.picked.length < 2) return "Pick at least 2 operators to create connections.";
    const linkable = this.computeAutoLinks().length;
    const possible = this.picked.length - 1;
    if (linkable === possible) return `${linkable} auto-link(s) — full chain.`;
    return `${linkable}/${possible} auto-link(s) — some adjacent operators have no compatible ports.`;
  }

  public canSave(): boolean {
    return !!this.name.trim() && this.picked.length >= 1;
  }

  public save(): void {
    if (!this.canSave()) return;
    // Preserve existing snippet operator properties when editing so users
    // don't lose configured fields on a structural edit. New picks get empty
    // properties (schema defaults are filled at placement time).
    const existingPropsByType = new Map<string, { [k: string]: any }>();
    if (this.data.editing) {
      for (const op of this.data.editing.operators) {
        if (op.operatorProperties && Object.keys(op.operatorProperties).length > 0) {
          existingPropsByType.set(op.operatorId, op.operatorProperties);
        }
      }
    }
    const operators: SnippetOperator[] = this.picked.map((p, idx) => ({
      operatorId: p.uid,
      operatorType: p.schema.operatorType,
      operatorVersion: p.schema.operatorVersion,
      operatorProperties: { ...(existingPropsByType.get(p.uid) ?? {}) },
      customDisplayName: p.customDisplayName.trim() || p.schema.additionalMetadata.userFriendlyName,
      position: { x: idx * 220, y: 0 },
    }));
    const links: SnippetLink[] = this.computeAutoLinks();

    if (this.data.editing) {
      this.workflowSnippetService.update(this.data.editing.id, {
        name: this.name.trim(),
        description: this.description.trim(),
        icon: this.icon,
        category: this.category.trim() || DEFAULT_SNIPPET_CATEGORY,
        operators,
        links,
        isPublic: this.isPublic,
      });
    } else {
      this.workflowSnippetService.create({
        name: this.name.trim(),
        description: this.description.trim(),
        icon: this.icon,
        category: this.category.trim() || DEFAULT_SNIPPET_CATEGORY,
        operators,
        links,
        author: this.data.author,
        isPublic: this.isPublic,
      });
    }
    this.modal.close();
  }

  public cancel(): void {
    this.modal.close();
  }

  // Linear chain: connect each adjacent pair via output-0 → input-0 when both ports exist.
  private computeAutoLinks(): SnippetLink[] {
    const links: SnippetLink[] = [];
    for (let i = 0; i < this.picked.length - 1; i++) {
      const source = this.picked[i];
      const target = this.picked[i + 1];
      const sourceHasOutput = (source.schema.additionalMetadata.outputPorts?.length ?? 0) > 0;
      const targetHasInput = (target.schema.additionalMetadata.inputPorts?.length ?? 0) > 0;
      if (!sourceHasOutput || !targetHasInput) continue;
      links.push({
        fromOperatorId: source.uid,
        fromPortId: "output-0",
        toOperatorId: target.uid,
        toPortId: "input-0",
      });
    }
    return links;
  }
}
