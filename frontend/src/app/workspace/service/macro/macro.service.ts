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
import { BehaviorSubject, firstValueFrom, Subject } from "rxjs";
import { v4 as uuid } from "uuid";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  MacroCatalog,
  WorkflowPersistService,
} from "../../../common/service/workflow-persist/workflow-persist.service";
import {
  MacroSource,
  WorkflowVersionService,
} from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowContent } from "../../../common/type/workflow";
import {
  extractMacro,
  readMacroInterface,
  isMacroTerminal,
  MacroDefinition,
  macroInstance,
  macroType,
  mergeMacroDefinitions,
  replaceMacroVersion,
} from "../../../common/util/workflow-macro";
import { LogicalPort } from "../../types/workflow-common.interface";

@Injectable({ providedIn: "root" })
export class MacroService {
  public readonly library$ = new BehaviorSubject<MacroDefinition[]>([]);
  public readonly usages$ = new BehaviorSubject<MacroCatalog["usages"]>([]);
  public readonly inspect$ = new Subject<string>();
  private refreshing?: Promise<MacroDefinition[]>;

  constructor(
    private actions: WorkflowActionService,
    private persist: WorkflowPersistService,
    private versions: WorkflowVersionService,
    private metadata: OperatorMetadataService
  ) {}

  public refresh(): Promise<MacroDefinition[]> {
    return (this.refreshing ??= this.loadLibrary().finally(() => {
      this.refreshing = undefined;
    }));
  }

  private async loadLibrary(): Promise<MacroDefinition[]> {
    const catalog = await firstValueFrom(this.persist.retrieveMacroCatalog());
    const errors = [...catalog.errors];
    const definitions = catalog.definitions.filter(definition => {
      try {
        this.checkPin(definition);
        return true;
      } catch (error) {
        errors.push(`${definition.name}: ${error instanceof Error ? error.message : String(error)}`);
        return false;
      }
    });
    this.metadata.registerMacros(definitions.flatMap(d => [d, ...(d.content.macroDependencies ?? [])]));
    this.library$.next(definitions);
    this.usages$.next(catalog.usages);
    if (errors.length)
      throw new Error(`Could not load library entries: ${errors.join(", ")}. Fix them or refresh to retry.`);
    return definitions;
  }

  public checkPin(definition: MacroDefinition): MacroDefinition {
    mergeMacroDefinitions(this.actions.getMacroDependencies(), [definition]);
    return definition;
  }

  public async pin(source?: Pick<MacroDefinition, "id" | "wid">, version?: number): Promise<MacroDefinition> {
    if (!source?.wid) throw new Error("Macro source unavailable; refresh the library before inserting it");
    return this.checkPin(await firstValueFrom(this.versions.pinMacro(source.wid, source.id, version)));
  }

  public async copyDefinition(
    definition: Pick<MacroDefinition, "name" | "content">,
    dependencies: MacroDefinition[]
  ): Promise<MacroDefinition> {
    return this.saveDefinition(
      {
        ...definition.content,
        macroDependencies: mergeMacroDefinitions(dependencies),
        macroInterface: { ...definition.content.macroInterface!, id: uuid() },
      },
      definition.name + " copy"
    );
  }

  public async sourceFor(definition: MacroDefinition, head = false): Promise<MacroSource> {
    if (!definition.wid) throw new Error("Macro source unavailable");
    return firstValueFrom(this.versions.macroSource(definition.wid, definition.id, head));
  }

  public async latestFor(type: string): Promise<MacroDefinition> {
    const latest = await this.pin(this.metadata.getMacroDefinition(type));
    this.metadata.registerMacros([latest, ...(latest.content.macroDependencies ?? [])]);
    return latest;
  }

  private async saveDefinition(content: WorkflowContent, name: string): Promise<MacroDefinition> {
    const created = await firstValueFrom(this.persist.createWorkflow(content, name));
    const definition = await this.pin({ id: content.macroInterface!.id, wid: created.workflow.wid });
    this.metadata.registerMacros([definition, ...(definition.content.macroDependencies ?? [])]);
    this.library$.next([...this.library$.value, definition]);
    return definition;
  }

  public async createFromSelection(name: string): Promise<MacroDefinition> {
    if (!this.actions.checkWorkflowModificationEnabled()) throw new Error("Workflow is read-only");
    if (!name.trim()) throw new Error("Enter a macro name");
    const content = this.actions.getWorkflowContent();
    const before = this.snapshot(content);
    const selected = this.actions
      .getJointGraphWrapper()
      .getCurrentHighlightedOperatorIDs()
      .filter(id => !isMacroTerminal(this.actions.getTexeraGraph().getOperator(id)));
    const body = extractMacro(content, selected, uuid());
    const d = await this.saveDefinition(body, name.trim());
    if (!this.unchanged(before)) {
      throw new Error("The workflow changed while saving. The macro was saved; add it from the Macros panel.");
    }
    const instance = macroInstance(d, `WorkflowMacro-${uuid()}`);
    const ids = new Set(selected);
    const exposed = readMacroInterface(body)!;
    const boundary = (p: LogicalPort, direction: "inputs" | "outputs"): LogicalPort => {
      if (!ids.has(p.operatorID)) return p;
      const port = exposed[direction].find(b =>
        b.targets.some(t => t.operatorID === p.operatorID && t.portID === p.portID)
      );
      if (!port) throw new Error("Missing boundary mapping");
      return { operatorID: instance.operatorID, portID: port.portID };
    };
    const links = content.links
      .filter(l => ids.has(l.source.operatorID) !== ids.has(l.target.operatorID))
      .map(l => ({ ...l, source: boundary(l.source, "outputs"), target: boundary(l.target, "inputs") }));
    this.actions.bundleMacroActions(() => {
      this.actions.setMacroDependencies([...this.actions.getMacroDependencies(), d]);
      this.actions.deleteOperatorsAndLinks(selected);
      this.actions.addOperatorsAndLinks([{ op: instance, pos: content.operatorPositions[selected[0]] }], links);
      if (content.formBinding)
        this.actions.setFormBinding({
          ...content.formBinding,
          fields: content.formBinding.fields.map(f =>
            ids.has(f.operatorID) ? { ...f, operatorID: instance.operatorID, propertyKey: f.id } : f
          ),
          ...(content.formBinding.shownResultIds === undefined
            ? {}
            : {
                shownResultIds: [
                  ...new Set(content.formBinding.shownResultIds.map(id => (ids.has(id) ? instance.operatorID : id))),
                ],
              }),
        });
    });
    return d;
  }

  public adoptVersion(operatorID: string, latest: MacroDefinition): void {
    if (!this.actions.checkWorkflowModificationEnabled()) throw new Error("Workflow is read-only");
    const content = this.actions.getWorkflowContent();
    const result = replaceMacroVersion(content, operatorID, latest);
    if (this.actions.getTexeraGraph().getOperator(operatorID).operatorType !== macroType(latest)) {
      this.actions.updateMacroUsage(operatorID, result);
    }
  }

  private snapshot(content = this.actions.getWorkflowContent()): string {
    return JSON.stringify([this.actions.getWorkflowMetadata().wid, content]);
  }

  private unchanged(snapshot: string): boolean {
    return this.actions.checkWorkflowModificationEnabled() && this.snapshot() === snapshot;
  }
}
