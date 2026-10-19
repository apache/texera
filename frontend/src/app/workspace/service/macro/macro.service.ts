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
import { BehaviorSubject, firstValueFrom, from, mergeMap, Subject, toArray } from "rxjs";
import { v4 as uuid } from "uuid";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import {
  adoptMacroVersion,
  expandMacros,
  extractMacro,
  MacroDefinition,
  macroInstance,
  macroSchema,
  macroType,
  mergeMacroDefinitions,
} from "../../../common/util/workflow-macro";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { LogicalPort } from "../../types/workflow-common.interface";
import Ajv from "ajv";

@Injectable({ providedIn: "root" })
export class MacroService {
  public readonly library$ = new BehaviorSubject<MacroDefinition[]>([]);
  public readonly inspect$ = new Subject<string>();
  private refreshing?: Promise<MacroDefinition[]>;

  constructor(
    private actions: WorkflowActionService,
    private persist: WorkflowPersistService,
    private versions: WorkflowVersionService,
    private metadata: OperatorMetadataService
  ) {}

  /** The list endpoint omits content; bound concurrent reads rather than firing one request per saved workflow at once. */
  public refresh(): Promise<MacroDefinition[]> {
    return (this.refreshing ??= this.loadLibrary().finally(() => {
      this.refreshing = undefined;
    }));
  }

  private async loadLibrary(): Promise<MacroDefinition[]> {
    const entries = await firstValueFrom(this.persist.retrieveWorkflowsBySessionUser());
    const failed: string[] = [];
    const loaded = await firstValueFrom(
      from(entries).pipe(
        mergeMap(async ({ workflow: entry }) => {
          try {
            const workflow = await firstValueFrom(this.persist.retrieveWorkflow(entry.wid!));
            return workflow.content.macroInterface ? await this.pin(workflow) : undefined;
          } catch (error) {
            failed.push(`${entry.name ?? entry.wid}: ${error instanceof Error ? error.message : String(error)}`);
            return undefined;
          }
        }, 6),
        toArray()
      )
    );
    const definitions = loaded.filter((d): d is MacroDefinition => d !== undefined);
    definitions.sort((a, b) => a.name.localeCompare(b.name));
    this.metadata.registerMacros(definitions.flatMap(d => [d, ...(d.content.macroDependencies ?? [])]));
    this.library$.next(definitions);
    if (failed.length)
      throw new Error(`Could not load library entries: ${failed.join(", ")}. Fix them or refresh to retry.`);
    return definitions;
  }

  private async pin(workflow: Workflow): Promise<MacroDefinition> {
    const versions = await firstValueFrom(this.versions.retrieveVersionsOfWorkflow(workflow.wid!));
    const version = Math.max(...versions.map(v => v.vId));
    if (!Number.isSafeInteger(version) || version <= 0) throw new Error("Macro has no saved version");
    // History applies inverse patches inclusively; skip this save's patch to pin its resulting content.
    const snapshot = await firstValueFrom(this.versions.retrieveWorkflowByVersion(workflow.wid!, version + 1));
    const content = WorkflowUtilService.parseWorkflowInfo(snapshot).content;
    if (!content.macroInterface) throw new Error("Saved version is not a macro definition");
    expandMacros(content);
    const definition = { id: content.macroInterface.id, version, name: workflow.name, wid: workflow.wid, content };
    mergeMacroDefinitions(this.actions.getMacroDependencies(), [definition], content.macroDependencies ?? []);
    macroSchema(definition, type => this.metadata.getOperatorSchema(type, content.macroDependencies ?? []));
    return definition;
  }

  public async copyDefinition(definition: MacroDefinition, dependencies: MacroDefinition[]): Promise<MacroDefinition> {
    return this.saveDefinition(
      {
        ...definition.content,
        macroDependencies: mergeMacroDefinitions(dependencies),
        macroInterface: { ...definition.content.macroInterface!, id: uuid() },
      },
      definition.name + " copy"
    );
  }

  private async saveDefinition(content: WorkflowContent, name: string): Promise<MacroDefinition> {
    expandMacros(content);
    const created = await firstValueFrom(this.persist.createWorkflow(content, name));
    const definition = await this.pin(WorkflowUtilService.parseWorkflowInfo(created.workflow));
    this.metadata.registerMacros([definition, ...(definition.content.macroDependencies ?? [])]);
    this.library$.next([...this.library$.value, definition]);
    return definition;
  }

  public async createFromSelection(name: string): Promise<MacroDefinition> {
    if (!this.actions.checkWorkflowModificationEnabled()) throw new Error("Workflow is read-only");
    if (!name.trim()) throw new Error("Enter a macro name");
    const content = this.actions.getWorkflowContent();
    const before = this.snapshot(content);
    const selected = [...this.actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()];
    const body = extractMacro(content, selected, uuid());
    const d = await this.saveDefinition(body, name.trim());
    if (!this.unchanged(before)) {
      throw new Error("The workflow changed while saving. The macro was saved; add it from the Macros panel.");
    }
    const instance = macroInstance(d, `WorkflowMacro-${uuid()}`);
    const ids = new Set(selected);
    const boundary = (p: LogicalPort, direction: "inputs" | "outputs"): LogicalPort => {
      if (!ids.has(p.operatorID)) return p;
      const port = body.macroInterface![direction].find(b =>
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
      if (content.macroInterface) {
        const remap = (direction: "inputs" | "outputs") =>
          content.macroInterface![direction].map(port => ({
            ...port,
            targets: port.targets.map(target => boundary(target, direction)),
          }));
        this.actions.setMacroInterface({
          ...content.macroInterface,
          inputs: remap("inputs"),
          outputs: remap("outputs"),
        });
      }
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

  public async adoptLatest(operatorID: string): Promise<void> {
    if (!this.actions.checkWorkflowModificationEnabled()) throw new Error("Workflow is read-only");
    const content = this.actions.getWorkflowContent();
    const before = this.snapshot(content);
    const op = content.operators.find(o => o.operatorID === operatorID);
    const previous = content.macroDependencies?.find(d => macroType(d) === op?.operatorType);
    if (!previous) throw new Error("Missing pinned macro definition");
    const matches = (await this.refresh()).filter(d => d.id === previous.id);
    if (matches.length > 1)
      throw new Error(
        "Multiple definitions share this identity. Insert the intended version from the library explicitly."
      );
    const latest = matches[0];
    if (!latest) throw new Error("Definition is not in this deployment's accessible macro library");
    const definitions = mergeMacroDefinitions(
      this.actions.getMacroDependencies(),
      [latest],
      latest.content.macroDependencies ?? []
    );
    const updated = adoptMacroVersion(op!, previous, latest);
    if (
      !new Ajv({ strict: false }).validate(
        macroSchema(latest, type => this.metadata.getOperatorSchema(type, latest.content.macroDependencies ?? []))
          .jsonSchema,
        updated.operatorProperties
      )
    ) {
      throw new Error("Macro properties are incompatible with the new version; insert it explicitly.");
    }
    if (!this.unchanged(before)) {
      throw new Error("The workflow changed while checking versions. Try again.");
    }
    if (latest.version === previous.version) return;
    const links = content.links.filter(l => l.source.operatorID === operatorID || l.target.operatorID === operatorID);
    this.actions.bundleMacroActions(() => {
      this.actions.setMacroDependencies(definitions);
      this.actions.deleteOperatorsAndLinks([operatorID]);
      this.actions.addOperatorsAndLinks([{ op: updated, pos: content.operatorPositions[operatorID] }], links);
    });
  }

  private snapshot(content = this.actions.getWorkflowContent()): string {
    return JSON.stringify([this.actions.getWorkflowMetadata().wid, content]);
  }

  private unchanged(snapshot: string): boolean {
    return this.actions.checkWorkflowModificationEnabled() && this.snapshot() === snapshot;
  }
}
