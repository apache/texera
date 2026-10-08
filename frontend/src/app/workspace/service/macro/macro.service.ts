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
import { BehaviorSubject, firstValueFrom } from "rxjs";
import { v4 as uuid } from "uuid";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { Workflow } from "../../../common/type/workflow";
import {
  adoptMacroVersion,
  expandMacros,
  extractMacro,
  MacroDefinition,
  macroInstance,
  macroSchema,
  macroType,
} from "../../../common/util/workflow-macro";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { LogicalPort } from "../../types/workflow-common.interface";
import Ajv from "ajv";

@Injectable({ providedIn: "root" })
export class MacroService {
  public readonly library$ = new BehaviorSubject<MacroDefinition[]>([]);
  public readonly inspect$ = new BehaviorSubject<string | undefined>(undefined);
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
    const definitions: MacroDefinition[] = [];
    for (let i = 0; i < entries.length; i += 6) {
      const batch = await Promise.all(
        entries.slice(i, i + 6).map(async entry => {
          const workflow = await firstValueFrom(this.persist.retrieveWorkflow(entry.workflow.wid!));
          return workflow.content.macroInterface ? this.pin(workflow) : undefined;
        })
      );
      definitions.push(...batch.filter((d): d is MacroDefinition => d !== undefined));
    }
    definitions.sort((a, b) => a.name.localeCompare(b.name));
    this.metadata.registerMacros(definitions.flatMap(d => [d, ...(d.content.macroDependencies ?? [])]));
    this.library$.next(definitions);
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
    this.metadata.registerMacros(content.macroDependencies ?? []);
    macroSchema(definition, type => this.metadata.getOperatorSchema(type));
    return definition;
  }

  public async copyDefinition(definition: MacroDefinition, dependencies: MacroDefinition[]): Promise<MacroDefinition> {
    const content = {
      ...definition.content,
      macroDependencies: dependencies,
      macroInterface: { ...definition.content.macroInterface!, id: uuid() },
    };
    expandMacros(content);
    const created = await firstValueFrom(this.persist.createWorkflow(content, definition.name + " copy"));
    const copy = await this.pin(WorkflowUtilService.parseWorkflowInfo(created.workflow));
    this.metadata.registerMacros([copy, ...dependencies]);
    this.library$.next([...this.library$.value, copy]);
    return copy;
  }

  public async createFromSelection(name: string): Promise<MacroDefinition> {
    if (!this.actions.checkWorkflowModificationEnabled()) throw new Error("Workflow is read-only");
    if (!name.trim()) throw new Error("Enter a macro name");
    const content = this.actions.getWorkflowContent();
    const before = JSON.stringify(content);
    const selected = [...this.actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()];
    const body = extractMacro(content, selected, uuid());
    expandMacros(body);
    const created = await firstValueFrom(this.persist.createWorkflow(body, name.trim()));
    const d = await this.pin(WorkflowUtilService.parseWorkflowInfo(created.workflow));
    this.metadata.registerMacros([d, ...(d.content.macroDependencies ?? [])]);
    this.library$.next([...this.library$.value, d]);
    if (
      !this.actions.checkWorkflowModificationEnabled() ||
      JSON.stringify(this.actions.getWorkflowContent()) !== before
    ) {
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
    const updated = adoptMacroVersion(op!, previous, latest);
    if (
      !new Ajv({ strict: false }).validate(
        macroSchema(latest, type => this.metadata.getOperatorSchema(type)).jsonSchema,
        updated.operatorProperties
      )
    ) {
      throw new Error("Macro properties are incompatible with the new version; insert it explicitly.");
    }
    if (
      !this.actions.checkWorkflowModificationEnabled() ||
      JSON.stringify(this.actions.getWorkflowContent()) !== JSON.stringify(content)
    ) {
      throw new Error("The workflow changed while checking versions. Try again.");
    }
    if (latest.version === previous.version) return;
    const links = content.links.filter(l => l.source.operatorID === operatorID || l.target.operatorID === operatorID);
    this.actions.bundleMacroActions(() => {
      const definitions = [...this.actions.getMacroDependencies(), latest, ...(latest.content.macroDependencies ?? [])];
      this.actions.setMacroDependencies([...new Map(definitions.map(d => [macroType(d), d])).values()]);
      this.actions.deleteOperatorsAndLinks([operatorID]);
      this.actions.addOperatorsAndLinks([{ op: updated, pos: content.operatorPositions[operatorID] }], links);
    });
  }
}
