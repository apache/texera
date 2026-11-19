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
import { firstValueFrom, Subject, merge } from "rxjs";
import { auditTime } from "rxjs/operators";
import { isEqual } from "lodash-es";
import { ResultTarget, toLogicalGraph } from "@texera/workflow-macros";
import { WorkflowMetadata } from "../../../dashboard/type/workflow-metadata.interface";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import {
  MacroSaveStep,
  WorkflowPersistService,
} from "../../../common/service/workflow-persist/workflow-persist.service";
import {
  MacroDefinition,
  MacroExpansion,
  isMacro,
  readMacroInterface,
  retargetMacro,
  projectMacroExpansion,
  compiledInputLinks,
  macroType,
  replaceMacroVersion,
  macroRevisionLabel,
  mergeMacroDefinitions,
} from "../../../common/util/workflow-macro";
import {
  WorkflowActionService,
  WorkflowEditorSession,
  DEFAULT_WORKFLOW,
} from "../workflow-graph/model/workflow-action.service";
import { MacroService } from "./macro.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { OperatorRuntimeStatus } from "../../types/execute-workflow.interface";
import { OperatorPredicate } from "../../types/workflow-common.interface";

import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";

import { MacroExpansionService } from "./macro-expansion.service";

function runtimePlan(graph: WorkflowContent) {
  return {
    ...toLogicalGraph(graph),
    disabled: graph.operators.filter(op => op.isDisabled).map(op => op.operatorID),
  };
}

function macroHead({ name, description }: WorkflowMetadata, version: number) {
  return { name, description, version };
}

export interface MacroDraftRequest {
  content: WorkflowContent;
  drafts: { path: string[]; content: WorkflowContent }[];
}

interface MacroNavigationEntry {
  session: WorkflowEditorSession;
  operatorID: string;
  instanceName: string;
  head?: MacroSaveStep["head"];
  definition: MacroDefinition;
  source?: Pick<Workflow, "wid" | "readonly">;
  saved: Workflow;
}

/** A breadcrumb is an editor document, not another canvas or another kind of version history. */
@Injectable({ providedIn: "root" })
export class MacroNavigationService {
  public readonly parents: MacroNavigationEntry[] = [];
  public busy = false;
  public dirty = false;
  public readonly changed$ = new Subject<void>();
  public get changing$(): Subject<void> {
    return this.actions.editorChanging$;
  }
  private generation = 0;
  private rootBefore?: Workflow;
  private readonly compiledOverrides = new WeakMap<object, unknown[]>();
  private attemptedRuntimeEpoch = -1;
  private saving: Promise<unknown> = Promise.resolve();

  constructor(
    public actions: WorkflowActionService,
    private macros: MacroService,
    private persist: WorkflowPersistService,
    private status: WorkflowStatusService,
    private executions: WorkflowExecutionsService,
    private expansion: MacroExpansionService
  ) {
    actions.executionWorkflow = () => this.rootWorkflow;
    actions.runtimeOperatorIDs = () => (this.parents.length ? this.runtimeContext()?.ids ?? {} : undefined);
    actions.runtimeBoundaryLinks = () => this.boundaryLinks();
    status.setProjection(
      raw => this.projectStatistics(raw),
      () => this.rootWorkflow.content
    );
    status.getStatisticsUpdateStream().subscribe(() => this.loadRuntimeSnapshot());
    expansion.changed$.subscribe(() => status.refresh());
    merge(actions.workflowChanged(), actions.workflowMetaDataChanged())
      .pipe(auditTime(0))
      .subscribe(() => {
        if (!actions.getJointGraphWrapper().getReloadingWorkflow()) {
          this.dirty = this.hasUnsavedChanges;
          if (this.parents.length || this.rootWorkflow.content.operators.some(isMacro)) status.refresh();
        }
      });
  }

  private get path(): string[] {
    return this.parents.map(parent => parent.operatorID);
  }
  public get rootWorkflow(): Workflow {
    return this.parents.length ? this.actions.getSessionWorkflow(this.parents[0].session) : this.actions.getWorkflow();
  }
  public get hasUnsavedChanges(): boolean {
    return !!this.current && this.draftChanged(this.actions.getWorkflow(), this.current.saved);
  }
  private draftChanged(current: Workflow, saved: Workflow): boolean {
    return (
      !current.readonly &&
      (!isEqual(current.content, saved.content) ||
        current.name !== saved.name ||
        current.description !== saved.description)
    );
  }
  public get current(): MacroNavigationEntry | undefined {
    return this.parents.at(-1);
  }
  public breadcrumb(index: number): string {
    const entered = this.parents[index - 1];
    return entered
      ? `${entered.instanceName} · ${macroRevisionLabel(entered.definition)}`
      : this.parents[0]?.session.metadata.name ?? "";
  }
  public get readOnlyReason(): string {
    const entry = this.current;
    if (!entry?.saved.readonly) return "";
    if (!entry.source) return "Source unavailable. This saved snapshot still runs and exports with its workflow.";
    if (entry.source.readonly) return "You have read-only access to this macro definition.";
    if (!entry.session.modifiable) return "The parent workflow is read-only or currently locked.";
    return "The latest source revision could not be verified. This saved snapshot remains read-only.";
  }
  public get hint(): string {
    if (!this.parents.length) return "";
    if (this.readOnlyReason) return this.readOnlyReason;
    if (!this.actions.checkWorkflowModificationEnabled()) return "Editing is currently locked.";
    return `Editing ${macroRevisionLabel(this.current!.definition)}. Saving creates new revisions along this parent path; other usages keep their pins.`;
  }

  public async open(operatorID: string): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    const generation = this.generation;
    try {
      this.changing$.next();
      if (this.parents.length) await this.save();
      else if (this.actions.checkWorkflowModificationEnabled()) {
        const current = this.actions.getWorkflow();
        const stored = current.wid ? await firstValueFrom(this.persist.retrieveWorkflow(current.wid)) : undefined;
        this.rootBefore = stored;
        if (!stored) {
          this.rootBefore = await firstValueFrom(this.persist.persistWorkflow(current));
          this.actions.setWorkflowMetadata(this.rootBefore);
        }
      }
      const workflow = this.actions.getWorkflow();
      const shared = this.actions.editorDocument;
      const op = workflow.content.operators.find(o => o.operatorID === operatorID);
      const definition = workflow.content.macroDependencies?.find(d => macroType(d) === op?.operatorType);
      if (!definition) throw new Error("Missing pinned macro definition");
      if (this.parents.length >= 32) throw new Error("Macro nesting exceeds 32 levels");
      let source: WorkflowMetadata | undefined, latest: MacroDefinition | undefined;
      try {
        const details = await this.macros.sourceFor(definition, true);
        source = details.metadata;
        latest = details.head && this.macros.checkPin(details.head);
      } catch {
        // The pinned snapshot can still open when its source or revision is unavailable.
      }
      const session = this.actions.suspendWorkflow();
      if (
        generation !== this.generation ||
        session.shared !== shared ||
        !isEqual(this.actions.getWorkflowContent(), workflow.content)
      )
        throw new Error("The workflow changed while opening the macro. Open it again.");
      const atLatest = latest?.version === definition.version;
      const editable = session.modifiable && !source?.readonly && latest?.id === definition.id;
      const { macroDependencies: dependencies, ...content } =
        editable && atLatest
          ? latest!.content
          : { ...definition.content, macroDependencies: this.actions.getMacroDependencies() };
      const draft: Workflow = {
        ...DEFAULT_WORKFLOW,
        ...source,
        wid: source?.wid,
        name: editable ? source!.name : definition.name,
        readonly: !editable,
        content: {
          ...content,
          ...(dependencies?.length ? { macroDependencies: mergeMacroDefinitions(dependencies) } : {}),
        },
      };
      this.parents.push({
        session,
        operatorID,
        instanceName: op!.customDisplayName?.trim() || definition.name,
        head: source && latest ? macroHead(source, latest.version) : undefined,
        definition: atLatest ? { ...definition, revision: latest!.revision } : definition,
        source: source && { wid: source.wid, readonly: source.readonly },
        saved: draft,
      });
      this.dirty = false;
      try {
        this.actions.openWorkflowDraft(draft, editable && atLatest);
      } catch (error) {
        this.parents.pop();
        this.actions.resumeWorkflow(session);
        throw error;
      }
      this.actions.disableWorkflowModification();
      if (editable) this.actions.enableWorkflowModification();
      this.status.refresh();
      this.actions.getTexeraGraph().triggerCenterEvent();
      this.changed$.next();
    } finally {
      this.busy = false;
    }
  }

  /** The existing autosave/manual-save callers share one serialized chain, including parent propagation. */
  public save(): Promise<Workflow> {
    const save = this.saving.catch(() => this.actions.getWorkflow()).then(() => this.saveCurrent());
    this.saving = save;
    return save;
  }

  private async saveCurrent(): Promise<Workflow> {
    const workflow = this.actions.getWorkflow();
    if (!this.actions.checkWorkflowModificationEnabled() || workflow.readonly) return workflow;
    if (!this.parents.length) return firstValueFrom(this.persist.persistWorkflow(workflow));
    const entry = this.parents.at(-1)!;
    if (!this.draftChanged(workflow, entry.saved)) return workflow;
    const undo = this.actions.captureMacroUndo();
    const generation = this.generation;
    const path = [...this.parents].reverse();
    const steps: MacroSaveStep[] = [{ workflow, head: entry.head }];
    const originals: { content: WorkflowContent; type: string }[] = [];
    for (const [i, parent] of path.entries()) {
      const document = this.actions.getSessionWorkflow(parent.session);
      const type = document.content.operators.find(op => op.operatorID === parent.operatorID)?.operatorType;
      const pin = document.content.macroDependencies?.find(d => macroType(d) === type);
      if (!pin || pin.id !== parent.definition.id)
        throw new Error("The parent macro usage changed. Your draft is still open.");
      const content = retargetMacro(document.content, parent.operatorID, {
        ...pin,
        content: steps[i].workflow.content,
      });
      steps.push({ workflow: { ...document, content }, operatorID: parent.operatorID, head: path[i + 1]?.head });
      originals.push({ content: document.content, type: macroType(pin) });
    }
    const receipts = await firstValueFrom(this.persist.persistMacroPath(steps, this.rootBefore!));
    if (generation !== this.generation) return workflow;
    const committed = steps.map(({ workflow }, i) => ({ ...workflow, content: receipts[i].content }));
    const heads = new Map(committed.map((w, i) => [w.wid, macroHead(w, receipts[i].version)]));
    for (const [i, parent] of path.entries()) {
      const { operatorID, session } = parent;
      const original = originals[i];
      const saved = committed[i];
      parent.definition = { ...parent.definition, ...receipts[i], name: saved.name };
      parent.saved = saved;
      parent.head = heads.get(saved.wid);
      const live = this.actions.getWorkflowContent(session.shared);
      if (live.operators.find(op => op.operatorID === operatorID)?.operatorType === original.type) {
        const content = isEqual(live, original.content)
          ? committed[i + 1].content
          : replaceMacroVersion(live, operatorID, parent.definition);
        this.actions.updateMacroUsage(operatorID, content, session, undo);
      }
    }
    this.rootBefore = committed.at(-1)!;
    this.status.refresh();
    return workflow;
  }

  public compilationDraft(): MacroDraftRequest | undefined {
    if (!this.parents.length) return;
    const drafts: MacroDraftRequest["drafts"] = [];
    let content = this.actions.getWorkflowContent();
    const path = this.path;
    for (let i = this.parents.length - 1; i >= 0; i--) {
      const parent = this.parents[i];
      const document = this.actions.getSessionWorkflow(parent.session).content;
      if (isEqual(content, parent.saved.content)) {
        content = document;
      } else {
        drafts.unshift({ path: path.slice(0, i + 1), content });
        content = retargetMacro(document, parent.operatorID, { ...parent.definition, content });
      }
    }
    return { content, drafts };
  }

  public resultAliases(portIndex = 0): Record<string, ResultTarget | null> {
    const nested = this.parents.length > 0;
    const current = this.actions.getWorkflowContent();
    const operators = nested ? current.operators : current.operators.filter(isMacro);
    if (!operators.length) return {};
    const context = nested ? this.runtimeContext() : undefined;
    const executed = !nested && this.matchingExecution(current);
    const root = executed ? this.expansion.get(executed) : context?.root;
    return Object.fromEntries(
      operators.map(op => {
        const id = nested ? context?.ids[op.operatorID] : op.operatorID;
        if (!id || !root) return [op.operatorID, null];
        if (!isMacro(op)) return [op.operatorID, { operatorID: id, portIndex }];
        const target = portIndex === 0 ? root.resultTargets?.[id] : root.resultPorts?.[id]?.[portIndex];
        return [op.operatorID, target ?? null];
      })
    );
  }

  private boundaryLinks() {
    const context = this.runtimeContext();
    if (!context) return [];
    return context.inspector.terminals.map(({ direction, portID, links }) => ({
      terminal: { direction, portID },
      links: links.map(i => context.root.content.links[i]),
    }));
  }

  private parentOperators() {
    return this.parents.map(({ session, operatorID }) =>
      this.actions.getSessionWorkflow(session).content.operators.find(op => op.operatorID === operatorID)
    );
  }

  public parentOverrides(): unknown[] {
    return this.parentOperators().map(op => op?.operatorProperties);
  }

  public compilationContext(
    root: MacroExpansion,
    content = this.actions.getWorkflowContent(),
    overrides = this.parentOverrides()
  ) {
    this.compiledOverrides.set(this.actions.editorDocument, overrides);
    const path = this.path;
    const { local, ids, inspector } = projectMacroExpansion(content, root, path);
    if (inspector) this.expansion.remember(content, local);
    const inputLinks = (id: string) => {
      const op = content.operators.find(op => op.operatorID === id);
      return op ? compiledInputLinks(root, path, op) : [];
    };
    return { content, local, root, path, ids, inputLinks, doc: this.actions.editorDocument };
  }

  public async back(index: number, discard = false): Promise<void> {
    if (this.busy || index < 0 || index >= this.parents.length) return;
    this.busy = true;
    try {
      this.changing$.next();
      if (discard) await this.saving.catch(() => undefined);
      if (!discard) {
        await this.save();
        // Keep edits made during a save; leave only after the visible draft has been saved too.
        for (let retry = 0; this.hasUnsavedChanges; retry++) {
          if (retry === 3) throw new Error("The draft is still changing. Save it before leaving.");
          await this.save();
        }
      }
      const operatorID = this.parents[index].operatorID;
      while (this.parents.length > index) this.actions.resumeWorkflow(this.parents.pop()!.session);
      this.actions.highlightElements(false, operatorID);
      this.status.refresh();
      this.changed$.next();
    } finally {
      this.busy = false;
    }
  }

  public clear(): void {
    this.generation++;
    this.rootBefore = undefined;
    this.attemptedRuntimeEpoch = -1;
    this.dirty = false;
    this.parents.splice(0).forEach(p => p.session.shared.destroy());
    this.status.refresh();
  }

  /** Resolve live and restored statistics against the graph submitted for that execution. */
  private async loadRuntimeSnapshot(): Promise<void> {
    const { runtimeEpoch: epoch, runtimeExecutionId: eid } = this.status;
    if (!eid || epoch === this.attemptedRuntimeEpoch || this.status.getRuntimeContent()) return;
    const root = this.rootWorkflow;
    if (!root.wid || (!this.parents.length && !root.content.operators.some(isMacro))) return;
    this.attemptedRuntimeEpoch = epoch;
    try {
      const content = await firstValueFrom(this.executions.retrieveMacroContent(root.wid, eid));
      if (epoch !== this.status.runtimeEpoch || this.rootWorkflow.wid !== root.wid) return;
      this.status.setRuntimeContent(content);
      this.status.refresh();
      await Promise.all([this.expansion.expand(content), this.expansion.expand(this.actions.getWorkflowContent())]);
    } catch {
      // Statistics need both the recorded execution and a valid projection of the editor graph.
    }
  }

  private runtimeContext() {
    let content = this.status.getRuntimeContent();
    if (!this.current || !content) return;
    try {
      const runtime = this.expansion.get(content),
        current = this.actions.getWorkflowContent();
      const expanded = this.expansion.get(current);
      if (!runtime || !expanded) return;
      const definitions = new Map(mergeMacroDefinitions(content.macroDependencies ?? []).map(d => [macroType(d), d]));
      const ancestors = this.parentOperators();
      if (ancestors.some(op => !op || op.isDisabled)) return;
      const overrides = ancestors.map(op => op?.operatorProperties);
      let overridesChanged = false;
      for (const [index, parent] of this.parents.entries()) {
        const op: OperatorPredicate | undefined = content.operators.find(op => op.operatorID === parent.operatorID);
        const definition: MacroDefinition | undefined = op && definitions.get(op.operatorType);
        if (!definition || definition.id !== parent.definition.id) return;
        overridesChanged ||= !isEqual(op?.operatorProperties, overrides[index]);
        content = definition.content;
      }
      if (overridesChanged && !isEqual(overrides, this.compiledOverrides.get(this.actions.editorDocument))) return;
      const projection = projectMacroExpansion(current, runtime, this.path);
      if (!projection.inspector) return;
      const usages = new Set(current.operators.filter(isMacro).map(op => op.operatorID));
      const recipe = (graph: MacroExpansion) => ({
        ...runtimePlan(graph.content),
        ports: Object.fromEntries(Object.entries(graph.ports).filter(([key]) => usages.has(JSON.parse(key)[0]))),
      });
      if (
        isEqual(readMacroInterface(current), readMacroInterface(content)) &&
        isEqual(recipe(expanded), recipe(projection.local))
      )
        return { ...projection, root: runtime };
    } catch {
      return;
    }
  }

  private matchingExecution(current: WorkflowContent): WorkflowContent | undefined {
    const executed = this.status.getRuntimeContent();
    try {
      if (executed && isEqual(runtimePlan(current), runtimePlan(executed))) return executed;
    } catch {
      // A link can briefly outlive an endpoint during a collaborative edit.
    }
  }

  private projectStatistics(raw: Record<string, OperatorRuntimeStatus>): Record<string, OperatorRuntimeStatus> {
    if (!this.parents.length) {
      const current = this.actions.getWorkflowContent();
      if (!current.operators.some(isMacro)) return raw;
      if (this.matchingExecution(current)) return raw;
      return Object.fromEntries(
        current.operators
          .filter(op => !isMacro(op) && raw[op.operatorID])
          .map(op => [op.operatorID, raw[op.operatorID]])
      );
    }
    const context = this.runtimeContext();
    if (this.actions.getTempWorkflow() || !context) return {};
    if (
      this.parents.some(
        p =>
          this.actions.getSessionWorkflow(p.session).content.operators.find(op => op.operatorID === p.operatorID)
            ?.operatorType !== macroType(p.definition)
      )
    )
      return {};
    return Object.fromEntries(
      Object.entries(context.ids).flatMap(([local, id]) => (raw[id] ? [[local, raw[id]]] : []))
    );
  }
}
