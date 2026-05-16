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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import * as dagre from "dagre";
import { BehaviorSubject, Observable, ReplaySubject, of, shareReplay } from "rxjs";
import { tap, map, catchError } from "rxjs/operators";
import { AppSettings } from "../../../common/app-setting";
import { ExecutionMode, Workflow, WorkflowContent } from "../../../common/type/workflow";
import {
  OperatorLink,
  OperatorPredicate,
  PortDescription,
  Point,
} from "../../types/workflow-common.interface";
import { PortIdentity } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { v4 as uuid } from "uuid";

// Per-instance runtime mapping from the macro's external ports back to the
// boundary inner-op port that actually carries the data. The `innerOpId` is
// the engine's runtime op id post macro expansion — ready to look up against
// `OperatorStatisticsUpdateEvent.operatorStatistics`.
//
// Resolution: `MacroService.getRuntimeMacroMapping(wid)` fetches
// `/api/workflow/{wid}/macro-mapping` populated by the backend MacroExpander
// (Map<runtime_uuid, { macroChain, bodyOpId }>). For each MacroInput marker
// in the macro definition body, we find the corresponding runtime UUID by
// matching `macroChain[0] === macroInstanceId` and `bodyOpId === inner-op-id-
// connected-to-the-marker`.
export interface MacroPortBinding {
  externalPortIndex: number;
  innerOpId: string; // post-expansion / runtime ID, ready to look up against engine stats
  innerPortIndex: number;
}

export interface MacroBindings {
  inputBindings: MacroPortBinding[];
  outputBindings: MacroPortBinding[];
}

/**
 * Mirrors `MacroExpander.MacroProvenance` from the backend (Scala). For each
 * runtime op id present in the engine's execution stats, the chain records the
 * macro instance ids it sits under (outermost → innermost) and the original
 * definition-time op id inside the innermost macro body. Used to (a) roll
 * inner-op stats up to the macro op on the canvas and (b) attach stats to
 * body-level positions when drilling into a macro.
 */
export interface MacroProvenanceEntry {
  macroChain: string[];
  bodyOpId: string;
}

export const MACRO_BASE_URL = "macro";
export const MACRO_CREATE_URL = MACRO_BASE_URL + "/create";
export const MACRO_LIST_URL = MACRO_BASE_URL + "/list";

// Mirrors the case classes on `MacroResource` (amber). Keeping the shapes
// hand-typed (rather than generating) so the dev loop stays simple.
export interface MacroPortSpec {
  index: number;
  displayName?: string;
}

export interface PortSpec {
  inputs: MacroPortSpec[];
  outputs: MacroPortSpec[];
}

export interface MacroCreateRequest {
  name: string;
  description?: string;
  content: string;
  isPublic?: boolean;
  portSpec: PortSpec;
  paramSpec?: unknown;
  category?: string;
  icon?: string;
}

export interface MacroDetail {
  wid: number;
  name: string;
  description: string;
  content: string;
  creationTime: string;
  lastModifiedTime: string;
  isPublic: boolean;
  portSpec: PortSpec;
  paramSpec: unknown;
  category?: string;
  icon?: string;
  isOwner: boolean;
  readonly: boolean;
}

export interface MacroSummary {
  wid: number;
  name: string;
  description: string;
  lastModifiedTime: string;
  portSpec: PortSpec;
  category?: string;
  icon?: string;
  // Number of distinct non-macro workflows that reference this macro. Surfaced
  // in the "Your Macros" palette as a small reuse-count chip. Optional so
  // older backend builds (without the usageCount field) still work — frontend
  // treats `undefined` as "unknown" and hides the chip.
  usageCount?: number;
}

// Shape that MacroExpander (backend) reads off `workflow.content`. Matches the
// MacroBody / MacroLink case classes in `common/workflow-operator`.
interface MacroBodyLink {
  fromOpId: string;
  fromPortId: PortIdentity;
  toOpId: string;
  toPortId: PortIdentity;
}

interface MacroBody {
  operators: unknown[];
  links: MacroBodyLink[];
  inputs: MacroPortSpec[];
  outputs: MacroPortSpec[];
}

@Injectable({
  providedIn: "root",
})
export class MacroService {
  constructor(
    private http: HttpClient,
    private workflowResultService: WorkflowResultService,
    private workflowUtilService: WorkflowUtilService
  ) {}

  /**
   * Convenience: take a selection of operators, build a macro definition
   * from them via `buildMacroFromSelection`, POST to `createMacro`, and on
   * success replace the selection on the canvas with a single Macro op.
   * Returns the `MacroDetail` so callers can chain on it (e.g. surface a
   * toast / update local state).
   *
   * Mirrors `ContextMenuComponent.onCreateMacro` + `swapSelectionWithMacroNode`
   * so that callers without right-click access (e.g. the
   * suggestMacros panel's "materialize" action) can do the same thing.
   */
  public createMacroFromSelection(
    workflowActionService: WorkflowActionService,
    selectedOperatorIDs: readonly string[],
    name: string
  ): Observable<MacroDetail> {
    const built = this.buildMacroFromSelection(workflowActionService, selectedOperatorIDs, name);
    return this.createMacro(built.request).pipe(
      tap(detail =>
        this.swapSelectionWithMacroNode(workflowActionService, detail, selectedOperatorIDs, built)
      )
    );
  }

  /**
   * Reuse an existing macro definition for another sub-DAG on the canvas.
   * Used by the pattern-batch materialize flow: after the FIRST occurrence
   * has been encapsulated (creating the macro), each subsequent occurrence
   * is swapped with a FRESH instance pointing at the same `detail.wid`.
   *
   * Validates that the candidate selection has the same boundary port count
   * as the macro definition. The pattern-detector ensures the operator-type
   * signature matches, which (for linear patterns) implies the same boundary
   * structure — but we still defensively check the counts before swapping.
   * Returns true on success, false if shapes don't match.
   */
  public swapSelectionWithExistingMacro(
    workflowActionService: WorkflowActionService,
    detail: MacroDetail,
    selectedOpIDs: readonly string[]
  ): boolean {
    // Build a *throwaway* macro definition request from the selection just to
    // get the boundary metadata (incomingEdges, outgoingEdges, port counts).
    // The request payload is discarded — we're not POSTing it.
    const built = this.buildMacroFromSelection(workflowActionService, selectedOpIDs, "_throwaway_");
    if (
      built.inputPortCount !== detail.portSpec.inputs.length ||
      built.outputPortCount !== detail.portSpec.outputs.length
    ) {
      return false;
    }
    this.swapSelectionWithMacroNode(workflowActionService, detail, selectedOpIDs, built);
    return true;
  }

  /**
   * Replace the selected operators on the canvas with a single Macro op
   * pointing at the just-created definition. Extracted from
   * `ContextMenuComponent.swapSelectionWithMacroNode` so it can be
   * called from the suggestMacros materialize action too.
   */
  private swapSelectionWithMacroNode(
    workflowActionService: WorkflowActionService,
    detail: MacroDetail,
    selectedOpIDs: readonly string[],
    built: {
      incomingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
      outgoingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
      inputPortCount: number;
      outputPortCount: number;
    }
  ): void {
    const inputPorts = Array.from({ length: built.inputPortCount }, (_, i) => ({
      portID: `input-${i}`,
      displayName: `in-${i}`,
      disallowMultiInputs: false,
      isDynamicPort: false,
      dependencies: [],
    }));
    const outputPorts = Array.from({ length: built.outputPortCount }, (_, i) => ({
      portID: `output-${i}`,
      displayName: `out-${i}`,
      disallowMultiInputs: false,
      isDynamicPort: false,
    }));
    const macroPredicate: OperatorPredicate = {
      operatorID: `Macro-operator-${this.workflowUtilService.getOperatorRandomUUID()}`,
      operatorType: "Macro",
      operatorVersion: "",
      operatorProperties: {
        macroId: detail.wid.toString(),
        macroVersion: 1,
        linkMode: "LIVE",
        inputPortCount: built.inputPortCount,
        outputPortCount: built.outputPortCount,
        displayName: detail.name,
        // Newly-created instance is in-sync with the definition we just
        // POSTed; stamp the modify time so the staleness check in the
        // context-menu sees this as fresh until the definition is edited.
        macroSyncedAt:
          typeof detail.lastModifiedTime === "number"
            ? detail.lastModifiedTime
            : new Date(detail.lastModifiedTime as unknown as string).getTime(),
      },
      inputPorts,
      outputPorts,
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: detail.name,
      dynamicInputPorts: false,
      dynamicOutputPorts: false,
    };
    const jointWrapper = workflowActionService.getJointGraphWrapper();
    const positions = selectedOpIDs
      .map(id => {
        try {
          return jointWrapper.getElementPosition(id);
        } catch {
          return undefined;
        }
      })
      .filter((p): p is Point => !!p);
    const centroid: Point =
      positions.length > 0
        ? {
            x: positions.reduce((sum, p) => sum + p.x, 0) / positions.length,
            y: positions.reduce((sum, p) => sum + p.y, 0) / positions.length,
          }
        : { x: 200, y: 200 };
    workflowActionService.getTexeraGraph().bundleActions(() => {
      workflowActionService.addOperator(macroPredicate, centroid);
      workflowActionService.deleteOperatorsAndLinks(Array.from(selectedOpIDs));
      built.incomingEdges.forEach(edge =>
        workflowActionService.addLink({
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: edge.externalOpId, portID: edge.externalPortID },
          target: { operatorID: macroPredicate.operatorID, portID: `input-${edge.macroPortIndex}` },
        })
      );
      built.outgoingEdges.forEach(edge =>
        workflowActionService.addLink({
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: macroPredicate.operatorID, portID: `output-${edge.macroPortIndex}` },
          target: { operatorID: edge.externalOpId, portID: edge.externalPortID },
        })
      );
    });
  }

  // Runtime macro-provenance map. Fetched once per (workflowId, execution)
  // from `/api/workflow/{wid}/macro-mapping`. Indexed by runtime op id.
  // Empty until the user clicks Run AND the compile finishes server-side.
  private runtimeMacroMapping = new Map<string, MacroProvenanceEntry>();
  private runtimeMacroMappingLoadedFor: number | undefined = undefined;
  // Inverse index: macroChain[0] (the canvas-level macro instance id) → list
  // of runtime op ids belonging to that instance. Rebuilt whenever
  // runtimeMacroMapping is refreshed. Lets the stats consumer look up
  // "all runtime ops under macro X" in O(1).
  private runtimeOpsByMacroInstance = new Map<string, string[]>();
  // Subscribers (e.g. result-panel drill-down alias, status aggregator) can
  // re-emit when the runtime macro-mapping is refreshed. Tick is opaque —
  // consumers just need to know "the mapping changed, re-read it now."
  private runtimeMacroMappingTick = new BehaviorSubject<number>(0);

  /** Stream that ticks whenever the runtime-mapping cache is refreshed. */
  public getRuntimeMacroMappingTick(): Observable<number> {
    return this.runtimeMacroMappingTick.asObservable();
  }

  /**
   * Fetch the macro-instance provenance map for the most-recent compile of
   * the given workflow. The backend populates this map during MacroExpander
   * (see `MacroMappingCache`) and exposes it via this REST endpoint.
   *
   * Cached per workflow id; call `refreshRuntimeMacroMapping(wid)` to force
   * a refresh after Run is clicked or after a workflow content change.
   */
  public getRuntimeMacroMapping(wid: number): Observable<Map<string, MacroProvenanceEntry>> {
    if (this.runtimeMacroMappingLoadedFor === wid && this.runtimeMacroMapping.size > 0) {
      return of(this.runtimeMacroMapping);
    }
    return this.refreshRuntimeMacroMapping(wid);
  }

  /**
   * Force a refresh of the runtime macro-mapping. Called by the execute path
   * immediately after the user clicks Run so the cache reflects the latest
   * compile output.
   */
  public refreshRuntimeMacroMapping(wid: number): Observable<Map<string, MacroProvenanceEntry>> {
    return this.http
      .get<Record<string, MacroProvenanceEntry>>(
        `${AppSettings.getApiEndpoint()}/workflow/${wid}/macro-mapping`
      )
      .pipe(
        map(raw => {
          this.runtimeMacroMapping.clear();
          this.runtimeOpsByMacroInstance.clear();
          for (const [runtimeOpId, entry] of Object.entries(raw)) {
            this.runtimeMacroMapping.set(runtimeOpId, entry);
            const outerInstance = entry.macroChain?.[0];
            if (outerInstance) {
              if (!this.runtimeOpsByMacroInstance.has(outerInstance)) {
                this.runtimeOpsByMacroInstance.set(outerInstance, []);
              }
              this.runtimeOpsByMacroInstance.get(outerInstance)!.push(runtimeOpId);
            }
          }
          this.runtimeMacroMappingLoadedFor = wid;
          // Tick so downstream subscribers (drill-down alias, stats roll-up)
          // can re-read with the now-populated cache. Required because the
          // initial render typically happens BEFORE this fetch completes; we
          // need to nudge them once the data lands.
          this.runtimeMacroMappingTick.next(this.runtimeMacroMappingTick.value + 1);
          return this.runtimeMacroMapping;
        }),
        catchError(() => {
          // No mapping yet (e.g. user hasn't clicked Run, or workflow has no
          // macros). Return the (empty) cache and don't poison future calls.
          this.runtimeMacroMappingLoadedFor = undefined;
          return of(this.runtimeMacroMapping);
        })
      );
  }

  /** Synchronous lookup: which macro instance owns this runtime op id? */
  public macroInstanceForRuntimeOp(runtimeOpId: string): string | undefined {
    return this.runtimeMacroMapping.get(runtimeOpId)?.macroChain[0];
  }

  /**
   * Full macro chain (outermost → innermost) for a runtime op id, or
   * `undefined` if it isn't inside a macro. Used by the stats aggregator
   * to roll up to EVERY macro level the op belongs to — so a runtime op
   * deep inside a nested macro contributes to both the outer macro's
   * aggregate (visible on the parent canvas) AND each inner macro's
   * aggregate (visible inside the outer's drill-down view).
   */
  public macroChainForRuntimeOp(runtimeOpId: string): string[] | undefined {
    return this.runtimeMacroMapping.get(runtimeOpId)?.macroChain;
  }

  /** Synchronous lookup: which body op id did this runtime op come from? */
  public bodyOpIdForRuntimeOp(runtimeOpId: string): string | undefined {
    return this.runtimeMacroMapping.get(runtimeOpId)?.bodyOpId;
  }

  /** All runtime op ids belonging to the given canvas-level macro instance. */
  public runtimeOpsForMacroInstance(macroInstanceId: string): string[] {
    return this.runtimeOpsByMacroInstance.get(macroInstanceId) ?? [];
  }

  /**
   * Synthesize macro-op port-level + aggregated stats from its boundary
   * bindings. The macro's external input port i shows the row count on the
   * specific inner port that `MacroInput(i)` feeds (recursively, through any
   * nested macros). Same for output. Aggregated totals are the SUM of the
   * macro's external port counts — NOT the sum of every inner op's
   * row count (which double-counts internal traffic).
   *
   * Returns null if bindings aren't loaded yet. Caller can fall back to a
   * state-only entry while waiting.
   *
   * Lives on MacroService (rather than workflow-editor) so both the canvas
   * statistics renderer AND the WorkflowStatusService aggregator can use
   * the same source of truth.
   */
  public synthesizeMacroOpStats(
    macroInstanceId: string,
    macroId: string,
    rawStatusByRuntimeOpId: Record<string, { inputPortMetrics?: Record<string, number>; outputPortMetrics?: Record<string, number> }>
  ): {
    inputPortMetrics: Record<string, number>;
    outputPortMetrics: Record<string, number>;
    aggregatedInputRowCount: number;
    aggregatedOutputRowCount: number;
  } | null {
    const bindings = this.getBindingsForInstance(macroInstanceId, macroId);
    if (!bindings) return null;
    const inputPortMetrics: Record<string, number> = {};
    const outputPortMetrics: Record<string, number> = {};
    for (const b of bindings.inputBindings) {
      const innerStats = rawStatusByRuntimeOpId[b.innerOpId];
      if (!innerStats) continue;
      const cnt = innerStats.inputPortMetrics?.[String(b.innerPortIndex)] ?? 0;
      const key = String(b.externalPortIndex);
      inputPortMetrics[key] = (inputPortMetrics[key] ?? 0) + cnt;
    }
    for (const b of bindings.outputBindings) {
      const innerStats = rawStatusByRuntimeOpId[b.innerOpId];
      if (!innerStats) continue;
      const cnt = innerStats.outputPortMetrics?.[String(b.innerPortIndex)] ?? 0;
      outputPortMetrics[String(b.externalPortIndex)] = cnt;
    }
    return {
      inputPortMetrics,
      outputPortMetrics,
      aggregatedInputRowCount: Object.values(inputPortMetrics).reduce((a, b) => a + b, 0),
      aggregatedOutputRowCount: Object.values(outputPortMetrics).reduce((a, b) => a + b, 0),
    };
  }

  /**
   * For a macro instance whose body op id is also its instance id (this is
   * the case for nested macros visible inside a parent's drill-down view),
   * return its `macroId` (the wid of the macro definition) by walking the
   * outer macro definition's body. Returns undefined if not found in
   * cache.
   *
   * Why: in drill-down view of outer macro O, a nested macro N appears as a
   * canvas op with body-relative id (which is also its instance id). To
   * compute N's external port stats we need its macroId so we can look up
   * its body bindings.
   */
  public macroIdForBodyOpId(parentMacroId: string, bodyOpId: string): string | undefined {
    return this.bodyBindingsSnapshot.get(parentMacroId)?.nestedMacros.get(bodyOpId);
  }

  // Track (macroInstanceId → macroId) so other services (e.g. WorkflowStatus
  // for aggregation) can look up the macro definition wid by instance id
  // without grabbing a reference to WorkflowActionService. Populated by
  // `registerMacroInstance(...)` whenever the workflow editor / palette adds
  // a Macro op to the graph.
  private macroDefByInstance = new Map<string, string>();

  /** Record that `macroInstanceId` (a canvas op id) instantiates macro `macroId`. */
  public registerMacroInstance(macroInstanceId: string, macroId: string): void {
    if (macroId) this.macroDefByInstance.set(macroInstanceId, macroId);
  }

  /** Lookup macro definition wid for a given instance id. */
  public macroDefIdForInstance(macroInstanceId: string): string | undefined {
    const direct = this.macroDefByInstance.get(macroInstanceId);
    if (direct) return direct;
    // Fallback: scan body bindings — `macroInstanceId` might be a nested
    // macro's body-relative id inside some parent body that's in the
    // bindings cache.
    for (const [parentMacroId, snapshot] of this.bodyBindingsSnapshot.entries()) {
      const nested = snapshot.nestedMacros.get(macroInstanceId);
      if (nested) return nested;
      // (parentMacroId left unused; pattern is `(_, snapshot)` essentially)
      void parentMacroId;
    }
    return undefined;
  }

  /**
   * Build a body-op-id → runtime-uuid lookup for the macro DEFINITION whose
   * canvas instance is `macroInstanceId`. Used by the drill-down view: the
   * canvas ops there carry body-relative IDs (from the macro definition),
   * but engine stats are keyed by runtime UUIDs. This map lets the view
   * translate `body-op-id → runtime UUID → status[runtime UUID]`.
   *
   * For nested macros: we pick the runtime UUID whose macroChain INCLUDES
   * this instance (anywhere in the chain) AND whose bodyOpId matches. That
   * way drilling into the OUTERMOST macro of a nested chain shows the
   * outer body's macro ops (themselves still macros in the drill-down
   * view) — clicking those drills further; their inner ops get their own
   * map via the same call with a different instance id.
   */
  public buildBodyOpIdToRuntimeUuidMap(macroInstanceId: string): Map<string, string> {
    const map = new Map<string, string>();
    for (const [runtimeUuid, prov] of this.runtimeMacroMapping.entries()) {
      if (!prov.macroChain.includes(macroInstanceId)) continue;
      // Only record if this entry's INNERMOST chain element matches the
      // requested instance — otherwise a runtime UUID for a deeper-nested
      // op would shadow a same-bodyOpId body-level op at this level.
      if (prov.macroChain[prov.macroChain.length - 1] !== macroInstanceId) continue;
      map.set(prov.bodyOpId, runtimeUuid);
    }
    return map;
  }

  /**
   * Resolve macro port bindings for a specific macro instance using the
   * runtime mapping. For each MacroInput/Output marker, walks the macro
   * body (recursing through any nested macros) until it hits a terminal
   * non-macro inner op, then looks up that op's runtime UUID via the
   * macro-mapping side-table.
   *
   * The recursion is essential: a top-level macro's input port may be
   * connected to a nested macro's input port, whose body connects it to
   * yet another op, etc. We need the FINAL terminal runtime op so its
   * port-level stats can drive the outer macro's external port display.
   */
  public resolveBindingsViaRuntimeMapping(
    macroInstanceId: string,
    macroId: string
  ): MacroBindings | undefined {
    const snapshot = this.bodyBindingsSnapshot.get(macroId);
    if (!snapshot) {
      this.getBodyBindings(macroId).subscribe({ error: () => undefined });
      return undefined;
    }
    // Resolve one body-level binding to one or more terminal runtime bindings.
    // `accumulatedChain` accumulates the macro-instance chain we've descended
    // through, used to disambiguate which runtime op matches when a body op
    // id is reused across macro definitions.
    const resolveOne = (
      b: MacroPortBinding,
      definition: {
        inputBindings: MacroPortBinding[];
        outputBindings: MacroPortBinding[];
        nestedMacros: Map<string, string>;
        innerSinks: string[];
      },
      accumulatedChain: string[],
      isInput: boolean
    ): MacroPortBinding[] => {
      const nestedMacroId = definition.nestedMacros.get(b.innerOpId);
      if (!nestedMacroId) {
        // Terminal: find the runtime op whose chain ENDS WITH accumulatedChain
        // (the chain of macro instances we descended through from the call
        // site) and whose bodyOpId matches this binding's innerOpId.
        //
        // Suffix match (not exact length) so that a synthesize() call rooted
        // at an INNER macro instance (e.g. d3188a84 when computing the nested
        // macro op's stats in drill-down view) still finds its runtime ops —
        // those carry full chains like [outerInstance, innerInstance], so the
        // accumulatedChain [innerInstance] is a suffix.
        const candidates: string[] = [];
        const matchesSuffix = (chain: string[]): boolean => {
          if (chain.length < accumulatedChain.length) return false;
          const offset = chain.length - accumulatedChain.length;
          for (let i = 0; i < accumulatedChain.length; i++) {
            if (chain[offset + i] !== accumulatedChain[i]) return false;
          }
          return true;
        };
        for (const [runtimeOpId, prov] of this.runtimeMacroMapping.entries()) {
          if (prov.bodyOpId !== b.innerOpId) continue;
          if (matchesSuffix(prov.macroChain)) candidates.push(runtimeOpId);
        }
        return candidates.map(runtimeOpId => ({
          externalPortIndex: b.externalPortIndex,
          innerOpId: runtimeOpId,
          innerPortIndex: b.innerPortIndex,
        }));
      }
      // Nested macro: drill into its body and continue down to the next
      // boundary in/out the binding's innerPortIndex maps to.
      const nestedSnapshot = this.bodyBindingsSnapshot.get(nestedMacroId);
      if (!nestedSnapshot) {
        // Snapshot not loaded yet — kick off and bail (caller will re-resolve
        // on the next stats tick).
        this.getBodyBindings(nestedMacroId).subscribe({ error: () => undefined });
        return [];
      }
      // The nested macro op's BODY definition id (b.innerOpId) is also its
      // canvas-level instance id in the outer body. That's the macroChain
      // element we add as we descend.
      const nextChain = [...accumulatedChain, b.innerOpId];
      const nestedSideBindings = isInput
        ? nestedSnapshot.inputBindings
        : nestedSnapshot.outputBindings;
      const matched = nestedSideBindings.filter(nb => nb.externalPortIndex === b.innerPortIndex);
      const resolved: MacroPortBinding[] = [];
      for (const nb of matched) {
        const carriedOver: MacroPortBinding = {
          externalPortIndex: b.externalPortIndex, // preserve outer's external port index
          innerOpId: nb.innerOpId,
          innerPortIndex: nb.innerPortIndex,
        };
        resolved.push(...resolveOne(carriedOver, nestedSnapshot, nextChain, isInput));
      }
      return resolved;
    };

    const startChain = [macroInstanceId];
    const inputBindings: MacroPortBinding[] = [];
    for (const b of snapshot.inputBindings) {
      inputBindings.push(...resolveOne(b, snapshot, startChain, /* isInput */ true));
    }
    const outputBindings: MacroPortBinding[] = [];
    for (const b of snapshot.outputBindings) {
      outputBindings.push(...resolveOne(b, snapshot, startChain, /* isInput */ false));
    }
    return { inputBindings, outputBindings };
  }

  // Cached per-definition body bindings, keyed by `${macroId}` (the macro
  // definition's wid). Each entry is a hot Observable so multiple subscribers
  // share the same HTTP fetch. The body of a macro definition is immutable
  // for the lifetime of a given (macroId, vid) tuple, so caching by macroId
  // alone is safe — definition edits go through a new wid in the v1 LIVE mode.
  // The cached shape also carries `nestedMacros: Map<innerOpId, nestedMacroId>`
  // so recursive resolution (for nested macros) can follow the chain without
  // re-parsing the body.
  private bodyBindingsCache = new Map<
    string,
    Observable<{
      inputBindings: MacroPortBinding[];
      outputBindings: MacroPortBinding[];
      nestedMacros: Map<string, string>;
      innerSinks: string[];
    }>
  >();
  // Latest-known synchronous snapshot — populated by `getBindingsForInstance`
  // after the first successful fetch so synchronous stat-update handlers can
  // look up bindings without re-triggering the network call.
  private bodyBindingsSnapshot = new Map<
    string,
    {
      inputBindings: MacroPortBinding[];
      outputBindings: MacroPortBinding[];
      nestedMacros: Map<string, string>;
      innerSinks: string[];
    }
  >();

  public createMacro(req: MacroCreateRequest): Observable<MacroDetail> {
    return this.http.post<MacroDetail>(`${AppSettings.getApiEndpoint()}/${MACRO_CREATE_URL}`, req);
  }

  /**
   * Trigger a browser download of a portable JSON dump of one macro. The file
   * is everything `createMacro` accepts as input — name, content, portSpec,
   * paramSpec — so it can be re-imported on a different Texera instance via
   * `importMacroFromJson`. We deliberately exclude wid and timestamps because
   * the importer always creates a fresh definition with a new wid.
   *
   * Transitive: if the macro's body references nested macros, those are
   * fetched too and embedded as `nestedMacros[oldWid] = detailPayload`. The
   * importer reconstructs them in dependency order before the root macro,
   * stitching the new wids into the root's body so the import is fully self-
   * contained. (Currently the importer creates the root only; transitive
   * import is a v2 enhancement, but the export side records everything so
   * a manual rebuild is possible.)
   *
   * The exported `content` is the raw MacroBody JSON string; consumer just
   * needs to round-trip it through `JSON.parse(JSON.stringify(...))` to stay
   * Jackson-friendly on re-import.
   */
  public exportMacroToFile(wid: number): Observable<void> {
    return new Observable<void>(subscriber => {
      this.exportBundleForMacro(wid).subscribe({
        next: bundle => {
          const blob = new Blob([JSON.stringify(bundle, null, 2)], {
            type: "application/json",
          });
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          const safeName = bundle.name.replace(/[^a-zA-Z0-9_-]+/g, "_").slice(0, 60);
          a.download = `macro-${safeName}-${wid}.json`;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
          subscriber.next();
          subscriber.complete();
        },
        error: err => subscriber.error(err),
      });
    });
  }

  /**
   * Build the transitive export bundle for a macro: the root payload plus
   * full definitions of every nested macro it references (and their nested
   * macros, recursively). The result is self-contained — importable on a
   * fresh Texera instance with no other prep — and structured to be applied
   * dependency-first so each parent's body can be rewritten to reference
   * the new wids of its children.
   *
   * The bundle has a `bundleVersion: 2` marker distinguishing it from the
   * v1 single-macro export (`schemaVersion: 1`). Both shapes round-trip
   * through `importMacroFromJson`.
   */
  public exportBundleForMacro(rootWid: number): Observable<{
    bundleVersion: 2;
    name: string;
    description: string;
    rootContent: string;
    portSpec: PortSpec;
    paramSpec: unknown;
    category?: string;
    icon?: string;
    exportedAt: string;
    exportedFromTexera: string;
    nestedMacros: Array<{
      originalWid: number;
      name: string;
      description: string;
      content: string;
      portSpec: PortSpec;
      paramSpec: unknown;
    }>;
  }> {
    // Walk the dependency graph depth-first, collecting every reachable
    // macro id starting from the root. Cycles can't happen for macros
    // (MacroExpander guards against them) but we still guard with `seen`.
    return new Observable(subscriber => {
      const seen = new Set<number>();
      const order: number[] = [];
      const details = new Map<number, MacroDetail>();
      const visit = (w: number): Promise<void> =>
        new Promise((resolve, reject) => {
          if (seen.has(w)) return resolve();
          seen.add(w);
          this.getMacro(w).subscribe({
            next: async d => {
              details.set(w, d);
              const nestedWids = this.collectNestedMacroIds(d.content);
              for (const nw of nestedWids) {
                try {
                  await visit(nw);
                } catch (e) {
                  return reject(e);
                }
              }
              order.push(w);
              resolve();
            },
            error: reject,
          });
        });
      visit(rootWid).then(
        () => {
          const root = details.get(rootWid);
          if (!root) {
            subscriber.error(new Error("Root macro fetch failed"));
            return;
          }
          // Nested macros are everything in `order` except the root, in
          // dependency-first order (children before their parents).
          const nestedMacros = order
            .filter(w => w !== rootWid)
            .map(w => {
              const d = details.get(w)!;
              return {
                originalWid: w,
                name: d.name,
                description: d.description,
                content: d.content,
                portSpec: d.portSpec,
                paramSpec: d.paramSpec,
              };
            });
          subscriber.next({
            bundleVersion: 2 as const,
            name: root.name,
            description: root.description,
            rootContent: root.content,
            portSpec: root.portSpec,
            paramSpec: root.paramSpec,
            category: root.category,
            icon: root.icon,
            exportedAt: new Date().toISOString(),
            exportedFromTexera: window.location.host,
            nestedMacros,
          });
          subscriber.complete();
        },
        err => subscriber.error(err)
      );
    });
  }

  /**
   * Scan a macro's content (JSON string) for nested macroId references. The
   * scan is regex-based for speed and resilience — body shape may have
   * additional fields we don't care about. Used by `exportMacroToFile` to
   * record dependencies in the export payload.
   */
  private collectNestedMacroIds(content: string): number[] {
    const matches = content.match(/"macroId"\s*:\s*"(\d+)"/g) ?? [];
    const wids = new Set<number>();
    for (const m of matches) {
      const numMatch = m.match(/(\d+)/);
      if (numMatch) wids.add(Number(numMatch[1]));
    }
    return Array.from(wids);
  }

  /**
   * Reverse of `exportMacroToFile`: parse an uploaded JSON file and POST it
   * as a brand-new macro definition. The new definition's wid is fresh —
   * any cross-references inside the original `content` to its own wid are
   * left as-is (they'd be self-referential and unused).
   *
   * Bundle support (v2): if the JSON has `bundleVersion: 2`, all nested
   * macros are created first (in dependency order), then the root content
   * is rewritten to point at the new wids, then the root is created. The
   * caller still receives the root's MacroDetail — the nested macros land
   * in the user's library silently. Schema v1 (single-macro JSON) still
   * works for back-compat.
   */
  public importMacroFromJson(rawJson: string): Observable<MacroDetail> {
    const parsed = JSON.parse(rawJson) as Record<string, unknown>;
    if (parsed["bundleVersion"] === 2) {
      return this.importMacroBundle(parsed as never);
    }
    const v1 = parsed as {
      schemaVersion?: number;
      name?: string;
      description?: string;
      content?: string;
      portSpec?: PortSpec;
      paramSpec?: unknown;
      category?: string;
      icon?: string;
    };
    if (!v1.name || !v1.content || !v1.portSpec) {
      throw new Error("Invalid macro JSON: missing name / content / portSpec.");
    }
    const req: MacroCreateRequest = {
      name: `${v1.name} (imported)`,
      description: v1.description ?? "Imported macro",
      content: v1.content,
      portSpec: v1.portSpec,
      paramSpec: v1.paramSpec,
      category: v1.category,
      icon: v1.icon,
    };
    return this.createMacro(req);
  }

  /**
   * Apply a v2 export bundle: walk the nested macros in dependency order,
   * create each one (collecting a `oldWid → newWid` map), rewrite the next
   * pending body's macroId references to the new wids before creating it.
   * Finally rewrite the root body the same way and create it.
   *
   * Failures abort the bundle (best-effort; partial state may persist if a
   * mid-bundle create fails — surfacing this cleanly is a v3 follow-up).
   */
  private importMacroBundle(bundle: {
    name: string;
    description: string;
    rootContent: string;
    portSpec: PortSpec;
    paramSpec: unknown;
    category?: string;
    icon?: string;
    nestedMacros: Array<{
      originalWid: number;
      name: string;
      description: string;
      content: string;
      portSpec: PortSpec;
      paramSpec: unknown;
    }>;
  }): Observable<MacroDetail> {
    return new Observable<MacroDetail>(subscriber => {
      const idRewrite = new Map<number, number>();
      const rewriteContent = (content: string): string =>
        content.replace(/"macroId"\s*:\s*"(\d+)"/g, (match, oldWidStr) => {
          const oldWid = Number(oldWidStr);
          const newWid = idRewrite.get(oldWid);
          if (newWid === undefined) return match;
          return `"macroId":"${newWid}"`;
        });
      const createOne = (i: number): Promise<void> =>
        new Promise((resolve, reject) => {
          if (i >= bundle.nestedMacros.length) return resolve();
          const nested = bundle.nestedMacros[i];
          const rewrittenContent = rewriteContent(nested.content);
          this.createMacro({
            name: `${nested.name} (imported nested)`,
            description: nested.description ?? "Imported macro (nested dep)",
            content: rewrittenContent,
            portSpec: nested.portSpec,
            paramSpec: nested.paramSpec,
          }).subscribe({
            next: created => {
              idRewrite.set(nested.originalWid, created.wid);
              createOne(i + 1).then(resolve, reject);
            },
            error: reject,
          });
        });
      createOne(0).then(
        () => {
          const rootContent = rewriteContent(bundle.rootContent);
          this.createMacro({
            name: `${bundle.name} (imported)`,
            description: bundle.description ?? "Imported macro bundle",
            content: rootContent,
            portSpec: bundle.portSpec,
            paramSpec: bundle.paramSpec,
            category: bundle.category,
            icon: bundle.icon,
          }).subscribe({
            next: rootDetail => {
              subscriber.next(rootDetail);
              subscriber.complete();
            },
            error: err => subscriber.error(err),
          });
        },
        err => subscriber.error(err)
      );
    });
  }

  public listMacros(): Observable<MacroSummary[]> {
    return this.http
      .get<MacroSummary[]>(`${AppSettings.getApiEndpoint()}/${MACRO_LIST_URL}`)
      .pipe(
        tap(summaries => {
          // Mirror into the latest-modified cache so canvas-side consumers can
          // detect stale instances without re-fetching. lastModifiedTime is a
          // string in transport (LDT JSON) but a number once Jackson serializes
          // a Timestamp; coerce both into ms-since-epoch for easy compare.
          for (const m of summaries) {
            const tsRaw = m.lastModifiedTime as unknown;
            const tsMs =
              typeof tsRaw === "number" ? tsRaw : new Date(tsRaw as string).getTime();
            this.latestModifiedByWid.set(m.wid, tsMs);
          }
        })
      );
  }

  /**
   * Map of `macroId → most recently seen lastModifiedTime` (epoch ms),
   * populated by every `listMacros` response. Used by the "refresh macro
   * instance" context-menu action to decide whether a canvas instance is
   * stale, and to imprint the freshness timestamp when re-syncing.
   */
  private latestModifiedByWid = new Map<number, number>();

  /**
   * Lookup helper for callers (e.g. the JointUI service when it renders a
   * Macro op) — returns the most recent lastModifiedTime we've seen for the
   * given macro definition, in ms since epoch. Returns 0 if we haven't seen
   * the macro yet (i.e. listMacros hasn't been called or the macro is
   * inaccessible to the current user).
   */
  public getLatestModifiedTime(macroId: number | string): number {
    const wid = typeof macroId === "number" ? macroId : Number(macroId);
    if (!Number.isFinite(wid)) return 0;
    return this.latestModifiedByWid.get(wid) ?? 0;
  }

  public getMacro(wid: number): Observable<MacroDetail> {
    return this.http.get<MacroDetail>(`${AppSettings.getApiEndpoint()}/${MACRO_BASE_URL}/${wid}`);
  }

  /**
   * Compute body-level port bindings for the macro DEFINITION identified by
   * `macroId` (the definition's wid). The bindings name body-relative inner
   * op IDs — callers that need *runtime* IDs (after MacroExpander's prefix
   * rewrite) should use `getBindingsForInstance` instead.
   *
   * Body bindings are derived from the persisted `MacroBody`:
   *  - each `MacroInput(portIndex=i)` is followed by one or more links
   *    `marker → innerOp@(p)`; we record (i → innerOp, p) for stats fan-out
   *  - each `MacroOutput(portIndex=i)` is preceded by exactly one link
   *    `innerOp@(p) → marker`; we record (i → innerOp, p) for stats/results
   *
   * Cached and shared across subscribers.
   */
  public getBodyBindings(macroId: string): Observable<{
    inputBindings: MacroPortBinding[];
    outputBindings: MacroPortBinding[];
    nestedMacros: Map<string, string>;
    innerSinks: string[];
  }> {
    const cached = this.bodyBindingsCache.get(macroId);
    if (cached) return cached;
    const widNum = Number(macroId);
    if (!Number.isFinite(widNum)) {
      const empty = {
        inputBindings: [],
        outputBindings: [],
        nestedMacros: new Map<string, string>(),
        innerSinks: [],
      };
      this.bodyBindingsSnapshot.set(macroId, empty);
      return of(empty);
    }
    const fetched = this.getMacro(widNum).pipe(
      map(detail => this.computeBodyBindings(detail)),
      tap(bindings => {
        this.bodyBindingsSnapshot.set(macroId, bindings);
        // Eagerly recurse: fetch bindings for any nested macro definitions
        // we discovered, so the synchronous resolution path in
        // `getBindingsForInstance` finds everything in the snapshot cache.
        for (const nestedMacroId of bindings.nestedMacros.values()) {
          this.getBodyBindings(nestedMacroId).subscribe({ error: () => undefined });
        }
      }),
      catchError(() =>
        of({
          inputBindings: [] as MacroPortBinding[],
          outputBindings: [] as MacroPortBinding[],
          nestedMacros: new Map<string, string>(),
          innerSinks: [] as string[],
        })
      ),
      shareReplay(1)
    );
    this.bodyBindingsCache.set(macroId, fetched);
    return fetched;
  }

  /**
   * Resolve bindings to runtime IDs for one macro instance on the parent
   * canvas. `${instanceId}--` is the prefix MacroExpander adds to every
   * inner-op ID when it inlines the body (see
   * `workflow-compiling-service/.../MacroExpander.scala`). After this rewrite
   * the engine reports stats keyed by the prefixed strings — so we apply the
   * same rewrite here so callers can do straight-up `stats[innerOpId]` lookups.
   *
   * Recursive: when a binding's `innerOpId` points to a nested macro, follow
   * its body bindings (recursively, prefixed at each layer) until we reach a
   * terminal non-macro inner op. A fan-out at an input port can produce
   * multiple terminal bindings for one external port — those get summed by
   * the stats consumer.
   *
   * Returns the cached snapshot synchronously when available so stats-update
   * handlers don't have to await; preload via `prefetchBindingsForOperators`
   * to make sure the snapshot is populated by the time execution starts.
   */
  public getBindingsForInstance(macroInstanceId: string, macroId: string): MacroBindings | undefined {
    // Delegate to the runtime-mapping-based resolver. The old prefix-based
    // approach broke when MacroExpander switched to fresh UUIDs for inner
    // op IDs (see backend MacroExpander.spliceIntoParent).
    return this.resolveBindingsViaRuntimeMapping(macroInstanceId, macroId);
  }

  /**
   * Walk a single body-relative binding down through any nested macros until
   * we hit a terminal non-macro inner op. At each level we prefix the inner
   * op ID with the accumulated instance prefix (so the final ID matches the
   * engine's `${outerInstanceId}--${nestedInstanceId}--…--${terminalOp}`
   * key).
   *
   * `externalPortIndex` is preserved through the chain — it identifies the
   * MACRO'S external port we started from, not the nested macro's port.
   * That's correct: every terminal binding still belongs to the same outer
   * macro port.
   */
  private resolveBinding(
    accumulatedPrefix: string,
    snapshot: {
      inputBindings: MacroPortBinding[];
      outputBindings: MacroPortBinding[];
      nestedMacros: Map<string, string>;
      innerSinks: string[];
    },
    binding: MacroPortBinding,
    isInput: boolean
  ): MacroPortBinding[] {
    const nestedMacroId = snapshot.nestedMacros.get(binding.innerOpId);
    if (!nestedMacroId) {
      // Terminal — return the binding with the full accumulated prefix.
      return [
        {
          externalPortIndex: binding.externalPortIndex,
          innerOpId: `${accumulatedPrefix}--${binding.innerOpId}`,
          innerPortIndex: binding.innerPortIndex,
        },
      ];
    }
    // Nested macro: load its bindings and follow the chain. The nested
    // macro's runtime instance ID is `${accumulatedPrefix}--${nestedInstanceId}`
    // (where nestedInstanceId is the body-relative ID we'd otherwise return).
    const nestedSnapshot = this.bodyBindingsSnapshot.get(nestedMacroId);
    if (!nestedSnapshot) {
      // Not yet cached — kick off fetch and return what we have so far. The
      // outer caller will see the partial resolution; once the nested macro's
      // body loads, the next stats emission will re-resolve correctly.
      this.getBodyBindings(nestedMacroId).subscribe({ error: () => undefined });
      return [
        {
          externalPortIndex: binding.externalPortIndex,
          innerOpId: `${accumulatedPrefix}--${binding.innerOpId}`,
          innerPortIndex: binding.innerPortIndex,
        },
      ];
    }
    // Find nested bindings matching the macro's port the outer binding
    // points to (binding.innerPortIndex is the nested macro's external port).
    const nestedBindings = isInput ? nestedSnapshot.inputBindings : nestedSnapshot.outputBindings;
    const nextLayerPrefix = `${accumulatedPrefix}--${binding.innerOpId}`;
    const matched = nestedBindings.filter(nb => nb.externalPortIndex === binding.innerPortIndex);
    if (matched.length === 0) {
      // Shouldn't happen for a well-formed body, but stay defensive.
      return [];
    }
    const resolved: MacroPortBinding[] = [];
    for (const nb of matched) {
      const carriedOver: MacroPortBinding = {
        externalPortIndex: binding.externalPortIndex, // preserve outer macro's external port
        innerOpId: nb.innerOpId, // body-relative inside the nested macro
        innerPortIndex: nb.innerPortIndex,
      };
      resolved.push(...this.resolveBinding(nextLayerPrefix, nestedSnapshot, carriedOver, isInput));
    }
    return resolved;
  }

  /**
   * Eagerly fetch bindings for every Macro op currently on the canvas, and
   * register the macro-instance → inner-op alias used by
   * `WorkflowResultService` so the result panel can show the macro's output
   * (we route to output port 0's inner producer as the canonical "macro
   * result"; a future multi-output UX could expose all outputs).
   * Idempotent (cache-keyed), so spamming on every op-add stream emission
   * does at most one HTTP per definition.
   */
  public prefetchBindingsForOperators(operators: readonly OperatorPredicate[]): void {
    for (const op of operators) {
      if (op.operatorType !== "Macro") continue;
      const macroId = op.operatorProperties?.["macroId"];
      if (typeof macroId !== "string" || macroId.length === 0) continue;
      const instanceId = op.operatorID;
      // Remember (instanceId → macroId) so cross-service lookups (e.g.
      // WorkflowStatusService.withMacroAggregates) can synthesize macro
      // stats without holding a reference to WorkflowActionService.
      this.registerMacroInstance(instanceId, macroId);
      this.getBodyBindings(macroId).subscribe({
        next: snapshot => {
          // After the first-level bindings load, ask for the recursive
          // resolved bindings — `getBindingsForInstance` chains through any
          // nested macros automatically. Output port 0 might resolve to a
          // single terminal inner op, OR (in the rare fan-out case) several;
          // for the v1 macro-result alias we still pick the first terminal.
          const resolved = this.getBindingsForInstance(instanceId, macroId);
          const out0 = resolved?.outputBindings.find(b => b.externalPortIndex === 0);
          if (out0) {
            this.workflowResultService.setMacroResultAlias(instanceId, out0.innerOpId);
            return;
          }
          // Mega-macro fallback: macro has 0 external outputs but its body may
          // contain sinks (e.g. CSVFileSink, SimpleSink for "View Results").
          // Engine auto-stores every terminal op's output (see
          // WorkflowCompiler.expandLogicalPlan), so the sink's result IS
          // materialized — clicking the macro op directly should reveal it.
          // We pick the first body sink and resolve it to its runtime UUID via
          // the macro-mapping cache. If the cache isn't populated yet (no Run
          // has happened), this is a no-op; the tick-driven re-prefetch in the
          // editor will re-run after the mapping fetch lands.
          if (snapshot.innerSinks.length === 0) return;
          const primarySinkBodyId = snapshot.innerSinks[0];
          for (const [runtimeUuid, prov] of this.runtimeMacroMapping.entries()) {
            if (prov.bodyOpId !== primarySinkBodyId) continue;
            if (prov.macroChain[prov.macroChain.length - 1] !== instanceId) continue;
            this.workflowResultService.setMacroResultAlias(instanceId, runtimeUuid);
            return;
          }
        },
        error: () => undefined,
      });
    }
  }

  private computeBodyBindings(detail: MacroDetail): {
    inputBindings: MacroPortBinding[];
    outputBindings: MacroPortBinding[];
    nestedMacros: Map<string, string>;
    innerSinks: string[];
  } {
    let body: MacroBody;
    try {
      body = JSON.parse(detail.content) as MacroBody;
    } catch {
      return { inputBindings: [], outputBindings: [], nestedMacros: new Map(), innerSinks: [] };
    }
    const inputMarkerByPortIndex = new Map<number, string>();
    const outputMarkerByPortIndex = new Map<number, string>();
    // Collect nested macro definitions: any Macro op inside the body whose
    // macroId we'll need to recursively resolve through. Keyed by the body-
    // relative operatorID since that's how the markers' links reference it.
    const nestedMacros = new Map<string, string>();
    // Inner sinks (body-relative IDs). Used as fallback result-alias targets
    // when the macro has 0 output ports: a "mega-macro" whose body contains
    // sinks but exposes nothing externally still wants its sink output to be
    // viewable in the result panel by clicking the macro op directly,
    // instead of forcing the user to drill in.
    const innerSinks: string[] = [];
    for (const raw of body.operators) {
      const op = raw as {
        operatorID?: string;
        operatorType?: string;
        portIndex?: number;
        macroId?: string;
      };
      if (typeof op.operatorID !== "string") continue;
      if (op.operatorType === "MacroInput" && typeof op.portIndex === "number") {
        inputMarkerByPortIndex.set(op.portIndex, op.operatorID);
      } else if (op.operatorType === "MacroOutput" && typeof op.portIndex === "number") {
        outputMarkerByPortIndex.set(op.portIndex, op.operatorID);
      } else if (op.operatorType === "Macro" && typeof op.macroId === "string" && op.macroId.length > 0) {
        nestedMacros.set(op.operatorID, op.macroId);
      } else if (
        typeof op.operatorType === "string" &&
        op.operatorType.toLowerCase().includes("sink")
      ) {
        innerSinks.push(op.operatorID);
      }
    }
    const markerIds = new Set([
      ...Array.from(inputMarkerByPortIndex.values()),
      ...Array.from(outputMarkerByPortIndex.values()),
    ]);
    // For each MacroInput, find body links marker -> innerOp@(p) — there can
    // be multiple if the macro's external input fans out to several inner
    // consumers (the rare "split feed" case in spliceIntoParent).
    const inputBindings: MacroPortBinding[] = [];
    for (const [portIndex, markerId] of inputMarkerByPortIndex.entries()) {
      for (const link of body.links) {
        if (link.fromOpId !== markerId) continue;
        if (markerIds.has(link.toOpId)) continue; // marker → marker is malformed; skip
        inputBindings.push({
          externalPortIndex: portIndex,
          innerOpId: link.toOpId,
          innerPortIndex: link.toPortId.id,
        });
      }
    }
    // For each MacroOutput, find body links innerOp@(p) -> marker — exactly
    // one producer per output marker (MacroExpander already enforces this).
    const outputBindings: MacroPortBinding[] = [];
    for (const [portIndex, markerId] of outputMarkerByPortIndex.entries()) {
      for (const link of body.links) {
        if (link.toOpId !== markerId) continue;
        if (markerIds.has(link.fromOpId)) continue;
        outputBindings.push({
          externalPortIndex: portIndex,
          innerOpId: link.fromOpId,
          innerPortIndex: link.fromPortId.id,
        });
      }
    }
    return { inputBindings, outputBindings, nestedMacros, innerSinks };
  }

  /**
   * Build a `MacroCreateRequest` from the operators the user has multi-selected
   * on the parent canvas, plus the boundary info the caller needs to swap the
   * selection out for a single MacroOp node on the canvas.
   *
   * Boundary handling: for every link crossing the selection edge we add a
   * `MacroInput` / `MacroOutput` marker inside the body (one per unique inner
   * port) and rewire it so MacroExpander can splice the body back into a
   * parent at compile time. Internal links (both endpoints inside the
   * selection) are passed through with port-ordinal IDs to match the
   * backend's PortIdentity shape.
   *
   * The returned `incomingEdges` / `outgoingEdges` describe each external link
   * that needs to be re-pointed at the new MacroOp instance (one entry per
   * link, where multiple external feeders can share the same `macroPortIndex`).
   */
  public buildMacroFromSelection(
    workflowActionService: WorkflowActionService,
    selectedOperatorIDs: readonly string[],
    name: string
  ): {
    request: MacroCreateRequest;
    incomingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
    outgoingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
    inputPortCount: number;
    outputPortCount: number;
  } {
    const graph = workflowActionService.getTexeraGraph();
    const selectedSet = new Set(selectedOperatorIDs);

    const innerOps = selectedOperatorIDs.map(opId => {
      const op = graph.getOperator(opId);
      // LogicalOp on the backend is reconstructed by Jackson from the same
      // shape the compiler uses — flat properties merged with the structural
      // bits (operatorID/Type/Version/ports).
      return {
        ...op.operatorProperties,
        operatorID: op.operatorID,
        operatorType: op.operatorType,
        operatorVersion: op.operatorVersion,
        inputPorts: op.inputPorts,
        outputPorts: op.outputPorts,
      };
    });

    const inputPortOrdinal = (operatorID: string, portID: string): number =>
      graph.getOperator(operatorID).inputPorts.findIndex(p => p.portID === portID);
    const outputPortOrdinal = (operatorID: string, portID: string): number =>
      graph.getOperator(operatorID).outputPorts.findIndex(p => p.portID === portID);

    const internal: { srcOp: string; srcPort: string; dstOp: string; dstPort: string }[] = [];
    const incoming: { srcOp: string; srcPort: string; dstOp: string; dstPort: string }[] = [];
    const outgoing: { srcOp: string; srcPort: string; dstOp: string; dstPort: string }[] = [];

    graph.getAllLinks().forEach(link => {
      const entry = {
        srcOp: link.source.operatorID,
        srcPort: link.source.portID,
        dstOp: link.target.operatorID,
        dstPort: link.target.portID,
      };
      const srcIn = selectedSet.has(entry.srcOp);
      const dstIn = selectedSet.has(entry.dstOp);
      if (srcIn && dstIn) internal.push(entry);
      else if (!srcIn && dstIn) incoming.push(entry);
      else if (srcIn && !dstIn) outgoing.push(entry);
    });

    // Preserve the sub-DAG's full external interface, not just the ports that
    // happen to be wired up at macro-creation time. Replacing a sub-DAG with a
    // macro op is a dataflow-equivalence transformation: every input port on
    // the selection that isn't fed by another selected op is a boundary input
    // (regardless of whether an external feeder is currently connected), and
    // symmetrically for output ports. That way a selection of
    // Filter → Projection where Projection's output is currently unwired still
    // surfaces that output as an external macro port the user can connect later.
    const internallyFedInputPorts = new Set(internal.map(l => `${l.dstOp}|${l.dstPort}`));
    const internallyConsumedOutputPorts = new Set(internal.map(l => `${l.srcOp}|${l.srcPort}`));

    type BoundaryPort = { innerOpId: string; innerPortID: string; innerPortIdx: number };
    const boundaryInputPorts: BoundaryPort[] = [];
    const boundaryOutputPorts: BoundaryPort[] = [];
    selectedOperatorIDs.forEach(opId => {
      const op = graph.getOperator(opId);
      op.inputPorts.forEach((port, idx) => {
        if (!internallyFedInputPorts.has(`${opId}|${port.portID}`)) {
          boundaryInputPorts.push({ innerOpId: opId, innerPortID: port.portID, innerPortIdx: idx });
        }
      });
      op.outputPorts.forEach((port, idx) => {
        if (!internallyConsumedOutputPorts.has(`${opId}|${port.portID}`)) {
          boundaryOutputPorts.push({ innerOpId: opId, innerPortID: port.portID, innerPortIdx: idx });
        }
      });
    });

    // Allocate one MacroInput/MacroOutput marker per boundary port. Marker
    // ordering follows the selection's visual order (selectedOperatorIDs ×
    // op.inputPorts), giving the user a stable mapping between macro ports
    // and the underlying sub-DAG ports.
    const inputMarkers = boundaryInputPorts.map((p, idx) => ({
      markerOpId: `MacroInput-operator-${uuid()}`,
      portIndex: idx,
      innerOpId: p.innerOpId,
      innerPortID: p.innerPortID,
      innerPortIdx: p.innerPortIdx,
    }));
    const outputMarkers = boundaryOutputPorts.map((p, idx) => ({
      markerOpId: `MacroOutput-operator-${uuid()}`,
      portIndex: idx,
      innerOpId: p.innerOpId,
      innerPortID: p.innerPortID,
      innerPortIdx: p.innerPortIdx,
    }));

    // Marker ports follow the backend's `PortDescription` shape (portID string,
    // disallowMultiInputs/isDynamicPort flags) so MacroBody parses cleanly when
    // DbMacroRegistry deserializes `workflow.content`. The actual port wiring
    // is derived from `portIndex` server-side via `operatorInfo`; these entries
    // exist purely to keep Jackson happy.
    const markerOps: unknown[] = [
      ...inputMarkers.map(m => ({
        operatorID: m.markerOpId,
        operatorType: "MacroInput",
        operatorVersion: "",
        portIndex: m.portIndex,
        displayName: "",
        inputPorts: [],
        outputPorts: [
          { portID: "output-0", displayName: "", disallowMultiInputs: false, isDynamicPort: false },
        ],
      })),
      ...outputMarkers.map(m => ({
        operatorID: m.markerOpId,
        operatorType: "MacroOutput",
        operatorVersion: "",
        portIndex: m.portIndex,
        displayName: "",
        inputPorts: [
          {
            portID: "input-0",
            displayName: "",
            disallowMultiInputs: false,
            isDynamicPort: false,
            dependencies: [],
          },
        ],
        outputPorts: [],
      })),
    ];

    const internalLinks: MacroBodyLink[] = internal.map(l => ({
      fromOpId: l.srcOp,
      fromPortId: { id: outputPortOrdinal(l.srcOp, l.srcPort), internal: false },
      toOpId: l.dstOp,
      toPortId: { id: inputPortOrdinal(l.dstOp, l.dstPort), internal: false },
    }));

    const inputMarkerLinks: MacroBodyLink[] = inputMarkers.map(m => ({
      fromOpId: m.markerOpId,
      fromPortId: { id: 0, internal: false },
      toOpId: m.innerOpId,
      toPortId: { id: m.innerPortIdx, internal: false },
    }));

    const outputMarkerLinks: MacroBodyLink[] = outputMarkers.map(m => ({
      fromOpId: m.innerOpId,
      fromPortId: { id: m.innerPortIdx, internal: false },
      toOpId: m.markerOpId,
      toPortId: { id: 0, internal: false },
    }));

    const portSpec: PortSpec = {
      inputs: inputMarkers.map(m => ({ index: m.portIndex })),
      outputs: outputMarkers.map(m => ({ index: m.portIndex })),
    };

    const body: MacroBody = {
      operators: [...innerOps, ...markerOps],
      links: [...internalLinks, ...inputMarkerLinks, ...outputMarkerLinks],
      inputs: portSpec.inputs,
      outputs: portSpec.outputs,
    };

    // Per-link rewire instructions. Several external links may share the same
    // macroPortIndex when they all target the same inner port.
    const inputIdxByInnerPort = new Map(
      inputMarkers.map(m => [`${m.innerOpId}|${m.innerPortID}`, m.portIndex])
    );
    const outputIdxByInnerPort = new Map(
      outputMarkers.map(m => [`${m.innerOpId}|${m.innerPortID}`, m.portIndex])
    );

    const incomingEdges = incoming.map(l => ({
      externalOpId: l.srcOp,
      externalPortID: l.srcPort,
      macroPortIndex: inputIdxByInnerPort.get(`${l.dstOp}|${l.dstPort}`) as number,
    }));
    const outgoingEdges = outgoing.map(l => ({
      externalOpId: l.dstOp,
      externalPortID: l.dstPort,
      macroPortIndex: outputIdxByInnerPort.get(`${l.srcOp}|${l.srcPort}`) as number,
    }));

    // Auto-generate a 1-line description so users don't get an empty
    // description on the dashboard / palette tooltip. Format:
    // "Filter → Projection block (2 ops, 1 in/1 out)".
    const innerOpTypes = selectedOperatorIDs.map(opId => graph.getOperator(opId).operatorType);
    const description = this.autoDescriptionForBody(
      innerOpTypes,
      inputMarkers.length,
      outputMarkers.length
    );

    return {
      request: {
        name,
        description,
        content: JSON.stringify(body),
        portSpec,
      },
      incomingEdges,
      outgoingEdges,
      inputPortCount: inputMarkers.length,
      outputPortCount: outputMarkers.length,
    };
  }

  /**
   * Compose a one-line description for a freshly-created macro based on the
   * operator-type composition of its body and its external port shape. The
   * resulting string lands on the macro definition's `description` field and
   * shows up in the palette tooltip + the dashboard macro browser.
   */
  private autoDescriptionForBody(
    innerOpTypes: readonly string[],
    inputPortCount: number,
    outputPortCount: number
  ): string {
    if (innerOpTypes.length === 0) return "Empty macro";
    const head = innerOpTypes.slice(0, 3).join(" → ");
    const chain = innerOpTypes.length > 3 ? `${head} +${innerOpTypes.length - 3}` : head;
    const portShape = `${inputPortCount} in / ${outputPortCount} out`;
    return `${chain} (${innerOpTypes.length} ops, ${portShape})`;
  }

  /**
   * Adapt a backend `MacroDetail` (whose `content` is a serialized `MacroBody`)
   * into a `Workflow`-shaped object the existing `reloadWorkflow` flow can
   * consume. Used by the drill-down editor route.
   *
   * v1 caveats:
   *  - operator positions are auto-laid-out (MacroInput on the left, regular
   *    inner ops in the middle, MacroOutput on the right) because the body
   *    doesn't carry positions yet.
   *  - inner ops that came from the canvas already have `PortDescription`
   *    ports; marker ops were authored with backend `PortIdentity` shape and
   *    are normalized here.
   */
  public macroDetailToWorkflow(detail: MacroDetail): Workflow {
    const body = JSON.parse(detail.content) as MacroBody;

    const operators = body.operators.map(raw => this.normalizeBodyOperator(raw));
    const operatorPositions = this.autoLayoutMacroBody(
      operators,
      body.links.map(l => ({ fromOpId: l.fromOpId, toOpId: l.toOpId }))
    );
    const links = body.links
      .map(ml => this.macroLinkToOperatorLink(ml, operators))
      .filter((l): l is OperatorLink => l !== null);

    const content: WorkflowContent = {
      operators,
      operatorPositions,
      links,
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
    };

    return {
      wid: detail.wid,
      name: detail.name,
      description: detail.description,
      creationTime: new Date(detail.creationTime).getTime(),
      lastModifiedTime: new Date(detail.lastModifiedTime).getTime(),
      isPublished: detail.isPublic ? 1 : 0,
      readonly: detail.readonly,
      content,
    };
  }

  private normalizeBodyOperator(raw: unknown): OperatorPredicate {
    const r = raw as Record<string, unknown>;
    const {
      operatorID,
      operatorType,
      operatorVersion,
      inputPorts,
      outputPorts,
      ...rest
    } = r as {
      operatorID: string;
      operatorType: string;
      operatorVersion?: string;
      inputPorts?: unknown[];
      outputPorts?: unknown[];
    } & Record<string, unknown>;

    return {
      operatorID,
      operatorType,
      operatorVersion: operatorVersion ?? "",
      operatorProperties: rest,
      inputPorts: this.normalizePortList(inputPorts ?? [], "input"),
      outputPorts: this.normalizePortList(outputPorts ?? [], "output"),
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: typeof rest["displayName"] === "string" ? (rest["displayName"] as string) : undefined,
      dynamicInputPorts: false,
      dynamicOutputPorts: false,
    };
  }

  private normalizePortList(ports: unknown[], dir: "input" | "output"): PortDescription[] {
    return ports.map((raw, idx) => {
      const p = raw as Record<string, unknown>;
      // Already PortDescription-shaped (came from the canvas serialization).
      if (typeof p?.["portID"] === "string") {
        return p as unknown as PortDescription;
      }
      // Backend PortIdentity shape ({id: {id, internal}, displayName, ...}) —
      // synthesize a portID using the ordinal.
      const displayName = typeof p?.["displayName"] === "string" ? (p["displayName"] as string) : "";
      const base: PortDescription = {
        portID: `${dir}-${idx}`,
        displayName,
        disallowMultiInputs: false,
        isDynamicPort: false,
      };
      return dir === "input" ? { ...base, dependencies: [] } : base;
    });
  }

  private macroLinkToOperatorLink(
    ml: MacroBodyLink,
    operators: OperatorPredicate[]
  ): OperatorLink | null {
    const fromOp = operators.find(o => o.operatorID === ml.fromOpId);
    const toOp = operators.find(o => o.operatorID === ml.toOpId);
    if (!fromOp || !toOp) return null;
    const fromPortID = fromOp.outputPorts[ml.fromPortId.id]?.portID;
    const toPortID = toOp.inputPorts[ml.toPortId.id]?.portID;
    if (!fromPortID || !toPortID) return null;
    return {
      linkID: `macro-link-${uuid()}`,
      source: { operatorID: ml.fromOpId, portID: fromPortID },
      target: { operatorID: ml.toOpId, portID: toPortID },
    };
  }

  /**
   * Auto-layout the macro body using dagre's directed-graph algorithm — the
   * same engine the main canvas's "Auto-layout" button uses. Edges come from
   * the body's link list so connected ops sit at logical ranks; MacroInput
   * markers act as source ranks (left edge) and MacroOutput markers as sink
   * ranks (right edge). Settings mirror `JointGraphWrapper.autoLayoutJoint`
   * for consistency between parent canvas and macro-body view.
   *
   * Why dagre, not the manual 3-column layout it replaces: the previous
   * placeholder put every middle op in a vertical stack, which made
   * non-linear bodies (joins, fan-outs) look like spaghetti. With dagre,
   * a Filter→Projection→Join body lays out naturally with the join's two
   * inputs side-by-side.
   *
   * We use dagre directly (not `joint.layout.DirectedGraph.layout`) because
   * at this point the body operators haven't been rendered into JointJS
   * cells yet — we're computing the positions that will be passed into
   * `WorkflowContent.operatorPositions` on the first drill-down load.
   */
  private autoLayoutMacroBody(
    operators: OperatorPredicate[],
    links: { fromOpId: string; toOpId: string }[]
  ): { [id: string]: Point } {
    if (operators.length === 0) return {};
    // Use dagre's bundled graphlib constructor so the types line up cleanly
    // with `dagre.layout(g)` below. `@types/graphlib` and `@types/dagre` are
    // independent packages whose Graph definitions don't unify directly.
    const g = new dagre.graphlib.Graph();
    g.setGraph({
      nodesep: 100,
      edgesep: 150,
      ranksep: 80,
      ranker: "tight-tree",
      rankdir: "LR",
    });
    g.setDefaultEdgeLabel(() => ({}));
    // Approximate node size — close enough to a typical Texera operator card.
    // Dagre uses these for collision avoidance + edge routing only; the actual
    // rendered op size is fixed by the joint shape, so we don't need pixel
    // accuracy here.
    const NODE_W = 160;
    const NODE_H = 60;
    operators.forEach(op => {
      g.setNode(op.operatorID, { width: NODE_W, height: NODE_H });
    });
    links.forEach(l => {
      // dagre tolerates edges to/from unknown nodes silently, but we filter
      // anyway — body links can reference markers we haven't normalized into
      // operators in pathological cases.
      if (g.hasNode(l.fromOpId) && g.hasNode(l.toOpId)) {
        g.setEdge(l.fromOpId, l.toOpId);
      }
    });
    dagre.layout(g);
    const positions: { [id: string]: Point } = {};
    g.nodes().forEach(id => {
      const node: { x: number; y: number } = g.node(id);
      // dagre returns the CENTER of the node; joint expects the TOP-LEFT.
      // Subtract half the width/height.
      positions[id] = { x: node.x - NODE_W / 2, y: node.y - NODE_H / 2 };
    });
    return positions;
  }
}
