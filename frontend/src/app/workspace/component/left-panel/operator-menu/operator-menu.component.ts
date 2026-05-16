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

import { Component, ElementRef, ViewChild } from "@angular/core";
import Fuse from "fuse.js";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { GroupInfo, OperatorSchema } from "../../../types/operator-schema.interface";
import { DragDropService } from "../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../../service/workflow-graph/util/workflow-util.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  NzAutocompleteOptionComponent,
  NzAutocompleteTriggerDirective,
  NzAutocompleteComponent,
} from "ng-zorro-antd/auto-complete";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputDirective } from "ng-zorro-antd/input";
import { FormsModule } from "@angular/forms";
import { NgFor, NgIf, NgTemplateOutlet } from "@angular/common";
import { OperatorLabelComponent } from "./operator-label/operator-label.component";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { MacroService, MacroSummary } from "../../../service/macro/macro.service";
import { MacroSuggestionService, MacroSuggestion } from "../../../service/macro/macro-suggestion.service";
import { MacroFusionService } from "../../../service/macro/macro-fusion.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { forkJoin, of } from "rxjs";
import { catchError } from "rxjs/operators";
import { OperatorPredicate } from "../../../types/workflow-common.interface";
import { NzMessageService } from "ng-zorro-antd/message";

@UntilDestroy()
@Component({
  selector: "texera-operator-menu",
  templateUrl: "operator-menu.component.html",
  styleUrls: ["operator-menu.component.scss"],
  imports: [
    NzSpaceCompactItemDirective,
    NzInputDirective,
    FormsModule,
    NzAutocompleteTriggerDirective,
    NzAutocompleteComponent,
    NgFor,
    NgIf,
    NzAutocompleteOptionComponent,
    OperatorLabelComponent,
    NgTemplateOutlet,
    NzCollapseComponent,
    NzCollapsePanelComponent,
  ],
})
export class OperatorMenuComponent {
  public opList = new Map<string, Array<OperatorSchema>>();
  public groupNames: ReadonlyArray<GroupInfo> = [];

  // The user's saved macros — surfaced as a "Your Macros" section in the
  // palette so they can be reused on other workflows by clicking the entry.
  // We use the existing operator-label rendering by exposing each macro as
  // an OperatorSchema-shaped object whose operatorType is the literal
  // "Macro" and whose userFriendlyName is the macro name. The drag/click
  // handler peeks at `__macroSummary` on the schema to fill in macroId,
  // inputPortCount, outputPortCount when instantiating the operator
  // predicate.
  public macroList: (OperatorSchema & { __macroSummary?: MacroSummary })[] = [];
  // Search-box filter applied to `macroList` in the template. Case-insensitive
  // substring match on the macro's display name. Empty string = show all.
  public macroFilterText: string = "";
  public get filteredMacroList(): (OperatorSchema & { __macroSummary?: MacroSummary })[] {
    const q = this.macroFilterText.trim().toLowerCase();
    if (q.length === 0) return this.macroList;
    return this.macroList.filter(m =>
      (m.additionalMetadata.userFriendlyName || "").toLowerCase().includes(q)
    );
  }

  /**
   * Cache for the per-macro auto-inferred category. Keyed by macroId (the
   * wid as string). Populated lazily on demand and after each listMacros
   * refresh so the palette can group by category without an extra HTTP call
   * per macro on every render. Categories are derived from the macro body's
   * operator-type composition — see `inferCategory()`.
   */
  private macroCategoryCache = new Map<string, string>();

  /**
   * Cheap heuristic that maps a macro's body to a category label. We pull
   * the body once via `getMacro` and inspect the operator-type composition:
   *   - majority Aggregate/GroupBy/Sort → "aggregation"
   *   - majority Visualizer/Chart → "visualization"
   *   - majority PythonUDF/Lambda → "transformation"
   *   - else (Filter/Projection/Regex/...) → "preprocessing"
   *
   * Returns "uncategorized" if we can't load the body. The category is
   * cached the first time we look up a given macroId.
   */
  public categoryForMacro(macroSchema: OperatorSchema & { __macroSummary?: MacroSummary }): string {
    const wid = macroSchema.__macroSummary?.wid;
    if (!wid) return "uncategorized";
    const key = String(wid);
    const cached = this.macroCategoryCache.get(key);
    if (cached) return cached;
    // Fire and forget the body fetch; once it lands, fill the cache so a
    // future render uses the real category.
    this.macroService.getMacro(wid).subscribe({
      next: detail => {
        try {
          const body = JSON.parse(detail.content) as {
            operators?: Array<{ operatorType?: string }>;
          };
          this.macroCategoryCache.set(key, this.inferCategory(body.operators ?? []));
        } catch {
          this.macroCategoryCache.set(key, "uncategorized");
        }
      },
      error: () => this.macroCategoryCache.set(key, "uncategorized"),
    });
    return "loading…";
  }

  private inferCategory(operators: Array<{ operatorType?: string }>): string {
    const inner = operators.filter(
      o => o.operatorType !== "MacroInput" && o.operatorType !== "MacroOutput"
    );
    if (inner.length === 0) return "uncategorized";
    const counts = { agg: 0, viz: 0, transform: 0, prep: 0 };
    for (const op of inner) {
      const t = (op.operatorType ?? "").toLowerCase();
      if (/aggregate|groupby|sort|reduce|count/.test(t)) counts.agg++;
      else if (/visualizer|chart|wordcloud|plot|piechart|barchart|linechart/.test(t)) counts.viz++;
      else if (/python|lambda|udf/.test(t)) counts.transform++;
      else counts.prep++;
    }
    const max = Math.max(counts.agg, counts.viz, counts.transform, counts.prep);
    if (max === counts.viz && counts.viz > 0) return "visualization";
    if (max === counts.agg && counts.agg > 0) return "aggregation";
    if (max === counts.transform && counts.transform > 0) return "transformation";
    return "preprocessing";
  }

  /**
   * Group the filtered macro list by auto-inferred category, returning a
   * stable category-ordered array so the palette renders deterministically.
   * Empty categories are omitted. Categories the user hasn't seen yet emit
   * a single "loading…" bucket that swaps in once getMacro lands.
   */
  public get groupedMacroList(): Array<{ category: string; macros: (OperatorSchema & { __macroSummary?: MacroSummary })[] }> {
    const categoryOrder = ["preprocessing", "transformation", "aggregation", "visualization", "uncategorized", "loading…"];
    const grouped = new Map<string, (OperatorSchema & { __macroSummary?: MacroSummary })[]>();
    for (const macro of this.filteredMacroList) {
      const cat = this.categoryForMacro(macro);
      if (!grouped.has(cat)) grouped.set(cat, []);
      grouped.get(cat)!.push(macro);
    }
    const result: Array<{ category: string; macros: (OperatorSchema & { __macroSummary?: MacroSummary })[] }> = [];
    for (const cat of categoryOrder) {
      if (grouped.has(cat)) result.push({ category: cat, macros: grouped.get(cat)! });
    }
    return result;
  }

  // Inline panel for "AI" macro suggestions. Populated on user click, then
  // cleared after a selection is materialized. Empty list means panel is
  // collapsed.
  public suggestions: MacroSuggestion[] = [];
  public isSuggesting: boolean = false;
  // Proactive count — how many candidates the heuristic would surface RIGHT
  // NOW if the user clicked the button. Refreshed whenever the canvas changes
  // (with a short debounce). Surfaced as a small chip on the Suggest button
  // so the user sees "4 candidates found" without having to click. This is
  // the "agent is watching your workflow" feel.
  public availableCandidateCount: number = 0;

  // input value of the search input box
  public searchInputValue: string = "";
  // search autocomplete suggestion list
  public autocompleteOptions: OperatorSchema[] = [];

  public canModify = true;

  // fuzzy search using fuse.js. See parameters in options at https://fusejs.io/
  public fuse = new Fuse([] as ReadonlyArray<OperatorSchema>, {
    shouldSort: true,
    threshold: 0.3,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["additionalMetadata.userFriendlyName"],
  });

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private dragDropService: DragDropService,
    private macroService: MacroService,
    private macroSuggestionService: MacroSuggestionService,
    private macroFusionService: MacroFusionService,
    private jointUIService: JointUIService,
    private message: NzMessageService
  ) {
    // Load the user's saved macros for the "Your Macros" palette section.
    this.macroService.listMacros().subscribe({
      next: (summaries: MacroSummary[]) => {
        this.macroList = summaries.map(m => this.macroSummaryToSchema(m));
      },
      error: () => undefined,
    });
    // clear the search box if an operator is dropped from operator search box
    this.dragDropService.operatorDropStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.searchInputValue = "";
      this.autocompleteOptions = [];
    });
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => (this.canModify = canModify));
    // Proactive macro-suggestion watcher: every time the workflow graph
    // changes (add/delete/relink), debounce 700ms then run the heuristic
    // suggester silently and update `availableCandidateCount`. The UI badges
    // the Suggest button so the user discovers patterns without clicking.
    // 700ms is long enough that mid-drag operator placements don't trigger
    // a flicker, short enough to feel responsive after a click settles.
    const refreshSuggestionCount = () => {
      try {
        const graph = this.workflowActionService.getTexeraGraph();
        const list = this.macroSuggestionService.suggestMacros(graph);
        this.availableCandidateCount = list.length;
      } catch {
        this.availableCandidateCount = 0;
      }
    };
    let debounceHandle: ReturnType<typeof setTimeout> | null = null;
    const scheduleRefresh = () => {
      if (debounceHandle) clearTimeout(debounceHandle);
      debounceHandle = setTimeout(refreshSuggestionCount, 700);
    };
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorAddStream()
      .pipe(untilDestroyed(this))
      .subscribe(scheduleRefresh);
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDeleteStream()
      .pipe(untilDestroyed(this))
      .subscribe(scheduleRefresh);
    this.workflowActionService
      .getTexeraGraph()
      .getLinkAddStream()
      .pipe(untilDestroyed(this))
      .subscribe(scheduleRefresh);
    this.workflowActionService
      .getTexeraGraph()
      .getLinkDeleteStream()
      .pipe(untilDestroyed(this))
      .subscribe(scheduleRefresh);
    // Kick off an initial scan once the canvas has settled.
    setTimeout(refreshSuggestionCount, 1200);

    this.operatorMetadataService
      .getOperatorMetadata()
      .pipe(untilDestroyed(this))
      .subscribe(operatorMetadata => {
        const ops = operatorMetadata.operators.filter(
          operatorSchema => operatorSchema.operatorType !== "PythonUDF" && operatorSchema.operatorType !== "Dummy"
        );
        this.groupNames = operatorMetadata.groups;
        ops.forEach(x => {
          if (x.operatorType !== "Sleep") {
            const group = x.additionalMetadata.operatorGroupName;
            const list = this.opList.get(group) || [];
            list.push(x);
            this.opList.set(group, list);
          }
        });
        this.opList.forEach(value => {
          value.sort((a, b) => a.operatorType.localeCompare(b.operatorType));
        });
        this.fuse.setCollection(ops);
      });
  }

  /**
   * create the search results observable
   * whenever the search box text is changed, perform the search using fuse.js
   */
  onInput(e: Event): void {
    const v = (e.target as HTMLInputElement).value;
    if (v === null || v.trim().length === 0) {
      this.autocompleteOptions = [];
    }
    this.autocompleteOptions = this.fuse.search(v).map(item => {
      return item.item;
    });
  }

  /**
   * handles the event when an operator search option is selected.
   * adds the operator to the canvas and clears the text in the search box
   */
  onSelectionChange(e: NzAutocompleteOptionComponent): void {
    const selectSchema = e.nzValue as OperatorSchema;
    // add the operator to the graph on select (position relative to the current viewpoint)
    const origin = this.workflowActionService.getJointGraphWrapper().getMainJointPaper()?.translate();
    const point = { x: 400 - (origin?.tx ?? 0), y: 200 - (origin?.ty ?? 0) };
    this.workflowActionService.addOperator(
      this.workflowUtilService.getNewOperatorPredicate(selectSchema.operatorType),
      point
    );

    // asynchronously immediately clear the search input and suggestions
    // because ng-zorro shows the selected value if it's synchronously
    setTimeout(() => {
      this.searchInputValue = "";
      this.autocompleteOptions = [];
    }, 0);
  }

  /**
   * Adapt a backend `MacroSummary` into an `OperatorSchema`-shaped row the
   * existing operator-label component can render. The macro's port count
   * and definition wid are stashed on `__macroSummary` so click-to-add can
   * build the right `OperatorPredicate` without re-fetching the macro.
   */
  private macroSummaryToSchema(m: MacroSummary): OperatorSchema & { __macroSummary: MacroSummary } {
    return {
      operatorType: "Macro",
      jsonSchema: { type: "object", properties: {} } as unknown as OperatorSchema["jsonSchema"],
      additionalMetadata: {
        userFriendlyName: m.name,
        operatorDescription: m.description ?? `Macro from workflow #${m.wid}`,
        operatorGroupName: "Your Macros",
        inputPorts: m.portSpec.inputs.map(p => ({ displayName: `in-${p.index}` })),
        outputPorts: m.portSpec.outputs.map(p => ({ displayName: `out-${p.index}` })),
        dynamicInputPorts: false,
        dynamicOutputPorts: false,
        supportReconfiguration: false,
        allowPortCustomization: false,
      } as unknown as OperatorSchema["additionalMetadata"],
      operatorVersion: "",
      __macroSummary: m,
    };
  }

  /**
   * Place a saved macro on the canvas. Builds a fresh `OperatorPredicate`
   * matching the shape created by `swapSelectionWithMacroNode` so the
   * downstream validation/render/execution paths see a normal Macro op.
   */
  public onAddMacro(macroSchema: OperatorSchema & { __macroSummary?: MacroSummary }): void {
    const m = macroSchema.__macroSummary;
    if (!m) return;
    const inputPortCount = m.portSpec.inputs.length;
    const outputPortCount = m.portSpec.outputs.length;
    const inputPorts = Array.from({ length: inputPortCount }, (_, i) => ({
      portID: `input-${i}`,
      displayName: `in-${i}`,
      disallowMultiInputs: false,
      isDynamicPort: false,
      dependencies: [],
    }));
    const outputPorts = Array.from({ length: outputPortCount }, (_, i) => ({
      portID: `output-${i}`,
      displayName: `out-${i}`,
      disallowMultiInputs: false,
      isDynamicPort: false,
    }));
    const predicate: OperatorPredicate = {
      operatorID: `Macro-operator-${this.workflowUtilService.getOperatorRandomUUID()}`,
      operatorType: "Macro",
      operatorVersion: "",
      operatorProperties: {
        macroId: String(m.wid),
        macroVersion: 1,
        linkMode: "LIVE",
        inputPortCount,
        outputPortCount,
        displayName: m.name,
        // Mark this instance as in-sync-with the macro's CURRENT
        // lastModifiedTime. If the macro is later edited, this stays put;
        // the "refresh macro (stale)" context-menu item then surfaces.
        macroSyncedAt:
          typeof m.lastModifiedTime === "number"
            ? m.lastModifiedTime
            : new Date(m.lastModifiedTime as unknown as string).getTime(),
      },
      inputPorts,
      outputPorts,
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: m.name,
      dynamicInputPorts: false,
      dynamicOutputPorts: false,
    };
    const origin = this.workflowActionService.getJointGraphWrapper().getMainJointPaper()?.translate();
    const point = { x: 400 - (origin?.tx ?? 0), y: 200 - (origin?.ty ?? 0) };
    this.workflowActionService.addOperator(predicate, point);
  }

  /**
   * "Suggest Macros (AI)" button — runs the heuristic suggester over the
   * current canvas and surfaces ranked candidates in the inline panel.
   * v1 is local heuristics; a future swap to chat-assistant-service for
   * LLM-ranked candidates would replace this body with an HTTP call that
   * returns the same `MacroSuggestion[]` shape.
   */
  public onSuggestMacros(): void {
    this.isSuggesting = true;
    // Defer to next tick so the spinner can paint — heuristic is fast (<10ms)
    // but pretending it's "thinking" matches the AI-agent UX the demo wants.
    setTimeout(() => {
      try {
        const graph = this.workflowActionService.getTexeraGraph();
        this.suggestions = this.macroSuggestionService.suggestMacros(graph);
        if (this.suggestions.length === 0) {
          this.message.info("No good macro candidates found. Try adding more operators!");
        } else {
          this.message.success(`Found ${this.suggestions.length} candidate(s).`);
          // Highlight the suggestion's operators on the canvas so the user
          // sees which ops would be encapsulated. Limit to the top suggestion
          // to avoid overwhelming the canvas.
          const jw = this.workflowActionService.getJointGraphWrapper();
          jw.unhighlightOperators(...jw.getCurrentHighlightedOperatorIDs());
          jw.setMultiSelectMode(true);
          jw.highlightOperators(...this.suggestions[0].operatorIds);
        }
      } finally {
        this.isSuggesting = false;
      }
    }, 250);
  }

  /**
   * Materialize a suggested macro directly: call
   * `MacroService.createMacroFromSelection` to build the definition, POST
   * it, and swap the selection on the canvas with a single Macro op — same
   * shape the right-click → Create Macro path produces. Pre-fix this only
   * highlighted+selected the operators and asked the user to right-click;
   * doing it inline removes one step from the demo and reads more like an
   * agent action.
   *
   * When the suggestion is a *recurring pattern* (id starts with "pattern-"),
   * we also offer to swap the other occurrences of the same pattern with
   * fresh instances of the same macro — the "agent did the refactor for me"
   * demo moment. The peer occurrences are detected on-the-fly by re-running
   * the suggester and matching on `suggestedName` (the pattern signature is
   * the same across all occurrences).
   */
  public onMaterializeSuggestion(suggestion: MacroSuggestion): void {
    const proposedName = suggestion.suggestedName || `macro-${Date.now()}`;
    const name = window.prompt("Macro name", proposedName);
    if (!name) return;
    const isPattern = suggestion.id.startsWith("pattern-");
    // Capture sibling occurrences BEFORE we mutate the canvas. We need IDs
    // that won't have been swapped out from under us, which is exactly the
    // current snapshot of `this.suggestions`.
    const peerOccurrences = isPattern
      ? this.suggestions.filter(
          s =>
            s.id.startsWith("pattern-") &&
            s.suggestedName === suggestion.suggestedName &&
            s.operatorIds.join("|") !== suggestion.operatorIds.join("|")
        )
      : [];
    this.macroService.createMacroFromSelection(this.workflowActionService, suggestion.operatorIds, name).subscribe({
      next: detail => {
        this.message.success(`Created macro "${detail.name}" (wid=${detail.wid})`);
        this.suggestions = [];
        if (peerOccurrences.length === 0) return;
        // Batch-swap remaining occurrences. Each may fail independently (e.g.
        // shape didn't match after all); count successes vs. skips for the
        // toast.
        let swapped = 0;
        let skipped = 0;
        for (const peer of peerOccurrences) {
          const ok = this.macroService.swapSelectionWithExistingMacro(
            this.workflowActionService,
            detail,
            peer.operatorIds
          );
          if (ok) swapped++;
          else skipped++;
        }
        if (swapped > 0) {
          this.message.success(
            `Refactored ${swapped} additional occurrence${swapped === 1 ? "" : "s"} ` +
              `to use "${detail.name}"` +
              (skipped > 0 ? ` (${skipped} skipped — shape didn't match)` : "")
          );
        } else if (skipped > 0) {
          this.message.warning(
            `Could not auto-refactor the other ${skipped} occurrence(s); shapes didn't match the macro's ports.`
          );
        }
      },
      error: err => this.message.error(`Failed to create macro: ${err?.message ?? err}`),
    });
  }

  public dismissSuggestions(): void {
    this.suggestions = [];
  }

  /**
   * Hovering a suggestion row should flash that suggestion's operators on
   * the canvas as a visual preview. We highlight via JointGraphWrapper —
   * same path that selection uses — so the canvas treatment matches the
   * "selected" look. On unhover we restore whatever the user had highlighted
   * before they started hovering (typically: nothing).
   *
   * Stash the prior highlight set in `preHoverHighlight` so unhover can
   * cleanly undo without clobbering other UI state.
   */
  private preHoverHighlight: string[] = [];
  public onSuggestionHover(suggestion: MacroSuggestion): void {
    const jw = this.workflowActionService.getJointGraphWrapper();
    this.preHoverHighlight = Array.from(jw.getCurrentHighlightedOperatorIDs());
    jw.unhighlightOperators(...this.preHoverHighlight);
    jw.setMultiSelectMode(true);
    jw.highlightOperators(...suggestion.operatorIds);
  }

  public onSuggestionUnhover(): void {
    const jw = this.workflowActionService.getJointGraphWrapper();
    jw.unhighlightOperators(...jw.getCurrentHighlightedOperatorIDs());
    if (this.preHoverHighlight.length > 0) {
      jw.highlightOperators(...this.preHoverHighlight);
    }
    this.preHoverHighlight = [];
  }

  /** Currently-running "fuse all" indicator — disables the button and renders progress. */
  public fuseAllInProgress: boolean = false;
  /** Count of Macro ops on the current canvas that are NOT yet fused. Drives the button label. */
  public unfusedMacroCountOnCanvas(): number {
    try {
      const graph = this.workflowActionService.getTexeraGraph();
      return graph.getAllOperators().filter(op => {
        if (op.operatorType !== "Macro") return false;
        const f = op.operatorProperties?.["fusion"] as { verified?: boolean } | undefined;
        return f?.verified !== true;
      }).length;
    } catch {
      return 0;
    }
  }

  /**
   * "Fuse all macros in workflow" — the one-click batch perf optimization.
   * Walks every Macro op on the parent canvas, calls MacroFusionService for
   * each, stamps the resulting fusion onto operatorProperties, and refreshes
   * the canvas visual. Errors per-macro are surfaced individually so a
   * single un-fusable macro doesn't abort the batch.
   */
  public onFuseAllMacros(): void {
    if (this.fuseAllInProgress) return;
    const graph = this.workflowActionService.getTexeraGraph();
    const macros = graph.getAllOperators().filter(op => {
      if (op.operatorType !== "Macro") return false;
      const f = op.operatorProperties?.["fusion"] as { verified?: boolean } | undefined;
      return f?.verified !== true;
    });
    if (macros.length === 0) {
      this.message.info("No fusable macros on the canvas.");
      return;
    }
    this.fuseAllInProgress = true;
    const requests = macros.map(op => {
      const macroId = op.operatorProperties?.["macroId"] as string | undefined;
      if (!macroId) return of({ opId: op.operatorID, fused: false, reason: "no macroId" });
      return this.macroFusionService.generateFusion(macroId).pipe(
        catchError(err => of({ opId: op.operatorID, fused: false, reason: String(err?.message ?? err) }))
      );
    });
    forkJoin(requests).subscribe({
      next: results => {
        let fusedCount = 0;
        let failedCount = 0;
        const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
        results.forEach((result: any, idx: number) => {
          const macroOp = macros[idx];
          if (result.fused === false || !result.verified) {
            failedCount++;
            return;
          }
          // result is a FusionResult
          const newProps = {
            ...macroOp.operatorProperties,
            fusion: this.macroFusionService.toFusionPayload(result),
          };
          this.workflowActionService.setOperatorProperty(macroOp.operatorID, newProps);
          if (paper) {
            this.jointUIService.refreshMacroFusionStyle(
              paper,
              macroOp.operatorID,
              true,
              result.estimatedSpeedup
            );
          }
          fusedCount++;
        });
        if (fusedCount > 0) {
          this.message.success(
            `Fused ${fusedCount} macro${fusedCount === 1 ? "" : "s"} for performance` +
              (failedCount > 0 ? ` (${failedCount} skipped)` : "")
          );
        } else {
          this.message.warning(`No macros could be fused (${failedCount} failed).`);
        }
        this.fuseAllInProgress = false;
      },
      error: err => {
        this.fuseAllInProgress = false;
        this.message.error(`Batch fuse failed: ${err?.message ?? err}`);
      },
    });
  }

  /** Reference to the hidden file input — clicked programmatically by `onTriggerImportMacro`. */
  @ViewChild("macroImportFile") macroImportFile?: ElementRef<HTMLInputElement>;

  /**
   * Trigger a browser download of one macro definition. Exposed off the
   * palette item's small "⤓" affordance. The actual HTTP fetch + Blob
   * creation lives in `MacroService.exportMacroToFile`.
   */
  public onExportMacro(summary: MacroSummary): void {
    this.macroService.exportMacroToFile(summary.wid).subscribe({
      next: () => this.message.success(`Exported "${summary.name}".`),
      error: err => this.message.error(`Export failed: ${err?.message ?? err}`),
    });
  }

  /**
   * Open the OS file picker for macro JSON files. The change handler is
   * `onImportMacroFile`. Using a hidden file input + button is the standard
   * dance for getting a click-styled "Upload" affordance.
   */
  public onTriggerImportMacro(): void {
    this.macroImportFile?.nativeElement.click();
  }

  /**
   * File picker callback — read the JSON, POST it as a fresh macro
   * definition, and refresh the "Your Macros" palette so the imported
   * macro shows up immediately.
   */
  public onImportMacroFile(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const text = String(reader.result);
      try {
        this.macroService.importMacroFromJson(text).subscribe({
          next: detail => {
            this.message.success(`Imported macro "${detail.name}" (wid=${detail.wid})`);
            // Refresh the palette to surface the new macro.
            this.macroService.listMacros().subscribe({
              next: (summaries: MacroSummary[]) => {
                this.macroList = summaries.map(m => this.macroSummaryToSchema(m));
              },
            });
          },
          error: err => this.message.error(`Import failed: ${err?.message ?? err}`),
        });
      } catch (e: any) {
        this.message.error(`Import failed: ${e?.message ?? e}`);
      } finally {
        // Reset so the same file can be re-picked if needed.
        input.value = "";
      }
    };
    reader.readAsText(file);
  }
}
