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

import { Component } from "@angular/core";
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowResultService } from "src/app/workspace/service/workflow-result/workflow-result.service";
import { WorkflowResultExportService } from "src/app/workspace/service/workflow-result-export/workflow-result-export.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { ResultExportationComponent } from "../../../result-exportation/result-exportation.component";
import { ValidationWorkflowService } from "src/app/workspace/service/validation/validation-workflow.service";
import { GuiConfigService } from "../../../../../common/service/gui-config.service";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NgIf } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { MacroService, MacroDetail } from "src/app/workspace/service/macro/macro.service";
import { MacroFusionService } from "src/app/workspace/service/macro/macro-fusion.service";
import { MacroSuggestionService } from "src/app/workspace/service/macro/macro-suggestion.service";
import { JointUIService } from "src/app/workspace/service/joint-ui/joint-ui.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { WorkflowUtilService } from "src/app/workspace/service/workflow-graph/util/workflow-util.service";
import { OperatorPredicate, Point } from "src/app/workspace/types/workflow-common.interface";

@UntilDestroy()
@Component({
  selector: "texera-context-menu",
  templateUrl: "./context-menu.component.html",
  styleUrls: ["./context-menu.component.scss"],
  imports: [NzMenuDirective, NgIf, NzMenuItemComponent, ɵNzTransitionPatchDirective, NzIconDirective],
})
export class ContextMenuComponent {
  public isWorkflowModifiable: boolean = false;
  public highlightedOperatorIds: readonly string[] = [];
  public highlightedCommentBoxIds: readonly string[] = [];

  constructor(
    public workflowActionService: WorkflowActionService,
    public operatorMenuService: OperatorMenuService,
    public workflowResultExportService: WorkflowResultExportService,
    protected config: GuiConfigService,
    private workflowResultService: WorkflowResultService,
    private modalService: NzModalService,
    private validationWorkflowService: ValidationWorkflowService,
    private macroService: MacroService,
    private notificationService: NotificationService,
    private workflowUtilService: WorkflowUtilService,
    private macroFusionService: MacroFusionService,
    private macroSuggestionService: MacroSuggestionService,
    private jointUIService: JointUIService
  ) {
    this.registerWorkflowModifiableChangedHandler();
    this.operatorMenuService.highlightedOperators$
      .pipe(untilDestroyed(this))
      .subscribe(ids => (this.highlightedOperatorIds = ids));
    this.operatorMenuService.highlightedCommentBoxes$
      .pipe(untilDestroyed(this))
      .subscribe(ids => (this.highlightedCommentBoxIds = ids));
  }

  public canExecuteOperator(): boolean {
    if (!this.hasExactlyOneOperatorSelected() || !this.isWorkflowModifiable) {
      return false;
    }

    const operatorID = this.getSelectedOperatorID();
    return this.isOperatorExecutable(operatorID);
  }

  private hasExactlyOneOperatorSelected(): boolean {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length === 1;
  }

  private getSelectedOperatorID(): string {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
  }

  private isOperatorExecutable(operatorID: string): boolean {
    return (
      this.validationWorkflowService.validateOperator(operatorID).isValid &&
      !this.workflowActionService.getTexeraGraph().isOperatorDisabled(operatorID)
    );
  }

  public hasHighlightedLinks(): boolean {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs().length > 0;
  }

  public onCopy(): void {
    this.operatorMenuService.saveHighlightedElements();
  }

  public onPaste(): void {
    this.operatorMenuService.performPasteOperation();
  }

  public onCut(): void {
    this.onCopy();
    this.onDelete();
  }

  public onDelete(): void {
    // Capture all highlighted IDs before starting deletion to avoid modification during iteration
    const highlightedOperatorIDs = Array.from(
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()
    );
    const highlightedCommentBoxIDs = Array.from(
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedCommentBoxIDs()
    );
    const highlightedLinkIDs = Array.from(
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs()
    );

    // Bundle all deletions together for proper undo/redo support
    this.workflowActionService.getTexeraGraph().bundleActions(() => {
      // Delete operators and their connected links
      this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs);

      // Delete standalone selected links
      highlightedLinkIDs.forEach(highlightedLinkID => {
        // Only delete if the link still exists (might have been deleted with operators)
        if (this.workflowActionService.getTexeraGraph().hasLinkWithID(highlightedLinkID)) {
          this.workflowActionService.deleteLinkWithID(highlightedLinkID);
        }
      });

      // Delete comment boxes
      highlightedCommentBoxIDs.forEach(highlightedCommentBoxID =>
        this.workflowActionService.deleteCommentBox(highlightedCommentBoxID)
      );
    });
  }

  private registerWorkflowModifiableChangedHandler() {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(modifiable => (this.isWorkflowModifiable = modifiable));
  }

  /**
   * This is the handler for the execution result export button for only highlighted operators.
   *
   */
  /**
   * Bundles the highlighted operators into a new macro definition on the
   * backend, then replaces the selection on the canvas with a single MacroOp
   * node that has the same external boundary (input/output ports rewired to
   * whichever external operators were feeding / consuming the selection).
   */
  /**
   * Inverse of `onCreateMacro` — the user has highlighted exactly one Macro
   * instance and wants to "expand" it back into its constituent sub-DAG on
   * the parent canvas. Inline the body operators (with fresh IDs to avoid
   * collisions), reproduce internal links, and rewire each external link
   * touching the macro from its port back to the corresponding boundary
   * inner op + port. The macro op is then removed.
   *
   * v1: only LIVE-linked macros are supported (body fetched via the registry).
   * SNAPSHOT (embedded body) would need to read `operatorProperties.snapshot`
   * instead of going through `getMacro`.
   */
  public canExpandMacro(): boolean {
    if (!this.isWorkflowModifiable) return false;
    if (this.highlightedOperatorIds.length !== 1) return false;
    const opId = this.highlightedOperatorIds[0];
    const op = (() => {
      try {
        return this.workflowActionService.getTexeraGraph().getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    return op?.operatorType === "Macro" && typeof op.operatorProperties?.["macroId"] === "string";
  }

  public onExpandMacro(): void {
    const opId = this.highlightedOperatorIds[0];
    if (!opId) return;
    const graph = this.workflowActionService.getTexeraGraph();
    const macroOp = (() => {
      try {
        return graph.getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    if (!macroOp) return;
    const macroId = macroOp.operatorProperties?.["macroId"];
    if (typeof macroId !== "string" || macroId.length === 0) {
      this.notificationService.error("Macro has no macroId — can't expand.");
      return;
    }
    const widNum = Number(macroId);
    if (!Number.isFinite(widNum)) {
      this.notificationService.error(`Invalid macroId: ${macroId}`);
      return;
    }
    this.macroService
      .getMacro(widNum)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: detail => {
          try {
            this.inlineMacroBody(macroOp, detail);
            this.notificationService.success(`Expanded "${detail.name}" onto the canvas.`);
          } catch (e) {
            this.notificationService.error(`Expand failed: ${(e as Error)?.message ?? e}`);
          }
        },
        error: err => this.notificationService.error(`Failed to load macro body: ${err?.message ?? err}`),
      });
  }

  /**
   * Inline the macro's body operators + links onto the parent canvas, rewire
   * external links so each one targets the right boundary inner op + port,
   * and remove the macro op + its outer links. New unique IDs are assigned
   * to body operators so re-expanding the same macro elsewhere doesn't
   * collide.
   *
   * Layout: body ops are laid out around the macro op's former position.
   * Crude column layout (input markers → inner → output markers) gets the
   * job done without a real layout pass.
   */
  private inlineMacroBody(macroOp: OperatorPredicate, detail: MacroDetail): void {
    const graph = this.workflowActionService.getTexeraGraph();
    // Parse the body via the existing macroDetailToWorkflow normalizer so we
    // get OperatorPredicate-shaped ops and OperatorLink-shaped links.
    const macroWorkflow = this.macroService.macroDetailToWorkflow(detail);
    const bodyOps = macroWorkflow.content.operators.filter(
      o => o.operatorType !== "MacroInput" && o.operatorType !== "MacroOutput"
    );
    const inputMarkers = macroWorkflow.content.operators.filter(o => o.operatorType === "MacroInput");
    const outputMarkers = macroWorkflow.content.operators.filter(o => o.operatorType === "MacroOutput");
    const markerIds = new Set([...inputMarkers, ...outputMarkers].map(o => o.operatorID));

    // Assign fresh IDs to inner ops so re-using the same macro elsewhere
    // doesn't collide. Map body-relative ID → fresh canvas ID.
    const idRewrite = new Map<string, string>();
    bodyOps.forEach(op => {
      const fresh = `${op.operatorType}-operator-${this.workflowUtilService.getOperatorRandomUUID()}`;
      idRewrite.set(op.operatorID, fresh);
    });

    // Anchor positions around the macro's old location (crude column layout).
    const macroPos = this.workflowActionService.getJointGraphWrapper().getElementPosition(macroOp.operatorID);
    const baseX = macroPos.x;
    const baseY = macroPos.y;
    const colSpacing = 180;
    const rowSpacing = 120;

    const positionedOps: { op: OperatorPredicate; pos: Point }[] = bodyOps.map((op, idx) => ({
      op: { ...op, operatorID: idRewrite.get(op.operatorID)! },
      pos: { x: baseX + (idx % 3) * colSpacing, y: baseY + Math.floor(idx / 3) * rowSpacing },
    }));

    // Internal links (not touching marker ops). Rewrite both endpoints.
    const internalLinks = macroWorkflow.content.links
      .filter(l => !markerIds.has(l.source.operatorID) && !markerIds.has(l.target.operatorID))
      .map(l => ({
        linkID: this.workflowUtilService.getLinkRandomUUID(),
        source: { operatorID: idRewrite.get(l.source.operatorID)!, portID: l.source.portID },
        target: { operatorID: idRewrite.get(l.target.operatorID)!, portID: l.target.portID },
      }));

    // Body links from MacroInput markers to inner ops give us (portIndex →
    // [(innerOpId, innerPortID)]) — the same lookup table MacroExpander uses
    // on the backend. We need it here to rewire each external incoming link
    // (which currently terminates at `macroOp@port_X`) to the corresponding
    // inner op port.
    const inputBindings = new Map<number, { innerOpId: string; innerPortID: string }[]>();
    for (const m of inputMarkers) {
      const portIndex = m.operatorProperties?.["portIndex"];
      if (typeof portIndex !== "number") continue;
      const consumers = macroWorkflow.content.links
        .filter(l => l.source.operatorID === m.operatorID && !markerIds.has(l.target.operatorID))
        .map(l => ({
          innerOpId: idRewrite.get(l.target.operatorID)!,
          innerPortID: l.target.portID,
        }));
      inputBindings.set(portIndex, consumers);
    }
    const outputBindings = new Map<number, { innerOpId: string; innerPortID: string }>();
    for (const m of outputMarkers) {
      const portIndex = m.operatorProperties?.["portIndex"];
      if (typeof portIndex !== "number") continue;
      const producer = macroWorkflow.content.links.find(
        l => l.target.operatorID === m.operatorID && !markerIds.has(l.source.operatorID)
      );
      if (producer) {
        outputBindings.set(portIndex, {
          innerOpId: idRewrite.get(producer.source.operatorID)!,
          innerPortID: producer.source.portID,
        });
      }
    }

    // Find the parent canvas links that touch the macro and need rewiring.
    // Frontend port IDs are `input-i` / `output-j`; the trailing integer is
    // the external port index we map against.
    const portIdToIndex = (portID: string): number | undefined => {
      const m = portID.match(/(\d+)$/);
      return m ? Number(m[1]) : undefined;
    };
    const incomingRewires: { source: { operatorID: string; portID: string }; targets: { operatorID: string; portID: string }[] }[] = [];
    const outgoingRewires: { source: { operatorID: string; portID: string }; target: { operatorID: string; portID: string } }[] = [];
    for (const link of graph.getAllLinks()) {
      if (link.target.operatorID === macroOp.operatorID) {
        const portIndex = portIdToIndex(link.target.portID);
        if (portIndex === undefined) continue;
        const consumers = inputBindings.get(portIndex) ?? [];
        incomingRewires.push({ source: link.source, targets: consumers.map(c => ({ operatorID: c.innerOpId, portID: c.innerPortID })) });
      } else if (link.source.operatorID === macroOp.operatorID) {
        const portIndex = portIdToIndex(link.source.portID);
        if (portIndex === undefined) continue;
        const producer = outputBindings.get(portIndex);
        if (producer) {
          outgoingRewires.push({ source: { operatorID: producer.innerOpId, portID: producer.innerPortID }, target: link.target });
        }
      }
    }

    // Apply all of it atomically so undo collapses to one step.
    graph.bundleActions(() => {
      this.workflowActionService.addOperatorsAndLinks(positionedOps, internalLinks);
      for (const rw of incomingRewires) {
        for (const target of rw.targets) {
          this.workflowActionService.addLink({
            linkID: this.workflowUtilService.getLinkRandomUUID(),
            source: rw.source,
            target,
          });
        }
      }
      for (const rw of outgoingRewires) {
        this.workflowActionService.addLink({
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: rw.source,
          target: rw.target,
        });
      }
      this.workflowActionService.deleteOperatorsAndLinks([macroOp.operatorID]);
    });
  }

  /**
   * "Fuse for performance" action on a Macro instance — generate an
   * equivalent PythonUDF, run sample-diff verification, and attach the
   * verified `fusion` payload to the macro's properties. MacroExpander
   * picks it up at compile time and substitutes a single UDF for the
   * inlined body, eliminating inter-actor handoffs.
   *
   * v1 codegen is template-based (no LLM). Verification is faked at the
   * generator level — sampleSize is recorded but a real sample-diff
   * against the original is a follow-up. The substitution gate the
   * backend reads is `fusion.verified`; once it's true the original body
   * is bypassed.
   */
  public canFuseMacro(): boolean {
    if (!this.isWorkflowModifiable) return false;
    if (this.highlightedOperatorIds.length !== 1) return false;
    const opId = this.highlightedOperatorIds[0];
    const op = (() => {
      try {
        return this.workflowActionService.getTexeraGraph().getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    if (op?.operatorType !== "Macro") return false;
    // Don't offer "fuse" again on a macro that's already verified-fused —
    // the substitution will already be in effect.
    const existing = op.operatorProperties?.["fusion"] as { verified?: boolean } | undefined;
    return !existing?.verified;
  }

  /**
   * Reverse of "Fuse for performance" — drop the `fusion` field from the
   * macro's properties so the next compile inlines the body again. Useful
   * if the user wants to inspect the body (e.g., debug a behavior change)
   * or re-fuse after editing the macro definition.
   */
  public canUnfuseMacro(): boolean {
    if (!this.isWorkflowModifiable) return false;
    if (this.highlightedOperatorIds.length !== 1) return false;
    const opId = this.highlightedOperatorIds[0];
    const op = (() => {
      try {
        return this.workflowActionService.getTexeraGraph().getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    if (op?.operatorType !== "Macro") return false;
    const existing = op.operatorProperties?.["fusion"] as { verified?: boolean } | undefined;
    return existing?.verified === true;
  }

  public onUnfuseMacro(): void {
    const opId = this.highlightedOperatorIds[0];
    if (!opId) return;
    const graph = this.workflowActionService.getTexeraGraph();
    const op = (() => {
      try {
        return graph.getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    if (!op) return;
    const newProperties: Record<string, unknown> = { ...op.operatorProperties };
    delete newProperties["fusion"];
    this.workflowActionService.setOperatorProperty(opId, newProperties);
    const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
    if (paper) this.jointUIService.refreshMacroFusionStyle(paper, opId, false);
    this.notificationService.info("Unfused — macro body will inline on next run.");
  }

  /**
   * "Refresh macro instance" — re-pull the latest macroVersion + syncedAt
   * timestamp from the source definition and stamp them onto this instance.
   * If the macro definition has been edited since the instance was placed,
   * this is how the user picks up the new body without re-instantiating.
   *
   * The instance's `macroSyncedAt` (epoch ms) is bumped to NOW; the engine
   * still resolves LIVE-mode bodies via the current macro definition at
   * compile time, so this action is mostly UI cosmetic — but it surfaces
   * the freshness story to the user (and clears any "stale" indicator the
   * canvas might paint based on comparing syncedAt to lastModifiedTime).
   */
  public canRefreshMacroInstance(): boolean {
    if (!this.isWorkflowModifiable) return false;
    if (this.highlightedOperatorIds.length !== 1) return false;
    const opId = this.highlightedOperatorIds[0];
    const op = (() => {
      try {
        return this.workflowActionService.getTexeraGraph().getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    if (op?.operatorType !== "Macro") return false;
    const macroId = op.operatorProperties?.["macroId"];
    if (typeof macroId !== "string" || macroId.length === 0) return false;
    // Only worth offering if we actually know of a newer-than-instance time.
    const syncedAt = Number(op.operatorProperties?.["macroSyncedAt"] ?? 0);
    const latest = this.macroService.getLatestModifiedTime(macroId);
    return latest > 0 && latest > syncedAt;
  }

  public onRefreshMacroInstance(): void {
    const opId = this.highlightedOperatorIds[0];
    if (!opId) return;
    const graph = this.workflowActionService.getTexeraGraph();
    const op = (() => {
      try {
        return graph.getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    if (!op) return;
    const macroId = op.operatorProperties?.["macroId"] as string;
    const latest = this.macroService.getLatestModifiedTime(macroId);
    const newProperties: Record<string, unknown> = { ...op.operatorProperties };
    newProperties["macroSyncedAt"] = latest > 0 ? latest : Date.now();
    // The fusion's contract is "verified for THIS body's hash". When the
    // body changes (the trigger for refresh), drop the verified flag so
    // the next compile re-inlines the up-to-date body. The user can re-
    // fuse against the new body if desired.
    if (newProperties["fusion"]) {
      delete newProperties["fusion"];
      const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
      if (paper) this.jointUIService.refreshMacroFusionStyle(paper, opId, false);
    }
    this.workflowActionService.setOperatorProperty(opId, newProperties);
    this.notificationService.info("Macro instance refreshed to latest definition.");
  }

  public onFuseMacro(): void {
    const opId = this.highlightedOperatorIds[0];
    if (!opId) return;
    const graph = this.workflowActionService.getTexeraGraph();
    const macroOp = (() => {
      try {
        return graph.getOperator(opId);
      } catch {
        return undefined;
      }
    })();
    if (!macroOp) return;
    const macroId = macroOp.operatorProperties?.["macroId"];
    if (typeof macroId !== "string" || macroId.length === 0) {
      this.notificationService.error("Macro has no macroId — can't fuse.");
      return;
    }
    this.macroFusionService
      .generateFusion(macroId)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: result => {
          if (!result.verified) {
            this.notificationService.error(`Fusion failed verification: ${result.rationale}`);
            return;
          }
          // Attach the verified fusion to the macro's properties. The
          // backend's MacroExpander will see `fusion.verified = true`
          // when the workflow is submitted and substitute a single
          // PythonUDFOpDescV2 for the inlined body.
          const newProperties = {
            ...macroOp.operatorProperties,
            fusion: this.macroFusionService.toFusionPayload(result),
          };
          this.workflowActionService.setOperatorProperty(opId, newProperties);
          // Update the visual immediately — solid gold stroke + ⚡FUSED badge,
          // with the speedup metric appended so the perf claim is on-canvas.
          const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
          if (paper)
            this.jointUIService.refreshMacroFusionStyle(paper, opId, true, result.estimatedSpeedup);
          this.notificationService.success(
            `Fused "${macroOp.customDisplayName ?? macroOp.operatorID}" — ${result.rationale}`
          );
        },
        error: err => this.notificationService.error(`Failed to fuse: ${err?.message ?? err}`),
      });
  }

  public onCreateMacro(): void {
    const selected = Array.from(this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs());
    if (selected.length < 2) {
      return;
    }
    // Pre-fill the prompt with a smart default derived from the selected
    // operators' types so the user gets a readable name (e.g.
    // "filter_projection_block") rather than a UNIX-time tag. Falls back to
    // the legacy timestamp if no type info is available.
    const defaultName = this.suggestedMacroNameForSelection(selected) || `macro-${Date.now()}`;
    const name = window.prompt("Macro name", defaultName);
    if (!name) {
      return;
    }
    const built = this.macroService.buildMacroFromSelection(this.workflowActionService, selected, name);
    this.macroService
      .createMacro(built.request)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: detail => {
          try {
            this.swapSelectionWithMacroNode(detail, selected, built);
          } catch (e) {
            this.notificationService.error(`Swap failed: ${(e as Error)?.message ?? e}`);
            return;
          }
          this.notificationService.success(`Macro "${detail.name}" created (wid=${detail.wid})`);
        },
        error: err => this.notificationService.error(`Failed to create macro: ${err?.message ?? err}`),
      });
  }

  /**
   * Default name for a fresh macro built from this selection. Delegates to
   * `MacroSuggestionService.smartNameFromTypes` so right-click create-macro
   * uses the same domain-aware naming as the AI-suggestions panel (e.g.
   * "csv_preprocessing" instead of "csvfilescan_filter_projection_block").
   * Falls back to undefined when the selection's op types can't be read;
   * the caller defaults to a timestamp-based name in that case.
   */
  private suggestedMacroNameForSelection(selectedIds: readonly string[]): string | undefined {
    if (selectedIds.length === 0) return undefined;
    const graph = this.workflowActionService.getTexeraGraph();
    const types: string[] = [];
    for (const id of selectedIds) {
      try {
        types.push(graph.getOperator(id).operatorType);
      } catch {
        return undefined;
      }
    }
    if (types.length === 0) return undefined;
    return this.macroSuggestionService.smartNameFromTypes(types);
  }

  private swapSelectionWithMacroNode(
    detail: MacroDetail,
    selectedOpIDs: readonly string[],
    built: {
      incomingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
      outgoingEdges: { externalOpId: string; externalPortID: string; macroPortIndex: number }[];
      inputPortCount: number;
      outputPortCount: number;
    }
  ): void {
    // Construct the predicate manually rather than going through
    // WorkflowUtilService.getNewOperatorPredicate("Macro"): that path runs the
    // schema through Ajv, and MacroOpDesc's generated schema is currently
    // Ajv-invalid (Option[MacroBody] / Option[MacroFusion] produce
    // `"nullable": true` without a sibling `"type"`). We override every field
    // anyway, so the schema-default route adds no value here.
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
        // TODO: backend should expose the pinned vid on MacroDetail; defaulting
        // to 1 until then (DbMacroRegistry ignores version in v1 anyway).
        macroVersion: 1,
        linkMode: "LIVE",
        inputPortCount: built.inputPortCount,
        outputPortCount: built.outputPortCount,
        displayName: detail.name,
      },
      inputPorts,
      outputPorts,
      showAdvanced: false,
      isDisabled: false,
      customDisplayName: detail.name,
      dynamicInputPorts: false,
      dynamicOutputPorts: false,
    };

    const jointWrapper = this.workflowActionService.getJointGraphWrapper();
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

    this.workflowActionService.getTexeraGraph().bundleActions(() => {
      // Order matters: add the macro node first so the rewired external links
      // have a valid target/source. deleteOperatorsAndLinks then cleans up the
      // old internal + boundary links automatically.
      this.workflowActionService.addOperator(macroPredicate, centroid);
      this.workflowActionService.deleteOperatorsAndLinks(Array.from(selectedOpIDs));
      built.incomingEdges.forEach(edge =>
        this.workflowActionService.addLink({
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: edge.externalOpId, portID: edge.externalPortID },
          target: { operatorID: macroPredicate.operatorID, portID: `input-${edge.macroPortIndex}` },
        })
      );
      built.outgoingEdges.forEach(edge =>
        this.workflowActionService.addLink({
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: macroPredicate.operatorID, portID: `output-${edge.macroPortIndex}` },
          target: { operatorID: edge.externalOpId, portID: edge.externalPortID },
        })
      );
    });
  }

  public onClickExportHighlightedExecutionResult(): void {
    this.modalService.create({
      nzTitle: "Export Highlighted Operators Result",
      nzContent: ResultExportationComponent,
      nzData: {
        workflowName: this.workflowActionService.getWorkflowMetadata()?.name,
        sourceTriggered: "context-menu",
      },
      nzFooter: null,
    });
  }
}
