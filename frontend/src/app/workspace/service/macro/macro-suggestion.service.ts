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
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { WorkflowGraphReadonly } from "../workflow-graph/model/workflow-graph";

/**
 * One macro-encapsulation candidate the suggester surfaces to the user.
 * `operatorIds` is the contiguous chain that would become the macro body;
 * `rationale` is a one-line human-readable explanation; `score` ranks it
 * against the other candidates (higher = better).
 */
export interface MacroSuggestion {
  id: string;
  operatorIds: string[];
  rationale: string;
  score: number;
  suggestedName: string;
}

/**
 * Frontend-only "agent" that proposes sub-DAGs worth encapsulating. v1 is a
 * pure heuristic — no LLM call — because the hackathon demo only needs the
 * UI moment of *suggesting + materializing*, not novel intelligence. Swap
 * in an LLM later by replacing the body of `suggestMacros` with a call to
 * `chat-assistant-service` that returns the same `MacroSuggestion[]` shape.
 *
 * Heuristics in v1 (combined into one ranked list):
 *
 *   1. Linear chains: ≥2 contiguous operators where each interior op has
 *      exactly one upstream and one downstream within the chain, and the
 *      chain is *not* a single sink. These are the easiest sub-DAGs to
 *      replace with a single Macro op — no port fan-out to worry about.
 *
 *   2. Repeated patterns: operator-type sequences that appear more than
 *      once in the same workflow (e.g. CSV → Filter → Projection twice).
 *      Repeating something is a strong "extract as macro" signal.
 *
 * Score = chain length × repeat multiplier × (sources/sinks excluded). We
 * deliberately under-suggest: long chains anchored on a source or sink are
 * surfaced too, but with a small penalty so the cleaner "middle" chains
 * float to the top.
 */
@Injectable({ providedIn: "root" })
export class MacroSuggestionService {
  /**
   * Run all heuristics on the current canvas graph. Macros and macro
   * markers are excluded so the suggester doesn't try to nest macros into
   * each other (would still work, but is rarely useful).
   */
  public suggestMacros(graph: WorkflowGraphReadonly): MacroSuggestion[] {
    const ops = graph.getAllOperators().filter(
      op => op.operatorType !== "Macro" && op.operatorType !== "MacroInput" && op.operatorType !== "MacroOutput"
    );
    const links = graph.getAllLinks();
    const inDeg = this.computeDegrees(ops, links, "target");
    const outDeg = this.computeDegrees(ops, links, "source");

    const linearChains = this.findLinearChains(ops, links, inDeg, outDeg);
    const patternSuggestions = this.findRepeatedPatterns(linearChains);

    // Merge: pattern suggestions get a multiplier; linear chains stand alone.
    const all: MacroSuggestion[] = [];
    let idx = 0;
    for (const chain of linearChains) {
      all.push({
        id: `linear-${idx++}`,
        operatorIds: chain,
        rationale: this.rationaleForLinearChain(chain, ops),
        score: this.scoreChain(chain, ops, inDeg, outDeg),
        suggestedName: this.suggestedNameForChain(chain, ops),
      });
    }
    for (const pat of patternSuggestions) {
      all.push(pat);
    }
    // Deduplicate by chain identity (sometimes a chain shows up twice).
    const seen = new Set<string>();
    const deduped = all.filter(s => {
      const key = s.operatorIds.join("|");
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    return deduped.sort((a, b) => b.score - a.score).slice(0, 10);
  }

  private computeDegrees(
    ops: readonly OperatorPredicate[],
    links: readonly OperatorLink[],
    end: "source" | "target"
  ): Map<string, number> {
    const m = new Map<string, number>();
    for (const op of ops) m.set(op.operatorID, 0);
    // Only count a link if BOTH endpoints are in the filtered `ops` set —
    // otherwise a Filter whose upstream is a Macro gets inDeg=1, blocking
    // it from being detected as a chain head. The intent of the filtered
    // view is "ignore macros entirely", which means edges incident on a
    // macro have no degree contribution to the non-macro nodes.
    const inOps = new Set(ops.map(o => o.operatorID));
    for (const link of links) {
      if (!inOps.has(link.source.operatorID) || !inOps.has(link.target.operatorID)) continue;
      const endId = link[end].operatorID;
      m.set(endId, (m.get(endId) ?? 0) + 1);
    }
    return m;
  }

  /**
   * Find maximal linear chains: sequences of operators connected by single
   * links where each interior node has exactly one in-degree and one
   * out-degree. We start a chain at any node whose predecessor is *not* in
   * a 1-out chain (i.e., the chain's "head") and walk forward.
   */
  private findLinearChains(
    ops: readonly OperatorPredicate[],
    links: readonly OperatorLink[],
    inDeg: Map<string, number>,
    outDeg: Map<string, number>
  ): string[][] {
    // Build the adjacency over the FILTERED graph — only edges where both
    // endpoints are non-macro count. Same rationale as `computeDegrees`:
    // we want to treat the macro-free subgraph as if macros never existed.
    const adjOut = new Map<string, string[]>();
    const inOps = new Set(ops.map(o => o.operatorID));
    for (const op of ops) adjOut.set(op.operatorID, []);
    for (const link of links) {
      if (!inOps.has(link.source.operatorID) || !inOps.has(link.target.operatorID)) continue;
      const list = adjOut.get(link.source.operatorID);
      if (list) list.push(link.target.operatorID);
    }
    const visited = new Set<string>();
    const chains: string[][] = [];
    for (const op of ops) {
      if (visited.has(op.operatorID)) continue;
      // Heads: nodes whose predecessor isn't part of a continuing linear
      // chain (in-degree != 1 or predecessor has out-degree > 1).
      const isHead =
        (inDeg.get(op.operatorID) ?? 0) !== 1 || this.predIsBranching(op.operatorID, links, outDeg, inOps);
      if (!isHead) continue;
      const chain: string[] = [];
      let cur: string | undefined = op.operatorID;
      while (cur && !visited.has(cur)) {
        chain.push(cur);
        visited.add(cur);
        const nexts: string[] = adjOut.get(cur) ?? [];
        // Only continue if cur has out-degree 1 AND next has in-degree 1
        if (nexts.length !== 1) break;
        const next: string = nexts[0];
        if ((inDeg.get(next) ?? 0) !== 1) break;
        cur = next;
      }
      if (chain.length >= 2) chains.push(chain);
    }
    return chains;
  }

  private predIsBranching(
    opId: string,
    links: readonly OperatorLink[],
    outDeg: Map<string, number>,
    inOps: Set<string>
  ): boolean {
    // Same as `computeDegrees`: only consider predecessors that are
    // themselves non-macro. A macro upstream of a non-macro op is treated
    // as "no predecessor" from the chain detector's perspective.
    const preds = links
      .filter(l => l.target.operatorID === opId && inOps.has(l.source.operatorID))
      .map(l => l.source.operatorID);
    if (preds.length !== 1) return true;
    return (outDeg.get(preds[0]) ?? 0) > 1;
  }

  /**
   * Recurring `(operatorType, operatorType, …)` sequences across the
   * workflow. Multiple instances of the same shape strongly suggest the
   * user is duplicating logic they'd want to share via a macro.
   */
  private findRepeatedPatterns(chains: string[][]): MacroSuggestion[] {
    return []; // v1: skip. The pure linear-chain heuristic already gives demo material.
    // Implementation sketch for v2: group chains by operatorType sequence,
    // surface groups with size > 1 as a single "Recurring pattern" suggestion.
  }

  private scoreChain(
    chain: string[],
    ops: readonly OperatorPredicate[],
    inDeg: Map<string, number>,
    outDeg: Map<string, number>
  ): number {
    const lenScore = chain.length;
    // Penalty if the chain head is a true source (no inputs) — wrapping a
    // source operator into a macro is less useful because the user usually
    // wants to swap the source.
    const head = chain[0];
    const tail = chain[chain.length - 1];
    const headPenalty = (inDeg.get(head) ?? 0) === 0 ? 0.5 : 1;
    const tailPenalty = (outDeg.get(tail) ?? 0) === 0 ? 0.7 : 1;
    return lenScore * headPenalty * tailPenalty;
  }

  private rationaleForLinearChain(chain: string[], ops: readonly OperatorPredicate[]): string {
    const types = chain
      .map(id => ops.find(o => o.operatorID === id)?.operatorType ?? "?")
      .map(t => t.replace(/([A-Z])/g, " $1").trim());
    const head = types[0];
    const tail = types[types.length - 1];
    if (chain.length === 2) {
      return `Two-step pipeline: ${head} → ${tail}. Reusable as a unit.`;
    }
    if (this.looksLikePreprocessing(types)) {
      return `Looks like a reusable preprocessing block (${chain.length} ops).`;
    }
    if (this.looksLikeAggregation(types)) {
      return `Looks like a reusable aggregation pipeline (${chain.length} ops).`;
    }
    return `Linear ${chain.length}-step chain — good macro candidate.`;
  }

  private looksLikePreprocessing(types: string[]): boolean {
    const lc = types.join(" ").toLowerCase();
    return /filter|projection|select|map|clean/.test(lc);
  }

  private looksLikeAggregation(types: string[]): boolean {
    const lc = types.join(" ").toLowerCase();
    return /aggregate|group|sum|count|reduce/.test(lc);
  }

  private suggestedNameForChain(chain: string[], ops: readonly OperatorPredicate[]): string {
    const types = chain.map(id => ops.find(o => o.operatorID === id)?.operatorType ?? "Op");
    // Compact 2-3 of the type names into a snake-cased candidate name.
    const condensed = types.slice(0, Math.min(3, types.length)).map(t => t.replace(/OpDesc$|Op$/, ""));
    return condensed.join("_").toLowerCase() + (chain.length > 3 ? "_block" : "");
  }
}
