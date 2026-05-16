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
    const patternSuggestions = this.findRepeatedPatterns(linearChains, ops);

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
    // Deduplicate by chain identity (sometimes a chain shows up twice). When
    // both a linear-chain and a pattern suggestion share the same operator
    // set, prefer the higher-scoring one — which after the pattern boost is
    // usually the pattern one with the "recurring" rationale.
    const byKey = new Map<string, MacroSuggestion>();
    for (const s of all) {
      const key = s.operatorIds.join("|");
      const prev = byKey.get(key);
      if (!prev || s.score > prev.score) byKey.set(key, s);
    }
    return [...byKey.values()].sort((a, b) => b.score - a.score).slice(0, 10);
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
   *
   * Strategy: slide every 2- and 3-window over each linear chain, key on the
   * tuple of operator types, and group by key. For each key with ≥2
   * occurrences, surface ONE suggestion per occurrence so the user can pick
   * which instance to materialize first (the others can be done after via
   * the same operator-type chain — or, future work, "materialize all").
   *
   * The score boost makes recurring shorter patterns out-rank a single
   * longer chain — usually what the user wants for refactoring duplication.
   */
  private findRepeatedPatterns(chains: string[][], ops: readonly OperatorPredicate[]): MacroSuggestion[] {
    if (chains.length === 0) return [];
    const opType = (id: string) => ops.find(o => o.operatorID === id)?.operatorType ?? "?";
    // Map signature → list of windows; each window is a contiguous slice of a chain.
    const windows = new Map<string, string[][]>();
    for (const chain of chains) {
      for (const winLen of [2, 3]) {
        if (chain.length < winLen) continue;
        for (let i = 0; i + winLen <= chain.length; i++) {
          const slice = chain.slice(i, i + winLen);
          const sig = slice.map(opType).join("→");
          if (!windows.has(sig)) windows.set(sig, []);
          windows.get(sig)!.push(slice);
        }
      }
    }
    const suggestions: MacroSuggestion[] = [];
    let idx = 0;
    for (const [sig, occurrences] of windows.entries()) {
      // Need ≥2 distinct occurrences. "Distinct" = no shared op IDs between
      // windows — overlapping windows in a 3-step chain don't count as
      // duplication (they're the same logic, just viewed differently).
      const distinct = this.distinctWindows(occurrences);
      if (distinct.length < 2) continue;
      // One suggestion per distinct occurrence. The first one wins the higher
      // score (so it floats to the top), the rest get a small decay.
      const sigPretty = sig.replace(/→/g, " → ");
      distinct.forEach((win, i) => {
        suggestions.push({
          id: `pattern-${idx++}`,
          operatorIds: win,
          rationale: `Recurring pattern: ${sigPretty} appears ${distinct.length}× in this workflow — extract as a shared macro.`,
          // Pattern score: occurrences × length × decay-per-rank. A 2-op
          // pattern appearing 3× scores 6 > a single 4-op chain (≈4).
          score: distinct.length * win.length * Math.pow(0.95, i),
          suggestedName: this.suggestedNameForPattern(sig),
        });
      });
    }
    return suggestions;
  }

  /**
   * Drop overlapping windows: if two occurrences share any operator ID, they
   * count as the same physical instance. Walks in input order so the earliest
   * (typically the upstream-most) occurrence wins.
   */
  private distinctWindows(occurrences: string[][]): string[][] {
    const result: string[][] = [];
    const claimed = new Set<string>();
    for (const win of occurrences) {
      if (win.some(id => claimed.has(id))) continue;
      result.push(win);
      win.forEach(id => claimed.add(id));
    }
    return result;
  }

  private suggestedNameForPattern(sig: string): string {
    return sig
      .toLowerCase()
      .replace(/→/g, "_")
      .replace(/opdesc$/g, "")
      .replace(/[^a-z0-9_]/g, "")
      .slice(0, 40);
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
