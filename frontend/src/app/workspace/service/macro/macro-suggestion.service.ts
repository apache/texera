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
 *
 * `confidence` is the score expressed as a user-facing tier: "recommended"
 * for the top-scoring repeated patterns the user almost certainly wants to
 * extract, "strong" for clean linear chains, "good" for everything else.
 * Rendered as a small chip in the suggestion panel instead of the raw
 * floating-point score, which read as engineering noise.
 */
export interface MacroSuggestion {
  id: string;
  operatorIds: string[];
  rationale: string;
  score: number;
  suggestedName: string;
  confidence: "recommended" | "strong" | "good";
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
      const score = this.scoreChain(chain, ops, inDeg, outDeg);
      all.push({
        id: `linear-${idx++}`,
        operatorIds: chain,
        rationale: this.rationaleForLinearChain(chain, ops),
        score,
        suggestedName: this.suggestedNameForChain(chain, ops),
        confidence: this.tierFor(score, /* isRepeatedPattern */ false),
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

  /**
   * Map a numeric score onto the three user-facing tiers shown as confidence
   * chips. Tiers are tuned to the score distribution of the v1 heuristics:
   *  - "recommended" — any repeated-pattern match (always a strong signal:
   *    duplicated logic = refactor opportunity) OR a very long clean chain
   *  - "strong"      — linear chains of 3+ ops anchored on neither source
   *    nor sink (the cleanest macro candidates)
   *  - "good"        — everything else that still cleared the heuristic
   */
  private tierFor(score: number, isRepeatedPattern: boolean): "recommended" | "strong" | "good" {
    if (isRepeatedPattern) return "recommended";
    if (score >= 4) return "strong";
    return "good";
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
        const score = distinct.length * win.length * Math.pow(0.95, i);
        suggestions.push({
          id: `pattern-${idx++}`,
          operatorIds: win,
          rationale: `Recurring ${sigPretty} pattern (×${distinct.length}). Encapsulating once de-duplicates the rest in place.`,
          // Pattern score: occurrences × length × decay-per-rank. A 2-op
          // pattern appearing 3× scores 6 > a single 4-op chain (≈4).
          score,
          suggestedName: this.suggestedNameForPattern(sig, ops, win),
          // Repeated patterns are the strongest signal we have for "the user
          // is duplicating logic" — tier them as `recommended` regardless of
          // raw score so they stand out from one-off chains.
          confidence: this.tierFor(score, /* isRepeatedPattern */ true),
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

  private suggestedNameForPattern(
    sig: string,
    ops?: readonly OperatorPredicate[],
    win?: readonly string[]
  ): string {
    const lc = sig.toLowerCase();
    const domain = this.domainAwareName(lc);
    if (domain) return domain;
    // Fallback: snake_case the operator types but strip noise like the
    // `OpDesc` suffix Texera-generated schemas carry. Caps at 40 chars so
    // the chip in the suggestion panel doesn't wrap.
    void ops;
    void win;
    return sig
      .toLowerCase()
      .replace(/→/g, "_")
      .replace(/opdesc$/g, "")
      .replace(/[^a-z0-9_]/g, "")
      .slice(0, 40);
  }

  /**
   * Map a pipeline-type signature (lowercased "op1 → op2 → op3") onto a
   * domain-aware snake_case name a human would actually pick. Keeps the
   * macro palette readable: "csv_preprocessing" beats "csvfilescan_filter_
   * projection_block". Returns undefined when no domain pattern matches;
   * caller falls back to the generic snake-case formatter.
   *
   * The patterns intentionally match LOOSELY (substring rather than full
   * sequence) because Texera ships dozens of related op types (Filter vs
   * SpecializedFilter vs ConditionFilter) and the user's mental model
   * groups them all as "filtering."
   */
  private domainAwareName(lc: string): string | undefined {
    const has = (re: RegExp) => re.test(lc);
    // Order matters: more specific patterns first.
    if (has(/csv.*scan.*filter.*projection/) || has(/csv.*scan.*projection.*filter/)) {
      return "csv_preprocessing";
    }
    if (has(/json.*scan.*filter/) || has(/json.*scan.*projection/)) return "json_preprocessing";
    if (has(/scan.*filter.*projection/)) return "data_preprocessing";
    if (has(/scan.*projection/)) return "data_loading";
    if (has(/regex.*filter/) || has(/filter.*regex/)) return "text_filtering";
    if (has(/wordcloud/) || has(/word_count/) || has(/tokeniz/)) return "text_analysis";
    if (has(/filter.*projection/) || has(/projection.*filter/)) return "data_cleaning";
    if (has(/hashjoin.*projection/) || has(/cartesian.*projection/) || has(/union.*projection/)) {
      return "joined_enrichment";
    }
    if (has(/aggregate.*projection/) || has(/aggregate.*filter/) || has(/groupby.*projection/)) {
      return "metric_summary";
    }
    if (has(/aggregate/) || has(/groupby/)) return "aggregation_block";
    if (has(/piechart/) || has(/barchart/) || has(/linechart/) || has(/scatter/)) {
      return "chart_pipeline";
    }
    if (has(/normalizer/) || has(/standardize/) || has(/imputer/)) return "feature_normalization";
    if (has(/sklearn.*trainer/) || has(/sklearn.*testing/)) return "ml_train_eval";
    if (has(/pythonudf/) && has(/projection/)) return "udf_pipeline";
    return undefined;
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
      return `${this.preprocessingHint(types)} (${chain.length} ops). Encapsulating this protects downstream consumers from the schema changes.`;
    }
    if (this.looksLikeAggregation(types)) {
      return `${this.aggregationHint(types)} (${chain.length} ops). Reusing this pipeline keeps your analytics consistent across workflows.`;
    }
    if (this.looksLikeVisualization(types)) {
      return `${this.visualizationHint(types)} (${chain.length} ops). Once captured, the same chart definition can be reused without recopying ops.`;
    }
    if (this.looksLikeJoinAndShape(types)) {
      return `Join + reshape pipeline (${chain.length} ops). Encapsulating hides the join's key contract behind a single macro port.`;
    }
    return `Linear ${chain.length}-step chain — good macro candidate. Extracts the unit and frees the parent canvas of intermediate ops.`;
  }

  private looksLikePreprocessing(types: string[]): boolean {
    const lc = types.join(" ").toLowerCase();
    return /filter|projection|select|map|clean/.test(lc);
  }

  private looksLikeAggregation(types: string[]): boolean {
    const lc = types.join(" ").toLowerCase();
    return /aggregate|group|sum|count|reduce/.test(lc);
  }

  private looksLikeVisualization(types: string[]): boolean {
    const lc = types.join(" ").toLowerCase();
    return /chart|plot|visualizer|wordcloud|piechart|barchart|linechart/.test(lc);
  }

  private looksLikeJoinAndShape(types: string[]): boolean {
    const lc = types.join(" ").toLowerCase();
    return /(hashjoin|cartesian|union).*(projection|filter|map)/.test(lc);
  }

  /**
   * Detailed rationale generators — slot in the user's actual op types so
   * the suggestion reads as concrete advice ("Filter → Projection block")
   * instead of a generic "preprocessing pipeline" pitch.
   */
  private preprocessingHint(types: string[]): string {
    const lc = types.join(" ").toLowerCase();
    if (lc.includes("filter") && lc.includes("projection")) return "Filter + project block";
    if (lc.includes("filter")) return "Row-filter block";
    if (lc.includes("projection")) return "Column-project block";
    return "Preprocessing block";
  }

  private aggregationHint(types: string[]): string {
    const lc = types.join(" ").toLowerCase();
    if (lc.includes("aggregate") && lc.includes("projection")) return "Aggregate + project block";
    if (lc.includes("groupby") || lc.includes("aggregate")) return "Grouping/aggregation block";
    return "Reduction pipeline";
  }

  private visualizationHint(types: string[]): string {
    const lc = types.join(" ").toLowerCase();
    if (lc.includes("wordcloud")) return "Text-summary visualization";
    if (lc.includes("piechart") || lc.includes("barchart") || lc.includes("linechart")) return "Categorical chart block";
    return "Visualization block";
  }

  private suggestedNameForChain(chain: string[], ops: readonly OperatorPredicate[]): string {
    const types = chain.map(id => ops.find(o => o.operatorID === id)?.operatorType ?? "Op");
    return this.nameFromTypes(types);
  }

  /**
   * Public helper for callers outside the suggester (e.g. the right-click
   * "create macro" flow) that want the SAME smart default name the
   * suggester panel would produce — so manually-created and AI-suggested
   * macros land in the palette with consistent naming.
   */
  public smartNameFromTypes(operatorTypes: readonly string[]): string {
    return this.nameFromTypes(operatorTypes);
  }

  private nameFromTypes(types: readonly string[]): string {
    const sig = types.join("_").toLowerCase();
    const domain = this.domainAwareName(sig);
    if (domain) return domain;
    // Fallback: compact 2-3 of the type names into a snake-cased candidate.
    const condensed = types.slice(0, Math.min(3, types.length)).map(t => t.replace(/OpDesc$|Op$/, ""));
    return condensed.join("_").toLowerCase() + (types.length > 3 ? "_block" : "");
  }
}
