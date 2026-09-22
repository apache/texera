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

import { OperatorLink, OperatorPredicate } from "../../../types/workflow-common.interface";

/** The operator types that open and close a control block, as the backend names them. */
export const LOOP_START_OP_TYPE = "LoopStart";
export const LOOP_END_OP_TYPE = "LoopEnd";

type BlockOperator = Pick<OperatorPredicate, "operatorID" | "operatorType">;
type BlockLink = Pick<OperatorLink, "source" | "target">;

/** A control operator met on a walk, with its shortest hop distance from the operator walked from. */
interface ControlOperatorHit {
  operatorID: string;
  hops: number;
}

/**
 * The LoopStart operators whose control block encloses `operatorID`, outermost first.
 *
 * An operator is inside a block when it lies on a path LoopStart -> ... -> operator -> ... -> LoopEnd.
 * The control operators are not inside their own block, but an inner block's LoopStart and LoopEnd are
 * inside the outer block, so with nested blocks the inner body gets several starts. They are ordered by
 * decreasing shortest hop distance upstream of the operator, which puts the outermost first.
 *
 * Blocks in sequence (S -> a -> E -> S' -> b -> E') do not leak into each other: walking upstream from b,
 * the LoopEnd E opens a block that S closes again, so only a LoopStart met with no unmatched LoopEnd in
 * between counts. The downstream walk to the closing LoopEnd is symmetric. The walk ends on any graph,
 * cycles through a LoopStart or LoopEnd included (see walk).
 *
 * Pure: takes the operators and links rather than the graph so that it can be tested and reused on any
 * snapshot of them.
 */
export function getEnclosingLoopStarts(
  operatorID: string,
  operators: ReadonlyArray<BlockOperator>,
  links: ReadonlyArray<BlockLink>
): string[] {
  const typeOf = new Map(operators.map(operator => [operator.operatorID, operator.operatorType]));
  const operatorType = typeOf.get(operatorID);
  if (operatorType === undefined) {
    return [];
  }
  const upstream = new Map<string, string[]>();
  const downstream = new Map<string, string[]>();
  for (const link of links) {
    addNeighbor(downstream, link.source.operatorID, link.target.operatorID);
    addNeighbor(upstream, link.target.operatorID, link.source.operatorID);
  }

  // Downstream, a LoopStart opens a nested block and a LoopEnd closes one; the operator's own block is
  // closed by a LoopEnd met with none open. A LoopStart starts the walk with its own block open.
  const closingEnds = walk(operatorID, downstream, typeOf, {
    opens: LOOP_START_OP_TYPE,
    closes: LOOP_END_OP_TYPE,
    initiallyOpen: operatorType === LOOP_START_OP_TYPE ? 1 : 0,
  });
  if (closingEnds.length === 0) {
    return [];
  }
  // Upstream it is the mirror image: a LoopEnd opens a block that lies entirely before the operator.
  const enclosingStarts = walk(operatorID, upstream, typeOf, {
    opens: LOOP_END_OP_TYPE,
    closes: LOOP_START_OP_TYPE,
    initiallyOpen: operatorType === LOOP_END_OP_TYPE ? 1 : 0,
  });
  return enclosingStarts.sort((a, b) => b.hops - a.hops).map(hit => hit.operatorID);
}

function addNeighbor(adjacency: Map<string, string[]>, from: string, to: string): void {
  const neighbors = adjacency.get(from);
  if (neighbors === undefined) {
    adjacency.set(from, [to]);
  } else {
    neighbors.push(to);
  }
}

/**
 * Breadth-first walk from `origin` over `adjacency`, counting the blocks opened and not yet closed along
 * each path. Returns the `closes`-typed operators met with no block open, each with its shortest hop
 * distance.
 *
 * A state is a (node, open count) pair, and the open count is capped at the number of `opens`-typed
 * operators plus the blocks open at the start. A path without repeated nodes passes each opener at most
 * once, so on a DAG the count never exceeds the cap and no result changes; on a cycle through an opener,
 * where every lap would open one more block, the cap keeps the states finite (nodes x (cap + 1)) and
 * the walk ends.
 */
function walk(
  origin: string,
  adjacency: ReadonlyMap<string, string[]>,
  typeOf: ReadonlyMap<string, string>,
  rule: { opens: string; closes: string; initiallyOpen: number }
): ControlOperatorHit[] {
  let openers = 0;
  typeOf.forEach(type => {
    if (type === rule.opens) {
      openers++;
    }
  });
  const maxOpen = openers + rule.initiallyOpen;
  const hits = new Map<string, number>();
  const visited = new Set<string>([`${origin}@${rule.initiallyOpen}`]);
  const queue: Array<{ node: string; open: number; hops: number }> = [
    { node: origin, open: rule.initiallyOpen, hops: 0 },
  ];
  for (let head = 0; head < queue.length; head++) {
    const { node, open, hops } = queue[head];
    for (const next of adjacency.get(node) ?? []) {
      let nextOpen = open;
      const nextType = typeOf.get(next);
      if (nextType === rule.opens) {
        nextOpen = open + 1;
        if (nextOpen > maxOpen) {
          continue; // only a lap around a cycle gets here: see the cap above
        }
      } else if (nextType === rule.closes) {
        if (open === 0) {
          if (!hits.has(next)) {
            hits.set(next, hops + 1);
          }
        } else {
          nextOpen = open - 1;
        }
      }
      const state = `${next}@${nextOpen}`;
      if (!visited.has(state)) {
        visited.add(state);
        queue.push({ node: next, open: nextOpen, hops: hops + 1 });
      }
    }
  }
  return Array.from(hits, ([operatorID, hops]) => ({ operatorID, hops }));
}
