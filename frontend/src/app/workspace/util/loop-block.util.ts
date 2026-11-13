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

import { OperatorLink, OperatorPredicate } from "../types/workflow-common.interface";

/** The operator types that open and close a loop block, as the backend names them. */
export const LOOP_START_OP_TYPE = "LoopStart";
export const LOOP_END_OP_TYPE = "LoopEnd";

type BlockOperator = Pick<OperatorPredicate, "operatorID" | "operatorType">;
type BlockLink = {
  source: Pick<OperatorLink["source"], "operatorID">;
  target: Pick<OperatorLink["target"], "operatorID">;
};

/**
 * For each operator inside a loop block, the Loop Starts of the blocks it is inside, outermost first; an
 * operator outside every block is absent. A port of the backend's LogicalPlan.enclosingLoopStarts: an
 * operator is inside a block when it is on a path Loop Start -> ... -> operator -> ... -> Loop End whose
 * two ends match, and the Loop Starts around it are those with a path to it on which no Loop End closes
 * their block (a Loop End closes the innermost block the path opened). A Loop Start or Loop End is not
 * inside its own block, but an inner block's are inside the outer one. The backend's LogicalPlanSpec and
 * this port's spec run the same cases (loop-blocks.json), so the two agree.
 *
 * Which blocks enclose an operator depends only on the operators' types and the links, so it is given
 * those of the plan the backend compiles and runs, valid or not: the enabled operators and the links
 * between them (loopVariablesInScope). An outer block's Loop Start encloses an inner block's, so the fewer
 * Loop Starts enclose a Loop Start, the further out its block is; Loop Starts of blocks that do not nest
 * follow their ids.
 *
 * The backend rejects a graph with a cycle, which the canvas lets a user draw: an operator on a cycle, or
 * after one, has no place in the topological order followed below, so it counts as outside every block,
 * and so does every path through it.
 */
export function enclosingLoopStarts(
  operators: ReadonlyArray<BlockOperator>,
  links: ReadonlyArray<BlockLink>
): ReadonlyMap<string, ReadonlyArray<string>> {
  // 1 for a Loop Start, which opens a block, -1 for a Loop End, which closes one, 0 otherwise.
  const change = new Map(
    operators.map(({ operatorID, operatorType }) => [
      operatorID,
      operatorType === LOOP_START_OP_TYPE ? 1 : operatorType === LOOP_END_OP_TYPE ? -1 : 0,
    ])
  );
  const upstream = new Map<string, string[]>();
  const downstream = new Map<string, string[]>();
  for (const { source, target } of links) {
    if (change.has(source.operatorID) && change.has(target.operatorID)) {
      neighbors(downstream, source.operatorID).push(target.operatorID);
      neighbors(upstream, target.operatorID).push(source.operatorID);
    }
  }
  const order = topologicalOrder([...change.keys()], upstream, downstream);
  const changeOf = (operatorID: string) => change.get(operatorID) ?? 0;

  // The most blocks a path from each operator closes that it did not open, the operator included; a block
  // closed below is one besides a Loop End's own.
  const closing = new Map<string, number>();
  for (const operatorID of [...order].reverse()) {
    const below = (downstream.get(operatorID) ?? []).flatMap(next => closing.get(next) ?? []);
    closing.set(operatorID, Math.max(0, ...below) - changeOf(operatorID));
  }
  const closedBelow = new Set(
    order.filter(operatorID => (closing.get(operatorID) ?? 0) > (changeOf(operatorID) < 0 ? 1 : 0))
  );

  const starts = new Map<string, string[]>();
  order.forEach((start, position) => {
    if (changeOf(start) <= 0) {
      return;
    }
    // The most blocks a path from `start` leaves open at each operator, its own still open.
    const open = new Map([[start, 1]]);
    for (const operatorID of order.slice(position + 1)) {
      const above = (upstream.get(operatorID) ?? []).flatMap(previous => open.get(previous) ?? []);
      const blocks = above.length === 0 ? 0 : Math.max(...above) + changeOf(operatorID);
      if (blocks > 0) {
        open.set(operatorID, blocks);
      }
    }
    open.forEach((_, operatorID) => {
      if (operatorID !== start && closedBelow.has(operatorID)) {
        neighbors(starts, operatorID).push(start);
      }
    });
  });

  const depth = (start: string) => starts.get(start)?.length ?? 0;
  return new Map(
    [...starts].map(([operatorID, around]) => [
      operatorID,
      around.sort((a, b) => depth(a) - depth(b) || (a < b ? -1 : a > b ? 1 : 0)),
    ])
  );
}

function neighbors(adjacency: Map<string, string[]>, operatorID: string): string[] {
  let list = adjacency.get(operatorID);
  if (list === undefined) {
    list = [];
    adjacency.set(operatorID, list);
  }
  return list;
}

/**
 * The operators in an order in which each comes after every operator upstream of it (Kahn's algorithm).
 * One on a cycle, or after one, never has every upstream operator placed, and is left out.
 */
function topologicalOrder(
  operatorIDs: ReadonlyArray<string>,
  upstream: ReadonlyMap<string, ReadonlyArray<string>>,
  downstream: ReadonlyMap<string, ReadonlyArray<string>>
): string[] {
  const unplaced = new Map(operatorIDs.map(operatorID => [operatorID, upstream.get(operatorID)?.length ?? 0]));
  const order = operatorIDs.filter(operatorID => unplaced.get(operatorID) === 0);
  for (let head = 0; head < order.length; head++) {
    for (const next of downstream.get(order[head]) ?? []) {
      const left = (unplaced.get(next) ?? 0) - 1;
      unplaced.set(next, left);
      if (left === 0) {
        order.push(next);
      }
    }
  }
  return order;
}
