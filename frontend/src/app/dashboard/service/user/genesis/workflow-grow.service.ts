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
import { WorkflowContent, WorkflowSettings } from "../../../../common/type/workflow";
import {
  OperatorLink,
  OperatorPredicate,
  Point,
  PortDescription,
} from "../../../../workspace/types/workflow-common.interface";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";

const GENESIS_GROW_LOG_PREFIX = "[Genesis grow]";

/** Backend / Iris-style templates use `allowMultiInputs`; the Texera UI graph uses `disallowMultiInputs`. */
type PortJson = PortDescription & Readonly<{ allowMultiInputs?: boolean }>;

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Defer work to after the current change-detection pass (avoids NG0100 during graph churn). */
const CHANGE_DETECTION_YIELD_MS = 50;

function normalizePorts(ports: readonly PortJson[] | undefined): PortDescription[] {
  if (ports == null || ports.length === 0) {
    return [];
  }
  return ports.map(p => {
    const { allowMultiInputs, ...rest } = p;
    let disallowMultiInputs = rest.disallowMultiInputs;
    if (disallowMultiInputs === undefined && typeof allowMultiInputs === "boolean") {
      disallowMultiInputs = !allowMultiInputs;
    }
    return {
      ...rest,
      ...(disallowMultiInputs !== undefined ? { disallowMultiInputs } : {}),
    };
  });
}

function normalizeOperatorForGrow(op: OperatorPredicate): OperatorPredicate {
  const opAny = op as OperatorPredicate & { inputPorts?: PortJson[]; outputPorts?: PortJson[] };
  return {
    ...op,
    inputPorts: normalizePorts(opAny.inputPorts ?? []),
    outputPorts: normalizePorts(opAny.outputPorts ?? []),
  };
}

/** Shift template coordinates so nothing starts left of / above the visible workspace. */
function computeGrowPositionOffset(
  positions: Readonly<Record<string, Point | undefined>>,
  minVisibleX = 80,
  minVisibleY = 60
): Readonly<{ dx: number; dy: number }> {
  const vals = Object.values(positions).filter((p): p is Point => p != null);
  if (vals.length === 0) {
    return { dx: 0, dy: 0 };
  }
  const minX = Math.min(...vals.map(p => p.x));
  const minY = Math.min(...vals.map(p => p.y));
  return {
    dx: minX < minVisibleX ? minVisibleX - minX : 0,
    dy: minY < minVisibleY ? minVisibleY - minY : 0,
  };
}

function resolveGrowPoint(
  opId: string,
  positions: Readonly<Record<string, Point | undefined>>,
  index: number,
  offset: Readonly<{ dx: number; dy: number }>
): Point {
  const raw = positions[opId];
  if (raw != null) {
    return { x: raw.x + offset.dx, y: raw.y + offset.dy };
  }
  return { x: 160 + index * 140, y: 200 };
}

function sortReadyQueue(
  queue: string[],
  opMap: Map<string, OperatorPredicate>,
  orderIndex: Map<string, number>
): void {
  queue.sort((a, b) => {
    /** Treat missing inputPorts like no inputs (source operators). */
    const ap = opMap.get(a)?.inputPorts?.length ?? 0;
    const bp = opMap.get(b)?.inputPorts?.length ?? 0;
    /** True sources (no input ports, e.g. CSVFileScan) must run before downstream ops when in-degree ties. */
    const as = ap === 0 ? 0 : 1;
    const bs = bp === 0 ? 0 : 1;
    if (as !== bs) {
      return as - bs;
    }
    return (orderIndex.get(a) ?? 0) - (orderIndex.get(b) ?? 0);
  });
}

/**
 * Topological order: operators with no incoming edges first. Among same layer, prefer
 * `inputPorts.length === 0` (CSVFileScan and other sources) so they render first.
 */
function topologicalOperatorOrder(operators: OperatorPredicate[], links: OperatorLink[]): OperatorPredicate[] {
  const opMap = new Map(operators.map(op => [op.operatorID, op]));
  const orderIndex = new Map(operators.map((op, i) => [op.operatorID, i]));
  const inDegree = new Map<string, number>();
  operators.forEach(op => inDegree.set(op.operatorID, 0));
  links.forEach(link => {
    const t = link.target.operatorID;
    if (inDegree.has(t)) {
      inDegree.set(t, (inDegree.get(t) ?? 0) + 1);
    }
  });
  const queue: string[] = [];
  inDegree.forEach((deg, id) => {
    if (deg === 0) {
      queue.push(id);
    }
  });
  const ordered: OperatorPredicate[] = [];
  while (queue.length > 0) {
    sortReadyQueue(queue, opMap, orderIndex);
    console.log(
      `${GENESIS_GROW_LOG_PREFIX} ready queue size:`,
      queue.length,
      queue.map(oid => `${oid}(${opMap.get(oid)?.operatorType ?? "?"})`)
    );
    const id = queue.shift()!;
    const op = opMap.get(id);
    if (!op) {
      console.warn(`${GENESIS_GROW_LOG_PREFIX} skipping operator because:`, `operatorID ${id} missing from graph`);
      continue;
    }
    ordered.push(op);
    links
      .filter(l => l.source.operatorID === id)
      .forEach(l => {
        const tid = l.target.operatorID;
        if (!inDegree.has(tid)) {
          return;
        }
        const next = (inDegree.get(tid) ?? 0) - 1;
        inDegree.set(tid, next);
        if (next === 0) {
          queue.push(tid);
        }
      });
    sortReadyQueue(queue, opMap, orderIndex);
  }
  if (ordered.length !== operators.length) {
    return [...operators].sort((a, b) => {
      const ap = a.inputPorts?.length ?? 0;
      const bp = b.inputPorts?.length ?? 0;
      if (ap === 0 && bp > 0) {
        return -1;
      }
      if (bp === 0 && ap > 0) {
        return 1;
      }
      return (orderIndex.get(a.operatorID) ?? 0) - (orderIndex.get(b.operatorID) ?? 0);
    });
  }
  return ordered;
}

@Injectable({
  providedIn: "root",
})
export class WorkflowGrowAnimator {
  constructor(private workflowActionService: WorkflowActionService) {}

  public async grow(workflowJson: Partial<WorkflowContent> | null | undefined, stepDelayMs = 500): Promise<void> {
    if (!workflowJson?.operators?.length) {
      if (workflowJson?.settings) {
        this.workflowActionService.setWorkflowSettings(workflowJson.settings as WorkflowSettings);
      }
      return;
    }

    const operators = workflowJson.operators as OperatorPredicate[];
    const links = (workflowJson.links ?? []) as OperatorLink[];
    const positions = workflowJson.operatorPositions ?? {};
    const positionOffset = computeGrowPositionOffset(positions);
    if (positionOffset.dx !== 0 || positionOffset.dy !== 0) {
      console.log(
        `${GENESIS_GROW_LOG_PREFIX} shifting operator positions by`,
        positionOffset,
        `(keeps off-canvas template coords visible)`
      );
    }
    const ordered = topologicalOperatorOrder(operators, links);

    if (workflowJson.settings) {
      this.workflowActionService.setWorkflowSettings(workflowJson.settings as WorkflowSettings);
    }

    let index = 0;
    for (const op of ordered) {
      await sleep(CHANGE_DETECTION_YIELD_MS);
      const normalized = normalizeOperatorForGrow(op);
      const point = resolveGrowPoint(op.operatorID, positions, index, positionOffset);
      const inputPortsForLog = normalized.inputPorts ?? [];
      console.log(
        `${GENESIS_GROW_LOG_PREFIX} growing operator:`,
        normalized.operatorType,
        "inputPorts length:",
        inputPortsForLog.length,
        "inputPorts:",
        inputPortsForLog
      );
      try {
        this.workflowActionService.addOperator(normalized, point);
      } catch (err) {
        console.error(`${GENESIS_GROW_LOG_PREFIX} addOperator FAILED for`, normalized.operatorType, err);
        throw err;
      }
      const incoming = links.filter(l => l.target.operatorID === op.operatorID);
      for (const link of incoming) {
        await sleep(CHANGE_DETECTION_YIELD_MS);
        try {
          this.workflowActionService.addLink(link);
        } catch (err) {
          console.error(`${GENESIS_GROW_LOG_PREFIX} addLink FAILED for`, link.linkID, err);
          throw err;
        }
      }
      index++;
      await sleep(stepDelayMs);
    }
  }
}
