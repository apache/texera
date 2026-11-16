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

import { executeOperatorAndFormat } from "../src/agent/tools/workflow-execution-tools";
import { WorkflowSystemMetadata } from "../src/agent/util/workflow-system-metadata";
import { WorkflowUtilService } from "../src/agent/util/workflow-utils";
import { WorkflowState } from "../src/agent/workflow-state";
import { persistWorkflow, retrieveWorkflow } from "../src/api/workflow-api";
import type { OperatorPredicate, WorkflowContent } from "../src/types/workflow";
import { allOf, fail, mentionsAll, mentionsInOrder, mentionsNumber, pass, type Verdict } from "./checks";
import { countAbove, firstAbove, topCategoriesBySales, type Order } from "./orders";
import type { Turn } from "./session";

/** What one run of a case works with. */
export interface CaseContext {
  token: string;
  /** The orders file, as a file scan takes it. */
  ordersPath: string;
  orders: Order[];
  /** The workflow made for this run; the agent is delegated to it. */
  workflowId: number;
  computingUnitId: number;
  warehouseId?: number;
}

export interface EvalCase {
  id: string;
  /** The workflow before the first prompt; an empty one when absent. */
  workflow?(ordersPath: string): WorkflowContent;
  /** What the user does once the workflow is saved, before the first prompt. */
  before?(ctx: CaseContext): Promise<void>;
  prompts(ctx: CaseContext): string[];
  /** What the user does between prompt `index` and the next one. */
  between?(ctx: CaseContext, index: number): Promise<void>;
  check(ctx: CaseContext, turns: Turn[]): Promise<Verdict>;
}

interface OperatorSpec {
  type: string;
  name: string;
  properties: Record<string, unknown>;
}

/** A new operator of the given type, with its schema's defaults under the given properties. */
function operator(state: WorkflowState, spec: OperatorSpec, id?: string): OperatorPredicate {
  const made = new WorkflowUtilService(WorkflowSystemMetadata.getInstance(), state).getNewOperatorPredicate(
    spec.type,
    spec.name
  );
  return {
    ...made,
    operatorID: id ?? made.operatorID,
    operatorProperties: { ...made.operatorProperties, ...spec.properties },
  };
}

function link(state: WorkflowState, from: OperatorPredicate, to: OperatorPredicate): void {
  state.addLink({
    linkID: state.generateLinkId(),
    source: { operatorID: from.operatorID, portID: from.outputPorts[0].portID },
    target: { operatorID: to.operatorID, portID: to.inputPorts[0].portID },
  });
}

/** A workflow of operators in a row, each fed by the one before it. */
function chain(...specs: OperatorSpec[]): WorkflowContent {
  const state = new WorkflowState();
  let previous: OperatorPredicate | undefined;
  specs.forEach((spec, i) => {
    const op = operator(state, spec);
    state.addOperator(op, { x: 200 + 250 * i, y: 200 });
    if (previous) link(state, previous, op);
    previous = op;
  });
  return state.getWorkflowContent();
}

const scanOrders = (path: string): OperatorSpec => ({
  type: "CSVFileScan",
  name: "Load orders",
  properties: { fileName: path, hasHeader: true },
});

/** The amount above which `checkAmounts` fails. */
const AMOUNT_LIMIT = 250;

/**
 * A Python operator that fails on the first order above the limit, and names that order in its
 * error. The order's id is in the execution's error and nowhere in the workflow, so an answer that
 * names it has read the execution rather than guessed from the operators.
 */
const checkAmounts: OperatorSpec = {
  type: "PythonUDFV2",
  name: "Check amounts",
  properties: {
    code: [
      "from pytexera import *",
      "",
      "",
      "class ProcessTupleOperator(UDFOperatorV2):",
      "    @overrides",
      "    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:",
      `        if tuple_["amount"] > ${AMOUNT_LIMIT}:`,
      `            raise ValueError(f"order {tuple_['order_id']} is over the limit")`,
      "        yield tuple_",
    ].join("\n"),
  },
};

/** The answer names the order the failed execution stopped at. */
const namesFailingOrder = (ctx: CaseContext, answer: string): Verdict => {
  const order = firstAbove(ctx.orders, AMOUNT_LIMIT);
  if (!order) return fail(`no order is above ${AMOUNT_LIMIT}, so nothing fails`);
  return mentionsNumber(answer, order.order_id).pass
    ? pass(`names order ${order.order_id}, the one the execution failed on`)
    : fail(`does not name order ${order.order_id}, the one the execution failed on`);
};

const saved = async (ctx: CaseContext): Promise<WorkflowContent> =>
  (await retrieveWorkflow(ctx.token, ctx.workflowId)).content;

/** The id the user's own operator gets in `user-edit-between-prompts`. */
const USER_LIMIT_ID = "user-limit";

export const CASES: EvalCase[] = [
  {
    id: "build-workflow",
    prompts: ctx => [
      `Load ${ctx.ordersPath} and tell me the three product categories with the highest total sales, highest first. Build it as a workflow and run it.`,
    ],
    check: async (ctx, turns) => mentionsInOrder(turns[0].answer, topCategoriesBySales(ctx.orders, 3)),
  },
  {
    id: "fix-failing-operator",
    workflow: path =>
      chain(scanOrders(path), {
        type: "Filter",
        name: "Large orders",
        properties: { predicates: [{ attribute: "amout", condition: ">", value: "100" }] },
      }),
    prompts: () => [
      "The filter in this workflow is broken. Fix it so it keeps the orders with an amount above 100, run it, and tell me how many orders it keeps.",
    ],
    check: async (ctx, turns) => {
      const filter = (await saved(ctx)).operators.find(op => op.operatorType === "Filter");
      const attribute = filter?.operatorProperties.predicates?.[0]?.attribute;
      return allOf(
        mentionsNumber(turns[0].answer, countAbove(ctx.orders, 100)),
        attribute === "amount" ? pass("fixed the filter") : fail(`left the filter on ${JSON.stringify(attribute)}`)
      );
    },
  },
  {
    id: "explain-execution-error",
    workflow: path => chain(scanOrders(path), checkAmounts),
    prompts: () => ["Run this workflow and tell me why it fails."],
    check: async (ctx, turns) => namesFailingOrder(ctx, turns[0].answer),
  },
  {
    id: "docs-question",
    prompts: () => ["What output port modes can a Texera operator have? Name each of them."],
    check: async (_, turns) => mentionsAll(turns[0].answer, ["set snapshot", "delta", "single snapshot"]),
  },
  {
    id: "user-edit-between-prompts",
    prompts: ctx => [
      `Load ${ctx.ordersPath} with a CSV scan and run it.`,
      "Add a filter after the CSV scan that keeps the orders with an amount above 100, and run it.",
    ],
    // The user adds a Limit after the agent's CSV scan and saves, as the canvas does.
    between: async ctx => {
      const workflow = await retrieveWorkflow(ctx.token, ctx.workflowId);
      const state = new WorkflowState();
      state.setWorkflowContent(workflow.content);
      const scan = state.getAllOperators().find(op => op.operatorType === "CSVFileScan");
      if (!scan) return;
      const limit = operator(state, { type: "Limit", name: "My limit", properties: { limit: 10 } }, USER_LIMIT_ID);
      state.addOperator(limit, { x: 450, y: 400 });
      link(state, scan, limit);
      await persistWorkflow(ctx.token, ctx.workflowId, workflow.name, state.getWorkflowContent());
    },
    check: async ctx =>
      (await saved(ctx)).operators.some(op => op.operatorID === USER_LIMIT_ID)
        ? pass("kept the user's Limit")
        : fail("lost the Limit the user added between the prompts"),
  },
  {
    id: "user-execution-failed",
    workflow: path => chain(scanOrders(path), checkAmounts),
    // The user runs the workflow, and it fails.
    before: async ctx => {
      const state = new WorkflowState();
      state.setWorkflowContent(await saved(ctx));
      const last = state.getAllOperators().find(op => op.operatorType === "PythonUDFV2")!;
      await executeOperatorAndFormat(
        state,
        {
          userToken: ctx.token,
          workflowId: ctx.workflowId,
          computingUnitId: ctx.computingUnitId,
          warehouseId: ctx.warehouseId,
        },
        last.operatorID
      );
    },
    prompts: () => ["My last execution of this workflow failed. Without running anything, tell me why."],
    check: async (ctx, turns) => {
      const executed = turns[0].steps.some(step => step.toolCalls?.some(call => /execute/i.test(call.toolName)));
      return executed ? fail("ran the workflow itself") : namesFailingOrder(ctx, turns[0].answer);
    },
  },
];
