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

// Runs the cases against a real model and prints how the agent did. See eval/README.md.

import { tmpdir } from "node:os";
import { join } from "node:path";
import { WorkflowSystemMetadata } from "../src/agent/util/workflow-system-metadata";
import { getBackendConfig } from "../src/api/backend-api";
import { CASES, type CaseContext, type EvalCase } from "./cases";
import { fail, type Verdict } from "./checks";
import { readEvalConfig, type EvalConfig } from "./config";
import { LlmRecorder } from "./llm-recorder";
import { generateOrders, ordersCsv } from "./orders";
import { formatReport, summarize, type RunRecord } from "./report";
import { AgentSession, type Turn } from "./session";
import { createWorkflow, deleteWorkflows, EMPTY_WORKFLOW, ensureDataset, login } from "./texera";

/** The agent saves its workflow 500 ms after a change; give that save time before reading. */
const SAVE_SETTLE_MS = 2000;

interface AgentService {
  url: string;
  log: string;
  stop(): void;
}

/** Starts this checkout's agent service on a free port, with its model calls sent to `llmEndpoint`. */
async function startAgentService(llmEndpoint: string): Promise<AgentService> {
  const probe = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = probe.port;
  probe.stop(true);
  const log = join(tmpdir(), `texera-agent-eval-${port}.log`);
  const child = Bun.spawn(["bun", "run", "src/server.ts"], {
    cwd: join(import.meta.dir, ".."),
    env: { ...process.env, PORT: String(port), LLM_ENDPOINT: llmEndpoint },
    stdout: Bun.file(log),
    stderr: Bun.file(log),
  });
  const url = `http://127.0.0.1:${port}`;
  for (let tries = 0; tries < 60; tries++) {
    const ok = await fetch(`${url}/api/healthcheck`).then(
      r => r.ok,
      () => false
    );
    if (ok) return { url, log, stop: () => child.kill() };
    await Bun.sleep(500);
  }
  child.kill();
  throw new Error(`the agent service did not start; see ${log}`);
}

async function runCase(
  evalCase: EvalCase,
  run: number,
  shared: Omit<CaseContext, "workflowId">,
  config: EvalConfig,
  agentUrl: string,
  recorder: LlmRecorder
): Promise<RunRecord> {
  const started = performance.now();
  const firstCall = recorder.calls.length;
  const content = evalCase.workflow?.(shared.ordersPath) ?? EMPTY_WORKFLOW;
  const workflowId = await createWorkflow(shared.token, `agent-eval ${evalCase.id} ${run}`, content);
  const ctx: CaseContext = { ...shared, workflowId };
  let session: AgentSession | undefined;
  let verdict: Verdict;
  try {
    await evalCase.before?.(ctx);
    session = await AgentSession.start(agentUrl, shared.token, {
      name: `eval ${evalCase.id} ${run}`,
      model: config.model,
      workflowId,
      computingUnitId: config.computingUnitId,
    });
    const prompts = evalCase.prompts(ctx);
    const turns: Turn[] = [];
    for (const [i, prompt] of prompts.entries()) {
      turns.push(await session.prompt(prompt, config.warehouseId));
      await Bun.sleep(SAVE_SETTLE_MS);
      if (i < prompts.length - 1) await evalCase.between?.(ctx, i);
    }
    verdict = await evalCase.check(ctx, turns);
  } catch (error) {
    verdict = fail(`the run broke: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    // The agent goes first, so its pending save cannot land on the deleted workflow.
    await session?.close();
    await deleteWorkflows(shared.token, [workflowId]);
  }
  return {
    caseId: evalCase.id,
    run,
    verdict,
    calls: recorder.calls.slice(firstCall),
    seconds: (performance.now() - started) / 1000,
  };
}

async function main(): Promise<void> {
  const config = readEvalConfig();
  const unknown = (config.caseIds ?? []).filter(id => !CASES.some(c => c.id === id));
  if (unknown.length > 0) throw new Error(`no such case: ${unknown.join(", ")}`);
  const cases = config.caseIds ? CASES.filter(c => config.caseIds!.includes(c.id)) : CASES;

  const recorder = new LlmRecorder(getBackendConfig().modelsEndpoint);
  const service = await startAgentService(recorder.start());
  console.log(`Agent service of this checkout at ${service.url} (log: ${service.log}), model ${config.model}`);
  try {
    const token = await login(config.username, config.password);
    await WorkflowSystemMetadata.getInstance().initializeFromBackend();
    const orders = generateOrders();
    const ordersPath = await ensureDataset(
      token,
      config.fileServiceEndpoint,
      "agent-eval-orders",
      "orders.csv",
      ordersCsv(orders)
    );
    const shared = {
      token,
      ordersPath,
      orders,
      computingUnitId: config.computingUnitId,
      warehouseId: config.warehouseId,
    };

    const records: RunRecord[] = [];
    for (const evalCase of cases) {
      for (let run = 1; run <= config.repeat; run++) {
        const record = await runCase(evalCase, run, shared, config, service.url, recorder);
        records.push(record);
        console.log(
          `${record.verdict.pass ? "PASS" : "FAIL"}  ${evalCase.id} ${run}/${config.repeat}: ${record.verdict.reason} ` +
            `(${record.calls.length} model calls, ${record.seconds.toFixed(0)} s)`
        );
      }
    }

    const summary = summarize(records);
    console.log(`\n${formatReport(summary)}`);
    if (config.out) {
      await Bun.write(config.out, JSON.stringify({ model: config.model, summary, records }, null, 2));
      console.log(`Wrote ${config.out}`);
    }
  } finally {
    service.stop();
    recorder.stop();
  }
}

await main();
