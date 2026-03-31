/**
 * End-to-end test: login, retrieve workflow, execute it, and print OperatorInfo for each operator.
 *
 * Usage:
 *   npx tsx src/test_e2e.ts
 *
 * Environment variables (all optional, defaults to localhost):
 *   API_ENDPOINT        - Main API (default http://localhost:8080)
 *   EXECUTION_ENDPOINT  - Execution service (default http://localhost:8085)
 *   TEXERA_USERNAME     - Login username (default "texera")
 *   TEXERA_PASSWORD     - Login password (default "texera")
 *   USER_JWT_TOKEN      - Pre-existing JWT token (skips login if set)
 */

// ─── Configuration ───────────────────────────────────────────────────────────

const API_ENDPOINT = process.env.API_ENDPOINT ?? "http://localhost:8080";
const EXECUTION_ENDPOINT = process.env.EXECUTION_ENDPOINT ?? "http://localhost:8085";
const COMPUTING_ENDPOINT = process.env.COMPUTING_ENDPOINT ?? "http://localhost:8888";
const USERNAME = process.env.TEXERA_USERNAME ?? "Bob";
const PASSWORD = process.env.TEXERA_PASSWORD ?? "123456";
const PRE_EXISTING_TOKEN = process.env.USER_JWT_TOKEN;
const WORKFLOW_ID = 5737;
const TIMEOUT_SECONDS = 240;

// ─── Types (inline, mirrors agent-service types) ─────────────────────────────

import type { SyncExecutionResult, OperatorInfo } from "./types/execution";

interface WorkflowContent {
  operators: any[];
  links: any[];
  settings?: any;
  [key: string]: any;
}

interface Workflow {
  wid: number;
  name: string;
  content: WorkflowContent;
}

// ─── Step 1: Login ───────────────────────────────────────────────────────────

async function login(username: string, password: string): Promise<string> {
  const url = `${API_ENDPOINT}/api/auth/login`;
  console.log(`[1/4] Logging in as "${username}" at ${url} ...`);

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Login failed: ${res.status} ${res.statusText} - ${text}`);
  }

  const data: { accessToken: string } = await res.json();
  console.log(`    Login successful. Token length: ${data.accessToken.length}`);
  return data.accessToken;
}

// ─── Step 1b: Get computing unit ─────────────────────────────────────────────

async function getComputingUnitId(token: string): Promise<number> {
  const url = `${COMPUTING_ENDPOINT}/api/computing-unit`;
  console.log(`[1b/4] Fetching computing units from ${url} ...`);

  const res = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    console.log(`    Could not fetch computing units (${res.status}), defaulting to 0`);
    return 0;
  }

  const units: any[] = await res.json();
  const running = units.find((u: any) => u.status === "Running");
  if (running) {
    const cuid = running.computingUnit.cuid;
    console.log(`    Using computing unit: ${running.computingUnit.name} (cuid=${cuid})`);
    return cuid;
  }

  console.log(`    No running computing unit found, defaulting to 0`);
  return 0;
}

// ─── Step 2: Retrieve workflow ───────────────────────────────────────────────

async function retrieveWorkflow(token: string, wid: number): Promise<Workflow> {
  const url = `${API_ENDPOINT}/api/workflow/${wid}`;
  console.log(`[2/4] Retrieving workflow ${wid} from ${url} ...`);

  const res = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to retrieve workflow: ${res.status} ${res.statusText} - ${text}`);
  }

  const data: Workflow = await res.json();
  if (typeof data.content === "string") {
    data.content = JSON.parse(data.content as unknown as string);
  }

  console.log(
    `    Workflow "${data.name}" loaded: ${data.content.operators.length} operators, ${data.content.links.length} links`
  );
  return data;
}

// ─── Step 3: Build logical plan ──────────────────────────────────────────────

function buildLogicalPlan(content: WorkflowContent) {
  const enabledOperators = content.operators.filter((op: any) => !op.isDisabled);
  const enabledOpIds = new Set(enabledOperators.map((op: any) => op.operatorID));

  // Build a port-index lookup per operator
  const getInputPortOrdinal = (operatorID: string, inputPortID: string): number => {
    const op = enabledOperators.find((o: any) => o.operatorID === operatorID);
    if (!op) return 0;
    const idx = op.inputPorts.findIndex((p: any) => p.portID === inputPortID);
    return idx >= 0 ? idx : 0;
  };

  const getOutputPortOrdinal = (operatorID: string, outputPortID: string): number => {
    const op = enabledOperators.find((o: any) => o.operatorID === operatorID);
    if (!op) return 0;
    const idx = op.outputPorts.findIndex((p: any) => p.portID === outputPortID);
    return idx >= 0 ? idx : 0;
  };

  const operators = enabledOperators.map((op: any) => ({
    ...op.operatorProperties,
    operatorID: op.operatorID,
    operatorType: op.operatorType,
    inputPorts: op.inputPorts,
    outputPorts: op.outputPorts,
  }));

  const links = content.links
    .filter((link: any) => enabledOpIds.has(link.source.operatorID) && enabledOpIds.has(link.target.operatorID))
    .map((link: any) => ({
      fromOpId: link.source.operatorID,
      fromPortId: { id: getOutputPortOrdinal(link.source.operatorID, link.source.portID), internal: false },
      toOpId: link.target.operatorID,
      toPortId: { id: getInputPortOrdinal(link.target.operatorID, link.target.portID), internal: false },
    }));

  // Sink operators: those with no outgoing links
  const fromOpIds = new Set(links.map((l: any) => l.fromOpId));
  const opsToViewResult = operators.filter((op: any) => !fromOpIds.has(op.operatorID)).map((op: any) => op.operatorID);

  console.log(`[3/4] Built logical plan: ${operators.length} operators, ${links.length} links`);
  console.log(`      Operators to view result: [${opsToViewResult.join(", ")}]`);

  return { operators, links, opsToViewResult };
}

// ─── Step 4: Execute workflow ────────────────────────────────────────────────

async function executeWorkflow(
  token: string,
  workflowId: number,
  computingUnitId: number,
  logicalPlan: { operators: any[]; links: any[]; opsToViewResult: string[] }
): Promise<SyncExecutionResult> {
  const url = `${EXECUTION_ENDPOINT}/api/execution/${workflowId}/${computingUnitId}/run`;
  console.log(`[4/4] Executing workflow at ${url} ...`);

  const request = {
    executionName: "e2e-test-execution",
    logicalPlan: {
      operators: logicalPlan.operators,
      links: logicalPlan.links,
      opsToViewResult: logicalPlan.opsToViewResult,
      opsToReuseResult: [],
    },
    targetOperatorIds: logicalPlan.opsToViewResult,
    timeoutSeconds: TIMEOUT_SECONDS,
    maxOperatorResultCharLimit: 40000,
    maxOperatorResultCellCharLimit: 20000,
    cacheEnabled: true,
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(request),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Execution failed: ${res.status} ${res.statusText} - ${text}`);
  }

  return await res.json();
}

// ─── Print OperatorInfo ──────────────────────────────────────────────────────

function printOperatorInfo(operatorId: string, info: OperatorInfo) {
  console.log(`\n  ── Operator: ${operatorId} ──`);

  // Print the full raw JSON (with result rows truncated for readability)
  const display = { ...info };
  if (display.result && display.result.length > 3) {
    display.result = [...display.result.slice(0, 3), { "...": `(${info.result!.length} rows total)` } as any];
  }
  console.log(JSON.stringify(display, null, 2));
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  console.log("=== Texera Agent Service E2E Test ===\n");

  // Step 1: Login (or use pre-existing token)
  let token: string;
  if (PRE_EXISTING_TOKEN) {
    console.log(`[1/4] Using pre-existing JWT token (length: ${PRE_EXISTING_TOKEN.length})`);
    token = PRE_EXISTING_TOKEN;
  } else {
    token = await login(USERNAME, PASSWORD);
  }

  // Step 1b: Get computing unit
  const computingUnitId = await getComputingUnitId(token);

  // Step 2: Retrieve workflow
  const workflow = await retrieveWorkflow(token, WORKFLOW_ID);

  // Step 3: Build logical plan
  const logicalPlan = buildLogicalPlan(workflow.content);

  // Step 4: Execute
  const result = await executeWorkflow(token, WORKFLOW_ID, computingUnitId, logicalPlan);

  // Print results
  console.log(`\n=== Execution Result ===`);
  console.log(`success: ${result.success}`);
  console.log(`state:   ${result.state}`);

  if (result.compilationErrors && Object.keys(result.compilationErrors).length > 0) {
    console.log(`\nCompilation errors:`);
    for (const [key, val] of Object.entries(result.compilationErrors)) {
      console.log(`  ${key}: ${val}`);
    }
  }

  if (result.errors && result.errors.length > 0) {
    console.log(`\nGeneral errors:`);
    for (const err of result.errors) {
      console.log(`  ${err}`);
    }
  }

  const operatorIds = Object.keys(result.operators);
  console.log(`\nOperators in result: ${operatorIds.length}`);

  for (const opId of operatorIds) {
    printOperatorInfo(opId, result.operators[opId]);
  }

  console.log("\n=== Done ===");
}

main().catch(err => {
  console.error("\nFATAL:", err);
  process.exit(1);
});
