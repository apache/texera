/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { writeFile, mkdir, readFile, stat } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { join, dirname, resolve, sep, extname } from "node:path";
import { fileURLToPath } from "node:url";
const frontend = dirname(dirname(fileURLToPath(import.meta.url)));
const output = resolve(process.env.TEXERA_BROWSER_ARTIFACTS ?? join(frontend, "playwright-report"));
const dist = resolve(frontend, process.env.TEXERA_FRONTEND_DIST ?? "dist");
const require = createRequire(join(frontend, "package.json"));
const { chromium } = require("playwright");
const { WebSocketServer } = require("ws");
const { setupWSConnection, docs } = require("y-websocket/bin/utils");
const rtc = new WebSocketServer({ noServer: true });
const mime = {
  ".js": "application/javascript",
  ".css": "text/css",
  ".html": "text/html",
  ".json": "application/json",
  ".wasm": "application/wasm",
  ".ttf": "font/ttf",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".jpg": "image/jpeg",
  ".ico": "image/x-icon",
};
const server = createHttpServer(async (request, response) => {
  try {
    let file = resolve(dist, "." + decodeURIComponent(new URL(request.url, "http://127.0.0.1").pathname));
    if (file !== dist && !file.startsWith(dist + sep)) {
      response.writeHead(403).end();
      return;
    }
    try {
      if ((await stat(file)).isDirectory()) file = join(file, "index.html");
    } catch {
      if (extname(file)) {
        response.writeHead(404).end();
        return;
      }
      file = join(dist, "index.html");
    }
    const body = await readFile(file);
    response.writeHead(200, { "Content-Type": mime[extname(file)] ?? "application/octet-stream" });
    response.end(body);
  } catch {
    response.writeHead(500).end();
  }
});
server.on("upgrade", (request, socket, head) => {
  if (!request.url.startsWith("/rtc/")) {
    socket.destroy();
    return;
  }
  rtc.handleUpgrade(request, socket, head, connection => setupWSConnection(connection, request));
});
const browser = await chromium.launch({ headless: true });
const errors = [],
  requests = [],
  commands = [],
  connections = [];
const jwt = `header.${Buffer.from(JSON.stringify({ userId: 7, sub: "React reviewer", email: "reviewer@example.org", role: "REGULAR", exp: 9999999999 })).toString("base64url")}.signature`;
const schema = {
  operatorType: "PythonUDFSourceV2",
  operatorVersion: "p1",
  additionalMetadata: { userFriendlyName: "Python UDF", operatorGroupName: "UDF", inputPorts: [], outputPorts: [{}] },
  jsonSchema: {
    type: "object",
    properties: {
      code: {
        type: "string",
        title: "Code",
        default: 'print("hello")',
        widget: { formlyConfig: { type: "codearea" } },
      },
      label: { type: "string", title: "Label", default: "日本語" },
    },
  },
};
const filterSchema = {
  operatorType: "Filter",
  operatorVersion: "1",
  additionalMetadata: {
    userFriendlyName: "Filter",
    operatorGroupName: "Analysis",
    inputPorts: [{ disallowMultiLinks: true }],
    outputPorts: [{}],
  },
  jsonSchema: { type: "object", properties: {} },
};
let workflow = {
  wid: 42,
  name: "Browser workflow",
  description: "",
  readonly: false,
  isPublished: 0,
  creationTime: 1,
  lastModifiedTime: 1,
  content: {
    operators: [
      {
        operatorID: "python",
        operatorType: "PythonUDFSourceV2",
        operatorVersion: "p1",
        customDisplayName: "Python UDF",
        operatorProperties: { code: 'print("hello")', label: "日本語" },
        inputPorts: [],
        outputPorts: [{ portID: "output-0" }],
        showAdvanced: false,
      },
    ],
    operatorPositions: { python: { x: 100, y: 80 } },
    links: [],
    commentBoxes: [],
    settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
    formBinding: {
      fields: [{ id: "label-input", operatorID: "python", propertyKey: "label", displayName: "Input label" }],
      instruction: { title: "Workflow inputs", body: "Set the input, then run the workflow." },
      shownResultIds: ["python"],
    },
  },
};
const units = [
  {
    computingUnit: { cuid: 9, uid: 7, name: "Local", type: "local", creationTime: 1, uri: "", resource: {} },
    status: "Running",
    metrics: { cpuUsage: "0", memoryUsage: "0" },
    isOwner: true,
    accessPrivilege: "WRITE",
  },
];
async function configure(context) {
  await context.addInitScript(token => localStorage.setItem("access_token", token), jwt);
  await context.route("**/api/**", async route => {
    const url = new URL(route.request().url()),
      path = url.pathname;
    requests.push(path);
    let value = {};
    if (path.endsWith("/config/pre-login")) value = { localLogin: true };
    else if (path.endsWith("/settings/public"))
      value = { your_work_enabled: "true", workflows_enabled: "true", about_enabled: "true" };
    else if (path.includes("/config/"))
      value = { formViewEnabled: true, productionSharedEditingServer: true, workflowExecutionsTrackingEnabled: true };
    else if (path.endsWith("/resources/operator-metadata")) value = { operators: [schema, filterSchema], groups: [] };
    else if (path.endsWith("/workflow/42")) value = workflow;
    else if (path.endsWith("/workflow/persist")) {
      const body = route.request().postDataJSON();
      workflow = { ...workflow, ...body, content: JSON.parse(body.content) };
      value = workflow;
    } else if (path.endsWith("/computing-unit")) value = units;
    else if (path.endsWith("/executions/42/latest")) value = { cuId: 9 };
    else if (path.endsWith("/compile"))
      value = {
        physicalPlan: {},
        operatorOutputSchemas: { python: { "0_false": [{ attributeName: "a", attributeType: "integer" }] } },
        operatorErrors: {},
      };
    else if (path.includes("/dashboard/search")) value = { results: [], more: false };
    else if (path.includes("/version/")) value = [];
    else if (path.includes("/executions/")) value = [];
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(value) });
  });
  await context.routeWebSocket("**/wsapi/**", ws => {
    connections.push(ws);
    const send = value => ws.send(JSON.stringify(value));
    send({ type: "ClusterStatusUpdateEvent", numWorkers: 1 });
    send({ type: "WorkflowStateEvent", state: "Uninitialized" });
    ws.onMessage(raw => {
      const msg = JSON.parse(raw);
      commands.push(msg);
      if (msg.type === "WorkflowExecuteRequest") send({ type: "WorkflowStateEvent", state: "Running" });
      if (msg.type === "WorkflowPauseRequest") send({ type: "WorkflowStateEvent", state: "Paused" });
      if (msg.type === "WorkflowResumeRequest") send({ type: "WorkflowStateEvent", state: "Running" });
      if (msg.type === "WorkflowKillRequest") send({ type: "WorkflowStateEvent", state: "Completed" });
      if (msg.type === "ResultPaginationRequest")
        send({
          type: "PaginatedResultEvent",
          requestID: msg.requestID,
          operatorID: msg.operatorID,
          portIndex: msg.portIndex,
          pageIndex: msg.pageIndex,
          table: [{ a: 0 }, { a: "日本語" }],
          schema: [{ attributeName: "a", attributeType: "string" }],
          totalTuples: 2,
        });
    });
  });
}
try {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(4381, "127.0.0.1", resolve);
  });
  await mkdir(join(output, "screenshots"), { recursive: true });
  const context = await browser.newContext({ viewport: { width: 1600, height: 1000 } });
  await configure(context);
  const page = await context.newPage();
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("http://127.0.0.1:4381/user/workflow/42");
  await page.getByLabel("Workflow canvas", { exact: true }).waitFor();
  await page.getByLabel("Workflow canvas", { exact: true }).locator('[model-id="python"]').waitFor();
  await page.getByLabel("Workflow canvas", { exact: true }).locator('[model-id="python"]').click();
  await page.getByLabel("Label", { exact: true }).fill("Edited 日本語");
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.getByText("All changes saved", { exact: true }).waitFor();
  assert.equal(workflow.content.operators[0].operatorProperties.label, "Edited 日本語");
  await page.getByRole("button", { name: "Add Python UDF", exact: true }).click();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.getByText("All changes saved", { exact: true }).waitFor();
  const secondId = workflow.content.operators[1].operatorID;
  await page.getByLabel("Workflow canvas", { exact: true }).locator('[model-id="python"]').click();
  await page
    .getByLabel("Workflow canvas", { exact: true })
    .locator(`[model-id="${secondId}"]`)
    .click({ modifiers: ["Control"] });
  const oldPosition = { ...workflow.content.operatorPositions.python };
  const drag = await page
    .getByLabel("Workflow canvas", { exact: true })
    .locator(`[model-id="${secondId}"]`)
    .boundingBox();
  await page.mouse.move(drag.x + drag.width / 2, drag.y + drag.height / 2);
  await page.mouse.down();
  await page.mouse.move(drag.x + drag.width / 2 + 80, drag.y + drag.height / 2 + 60, { steps: 5 });
  await page.mouse.up();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.getByText("All changes saved", { exact: true }).waitFor();
  assert.notDeepEqual(
    workflow.content.operatorPositions.python,
    oldPosition,
    "dragging a selection moves all selected operators"
  );
  await page.getByRole("button", { name: "Add Filter", exact: true }).click();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.getByText("All changes saved", { exact: true }).waitFor();
  const filterId = workflow.content.operators.find(operator => operator.operatorType === "Filter").operatorID;
  const mainCanvas = page.getByLabel("Workflow canvas", { exact: true });
  await mainCanvas
    .locator('[model-id="python"] .port-body[port="output-0"]')
    .dragTo(mainCanvas.locator(`[model-id="${filterId}"] .port-body[port="input-0"]`));
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.getByText("All changes saved", { exact: true }).waitFor();
  assert.ok(
    workflow.content.links.some(link => link.source.operatorID === "python" && link.target.operatorID === filterId),
    "a real SVG port drag persists the link"
  );
  const rejectedSource = await mainCanvas.locator(`[model-id="${secondId}"] .port-body[port="output-0"]`).boundingBox();
  const occupiedTarget = await mainCanvas.locator(`[model-id="${filterId}"] .port-body[port="input-0"]`).boundingBox();
  assert.ok(rejectedSource && occupiedTarget);
  await page.mouse.move(rejectedSource.x + rejectedSource.width / 2, rejectedSource.y + rejectedSource.height / 2);
  await page.mouse.down();
  await page.mouse.move(occupiedTarget.x + occupiedTarget.width / 2, occupiedTarget.y + occupiedTarget.height / 2, {
    steps: 5,
  });
  await page.mouse.up();
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await page.getByText("All changes saved", { exact: true }).waitFor();
  assert.equal(workflow.content.links.length, 1, "an occupied single-link input rejects a second wire");
  await page.screenshot({ path: join(output, "screenshots/react-workspace.png"), fullPage: true });
  const socketCount = connections.length;
  await page.getByRole("button", { name: "Form view", exact: true }).click();
  await page.getByLabel("Input label", { exact: true }).fill("Form value Ω");
  await page.getByRole("button", { name: "Canvas view", exact: true }).first().click();
  assert.equal(connections.length, socketCount, "view toggle must not reconnect execution socket");
  await page.getByLabel("Workflow canvas", { exact: true }).locator('[model-id="python"]').click();
  assert.equal(
    await page.getByLabel("Label", { exact: true }).inputValue(),
    "Form value Ω",
    "form inputs edit the same live canvas property"
  );
  await page.getByRole("button", { name: "Edit code", exact: true }).click();
  await page.locator(".monaco-editor .view-lines").first().waitFor({ timeout: 60000 });
  await page.screenshot({ path: join(output, "screenshots/react-monaco.png"), fullPage: true });
  await page
    .getByRole("button", { name: /Close.*editor|Close code|Close Python|Close dialog/i })
    .first()
    .click();
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await page.getByRole("button", { name: "Resume", exact: true }).click();
  await page.getByRole("button", { name: "Kill", exact: true }).click();
  assert.ok(commands.some(value => value.type === "WorkflowExecuteRequest"));
  for (const ws of connections)
    ws.send(
      JSON.stringify({
        type: "WebResultUpdateEvent",
        updates: { python: { mode: { type: "PaginationMode" }, totalNumTuples: 2, dirtyPageIndices: [1] } },
        tableStats: {},
      })
    );
  await page.getByRole("cell", { name: "日本語", exact: true }).waitFor();
  const visibleRow = await page.getByRole("cell", { name: "日本語", exact: true }).boundingBox();
  const resultsDock = await page.locator(".react-workspace-results").boundingBox();
  if (visibleRow && resultsDock && visibleRow.y + visibleRow.height > resultsDock.y + resultsDock.height)
    console.log({ visibleRow, resultsDock });
  assert.ok(
    visibleRow &&
      resultsDock &&
      visibleRow.y >= resultsDock.y &&
      visibleRow.y + visibleRow.height <= resultsDock.y + resultsDock.height,
    "result rows must be visible inside the dock without scrolling its header away"
  );
  await page.screenshot({ path: join(output, "screenshots/react-results.png"), fullPage: true });
  const other = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await configure(other);
  const page2 = await other.newPage();
  page2.on("pageerror", error => errors.push(error.message));
  await page2.goto("http://127.0.0.1:4381/user/workflow/42");
  await page2.getByLabel("Workflow canvas", { exact: true }).locator('[model-id="python"]').waitFor();
  await page.getByLabel("Workflow canvas", { exact: true }).locator('[model-id="python"]').click();
  await page.getByLabel("Label", { exact: true }).fill("Concurrent Ω");
  await page2.getByLabel("Workflow canvas", { exact: true }).locator('[model-id="python"]').click();
  await page2.getByLabel("Label", { exact: true }).waitFor();
  await page2.waitForFunction(() =>
    Array.from(document.querySelectorAll("input")).some(input => input.value === "Concurrent Ω")
  );
  await other.close();
  await writeFile(
    join(output, "workspace-browser-results.json"),
    JSON.stringify(
      {
        errors,
        requests,
        commands,
        checks: [
          "canvas mount",
          "metadata forms",
          "save",
          "view switch same socket",
          "Monaco workers and editor mount",
          "run/pause/resume/kill",
          "results",
          "independent browser contexts shared Yjs edit",
          "group drag preserves relative positions",
          "real SVG port drag and occupied input rejection",
        ],
        limits: [
          "Backend execution and resource API mocked",
          "Real local y-websocket relay",
          "Language servers unavailable",
        ],
      },
      null,
      2
    )
  );
  assert.deepEqual(errors, []);
  console.log("Workspace browser checks passed.");
} finally {
  await browser.close();
  for (const client of rtc.clients) client.terminate();
  rtc.close();
  for (const doc of docs.values()) doc.destroy();
  docs.clear();
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
}
