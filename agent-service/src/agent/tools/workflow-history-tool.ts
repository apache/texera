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

import { z } from "zod";
import { tool } from "ai";
import { getBackendConfig } from "../../api/backend-api";
import { createAuthHeaders } from "../../api/auth-api";
import { createToolResult, createErrorResult } from "./tools-utility";

export const TOOL_NAME_WORKFLOW_HISTORY = "workflowHistory";

interface WorkflowHistoryConfig {
  userToken: string;
  workflowId: number;
}

interface SnapshotEntry {
  sid: number;
  wid: number;
  version: number;
  changeType: string;
  changeSummary: string;
  changedOperators: string[];
  source: "user" | "agent";
  creationTime: string;
}

/**
 * Build the workflowHistory tool for the agent. The tool talks to the amber
 * `/api/time-machine/...` endpoints — it doesn't touch the audit rules
 * or the agent chat UI, so this file is independent of those features.
 *
 * Actions:
 *   - list:     return recent snapshots (default limit 20)
 *   - revert:   revert the workflow to a specific snapshot by version number
 *               (the agent should typically pick a version from `list` first)
 *   - snapshot: explicitly save a checkpoint tagged as agent-sourced. Call
 *               this after generating or modifying the workflow so the user
 *               sees a 🤖 entry in the Time Machine timeline.
 */
export function createWorkflowHistoryTool(getConfig: () => WorkflowHistoryConfig | undefined) {
  return tool({
    description:
      "Browse and manipulate the version history of the current workflow. " +
      "Use `list` to see recent changes (with timestamps, who made them, " +
      "what changed), `revert` to roll the workflow back to a specific " +
      "version, and `snapshot` to record an agent-tagged checkpoint after " +
      "you have generated or modified the workflow. " +
      "Useful when the user says things like 'undo the last change' or " +
      "'go back to before you added that operator'. Always call `snapshot` " +
      "with a short summary after a multi-step workflow change so the user " +
      "can revert to that point.",
    inputSchema: z.object({
      action: z
        .enum(["list", "revert", "snapshot"])
        .describe("'list' returns the timeline; 'revert' rolls back; 'snapshot' saves a checkpoint."),
      version: z
        .number()
        .int()
        .positive()
        .optional()
        .describe("Snapshot version number (required for revert). Get this from the `list` action first."),
      limit: z.number().int().min(1).max(50).optional().describe("Cap for the list action. Default 20."),
      summary: z
        .string()
        .optional()
        .describe(
          "Short summary for the `snapshot` action, e.g. 'Generated ML pipeline' or 'Added Random Forest'. <= 80 chars."
        ),
      changedOperators: z
        .array(z.string())
        .optional()
        .describe("Optional operator IDs that were added/modified, for the `snapshot` action."),
    }),
    execute: async (args: {
      action: "list" | "revert" | "snapshot";
      version?: number;
      limit?: number;
      summary?: string;
      changedOperators?: string[];
    }) => {
      const cfg = getConfig();
      if (!cfg) {
        return createErrorResult("workflowHistory tool requires a workflow context (token + workflow id).");
      }
      const base = `${getBackendConfig().apiEndpoint}/api/time-machine/${cfg.workflowId}`;
      const headers = createAuthHeaders(cfg.userToken);
      try {
        if (args.action === "snapshot") {
          // Fetch current workflow content from amber, then POST a snapshot.
          // We can't compose the snapshot content from the agent side without
          // the workflow JSON, so we read it back from the workflow endpoint.
          const workflowResp = await fetch(
            `${getBackendConfig().apiEndpoint}/api/workflow/${cfg.workflowId}`,
            { method: "GET", headers }
          );
          if (!workflowResp.ok) {
            const text = await workflowResp.text();
            return createErrorResult(`could not fetch current workflow content: ${workflowResp.status} ${text}`);
          }
          const wf = (await workflowResp.json()) as { content: unknown };
          const content = typeof wf.content === "string" ? wf.content : JSON.stringify(wf.content);
          const postResp = await fetch(`${base}/snapshots`, {
            method: "POST",
            headers: { ...headers, "Content-Type": "application/json" },
            body: JSON.stringify({
              content,
              changeType: "agent_generated",
              changeSummary: args.summary?.slice(0, 200) ?? "Agent-generated workflow change",
              changedOperators: args.changedOperators ?? [],
              source: "agent",
            }),
          });
          if (!postResp.ok) {
            const text = await postResp.text();
            return createErrorResult(`snapshot save failed: ${postResp.status} ${text}`);
          }
          const saved = (await postResp.json()) as { version: number; sid: number };
          return createToolResult(`Saved snapshot v${saved.version} (sid=${saved.sid}) tagged as agent-sourced.`);
        }
        if (args.action === "list") {
          const response = await fetch(`${base}/snapshots`, { method: "GET", headers });
          if (!response.ok) {
            const text = await response.text();
            return createErrorResult(`list snapshots failed: ${response.status} ${text}`);
          }
          const all = (await response.json()) as SnapshotEntry[];
          const limit = args.limit ?? 20;
          const items = all.slice(0, limit);
          const lines = items.map(
            e =>
              `v${e.version}  ${e.creationTime}  [${e.source}]  ${e.changeType}: ${e.changeSummary}` +
              (e.changedOperators?.length ? `  (ops: ${e.changedOperators.join(",")})` : "")
          );
          const summary =
            items.length === 0
              ? "No snapshots recorded yet."
              : `Showing ${items.length} of ${all.length} snapshots:\n${lines.join("\n")}`;
          return createToolResult(summary);
        }

        // revert
        if (args.version === undefined) {
          return createErrorResult("revert requires `version` (an integer from the `list` action).");
        }
        const listResp = await fetch(`${base}/snapshots`, { method: "GET", headers });
        if (!listResp.ok) {
          const text = await listResp.text();
          return createErrorResult(`could not look up version: ${listResp.status} ${text}`);
        }
        const all = (await listResp.json()) as SnapshotEntry[];
        const target = all.find(s => s.version === args.version);
        if (!target) {
          return createErrorResult(
            `no snapshot with version ${args.version}. Available: ${all.map(s => `v${s.version}`).join(", ") || "(none)"}.`
          );
        }
        const revertResp = await fetch(`${base}/snapshots/${target.sid}/revert`, {
          method: "POST",
          headers,
        });
        if (!revertResp.ok) {
          const text = await revertResp.text();
          return createErrorResult(`revert failed: ${revertResp.status} ${text}`);
        }
        return createToolResult(
          `Reverted workflow to v${target.version} ("${target.changeSummary}"). ` +
            "The user should reload their canvas to see the change."
        );
      } catch (err: any) {
        return createErrorResult(`workflowHistory failed: ${err?.message ?? String(err)}`);
      }
    },
  });
}
