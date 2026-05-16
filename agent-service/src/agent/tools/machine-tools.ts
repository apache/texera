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
import { env } from "../../config/env";

export const TOOL_NAME_RUN_ON_MACHINE = "runOnMachine";
export const TOOL_NAME_RUN_PYTHON_ON_MACHINE = "runPythonOnMachine";
export const TOOL_NAME_LIST_DATASETS = "listDatasets";
export const TOOL_NAME_UPLOAD_FILE_TO_DATASET = "uploadFileToDataset";
export const TOOL_NAME_GET_DATASET_FILE = "getDatasetFile";

interface MachineRecord {
  mid: number;
  name: string;
  url: string;
  token?: string | null;
}

async function lookupMachine(userToken: string, mid: number): Promise<MachineRecord> {
  const res = await fetch(`${env.TEXERA_DASHBOARD_SERVICE_ENDPOINT}/api/machines/${mid}`, {
    headers: { Authorization: `Bearer ${userToken}` },
  });
  if (!res.ok) {
    throw new Error(`Failed to look up machine ${mid}: HTTP ${res.status} ${await res.text()}`);
  }
  return (await res.json()) as MachineRecord;
}

interface DatasetSummary {
  did: number;
  name: string;
  ownerEmail?: string;
  isPublic?: boolean;
}

async function fetchDatasetList(userToken: string): Promise<DatasetSummary[]> {
  const res = await fetch(`${env.FILE_SERVICE_ENDPOINT}/api/dataset/list`, {
    headers: { Authorization: `Bearer ${userToken}` },
  });
  if (!res.ok) {
    throw new Error(`Failed to list datasets: HTTP ${res.status} ${await res.text()}`);
  }
  const body = (await res.json()) as Array<{
    dataset: { did: number; name: string; isPublic?: boolean };
    ownerEmail?: string;
  }>;
  return body.map(d => ({
    did: d.dataset.did,
    name: d.dataset.name,
    ownerEmail: d.ownerEmail,
    isPublic: d.dataset.isPublic,
  }));
}

interface DatasetFileNode {
  name: string;
  type?: string;
  parentDir?: string;
  size?: number;
  children?: DatasetFileNode[];
}

/**
 * Walk file-service's nested file-node tree and yield flat (relativePath, size) entries
 * for everything of type "file".
 */
function flattenFileNodes(
  node: DatasetFileNode,
  acc: { relativePath: string; size?: number }[] = []
): { relativePath: string; size?: number }[] {
  if (node.type === "file") {
    // parentDir comes from LakeFS as e.g. "/texera/<dataset>/<version>/<sub/dirs>";
    // strip the first three segments so we get just "<sub/dirs>/<file>" relative to
    // the dataset version root.
    const parent = (node.parentDir ?? "").split("/").filter(Boolean).slice(3);
    const relative = [...parent, node.name].join("/");
    acc.push({ relativePath: relative, size: node.size });
  }
  for (const c of node.children ?? []) flattenFileNodes(c, acc);
  return acc;
}

/**
 * `run-on-machine`: lets the agent execute a shell command on one of the user's
 * registered machines (Machines tab) via the remote machine-manager service.
 */
export function createRunOnMachineTool(userToken: string) {
  return tool({
    description:
      "Run a shell command on one of the user's registered machines (Machines tab) " +
      "by hitting that host's machine-manager service. Use this to inspect or prepare " +
      "the target environment (list files, check paths, install deps) before building " +
      "a workflow that uses the Machine UDF operator on the same host.",
    inputSchema: z.object({
      machineId: z
        .number()
        .int()
        .describe("The numeric machine id (`mid`) from the Machines tab."),
      command: z
        .string()
        .describe("The shell command to run on the target machine. Runs via `bash -c`."),
      cwd: z
        .string()
        .optional()
        .describe("Working directory on the target machine. Defaults to the user's home."),
      timeoutSeconds: z
        .number()
        .min(1)
        .max(600)
        .default(60)
        .describe("How long to wait for the command before timing out."),
    }),
    execute: async (args) => {
      try {
        const machine = await lookupMachine(userToken, args.machineId);
        const headers: Record<string, string> = { "Content-Type": "application/json" };
        if (machine.token && machine.token.trim().length > 0) {
          headers["Authorization"] = `Bearer ${machine.token.trim()}`;
        }
        const res = await fetch(`${machine.url.replace(/\/$/, "")}/exec`, {
          method: "POST",
          headers,
          body: JSON.stringify({
            cmd: args.command,
            cwd: args.cwd ?? null,
            timeout_seconds: args.timeoutSeconds,
          }),
        });
        const bodyText = await res.text();
        if (!res.ok) {
          return {
            success: false,
            error: `machine-manager returned HTTP ${res.status}: ${bodyText}`,
            machine: { mid: machine.mid, name: machine.name, url: machine.url },
          };
        }
        const body = JSON.parse(bodyText) as {
          exit_code: number;
          stdout: string;
          stderr: string;
        };
        return {
          success: body.exit_code === 0,
          machine: { mid: machine.mid, name: machine.name, url: machine.url },
          exit_code: body.exit_code,
          stdout: body.stdout,
          stderr: body.stderr,
        };
      } catch (e) {
        return {
          success: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
    },
  });
}

/**
 * `runPythonOnMachine`: run a self-contained Python script on the user's machine.
 *
 * Unlike `runOnMachine` (which only runs a shell command, intended for cheap
 * inspection / setup), this hits machine-manager's `/python` endpoint, which
 * executes the script under a data-science Python (sklearn, pandas, matplotlib,
 * numpy, ...). Use this for ANY analysis task where the data lives on the
 * user's laptop and the outputs (plots, reports, model files) should also be
 * written to the user's laptop — load the CSV, train models, save artifacts,
 * all in one call. No Texera workflow / dataset upload needed.
 *
 * The script can `print(json.dumps({...}))` on its last line to return a
 * structured result the agent can then read.
 */
export function createRunPythonOnMachineTool(userToken: string) {
  return tool({
    description:
      "DIAGNOSTICS ONLY. Run a tiny Python snippet on the user's machine to check the environment " +
      "(e.g. `import sklearn; print(sklearn.__version__)`). DO NOT use this to actually do the " +
      "user's data analysis — that always goes in a Texera workflow with the `MachineUDF` operator " +
      "in batch mode. The script's last `print(json.dumps({...}))` line is returned as `result`.",
    inputSchema: z.object({
      machineId: z
        .number()
        .int()
        .describe("Numeric machine id (`mid`) from the Machines tab."),
      code: z
        .string()
        .describe(
          "Self-contained Python source. The script's global scope already has `tuple_in` (None " +
          "for this use). Print a JSON object on the last line to return a structured result."
        ),
      timeoutSeconds: z
        .number()
        .min(1)
        .max(600)
        .default(120)
        .describe("Seconds before the script is killed. Default 120."),
    }),
    execute: async args => {
      try {
        const machine = await lookupMachine(userToken, args.machineId);
        const headers: Record<string, string> = { "Content-Type": "application/json" };
        if (machine.token && machine.token.trim().length > 0) {
          headers["Authorization"] = `Bearer ${machine.token.trim()}`;
        }
        const res = await fetch(`${machine.url.replace(/\/$/, "")}/python`, {
          method: "POST",
          headers,
          body: JSON.stringify({
            code: args.code,
            tuple_in: null,
            timeout_seconds: args.timeoutSeconds,
          }),
        });
        const bodyText = await res.text();
        if (!res.ok) {
          return {
            success: false,
            error: `machine-manager returned HTTP ${res.status}: ${bodyText}`,
            machine: { mid: machine.mid, name: machine.name, url: machine.url },
          };
        }
        const body = JSON.parse(bodyText) as {
          exit_code: number;
          stdout: string;
          stderr: string;
          result: unknown;
        };
        return {
          success: body.exit_code === 0,
          machine: { mid: machine.mid, name: machine.name, url: machine.url },
          exit_code: body.exit_code,
          stdout: body.stdout,
          stderr: body.stderr,
          result: body.result,
        };
      } catch (e) {
        return {
          success: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
    },
  });
}

/**
 * `getDatasetFile`: resolve a (datasetName, filename) pair to the canonical
 * scan-operator fileName the workflow needs, using whatever version is the latest.
 * Returns the exact string the agent should put in CSVFileScan.fileName.
 */
export function createGetDatasetFileTool(userToken: string) {
  return tool({
    description:
      "Resolve a Texera dataset file to the exact fileName string that CSVFileScan / TableFileScan operators expect. " +
      "Pass the dataset's human name (e.g. 'test-4') and the file path inside the dataset (e.g. 'customers-test.csv'). " +
      "Returns the canonical path /<ownerEmail>/<datasetName>/latest/<filePath> — copy that verbatim into the scan operator's fileName property.",
    inputSchema: z.object({
      datasetName: z.string().describe("Dataset name as shown in the Datasets tab, e.g. 'test-4'."),
      filename: z
        .string()
        .optional()
        .describe(
          "Optional file path inside the dataset (e.g. 'customers-test.csv'). If omitted, returns the list of files in the latest version so you can pick one."
        ),
    }),
    execute: async args => {
      try {
        const datasets = await fetchDatasetList(userToken);
        const match = datasets.find(d => d.name === args.datasetName);
        if (!match || !match.ownerEmail) {
          return {
            success: false,
            error: `Dataset "${args.datasetName}" not found. Available: ${datasets.map(d => d.name).join(", ") || "(none)"}.`,
          };
        }
        const latestResp = await fetch(
          `${env.FILE_SERVICE_ENDPOINT}/api/dataset/${match.did}/version/latest`,
          { headers: { Authorization: `Bearer ${userToken}` } }
        );
        if (!latestResp.ok) {
          return {
            success: false,
            error: `Failed to fetch latest version of dataset ${args.datasetName}: HTTP ${latestResp.status} ${await latestResp.text()}`,
          };
        }
        const latestBody = (await latestResp.json()) as {
          datasetVersion?: { name?: string };
          fileNodes?: DatasetFileNode[];
        };
        const versionName = latestBody.datasetVersion?.name ?? "latest";
        const files = (latestBody.fileNodes ?? []).flatMap(n => flattenFileNodes(n));

        if (!args.filename) {
          return {
            success: true,
            datasetName: match.name,
            ownerEmail: match.ownerEmail,
            latestVersion: versionName,
            files: files.map(f => ({
              path: f.relativePath,
              size: f.size,
              fileName_for_scan_operator: `/${match.ownerEmail}/${match.name}/latest/${f.relativePath}`,
            })),
            hint: "Pass `filename` next time to get a single canonical scan-operator fileName.",
          };
        }
        const target = files.find(f => f.relativePath === args.filename);
        if (!target) {
          return {
            success: false,
            error: `File "${args.filename}" not found in latest version of dataset "${args.datasetName}". Files present: ${files.map(f => f.relativePath).join(", ") || "(none)"}.`,
          };
        }
        const canonical = `/${match.ownerEmail}/${match.name}/latest/${target.relativePath}`;
        return {
          success: true,
          datasetName: match.name,
          ownerEmail: match.ownerEmail,
          latestVersion: versionName,
          file: target.relativePath,
          fileName_for_scan_operator: canonical,
          hint: `Set the scan operator's "fileName" property to exactly: ${canonical}`,
        };
      } catch (e) {
        return { success: false, error: e instanceof Error ? e.message : String(e) };
      }
    },
  });
}

/**
 * `listDatasets`: returns the user's accessible Texera datasets (id + name).
 * Use this to resolve a dataset *name* (what the user types) to a *did* (what
 * file-service and uploadFileToDataset need).
 */
export function createListDatasetsTool(userToken: string) {
  return tool({
    description:
      "List the user's accessible Texera datasets. Returns each dataset's numeric id (did) and name. " +
      "Use this when the user refers to a dataset by name and you need its id.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const datasets = await fetchDatasetList(userToken);
        return { success: true, datasets };
      } catch (e) {
        return {
          success: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
    },
  });
}

/**
 * `uploadFileToDataset`: have a registered machine read a local file and upload
 * it to a Texera dataset, creating a new dataset version.
 */
export function createUploadFileToDatasetTool(userToken: string) {
  return tool({
    description:
      "Upload a file that lives on a registered machine into a Texera dataset, creating a new version. " +
      "machine-manager on that machine reads the local file and pushes it to file-service. " +
      "Pass either datasetId (numeric did) OR datasetName (the human-readable name) — the tool resolves the name automatically. " +
      "If you pass both, datasetName wins. Do NOT guess datasetId from numbers in the name.",
    inputSchema: z.object({
      machineId: z
        .number()
        .int()
        .describe("Numeric machine id (`mid`) from the Machines tab."),
      localPath: z
        .string()
        .describe("Absolute path on the machine, e.g. /home/ali/Downloads/customers-100.csv."),
      datasetId: z
        .number()
        .int()
        .optional()
        .describe("Numeric dataset id (`did`). Optional if datasetName is provided."),
      datasetName: z
        .string()
        .optional()
        .describe(
          "Dataset name (e.g. 'test-4'). Resolved to did via listDatasets internally. Preferred over datasetId."
        ),
      datasetFilePath: z
        .string()
        .describe(
          "Destination path *inside the dataset*, e.g. customers-100.csv or subdir/file.csv."
        ),
    }),
    execute: async args => {
      try {
        console.log("[DBG-UPLOAD] args:", JSON.stringify(args), "tokenLen:", userToken?.length, "tokenPrefix:", userToken?.slice(0, 30));
        let resolvedDid = args.datasetId;
        if (args.datasetName) {
          const datasets = await fetchDatasetList(userToken);
          const match = datasets.find(d => d.name === args.datasetName);
          if (!match) {
            return {
              success: false,
              error: `Dataset name "${args.datasetName}" not found. Available: ${datasets.map(d => `${d.name} (did=${d.did})`).join(", ") || "(none)"}.`,
            };
          }
          resolvedDid = match.did;
          console.log("[DBG-UPLOAD] resolved name", args.datasetName, "->", resolvedDid);
        }
        if (resolvedDid == null) {
          return {
            success: false,
            error: "Must provide either datasetId or datasetName.",
          };
        }
        // Sanity check the id actually exists for this user, fail-fast with a useful message.
        const allDatasets = await fetchDatasetList(userToken);
        if (!allDatasets.some(d => d.did === resolvedDid)) {
          return {
            success: false,
            error: `Dataset did=${resolvedDid} is not accessible to this user. Available: ${allDatasets.map(d => `${d.name} (did=${d.did})`).join(", ") || "(none)"}.`,
          };
        }
        args = { ...args, datasetId: resolvedDid };
        const machine = await lookupMachine(userToken, args.machineId);
        const headers: Record<string, string> = { "Content-Type": "application/json" };
        if (machine.token && machine.token.trim().length > 0) {
          headers["Authorization"] = `Bearer ${machine.token.trim()}`;
        }
        const res = await fetch(`${machine.url.replace(/\/$/, "")}/upload-to-dataset`, {
          method: "POST",
          headers,
          body: JSON.stringify({
            local_path: args.localPath,
            dataset_id: resolvedDid,
            file_path: args.datasetFilePath,
            file_service_url: env.FILE_SERVICE_ENDPOINT,
            auth_token: userToken,
          }),
        });
        const bodyText = await res.text();
        console.log("[DBG-UPLOAD] mm response:", res.status, bodyText.slice(0, 300));
        if (!res.ok) {
          return {
            success: false,
            error: `machine-manager returned HTTP ${res.status}: ${bodyText}`,
          };
        }
        const parsed = JSON.parse(bodyText) as {
          dataset_id: number;
          file_path: string;
          bytes_uploaded: number;
          version_name?: string;
          dataset_name?: string;
        };
        // Build the canonical scan-operator fileName the workflow needs:
        //   /<ownerEmail>/<datasetName>/latest/<filePath>
        // Using the "latest" sentinel makes this robust to subsequent uploads — the
        // FileResolver in amber resolves "latest" to the newest dataset_version row.
        const ownerEmail = allDatasets.find(d => d.did === resolvedDid)?.ownerEmail;
        const datasetForScan = parsed.dataset_name ?? args.datasetName ?? null;
        const csvFileScanPath =
          ownerEmail != null && datasetForScan != null
            ? `/${ownerEmail}/${datasetForScan}/latest/${parsed.file_path}`
            : null;
        console.log("[DBG-UPLOAD] csvFileScanPath:", csvFileScanPath, "ownerEmail:", ownerEmail, "datasetForScan:", datasetForScan);
        return {
          success: true,
          result: parsed,
          fileName_for_scan_operator: csvFileScanPath,
          hint:
            csvFileScanPath != null
              ? [
                  `Upload succeeded. To wire a CSVFileScan / TableFileScan to this file,`,
                  `set its "fileName" property to EXACTLY this string (no quotes, no changes):`,
                  ``,
                  `    ${csvFileScanPath}`,
                  ``,
                  `Notes:`,
                  `  - The leading slash and the literal segment "latest" are required.`,
                  `  - "latest" auto-resolves to the newest version, so you never need to hard-code v1/v2/v3.`,
                  `  - Do NOT use an absolute filesystem path like /home/... — the scan operator reads from the Texera dataset, not from local disk.`,
                  `  - If a scan operator already exists with a different fileName, call modifyOperator to set fileName to the value above.`,
                ].join("\n")
              : "Upload succeeded but canonical path resolution failed; ask the user for the dataset path.",
        };
      } catch (e) {
        return {
          success: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
    },
  });
}
