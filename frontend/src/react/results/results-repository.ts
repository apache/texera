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

import type { ApiClient } from "../core/api-client";
import { v4 as uuid } from "uuid";
import type { WorkflowSocket } from "../workspace/workflow-socket";
import type { PaginatedResultEvent } from "../../core/workspace/types/workflow-websocket.interface";
import type { SchemaAttribute } from "../../core/workspace/types/workflow-compiling.interface";
import type { WorkflowExecutionsEntry } from "../../core/dashboard/type/workflow-executions-entry";
import type { WorkflowRuntimeStatistics } from "../../core/dashboard/type/workflow-runtime-statistics";
export type { WorkflowExecutionsEntry };
/** The server parses a signed 32-bit Int, not JavaScript's safe integer maximum. */
export const ALL_COLUMNS = 2147483647;
export interface ResultPageOptions {
  portIndex?: number;
  pageIndex: number;
  pageSize: number;
  columnOffset?: number;
  columnLimit?: number;
  columnSearch?: string;
}
const navigationBySocket = new WeakMap<object, Map<string, ResultPageOptions>>();
export interface ResultExportInput {
  workflowId: number;
  workflowName: string;
  operatorIds: readonly string[];
  format: string;
  filename: string;
  computingUnitId: number;
  datasetIds?: number[];
  rowIndex?: number;
  columnIndex?: number;
  outputTypes?: Readonly<Record<string, string>>;
}
export interface ResultExportRequest {
  exportType: string;
  workflowId: number;
  workflowName: string;
  operators: { id: string; outputType: string }[];
  datasetIds: number[];
  rowIndex: number;
  columnIndex: number;
  filename: string;
  computingUnitId: number;
}
function integer(value: number, name: string, min = 0): void {
  if (!Number.isSafeInteger(value) || value < min || value > ALL_COLUMNS) throw new Error("Invalid " + name + ".");
}
export function valueText(value: unknown): string {
  if (value === undefined) return "";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}
export function resultColumns(rows: readonly object[], schema: readonly SchemaAttribute[]): string[] {
  return (rows.length ? Object.keys(rows[0]) : schema.map(column => column.attributeName)).filter(
    name => name !== "_id"
  );
}
export function visualizationDocument(rows: readonly object[]): string {
  const latest = rows.at(-1) as Record<string, unknown> | undefined;
  if (typeof latest?.["html-content"] !== "string" || !latest["html-content"]) return "";
  const document = new DOMParser().parseFromString(latest["html-content"], "text/html");
  document.documentElement.style.height = "100%";
  document.body.style.height = "95%";
  const div = document.querySelector("div");
  if (div) div.style.height = "100%";
  return "<!DOCTYPE html>" + document.documentElement.outerHTML;
}
export class ResultsRepository {
  constructor(
    private readonly socket: Pick<WorkflowSocket, "request">,
    private readonly api?: ApiClient
  ) {}
  getNavigation(operatorID: string, portIndex: number): ResultPageOptions | undefined {
    return navigationBySocket.get(this.socket)?.get(operatorID + ":" + portIndex);
  }
  rememberNavigation(operatorID: string, options: ResultPageOptions): void {
    let navigation = navigationBySocket.get(this.socket);
    if (!navigation) {
      navigation = new Map();
      navigationBySocket.set(this.socket, navigation);
    }
    navigation.set(operatorID + ":" + (options.portIndex ?? 0), { ...options });
  }
  clearNavigation(): void {
    navigationBySocket.delete(this.socket);
  }
  async page(operatorID: string, options: ResultPageOptions, signal?: AbortSignal): Promise<PaginatedResultEvent> {
    if (!operatorID) throw new Error("Invalid operator.");
    const {
      pageIndex,
      pageSize,
      columnOffset = 0,
      columnLimit = ALL_COLUMNS,
      columnSearch = "",
      portIndex = 0,
    } = options;
    integer(pageIndex, "page", 1);
    integer(pageSize, "page size", 1);
    integer(columnOffset, "column offset");
    integer(columnLimit, "column limit", 1);
    integer(portIndex, "output port");
    const requestID = uuid();
    const response = await this.socket.request(
      "ResultPaginationRequest",
      {
        requestID,
        operatorID,
        pageIndex,
        pageSize,
        columnOffset,
        columnLimit,
        columnSearch,
        ...(portIndex ? { portIndex } : {}),
      },
      "PaginatedResultEvent",
      requestID,
      undefined,
      signal
    );
    if (
      response.operatorID !== operatorID ||
      (response.portIndex ?? 0) !== portIndex ||
      response.pageIndex !== pageIndex
    )
      throw new Error("Result reply belongs to a different operator, port, or page.");
    if (
      !Array.isArray(response.table) ||
      response.table.some(row => !row || typeof row !== "object" || Array.isArray(row)) ||
      !Array.isArray(response.schema) ||
      response.schema.some(
        attribute =>
          !attribute || typeof attribute.attributeName !== "string" || typeof attribute.attributeType !== "string"
      ) ||
      !Number.isFinite(response.totalTuples) ||
      response.totalTuples < 0
    )
      throw new Error("Malformed result page.");
    return response;
  }
  async row(
    operatorID: string,
    rowIndex: number,
    pageSize: number,
    portIndex = 0,
    signal?: AbortSignal
  ): Promise<Record<string, unknown> | undefined> {
    integer(rowIndex, "row");
    integer(pageSize, "page size", 1);
    const page = await this.page(
      operatorID,
      {
        pageIndex: Math.floor(rowIndex / pageSize) + 1,
        pageSize,
        portIndex,
        columnLimit: ALL_COLUMNS,
      },
      signal
    );
    return page.table[rowIndex % pageSize] as Record<string, unknown> | undefined;
  }
  async prepareExport(input: ResultExportInput): Promise<{ request: ResultExportRequest; blocked: string[] }> {
    const ids = [...new Set(input.operatorIds)];
    integer(input.workflowId, "workflow");
    integer(input.computingUnitId, "computing unit");
    if (!ids.length || ids.some(id => !id)) throw new Error("Select operators to export.");
    if (!["csv", "arrow", "parquet", "html", "data"].includes(input.format)) throw new Error("Invalid export format.");
    if (!input.filename.trim()) throw new Error("Enter an export filename.");
    for (const id of input.datasetIds ?? []) integer(id, "dataset");
    const rowIndex = input.rowIndex ?? -1,
      columnIndex = input.columnIndex ?? -1;
    integer(rowIndex, "row", -1);
    integer(columnIndex, "column", -1);
    const restriction = await this.requireApi().get<Record<string, string[]>>(
      "executions/" + input.workflowId + "/result/downloadability"
    );
    if (!restriction || typeof restriction !== "object" || Array.isArray(restriction))
      throw new Error("Malformed download permissions.");
    const exportable = ids.filter(id => !Object.prototype.hasOwnProperty.call(restriction, id));
    const blocked = [...new Set(ids.flatMap(id => (Array.isArray(restriction[id]) ? restriction[id] : [])))];
    if (!exportable.length)
      throw new Error(
        "Cannot export result: selection depends on dataset(s) that are not downloadable" +
          (blocked.length ? ": " + blocked.join(", ") : "")
      );
    return {
      request: {
        exportType: input.format,
        workflowId: input.workflowId,
        workflowName: input.workflowName,
        operators: exportable.map(id => ({
          id,
          outputType: input.format === "data" ? "data" : input.outputTypes?.[id] ?? input.format,
        })),
        datasetIds: [...(input.datasetIds ?? [])],
        rowIndex,
        columnIndex,
        filename: input.filename.trim(),
        computingUnitId: input.computingUnitId,
      },
      blocked,
    };
  }
  async exportToDataset(request: ResultExportRequest, unitType?: string): Promise<void> {
    if (!request.datasetIds.length) throw new Error("Select at least one dataset.");
    const response = await this.requireApi().post<{ status: string; message?: string }>(
      "executions/result/export/dataset" + (unitType === "kubernetes" ? "?cuid=" + request.computingUnitId : ""),
      request
    );
    if (response?.status !== "success") throw new Error(response?.message ?? "An error occurred during export.");
  }
  exportToLocal(
    request: ResultExportRequest,
    unitType?: string,
    getToken: () => string | null = () => localStorage.getItem("access_token")
  ): void {
    const frame = document.createElement("iframe");
    frame.name = "texera-result-download-" + uuid();
    frame.hidden = true;
    const form = document.createElement("form");
    form.method = "POST";
    form.action =
      "/api/executions/result/export/local" + (unitType === "kubernetes" ? "?cuid=" + request.computingUnitId : "");
    form.target = frame.name;
    form.enctype = "application/x-www-form-urlencoded";
    form.hidden = true;
    for (const [name, value] of [
      ["request", JSON.stringify(request)],
      ["token", getToken() ?? ""],
    ]) {
      const field = document.createElement("input");
      field.type = "hidden";
      field.name = name;
      field.value = value;
      form.appendChild(field);
    }
    document.body.append(frame, form);
    try {
      form.submit();
    } catch (error) {
      form.remove();
      frame.remove();
      throw error;
    }
    setTimeout(() => {
      form.remove();
      frame.remove();
    }, 10000);
  }
  history(wid: number): Promise<WorkflowExecutionsEntry[]> {
    integer(wid, "workflow");
    return this.requireApi().get("executions/" + wid);
  }
  async bookmarkExecutions(wid: number, eIds: readonly number[], previouslyBookmarked: boolean): Promise<void> {
    integer(wid, "workflow");
    eIds.forEach(id => integer(id, "execution"));
    await this.requireApi().put("executions/set_execution_bookmarks", {
      wid,
      eIds: [...eIds],
      isBookmarked: previouslyBookmarked,
    });
  }
  async deleteExecutions(wid: number, eIds: readonly number[]): Promise<void> {
    integer(wid, "workflow");
    eIds.forEach(id => integer(id, "execution"));
    await this.requireApi().put("executions/delete_executions", { wid, eIds: [...eIds] });
  }
  async renameExecution(wid: number, eId: number, executionName: string): Promise<void> {
    integer(wid, "workflow");
    integer(eId, "execution");
    if (!executionName.trim()) throw new Error("Enter an execution name.");
    await this.requireApi().post("executions/update_execution_name", { wid, eId, executionName });
  }
  runtimeStatistics(wid: number, eId: number, cuid: number): Promise<WorkflowRuntimeStatistics[]> {
    integer(wid, "workflow");
    integer(eId, "execution");
    integer(cuid, "computing unit");
    return this.requireApi().get("executions/" + wid + "/stats/" + eId + "?cuid=" + cuid);
  }
  async writableDatasets(): Promise<{ id: number; name: string }[]> {
    const entries =
      await this.requireApi().get<{ accessPrivilege: string; dataset: { did?: number; name: string } }[]>(
        "dataset/list"
      );
    if (!Array.isArray(entries)) throw new Error("Malformed dataset list.");
    return entries
      .filter(entry => entry.accessPrivilege === "WRITE" && Number.isSafeInteger(entry.dataset?.did))
      .map(entry => ({ id: entry.dataset.did!, name: entry.dataset.name }));
  }
  private requireApi(): ApiClient {
    if (!this.api) throw new Error("The results API is not available.");
    return this.api;
  }
}
