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
import { ApiClient, type RequestOptions } from "../core/api-client";

export interface Warehouse {
  whid: number;
  name: string;
  lakekeeperWarehouseName: string;
  flavor: "local" | "aws";
  createdAtMillis: number;
  ownerName: string | null;
  ownerAvatar: string | null;
}
export interface WarehouseStatus {
  enabled: boolean;
  warehouses: readonly Warehouse[];
}
export interface PythonEnvironment {
  veid: number;
  name: string;
  packages: Record<string, string>;
}
export interface LiveEnvironment {
  pveName: string;
  userPackages: string[];
}
export interface AdminUser {
  uid: number;
  name: string;
  email: string;
  role: "INACTIVE" | "RESTRICTED" | "REGULAR" | "ADMIN";
  comment: string;
  joiningReason?: string;
  affiliation?: string;
  googleId?: string;
  avatar?: string;
  lastLogin?: number;
  accountCreation?: number;
  isPlaceholder?: boolean;
}
export type UserUpdate = Pick<AdminUser, "uid" | "name" | "email" | "role" | "comment">;
export interface AdminExecution {
  workflowName: string;
  workflowId: number;
  userName: string;
  userId: number;
  executionId: number;
  executionStatus: string;
  executionTime: number;
  executionName: string;
  startTime: number;
  endTime: number;
  access: boolean;
}
export interface ExecutionQuery {
  pageSize: number;
  pageIndex: number;
  sortField: string;
  sortDirection: string;
  filter: string[];
}
export interface DatasetQuota {
  did: number;
  name: string;
  creationTime: number;
  size: number;
}
export interface QuotaWorkflow {
  userId: number;
  workflowId: number;
  workflowName: string;
  creationTime: number;
  lastModifiedTime: number;
}
export interface ExecutionQuota {
  eid: number;
  workflowId: number;
  workflowName: string;
  resultBytes: number;
  runTimeStatsBytes: number;
  logBytes: number;
}
export interface Feedback {
  fid: number;
  uid: number;
  message: string;
  creationTime: number;
}
export interface QuotaSummary {
  datasets: DatasetQuota[];
  workflows: QuotaWorkflow[];
  accessWorkflows: number[];
  executions: ExecutionQuota[];
}
export interface ComputingResources {
  cpuLimit: string;
  memoryLimit: string;
  gpuLimit: string;
  jvmMemorySize: string;
  shmSize: string;
  nodeAddresses: string[];
}
export interface ComputingUnit {
  computingUnit: {
    cuid: number;
    uid: number;
    name: string;
    creationTime: number;
    terminateTime?: number;
    type: "local" | "kubernetes";
    uri: string;
    resource: ComputingResources;
  };
  status: "Running" | "Pending" | "Failed" | "Unknown" | "Terminating";
  statusReason?: string | null;
  metrics: { cpuUsage: string; memoryUsage: string };
  isOwner: boolean;
  accessPrivilege: "READ" | "WRITE" | "NONE";
  ownerAvatar: string;
  ownerName: string;
}
export type ComputingUnitCreate =
  | { unitType: "local"; name: string; uri: string }
  | {
      unitType: "kubernetes";
      name: string;
      cpuLimit: string;
      memoryLimit: string;
      gpuLimit: string;
      jvmMemorySize: string;
      shmSize: string;
      iid?: number;
    };
export interface Capacity {
  total: number;
  allocated: number;
  available: number;
}
export interface ClusterCapacity {
  cpu: Capacity;
  memory: Capacity;
  gpu: Capacity;
}
export interface ComputingImage {
  iid: number;
  name: string;
  sourceRef: string;
  sourceDigest: string | null;
  status: "PENDING" | "VALIDATING" | "READY" | "FAILED";
  imageTag: string | null;
  attempt: number;
  creationTime: number;
  updateTime: number;
}

function identifier(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("Invalid resource identifier");
  return value;
}

/** The backend may store this column as JSON text, including legacy null or malformed values. */
export function parseComputingUnit(raw: ComputingUnit): ComputingUnit {
  const incoming: unknown = raw.computingUnit.resource;
  let resource: ComputingResources | null = null;
  if (typeof incoming === "string") {
    try {
      resource = JSON.parse(incoming) as ComputingResources | null;
    } catch {
      resource = null;
    }
  } else resource = incoming as ComputingResources | null;
  return {
    ...raw,
    computingUnit: {
      ...raw.computingUnit,
      resource: resource ?? {
        cpuLimit: "NaN",
        memoryLimit: "NaN",
        gpuLimit: "NaN",
        jvmMemorySize: "NaN",
        shmSize: "NaN",
        nodeAddresses: [],
      },
    },
  };
}

/** Framework-independent ports of the existing resource/admin services; backend payloads are unchanged. */
export class ResourceApi {
  private publicSettingsRequest?: Promise<Record<string, string>>;
  constructor(
    private readonly api: ApiClient,
    private readonly getToken: () => string | null = () => globalThis.localStorage?.getItem("access_token") ?? null
  ) {}

  warehouseStatus(options?: RequestOptions): Promise<WarehouseStatus> {
    return this.api.get("warehouse/status", options);
  }
  createWarehouse(name: string): Promise<Warehouse> {
    return this.api.post("warehouse", { name });
  }
  async deleteWarehouse(whid: number): Promise<void> {
    await this.api.delete(`warehouse/${identifier(whid)}`);
  }

  listEnvironments(options?: RequestOptions): Promise<PythonEnvironment[]> {
    return this.api.get("pve/db", options);
  }
  createEnvironment(name: string, packages: Record<string, string>): Promise<{ veid: number }> {
    return this.api.post("pve/db", { name, packages });
  }
  async updateEnvironment(veid: number, name: string, packages: Record<string, string>): Promise<{ veid: number }> {
    return this.api.put(`pve/db/${identifier(veid)}`, { name, packages });
  }
  async deleteEnvironment(veid: number): Promise<void> {
    await this.api.delete(`pve/db/${identifier(veid)}`);
  }
  private packageParams(cuid?: number): string {
    const params = new URLSearchParams();
    const token = this.getToken();
    if (token?.trim()) params.set("access-token", token);
    if (cuid !== undefined) params.set("cuid", String(identifier(cuid)));
    return params.size ? `?${params}` : "";
  }
  systemPackages(cuid: number): Promise<{ system: string[] }> {
    return this.api.get(`pve/system${this.packageParams(cuid)}`);
  }
  liveEnvironments(cuid: number): Promise<LiveEnvironment[]> {
    return this.api.get(`pve/pves${this.packageParams(cuid)}`);
  }
  async userPackages(cuid: number, name: string): Promise<string[]> {
    return (await this.liveEnvironments(cuid)).find(pve => pve.pveName === name)?.userPackages ?? [];
  }
  async deleteLiveEnvironments(cuid: number): Promise<void> {
    await this.api.delete(`pve/pves/${identifier(cuid)}`);
  }
  async deletePackage(cuid: number, environment: string, name: string): Promise<string[]> {
    return this.api.delete(
      `pve/${identifier(cuid)}/${encodeURIComponent(environment)}/packages/${encodeURIComponent(name)}${this.packageParams()}`
    );
  }
  environmentSocketUrl(
    cuid: number,
    name: string,
    action: string,
    packages: string[] = [],
    location: Pick<Location, "protocol" | "host"> = window.location
  ): string {
    const query = new URLSearchParams({
      packages: JSON.stringify(packages),
      cuid: String(identifier(cuid)),
      pveName: name,
      action,
    });
    const token = this.getToken();
    if (token?.trim()) query.set("access-token", token);
    return `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/wsapi/pve?${query}`;
  }

  publicSettings(): Promise<Record<string, string>> {
    if (!this.publicSettingsRequest)
      this.publicSettingsRequest = this.api.get<Record<string, string>>("config/settings/public").catch(error => {
        this.publicSettingsRequest = undefined;
        throw error;
      });
    return this.publicSettingsRequest;
  }
  settings(options?: RequestOptions): Promise<Record<string, string>> {
    return this.api.get("config/settings", options);
  }
  async updateSetting(key: string, value: string): Promise<void> {
    await this.api.put(`config/settings/${encodeURIComponent(key)}`, { value }, { credentials: "include" });
    this.publicSettingsRequest = undefined;
  }
  async resetSetting(key: string): Promise<void> {
    await this.api.post(`config/settings/reset/${encodeURIComponent(key)}`, {});
    this.publicSettingsRequest = undefined;
  }
  senderEmail(options?: RequestOptions): Promise<string> {
    return this.api.get("gmail/sender/email", { ...options, responseType: "text" });
  }
  async sendEmail(subject: string, content: string, receiver = ""): Promise<void> {
    await this.api.put("gmail/send", { receiver, subject, content });
  }
  adminUsers(options?: RequestOptions): Promise<AdminUser[]> {
    return this.api.get("admin/user/list", options);
  }
  addUser(): Promise<unknown> {
    return this.api.post("admin/user/add/", {});
  }
  async updateUser(user: UserUpdate): Promise<void> {
    await this.api.put("admin/user/update", user);
  }
  feedback(options?: RequestOptions, userId?: number): Promise<Feedback[]> {
    return this.api.get(userId === undefined ? "feedback" : `feedback/user?user_id=${identifier(userId)}`, options);
  }
  feedbackCounts(options?: RequestOptions): Promise<{ uid: number; count: number }[]> {
    return this.api.get("feedback/counts", options);
  }
  async submitFeedback(message: string): Promise<void> {
    await this.api.post("feedback", { message });
  }
  executionList(query: ExecutionQuery, options?: RequestOptions): Promise<AdminExecution[]> {
    const filter = new URLSearchParams({ filter: query.filter.join(",") });
    return this.api.get(
      `admin/execution/executionList/${query.pageSize}/${query.pageIndex}/${encodeURIComponent(query.sortField)}/${encodeURIComponent(query.sortDirection)}?${filter}`,
      options
    );
  }
  totalExecutions(options?: RequestOptions): Promise<number> {
    return this.api.get("admin/execution/totalWorkflow", options);
  }
  async quota(userId?: number, options?: RequestOptions): Promise<QuotaSummary> {
    const base = userId === undefined ? "quota" : "admin/user";
    const query = userId === undefined ? "" : `?user_id=${identifier(userId)}`;
    const [datasets, workflows, accessWorkflows, executions] = await Promise.all([
      this.api.get<DatasetQuota[]>(`${base}/created_datasets${query}`, options),
      this.api.get<QuotaWorkflow[]>(`${base}/created_workflows${query}`, options),
      this.api.get<number[]>(`${base}/access_workflows${query}`, options),
      this.api.get<ExecutionQuota[]>(`${base}/user_quota_size${query}`, options),
    ]);
    return { datasets, workflows, accessWorkflows, executions };
  }
  async deleteExecutionCollection(eid: number, admin = false): Promise<void> {
    await this.api.delete(`${admin ? "admin/user" : "quota"}/deleteCollection/${identifier(eid)}`);
  }

  async computingUnits(admin = false, options?: RequestOptions): Promise<ComputingUnit[]> {
    return (await this.api.get<ComputingUnit[]>(admin ? "computing-unit/admin/list" : "computing-unit", options)).map(
      parseComputingUnit
    );
  }
  async computingUnit(cuid: number): Promise<ComputingUnit> {
    return parseComputingUnit(await this.api.get(`computing-unit/${identifier(cuid)}`));
  }
  async createComputingUnit(request: ComputingUnitCreate): Promise<ComputingUnit> {
    const body =
      request.unitType === "local"
        ? { ...request, cpuLimit: "NaN", memoryLimit: "NaN", gpuLimit: "NaN", jvmMemorySize: "NaN", shmSize: "NaN" }
        : { ...request, uri: "" };
    return parseComputingUnit(await this.api.post("computing-unit/create", body));
  }
  async renameComputingUnit(cuid: number, name: string): Promise<void> {
    await this.api.put(`computing-unit/${identifier(cuid)}/rename/${encodeURIComponent(name)}`, {});
  }
  async terminateComputingUnit(cuid: number): Promise<void> {
    await this.api.delete(`computing-unit/${identifier(cuid)}/terminate`);
  }
  computingUnitLimits(): Promise<{
    cpuLimitOptions: string[];
    memoryLimitOptions: string[];
    gpuLimitOptions: string[];
  }> {
    return this.api.get("computing-unit/limits");
  }
  computingUnitTypes(): Promise<{ typeOptions: ("local" | "kubernetes")[] }> {
    return this.api.get("computing-unit/types");
  }
  clusterCapacity(): Promise<ClusterCapacity> {
    return this.api.get("computing-unit/admin/cluster-capacity");
  }
  images(options?: RequestOptions): Promise<ComputingImage[]> {
    return this.api.get("cu-image", options);
  }
  createImage(name: string, sourceRef: string): Promise<ComputingImage> {
    return this.api.post("cu-image", { name, sourceRef });
  }
  async refreshImage(iid: number): Promise<ComputingImage> {
    return this.api.post(`cu-image/${identifier(iid)}/refresh`, {});
  }
  async imageLog(
    iid: number
  ): Promise<{ iid: number; status: ComputingImage["status"]; attempt: number; log: string }> {
    return this.api.get(`cu-image/${identifier(iid)}/log`);
  }
  async deleteImage(iid: number): Promise<void> {
    await this.api.delete(`cu-image/${identifier(iid)}`);
  }
}
