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

import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnit,
  WorkflowComputingUnitResourceLimit,
} from "../type/workflow-computing-unit";

export const NAN_COMPUTING_UNIT_RESOURCE: WorkflowComputingUnitResourceLimit = {
  cpuLimit: "NaN",
  memoryLimit: "NaN",
  gpuLimit: "NaN",
  jvmMemorySize: "NaN",
  shmSize: "NaN",
  nodeAddresses: [],
};

export type ComputingUnitFixtureOverrides = Omit<Partial<DashboardWorkflowComputingUnit>, "computingUnit"> & {
  computingUnit?: Omit<Partial<WorkflowComputingUnit>, "resource"> & {
    resource?: Partial<WorkflowComputingUnitResourceLimit>;
  };
};

/**
 * Builds a {@link DashboardWorkflowComputingUnit} fixture, deep-merging optional
 * `computingUnit` and `computingUnit.resource` partial overrides.
 */
export function makeDashboardComputingUnit(
  overrides: ComputingUnitFixtureOverrides = {}
): DashboardWorkflowComputingUnit {
  const { computingUnit: cuOverride, ...topOverrides } = overrides;
  const defaultResource: WorkflowComputingUnitResourceLimit = {
    cpuLimit: "1",
    memoryLimit: "1Gi",
    gpuLimit: "0",
    jvmMemorySize: "0",
    shmSize: "0",
    nodeAddresses: [],
  };
  const resource = { ...defaultResource, ...(cuOverride?.resource ?? {}) };

  return {
    status: "Running",
    metrics: { cpuUsage: "N/A", memoryUsage: "N/A" },
    isOwner: true,
    accessPrivilege: "WRITE",
    ownerAvatar: "",
    ownerName: "owner",
    ...topOverrides,
    computingUnit: {
      cuid: 1,
      uid: 1,
      name: "unit-1",
      creationTime: 0,
      terminateTime: undefined,
      type: "local",
      uri: "",
      ...(cuOverride ?? {}),
      resource,
    },
  };
}

/** Builds a local computing-unit fixture with NaN resource placeholders. */
export function makeLocalComputingUnit(
  cuid = 1,
  base: ComputingUnitFixtureOverrides = {}
): DashboardWorkflowComputingUnit {
  return makeDashboardComputingUnit({
    ...base,
    computingUnit: {
      ...(base.computingUnit ?? {}),
      cuid,
      type: "local",
      resource: NAN_COMPUTING_UNIT_RESOURCE,
    },
  });
}
