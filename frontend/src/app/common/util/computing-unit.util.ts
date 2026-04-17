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

import { Component, inject } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { DashboardWorkflowComputingUnit } from "../type/workflow-computing-unit";

@Component({
  template: `
    <table class="ant-table">
      <tbody>
        <tr>
          <th style="width: 150px;">Name</th>
          <td>{{ unit.computingUnit.name }}</td>
        </tr>
        <tr>
          <th>Status</th>
          <td>{{ unit.status }}</td>
        </tr>
        <tr>
          <th>Type</th>
          <td>{{ unit.computingUnit.type }}</td>
        </tr>
        <tr>
          <th>CPU Limit</th>
          <td>{{ unit.computingUnit.resource.cpuLimit }}</td>
        </tr>
        <tr>
          <th>Memory Limit</th>
          <td>{{ unit.computingUnit.resource.memoryLimit }}</td>
        </tr>
        <tr>
          <th>GPU Limit</th>
          <td>{{ unit.computingUnit.resource.gpuLimit || "None" }}</td>
        </tr>
        <tr>
          <th>JVM Memory</th>
          <td>{{ unit.computingUnit.resource.jvmMemorySize }}</td>
        </tr>
        <tr>
          <th>Shared Memory</th>
          <td>{{ unit.computingUnit.resource.shmSize }}</td>
        </tr>
        <tr>
          <th>Created</th>
          <td>{{ createdAt }}</td>
        </tr>
        <tr>
          <th>Access</th>
          <td>{{ unit.isOwner ? "Owner" : unit.accessPrivilege }}</td>
        </tr>
      </tbody>
    </table>
  `,
})
export class ComputingUnitMetadataComponent {
  readonly unit: DashboardWorkflowComputingUnit = inject(NZ_MODAL_DATA);
  readonly createdAt = new Date(this.unit.computingUnit.creationTime).toLocaleString();
}

export function parseResourceUnit(resource: string): string {
  // check if has a capacity (is a number followed by a unit)
  if (!resource || resource === "NaN") return "NaN";
  const re = /^(\d+(\.\d+)?)([a-zA-Z]*)$/;
  const match = resource.match(re);
  if (match) {
    return match[3] || "";
  }
  return "";
}

export function parseResourceNumber(resource: string): number {
  // check if has a capacity (is a number followed by a unit)
  if (!resource || resource === "NaN") return 0;
  const re = /^(\d+(\.\d+)?)([a-zA-Z]*)$/;
  const match = resource.match(re);
  if (match) {
    return parseFloat(match[1]);
  }
  return 0;
}

export function cpuResourceConversion(from: string, toUnit: string): string {
  // cpu conversions
  type CpuUnit = "n" | "u" | "m" | "";
  const cpuScales: { [key in CpuUnit]: number } = {
    n: 1,
    u: 1_000,
    m: 1_000_000,
    "": 1_000_000_000,
  };
  const fromUnit = parseResourceUnit(from) as CpuUnit;
  const fromNumber = parseResourceNumber(from);

  // Handle empty unit in input (means cores)
  const effectiveFromUnit = (fromUnit || "") as CpuUnit;
  const effectiveToUnit = (toUnit || "") as CpuUnit;

  // Convert to base units (nanocores) then to target unit
  const fromScaled = fromNumber * (cpuScales[effectiveFromUnit] || cpuScales["m"]);
  const toScaled = fromScaled / (cpuScales[effectiveToUnit] || cpuScales[""]);

  // For display purposes, use appropriate precision
  if (effectiveToUnit === "") {
    return toScaled.toFixed(4); // 4 decimal places for cores
  } else if (effectiveToUnit === "m") {
    return toScaled.toFixed(2); // 2 decimal places for millicores
  } else {
    return Math.round(toScaled).toString(); // Whole numbers for smaller units
  }
}

export function memoryResourceConversion(from: string, toUnit: string): string {
  // memory conversion
  type MemoryUnit = "Ki" | "Mi" | "Gi" | "";
  const memoryScales: { [key in MemoryUnit]: number } = {
    "": 1,
    Ki: 1024,
    Mi: 1024 * 1024,
    Gi: 1024 * 1024 * 1024,
  };
  const fromUnit = parseResourceUnit(from) as MemoryUnit;
  const fromNumber = parseResourceNumber(from);

  // Handle empty unit in input (means bytes)
  const effectiveFromUnit = (fromUnit || "") as MemoryUnit;
  const effectiveToUnit = (toUnit || "") as MemoryUnit;

  // Convert to base units (bytes) then to target unit
  const fromScaled = fromNumber * (memoryScales[effectiveFromUnit] || 1);
  const toScaled = fromScaled / (memoryScales[effectiveToUnit] || 1);

  // For memory, we want to show in the same format as the limit (typically X.XXX Gi)
  return toScaled.toFixed(4);
}

export function cpuPercentage(usage: string, limit: string): number {
  if (usage === "N/A" || limit === "N/A") return 0;

  // Convert to the same unit for comparison
  const displayUnit = ""; // Convert to cores for percentage calculation

  // Use our existing conversion method to get values in the same unit
  const usageValue = parseFloat(cpuResourceConversion(usage, displayUnit));
  const limitValue = parseFloat(cpuResourceConversion(limit, displayUnit));

  if (limitValue <= 0) return 0;

  // Calculate percentage and ensure it doesn't exceed 100%
  const percentage = (usageValue / limitValue) * 100;

  return Math.min(percentage, 100);
}

export function memoryPercentage(usage: string, limit: string): number {
  if (usage === "N/A" || limit === "N/A") return 0;

  // Convert to the same unit for comparison
  const displayUnit = "Gi"; // Convert to GiB for percentage calculation

  // Use our existing conversion method to get values in the same unit
  const usageValue = parseFloat(memoryResourceConversion(usage, displayUnit));
  const limitValue = parseFloat(memoryResourceConversion(limit, displayUnit));

  if (limitValue <= 0) return 0;

  // Calculate percentage and ensure it doesn't exceed 100%
  const percentage = (usageValue / limitValue) * 100;

  return Math.min(percentage, 100);
}

export function findNearestValidStep(value: number, jvmMemorySteps: number[]): number {
  if (jvmMemorySteps.length === 0) return 1;
  if (jvmMemorySteps.includes(value)) return value;

  // Find the closest step value
  return jvmMemorySteps.reduce((prev, curr) => {
    return Math.abs(curr - value) < Math.abs(prev - value) ? curr : prev;
  });
}

export function validateName(trimmedName: string): string | null {
  if (!trimmedName) return "Computing unit name cannot be empty";
  if (trimmedName.length > 128) return "Computing unit name cannot exceed 128 characters";
  return null;
}

export function getComputingUnitBadgeColor(status: string): string {
  switch (status) {
    case "Running":
      return "green";
    case "Pending":
      return "gold";
    default:
      return "red";
  }
}

export function getComputingUnitStatusTooltip(entry: DashboardWorkflowComputingUnit): string {
  switch (entry.status) {
    case "Running":
      return "Ready to use";
    case "Pending":
      return "Computing unit is starting up";
    default:
      return entry.status;
  }
}

export function getComputingUnitCpuStatus(percentage: number): "success" | "exception" | "active" | "normal" {
  if (percentage > 90) return "exception";
  if (percentage > 50) return "normal";
  return "success";
}

export function getComputingUnitMemoryStatus(percentage: number): "success" | "exception" | "active" | "normal" {
  if (percentage > 90) return "exception";
  if (percentage > 50) return "normal";
  return "success";
}

export function getComputingUnitCpuLimitUnit(unit: string) {
  if (unit === "") {
    return "CPU";
  }
  return unit;
}

export function isComputingUnitShmTooLarge(
  selectedMemory: string,
  shmSizeValue: number,
  shmSizeUnit: "Mi" | "Gi"
): boolean {
  const total = parseResourceNumber(selectedMemory);
  const unit = parseResourceUnit(selectedMemory);
  const memoryInMi = unit === "Gi" ? total * 1024 : total;
  const shmInMi = shmSizeUnit === "Gi" ? shmSizeValue * 1024 : shmSizeValue;

  return shmInMi > memoryInMi;
}

function memoryToGb(selectedMemory: string): number {
  const memoryValue = parseResourceNumber(selectedMemory);
  const memoryUnit = parseResourceUnit(selectedMemory);

  if (memoryUnit === "Gi") {
    return memoryValue;
  }

  if (memoryUnit === "Mi") {
    return Math.max(1, Math.floor(memoryValue / 1024));
  }

  return 1;
}

function buildJvmMemorySteps(maxGb: number, start: number): number[] {
  const steps: number[] = [];
  let value = start;

  while (value <= maxGb) {
    steps.push(value);
    value *= 2;
  }

  return steps;
}

function buildJvmMemoryMarks(steps: number[]): Record<number, string> {
  return steps.reduce<Record<number, string>>((marks, step) => {
    marks[step] = `${step}G`;
    return marks;
  }, {});
}

export function getJvmMemorySliderConfig(selectedMemory: string): JvmMemorySliderConfig {
  const cuMemoryInGb = memoryToGb(selectedMemory);

  if (cuMemoryInGb <= 3) {
    const defaultValue = cuMemoryInGb === 1 ? 1 : 2;
    const steps = buildJvmMemorySteps(cuMemoryInGb, 1);

    return {
      jvmMemoryMax: cuMemoryInGb,
      showJvmMemorySlider: false,
      jvmMemorySteps: steps,
      jvmMemoryMarks: buildJvmMemoryMarks(steps),
      jvmMemorySliderValue: defaultValue,
      selectedJvmMemorySize: `${defaultValue}G`,
    };
  }

  const steps = buildJvmMemorySteps(cuMemoryInGb, 2);

  return {
    jvmMemoryMax: cuMemoryInGb,
    showJvmMemorySlider: true,
    jvmMemorySteps: steps,
    jvmMemoryMarks: buildJvmMemoryMarks(steps),
    jvmMemorySliderValue: 2,
    selectedJvmMemorySize: "2G",
  };
}

interface JvmMemorySliderConfig {
  jvmMemoryMax: number;
  showJvmMemorySlider: boolean;
  jvmMemorySteps: number[];
  jvmMemoryMarks: Record<number, string>;
  jvmMemorySliderValue: number;
  selectedJvmMemorySize: string;
}

export const unitTypeMessageTemplate = {
  local: {
    createTitle: "Connect to a Local Computing Unit",
    terminateTitle: "Disconnect from Local Computing Unit",
    terminateWarning: "", // no red warning
    createSuccess: "Successfully connected to the local computing unit",
    createFailure: "Failed to connect to the local computing unit",
    terminateSuccess: "Disconnected from the local computing unit",
    terminateFailure: "Failed to disconnect from the local computing unit",
    terminateTooltip: "Disconnect from this computing unit",
  },
  kubernetes: {
    createTitle: "Create Computing Unit",
    terminateTitle: "Terminate Computing Unit",
    terminateWarning:
      "<p style='color: #ff4d4f;'><strong>Warning:</strong> All execution results in this computing unit will be lost.</p>",
    createSuccess: "Successfully created the Kubernetes computing unit",
    createFailure: "Failed to create the Kubernetes computing unit",
    terminateSuccess: "Terminated Kubernetes computing unit",
    terminateFailure: "Failed to terminate Kubernetes computing unit",
    terminateTooltip: "Terminate this computing unit",
  },
} as const;
