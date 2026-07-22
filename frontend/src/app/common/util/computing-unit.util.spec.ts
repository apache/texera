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

import type { DashboardWorkflowComputingUnit } from "../type/workflow-computing-unit";
import {
  parseResourceUnit,
  parseResourceNumber,
  cpuResourceConversion,
  memoryResourceConversion,
  cpuPercentage,
  memoryPercentage,
  validateName,
  getComputingUnitBadgeColor,
  getComputingUnitStatusTooltip,
  getComputingUnitCpuStatus,
  getComputingUnitMemoryStatus,
  getComputingUnitCpuLimitUnit,
  isComputingUnitShmTooLarge,
  getJvmMemorySliderConfig,
  buildLocalComputingUnitUri,
} from "./computing-unit.util";

describe("parseResourceUnit", () => {
  it("should extract the unit from a resource string", () => {
    expect(parseResourceUnit("500Mi")).toBe("Mi");
    expect(parseResourceUnit("2Gi")).toBe("Gi");
    expect(parseResourceUnit("1000m")).toBe("m");
  });

  it("should return an empty string for a unitless number", () => {
    expect(parseResourceUnit("2")).toBe("");
    expect(parseResourceUnit("1.5")).toBe("");
  });

  it('should return "NaN" for empty or "NaN" input', () => {
    expect(parseResourceUnit("")).toBe("NaN");
    expect(parseResourceUnit("NaN")).toBe("NaN");
  });

  it("should return an empty string for a value that does not match the pattern", () => {
    expect(parseResourceUnit("abc")).toBe("");
  });
});

describe("parseResourceNumber", () => {
  it("should extract the numeric value from a resource string", () => {
    expect(parseResourceNumber("2Gi")).toBe(2);
    expect(parseResourceNumber("500Mi")).toBe(500);
  });

  it("should handle decimal values", () => {
    expect(parseResourceNumber("1.5Gi")).toBe(1.5);
  });

  it("should return 0 for empty, invalid, or NaN input", () => {
    expect(parseResourceNumber("")).toBe(0);
    expect(parseResourceNumber("NaN")).toBe(0);
    expect(parseResourceNumber("abc")).toBe(0);
  });
});

describe("cpuResourceConversion", () => {
  it("should convert millicores to cores with 4 decimals", () => {
    expect(cpuResourceConversion("1000m", "")).toBe("1.0000");
    expect(cpuResourceConversion("500m", "")).toBe("0.5000");
  });

  it("should convert cores to millicores with 2 decimals", () => {
    expect(cpuResourceConversion("2", "m")).toBe("2000.00");
  });

  it("should round to whole numbers for smaller units", () => {
    expect(cpuResourceConversion("1", "u")).toBe("1000000");
  });
});

describe("memoryResourceConversion", () => {
  it("should convert between memory units with 4 decimals", () => {
    expect(memoryResourceConversion("1Gi", "Gi")).toBe("1.0000");
    expect(memoryResourceConversion("1024Mi", "Gi")).toBe("1.0000");
    expect(memoryResourceConversion("512Mi", "Gi")).toBe("0.5000");
  });
});

describe("cpuPercentage", () => {
  it('should return 0 when usage or limit is "N/A"', () => {
    expect(cpuPercentage("N/A", "1000m")).toBe(0);
    expect(cpuPercentage("500m", "N/A")).toBe(0);
  });

  it("should return 0 when the limit is not positive", () => {
    expect(cpuPercentage("500m", "0")).toBe(0);
  });

  it("should compute the usage percentage", () => {
    expect(cpuPercentage("500m", "1000m")).toBe(50);
  });

  it("should clamp the percentage at 100", () => {
    expect(cpuPercentage("2000m", "1000m")).toBe(100);
  });
});

describe("memoryPercentage", () => {
  it('should return 0 when usage or limit is "N/A"', () => {
    expect(memoryPercentage("N/A", "1Gi")).toBe(0);
    expect(memoryPercentage("512Mi", "N/A")).toBe(0);
  });

  it("should compute the usage percentage", () => {
    expect(memoryPercentage("512Mi", "1Gi")).toBe(50);
  });

  it("should clamp the percentage at 100", () => {
    expect(memoryPercentage("2Gi", "1Gi")).toBe(100);
  });
});

describe("validateName", () => {
  it("should return an error for an empty name", () => {
    expect(validateName("")).toBe("Computing unit name cannot be empty");
  });

  it("should return an error for names longer than 128 characters", () => {
    expect(validateName("a".repeat(129))).toBe("Computing unit name cannot exceed 128 characters");
  });

  it("should return null for valid names", () => {
    expect(validateName("my-unit")).toBeNull();
    expect(validateName("a".repeat(128))).toBeNull();
  });
});

describe("getComputingUnitBadgeColor", () => {
  it("should map known statuses to colors", () => {
    expect(getComputingUnitBadgeColor("Running")).toBe("green");
    expect(getComputingUnitBadgeColor("Pending")).toBe("gold");
  });

  it("should default to red for unknown statuses", () => {
    expect(getComputingUnitBadgeColor("Terminated")).toBe("red");
  });
});

describe("getComputingUnitStatusTooltip", () => {
  it("should map statuses to tooltip text", () => {
    expect(getComputingUnitStatusTooltip({ status: "Running" } as unknown as DashboardWorkflowComputingUnit)).toBe(
      "Ready to use"
    );
    expect(getComputingUnitStatusTooltip({ status: "Pending" } as unknown as DashboardWorkflowComputingUnit)).toBe(
      "Computing unit is starting up"
    );
  });

  it("should fall back to the raw status for unknown statuses", () => {
    expect(getComputingUnitStatusTooltip({ status: "Terminated" } as unknown as DashboardWorkflowComputingUnit)).toBe(
      "Terminated"
    );
  });
});

describe("getComputingUnitCpuStatus / getComputingUnitMemoryStatus", () => {
  it("should derive status from the usage percentage", () => {
    expect(getComputingUnitCpuStatus(95)).toBe("exception");
    expect(getComputingUnitCpuStatus(60)).toBe("normal");
    expect(getComputingUnitCpuStatus(30)).toBe("success");

    expect(getComputingUnitMemoryStatus(95)).toBe("exception");
    expect(getComputingUnitMemoryStatus(60)).toBe("normal");
    expect(getComputingUnitMemoryStatus(30)).toBe("success");
  });
});

describe("getComputingUnitCpuLimitUnit", () => {
  it('should return "CPU" for an empty unit and the unit otherwise', () => {
    expect(getComputingUnitCpuLimitUnit("")).toBe("CPU");
    expect(getComputingUnitCpuLimitUnit("m")).toBe("m");
  });
});

describe("isComputingUnitShmTooLarge", () => {
  it("should return true only when shared memory exceeds the selected memory", () => {
    expect(isComputingUnitShmTooLarge("2Gi", 3, "Gi")).toBe(true);
    expect(isComputingUnitShmTooLarge("2Gi", 1, "Gi")).toBe(false);
  });

  it("should return false when shared memory equals the selected memory", () => {
    expect(isComputingUnitShmTooLarge("2Gi", 2, "Gi")).toBe(false);
  });

  it("should compare across mixed units", () => {
    expect(isComputingUnitShmTooLarge("1Gi", 2048, "Mi")).toBe(true);
    expect(isComputingUnitShmTooLarge("1Gi", 512, "Mi")).toBe(false);
  });
});

describe("getJvmMemorySliderConfig", () => {
  it("should hide the slider for small memory sizes", () => {
    const config = getJvmMemorySliderConfig("1Gi");
    expect(config.showJvmMemorySlider).toBe(false);
    expect(config.jvmMemorySteps).toEqual([1]);
    expect(config.selectedJvmMemorySize).toBe("1G");
  });

  it("should show the slider and build doubling steps for larger memory sizes", () => {
    const config = getJvmMemorySliderConfig("8Gi");
    expect(config.showJvmMemorySlider).toBe(true);
    expect(config.jvmMemorySteps).toEqual([2, 4, 8]);
    expect(config.selectedJvmMemorySize).toBe("2G");
  });
});

describe("buildLocalComputingUnitUri", () => {
  it("should build a uri including the port when present", () => {
    expect(buildLocalComputingUnitUri({ protocol: "http:", hostname: "localhost", port: "8080" })).toBe(
      "http://localhost:8080/wsapi"
    );
  });

  it("should omit the port when it is empty", () => {
    expect(buildLocalComputingUnitUri({ protocol: "https:", hostname: "example.com", port: "" })).toBe(
      "https://example.com/wsapi"
    );
  });
});
