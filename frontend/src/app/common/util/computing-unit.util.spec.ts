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

import { TestBed, ComponentFixture } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import type { DashboardWorkflowComputingUnit } from "../type/workflow-computing-unit";
import {
  ComputingUnitMetadataComponent,
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

function makeUnit(overrides: Partial<DashboardWorkflowComputingUnit> = {}): DashboardWorkflowComputingUnit {
  return {
    status: "Running",
    metrics: { cpuUsage: "N/A", memoryUsage: "N/A" },
    isOwner: true,
    accessPrivilege: "READ",
    ownerGoogleAvatar: "",
    ownerName: "owner",
    ...overrides,
    // Set computingUnit last so a `computingUnit` override merges into (rather than replaces)
    // the nested default object, keeping fixtures valid even for partial overrides.
    computingUnit: {
      cuid: 1,
      uid: 1,
      name: "unit-a",
      creationTime: 1700000000000,
      terminateTime: undefined,
      type: "kubernetes",
      uri: "http://localhost/wsapi",
      resource: {
        cpuLimit: "1000m",
        memoryLimit: "2Gi",
        gpuLimit: "1",
        jvmMemorySize: "2G",
        shmSize: "64Mi",
        nodeAddresses: [],
      },
      ...(overrides.computingUnit ?? {}),
    },
  };
}

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

describe("cpuResourceConversion (fallback units)", () => {
  it("should fall back to the millicore scale for an unknown source unit", () => {
    // "k" is not a known CPU unit, so cpuScales[from] is undefined and the "m" scale is used
    expect(cpuResourceConversion("5k", "m")).toBe("5.00");
  });

  it("should fall back to the core scale for an unknown target unit", () => {
    // "x" is not a known CPU unit, so cpuScales[to] is undefined and the "" (cores) scale is used
    expect(cpuResourceConversion("2", "x")).toBe("2");
  });
});

describe("memoryResourceConversion (edge branches)", () => {
  it("should treat a unitless source value as bytes", () => {
    expect(memoryResourceConversion("1073741824", "Gi")).toBe("1.0000");
  });

  it("should treat a unitless target unit as bytes", () => {
    expect(memoryResourceConversion("1Gi", "")).toBe("1073741824.0000");
  });

  it("should fall back to a byte scale for an unknown source unit", () => {
    // "Ti" is not a known memory unit, so memoryScales[from] is undefined and 1 (bytes) is used
    expect(memoryResourceConversion("5Ti", "")).toBe("5.0000");
  });

  it("should fall back to a byte scale for an unknown target unit", () => {
    // "Ti" is not a known memory unit, so memoryScales[to] is undefined and 1 (bytes) is used
    expect(memoryResourceConversion("1Gi", "Ti")).toBe("1073741824.0000");
  });
});

describe("memoryPercentage (limit guard)", () => {
  it("should return 0 when the limit is not positive", () => {
    expect(memoryPercentage("512Mi", "0")).toBe(0);
  });
});

describe("isComputingUnitShmTooLarge (non-Gi selected memory)", () => {
  it("should treat a Mi selected memory as-is when comparing", () => {
    expect(isComputingUnitShmTooLarge("2048Mi", 3, "Gi")).toBe(true);
    expect(isComputingUnitShmTooLarge("2048Mi", 1024, "Mi")).toBe(false);
  });
});

describe("getJvmMemorySliderConfig (memory unit handling)", () => {
  it("should convert a Mi memory value to whole GiB", () => {
    // 2048Mi -> 2Gi -> small path (<=3), defaultValue 2 (not 1)
    const config = getJvmMemorySliderConfig("2048Mi");
    expect(config.showJvmMemorySlider).toBe(false);
    expect(config.jvmMemorySteps).toEqual([1, 2]);
    expect(config.selectedJvmMemorySize).toBe("2G");
    expect(config.jvmMemorySliderValue).toBe(1);
  });

  it("should floor sub-GiB Mi values up to a minimum of 1 GiB", () => {
    // 512Mi -> floor(512/1024)=0 -> Math.max(1, 0) = 1
    const config = getJvmMemorySliderConfig("512Mi");
    expect(config.jvmMemorySteps).toEqual([1]);
    expect(config.selectedJvmMemorySize).toBe("1G");
  });

  it("should build slider steps from a larger Mi value", () => {
    // 8192Mi -> 8Gi -> slider path
    const config = getJvmMemorySliderConfig("8192Mi");
    expect(config.showJvmMemorySlider).toBe(true);
    expect(config.jvmMemorySteps).toEqual([2, 4, 8]);
    expect(config.selectedJvmMemorySize).toBe("2G");
  });

  it("should default to 1 GiB for a value with an unrecognized memory unit", () => {
    // "Ki" is neither Gi nor Mi, so memoryToGb falls back to 1
    const config = getJvmMemorySliderConfig("500Ki");
    expect(config.jvmMemorySteps).toEqual([1]);
    expect(config.selectedJvmMemorySize).toBe("1G");
  });
});

describe("ComputingUnitMetadataComponent", () => {
  let fixture: ComponentFixture<ComputingUnitMetadataComponent> | undefined;

  function render(unit: DashboardWorkflowComputingUnit): ComputingUnitMetadataComponent {
    TestBed.configureTestingModule({
      declarations: [ComputingUnitMetadataComponent],
      providers: [{ provide: NZ_MODAL_DATA, useValue: unit }],
    });
    fixture = TestBed.createComponent(ComputingUnitMetadataComponent);
    fixture.detectChanges();
    return fixture.componentInstance;
  }

  // Return the trimmed text of the <td> value cell whose <th> header matches `label`.
  function cellText(label: string): string {
    const nativeElement = (fixture as ComponentFixture<ComputingUnitMetadataComponent>).nativeElement as HTMLElement;
    const rows = Array.from(nativeElement.querySelectorAll("tr"));
    const row = rows.find(r => r.querySelector("th")?.textContent?.trim() === label);
    return row?.querySelector("td")?.textContent?.trim() ?? "";
  }

  afterEach(() => {
    fixture?.destroy();
    fixture = undefined;
    TestBed.resetTestingModule();
  });

  it("should expose the injected unit and a formatted creation time", () => {
    const unit = makeUnit();
    const instance = render(unit);
    expect(instance.unit).toBe(unit);
    expect(instance.createdAt).toBe(new Date(unit.computingUnit.creationTime).toLocaleString());
  });

  it("should render owner access and the gpu limit when present", () => {
    const unit = makeUnit({ isOwner: true });
    unit.computingUnit.resource.gpuLimit = "2";
    render(unit);
    expect(cellText("Access")).toBe("Owner");
    expect(cellText("GPU Limit")).toBe("2");
  });

  it('should render the access privilege and "None" when there is no gpu limit', () => {
    const unit = makeUnit({ isOwner: false, accessPrivilege: "WRITE" });
    unit.computingUnit.resource.gpuLimit = "";
    render(unit);
    expect(cellText("Access")).toBe("WRITE");
    expect(cellText("GPU Limit")).toBe("None");
  });
});
