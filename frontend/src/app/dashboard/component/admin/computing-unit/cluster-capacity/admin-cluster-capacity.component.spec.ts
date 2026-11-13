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

import { HttpErrorResponse } from "@angular/common/http";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { of, Subject, throwError } from "rxjs";
import { AdminClusterCapacityComponent } from "./admin-cluster-capacity.component";
import { WorkflowComputingUnitManagingService } from "../../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ClusterCapacity, ResourceCapacity } from "../../../../../common/type/workflow-computing-unit";
import { commonTestImports, commonTestProviders } from "../../../../../common/testing/test-utils";

const MiB = 1024 ** 2;
const GiB = 1024 ** 3;
const TiB = 1024 ** 4;

// A quarter of every resource allocated, so each row reads 25%.
function capacity(over: Partial<ClusterCapacity> = {}): ClusterCapacity {
  return {
    cpu: used(16000, 4000),
    memory: used(64 * GiB, 16 * GiB),
    gpu: used(4, 1),
    ...over,
  };
}

const used = (total: number, allocated: number): ResourceCapacity => ({
  total,
  allocated,
  available: Math.max(total - allocated, 0),
});

const failWith = (status: number, message: string) =>
  throwError(() => new HttpErrorResponse({ error: { message }, status }));

describe("AdminClusterCapacityComponent", () => {
  let fixture: ComponentFixture<AdminClusterCapacityComponent>;
  let service: WorkflowComputingUnitManagingService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [...commonTestProviders],
      imports: [AdminClusterCapacityComponent, ...commonTestImports],
    }).compileComponents();

    // Fake timers go in before the first `detectChanges`, so the poll's interval is fake too.
    vi.useFakeTimers();
    fixture = TestBed.createComponent(AdminClusterCapacityComponent);
    service = TestBed.inject(WorkflowComputingUnitManagingService);
    vi.spyOn(service, "getClusterCapacity").mockReturnValue(of(capacity()));
  });

  afterEach(() => {
    fixture.destroy();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  const render = () => fixture.detectChanges();
  const advance = (ms: number) => {
    vi.advanceTimersByTime(ms);
    fixture.detectChanges();
  };
  const panel = (): HTMLElement | null => fixture.nativeElement.querySelector(".cluster-capacity");
  const rowFor = (resource: string): HTMLElement => {
    const row = Array.from<HTMLElement>(fixture.nativeElement.querySelectorAll("tbody tr")).find(
      r => r.querySelector("th")?.textContent?.trim() === resource
    );
    expect(row).toBeDefined();
    return row!;
  };
  const text = (resource: string, cell: string) => rowFor(resource).querySelector(`.${cell}`)?.textContent?.trim();
  const figures = (resource: string) => ["allocated", "available", "total"].map(cell => text(resource, cell));
  const fillWidth = (resource: string) =>
    rowFor(resource).querySelector<HTMLElement>(".meter-fill")?.style.width ?? "no meter";
  const alertText = () => fixture.nativeElement.querySelector("nz-alert")?.textContent ?? "";

  describe("the ledger", () => {
    it("shows CPU in cores, memory in binary units and GPU as a count, each with its share allocated", () => {
      render();

      expect(figures("CPU")).toEqual(["4 cores", "12 cores", "16 cores"]);
      expect(figures("Memory")).toEqual(["16 GiB", "48 GiB", "64 GiB"]);
      expect(figures("GPU")).toEqual(["1", "3", "4"]);
      for (const resource of ["CPU", "Memory", "GPU"]) {
        expect(text(resource, "percent-used")).toBe("25%");
        expect(fillWidth(resource)).toBe("25%");
      }
      expect(panel()?.textContent).toContain("Updated");
    });

    it("says 1 core in the singular and groups the thousands", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(
        of(capacity({ cpu: { total: 2_048_000, allocated: 250, available: 1000 } }))
      );

      render();

      expect(text("CPU", "available")).toBe("1 core");
      expect(text("CPU", "total")).toBe("2,048 cores");
    });

    // 999m is just under a core, the figure that would round to a plural "1".
    it("shows cores to the millicore, without rounding", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(
        of(capacity({ cpu: { total: 3920, allocated: 125, available: 999 } }))
      );

      render();

      expect(text("CPU", "allocated")).toBe("0.125 cores");
      expect(text("CPU", "available")).toBe("0.999 cores");
      expect(text("CPU", "total")).toBe("3.92 cores");
    });

    it("picks the memory unit by size and drops a trailing .0", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(
        of(capacity({ memory: { total: 2 * TiB, allocated: 512 * MiB, available: 1.5 * GiB } }))
      );

      render();

      expect(text("Memory", "allocated")).toBe("512 MiB");
      expect(text("Memory", "available")).toBe("1.5 GiB");
      expect(text("Memory", "total")).toBe("2 TiB");
    });

    it("moves up a unit when rounding reaches it, so it never shows 1,024 of the smaller one", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(of(capacity({ memory: used(GiB - 1, 1023.99 * MiB) })));

      render();

      expect(text("Memory", "allocated")).toBe("1 GiB");
      expect(text("Memory", "total")).toBe("1 GiB");
    });

    it("shows zero bytes as 0 B", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(of(capacity({ memory: used(64 * GiB, 0) })));

      render();

      expect(text("Memory", "allocated")).toBe("0 B");
      expect(text("Memory", "percent-used")).toBe("0%");
    });

    // 15.99 of 16 cores must not read full, and 29 / 100 * 100 is 28.999… in floating point.
    it("rounds the share down, so 100% shows only when nothing is left", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(
        of(capacity({ cpu: used(16000, 15990), memory: used(64 * GiB, 64 * GiB), gpu: used(100, 29) }))
      );

      render();

      expect(text("CPU", "percent-used")).toBe("99%");
      expect(text("Memory", "percent-used")).toBe("100%");
      expect(text("GPU", "percent-used")).toBe("29%");
    });

    it("shows a share over 100% when pods request more than the nodes offer, with the bar capped at full", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(of(capacity({ cpu: used(1000, 1500) })));

      render();

      expect(text("CPU", "percent-used")).toBe("150%");
      expect(fillWidth("CPU")).toBe("100%");
      expect(text("CPU", "available")).toBe("0 cores");
    });

    it("flags a resource from 80% as running low and from 95% as nearly full, with an icon and a label", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(
        of(capacity({ cpu: used(100, 79), memory: used(100, 80), gpu: used(20, 19) }))
      );

      render();

      const status = (resource: string) => rowFor(resource).querySelector(".status-icon")?.getAttribute("aria-label");
      expect(rowFor("CPU").className).not.toMatch(/capacity-(warning|critical)/);
      expect(status("CPU")).toBeUndefined();
      expect(rowFor("Memory").classList).toContain("capacity-warning");
      expect(status("Memory")).toBe("Running low");
      expect(rowFor("GPU").classList).toContain("capacity-critical");
      expect(status("GPU")).toBe("Nearly full");
    });

    it("says the cluster has none of a resource instead of dividing by zero", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(of(capacity({ gpu: used(0, 0) })));

      render();

      expect(text("GPU", "usage")).toBe("None in the cluster");
      expect(text("GPU", "percent-used")).toBeUndefined();
      expect(fillWidth("GPU")).toBe("no meter");
      expect(text("GPU", "total")).toBe("0");
    });

    it("shows a resource the response leaves out or garbles as not reported, and still shows the rest", () => {
      const garbled = {
        cpu: { total: "lots", allocated: 1, available: 1 },
        memory: capacity().memory,
      } as unknown as ClusterCapacity;
      vi.mocked(service.getClusterCapacity).mockReturnValue(of(garbled));

      render();

      for (const resource of ["CPU", "GPU"]) {
        expect(text(resource, "usage")).toBe("Not reported");
        expect(figures(resource)).toEqual(["—", "—", "—"]);
      }
      expect(text("Memory", "percent-used")).toBe("25%");
    });

    // A browser drops a negative width, so the bar would show full.
    it("shows a resource with a negative figure as not reported", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(
        of(
          capacity({
            cpu: { total: 1000, allocated: -50, available: 1050 },
            gpu: { total: -1, allocated: 0, available: 0 },
          })
        )
      );

      render();

      expect(text("CPU", "usage")).toBe("Not reported");
      expect(text("GPU", "usage")).toBe("Not reported");
    });
  });

  describe("loading and polling", () => {
    it("renders nothing until the first answer", () => {
      const first = new Subject<ClusterCapacity>();
      vi.mocked(service.getClusterCapacity).mockReturnValue(first);

      render();
      expect(panel()).toBeNull();

      first.next(capacity());
      first.complete();
      render();
      expect(panel()).not.toBeNull();
    });

    it("stays hidden and stops polling when computing units do not run on Kubernetes", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(failWith(404, "not on Kubernetes"));

      render();
      advance(120_000);

      expect(panel()).toBeNull();
      expect(service.getClusterCapacity).toHaveBeenCalledTimes(1);
    });

    it("shows the cluster's message when the first load fails, e.g. a missing read grant", () => {
      vi.mocked(service.getClusterCapacity).mockReturnValue(failWith(500, "nodes is forbidden"));

      render();

      expect(panel()).not.toBeNull();
      expect(alertText()).toContain("Could not load cluster capacity: nodes is forbidden");
      expect(fixture.nativeElement.querySelector("table")).toBeNull();
    });

    // An empty message is falsy, and would leave the alert ending on a bare colon.
    it("still reports a failure whose message is empty, and says no details were given", () => {
      vi.mocked(service.getClusterCapacity)
        .mockReturnValueOnce(failWith(500, ""))
        .mockReturnValueOnce(of(capacity()))
        .mockReturnValueOnce(failWith(500, ""));

      render();
      expect(alertText()).toContain("Could not load cluster capacity: the server gave no details");

      advance(30_000);
      advance(30_000);
      expect(alertText()).toContain("The refresh is failing: the server gave no details");
    });

    it("refreshes every 30 seconds, not on the units table's 5 second cadence", () => {
      render();
      advance(29_999);
      expect(service.getClusterCapacity).toHaveBeenCalledTimes(1);

      advance(1);
      expect(service.getClusterCapacity).toHaveBeenCalledTimes(2);
    });

    it("keeps the last capacity and warns when a refresh fails, then clears the warning once one succeeds", () => {
      vi.mocked(service.getClusterCapacity)
        .mockReturnValueOnce(of(capacity()))
        .mockReturnValueOnce(failWith(500, "boom"))
        .mockReturnValueOnce(of(capacity({ cpu: used(16000, 8000) })));

      render();
      advance(30_000);

      expect(text("CPU", "percent-used")).toBe("25%");
      expect(alertText()).toContain("The refresh is failing: boom");

      advance(30_000);

      expect(text("CPU", "percent-used")).toBe("50%");
      expect(fixture.nativeElement.querySelector("nz-alert")).toBeNull();
    });

    it("restarts the 30 second clock when the admin clicks refresh", () => {
      render();
      advance(20_000);
      fixture.nativeElement.querySelector(".refresh-button").click();
      expect(service.getClusterCapacity).toHaveBeenCalledTimes(2);

      advance(29_999);
      expect(service.getClusterCapacity).toHaveBeenCalledTimes(2);

      advance(1);
      expect(service.getClusterCapacity).toHaveBeenCalledTimes(3);
    });

    it("ignores a refresh click while a fetch is still in flight", () => {
      const inFlight = new Subject<ClusterCapacity>();
      vi.mocked(service.getClusterCapacity).mockReturnValueOnce(of(capacity())).mockReturnValueOnce(inFlight);

      render();
      const button = fixture.nativeElement.querySelector(".refresh-button");
      button.click();
      button.click();

      expect(service.getClusterCapacity).toHaveBeenCalledTimes(2);
    });
  });
});
