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

import { ComponentFixture, TestBed, discardPeriodicTasks, fakeAsync, tick } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { UserComputingUnitComponent } from "./user-computing-unit.component";
import { ComputingUnitCreateModalComponent } from "../../../../common/component/computing-unit-create-modal/computing-unit-create-modal.component";
import { DashboardWorkflowComputingUnit } from "../../../../common/type/workflow-computing-unit";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzModalService } from "ng-zorro-antd/modal";
import { FileAddOutline } from "@ant-design/icons-angular/icons";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { ComputingUnitActionsService } from "../../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { of } from "rxjs";
import type { Mocked } from "vitest";
import { CdkVirtualScrollViewport } from "@angular/cdk/scrolling";
import { UserComputingUnitListItemComponent } from "./user-computing-unit-list-item/user-computing-unit-list-item.component";
describe("UserComputingUnitComponent", () => {
  let component: UserComputingUnitComponent;
  let fixture: ComponentFixture<UserComputingUnitComponent>;
  let mockComputingUnitService: Mocked<WorkflowComputingUnitManagingService>;

  beforeEach(async () => {
    mockComputingUnitService = {
      getComputingUnitTypes: vi.fn(),
      getComputingUnitLimitOptions: vi.fn(),
      createKubernetesBasedComputingUnit: vi.fn(),
      createLocalComputingUnit: vi.fn(),
    } as unknown as Mocked<WorkflowComputingUnitManagingService>;
    mockComputingUnitService.getComputingUnitTypes.mockReturnValue(of({ typeOptions: [] }));
    mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
      of({ cpuLimitOptions: [], memoryLimitOptions: [], gpuLimitOptions: [] })
    );

    await TestBed.configureTestingModule({
      providers: [
        NzModalService,
        { provide: UserService, useClass: StubUserService },
        { provide: WorkflowComputingUnitManagingService, useValue: mockComputingUnitService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
      imports: [
        UserComputingUnitComponent,
        HttpClientTestingModule,
        NzCardModule,
        NzIconModule.forChild([FileAddOutline]),
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(UserComputingUnitComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it("refreshes the computing unit list when the modal emits unitCreated", () => {
    fixture.detectChanges();
    const statusService = TestBed.inject(ComputingUnitStatusService);
    const refreshSpy = vi.spyOn(statusService, "refreshComputingUnitList").mockImplementation(() => {});
    const modal = fixture.debugElement.query(By.directive(ComputingUnitCreateModalComponent)).componentInstance;
    modal.unitCreated.emit({} as unknown as DashboardWorkflowComputingUnit);
    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it("syncs visibility when the embedded modal closes itself", () => {
    fixture.detectChanges();
    component.showAddComputeUnitModalVisible();
    expect(component.addComputeUnitModalVisible).toBe(true);
    const modal = fixture.debugElement.query(By.directive(ComputingUnitCreateModalComponent)).componentInstance;
    modal.visibleChange.emit(false);
    expect(component.addComputeUnitModalVisible).toBe(false);
  });

  describe("session, polling and termination", () => {
    function makeUnit(cuid: number): DashboardWorkflowComputingUnit {
      return {
        computingUnit: {
          cuid,
          uid: 1,
          name: `unit-${cuid}`,
          creationTime: 0,
          terminateTime: undefined,
          type: "kubernetes",
          uri: `uri-${cuid}`,
          resource: {
            cpuLimit: "1",
            memoryLimit: "1Gi",
            gpuLimit: "0",
            jvmMemorySize: "1Gi",
            shmSize: "64Mi",
            nodeAddresses: [],
          },
        },
        status: "Running",
        metrics: { cpuUsage: "N/A", memoryUsage: "N/A" },
        isOwner: true,
        accessPrivilege: "WRITE",
        ownerAvatar: "",
        ownerName: "owner",
      } as DashboardWorkflowComputingUnit;
    }

    afterEach(() => {
      // ngOnInit starts a 1s interval; destroying the fixture unsubscribes it so it
      // cannot tick into a later test.
      fixture.destroy();
    });

    it("follows the signed-in user when the session changes", () => {
      fixture.detectChanges();
      const stubUserService = TestBed.inject(UserService) as unknown as StubUserService;

      stubUserService.user = undefined;
      stubUserService.userChangeSubject.next(undefined);

      expect(component.isLogin).toBe(false);
      expect(component.currentUid).toBeUndefined();
    });

    it("maps the fetched computing units into dashboard entries", () => {
      const statusService = TestBed.inject(ComputingUnitStatusService);
      vi.spyOn(statusService, "getAllComputingUnits").mockReturnValue(of([makeUnit(7)]));

      fixture.detectChanges();

      expect(component.allComputingUnits.map(u => u.computingUnit.cuid)).toEqual([7]);
      expect(component.entries.map(e => e.id)).toEqual([7]);
    });

    it("refreshes the list on every poll tick, and stops once destroyed", fakeAsync(() => {
      const statusService = TestBed.inject(ComputingUnitStatusService);
      const refreshSpy = vi.spyOn(statusService, "refreshComputingUnitList").mockImplementation(() => {});

      // ngOnInit directly rather than through detectChanges: the fixture's NgZone was
      // created outside this fakeAsync zone, so a poll scheduled from there would land
      // on the real timer queue and tick() could not drive it.
      component.ngOnInit();
      expect(refreshSpy).not.toHaveBeenCalled();

      tick(1000);
      expect(refreshSpy).toHaveBeenCalledTimes(1);
      tick(1000);
      expect(refreshSpy).toHaveBeenCalledTimes(2);

      fixture.destroy();
      tick(2000);
      expect(refreshSpy).toHaveBeenCalledTimes(2);

      // The interval is unbounded, so drop it before leaving the fakeAsync zone.
      discardPeriodicTasks();
    }));

    it("hands a known computing unit to the actions service", () => {
      const statusService = TestBed.inject(ComputingUnitStatusService);
      vi.spyOn(statusService, "getAllComputingUnits").mockReturnValue(of([makeUnit(7)]));
      const actions = TestBed.inject(ComputingUnitActionsService);
      const terminateSpy = vi.spyOn(actions, "confirmAndTerminate").mockImplementation(() => {});
      fixture.detectChanges();

      component.terminateComputingUnit(7);

      expect(terminateSpy).toHaveBeenCalledWith(7, component.allComputingUnits[0]);
    });

    it("reports an error instead of terminating an unknown computing unit", () => {
      const actions = TestBed.inject(ComputingUnitActionsService);
      const terminateSpy = vi.spyOn(actions, "confirmAndTerminate").mockImplementation(() => {});
      const notificationService = TestBed.inject(NotificationService);
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      fixture.detectChanges();

      component.terminateComputingUnit(404);

      expect(errorSpy).toHaveBeenCalledWith("Invalid computing unit.");
      expect(terminateSpy).not.toHaveBeenCalled();
    });
  });

  /**
   * Everything above reaches the component through its methods, so the template's own wiring — the
   * toolbar button and the row bindings inside the virtual-scroll list — never ran. jsdom performs
   * no layout, which is why the list looked empty: the viewport measures 0px and renders no rows.
   */
  describe("rendered page", () => {
    function makeUnit(cuid: number): DashboardWorkflowComputingUnit {
      return {
        computingUnit: {
          cuid,
          uid: 1,
          name: `unit-${cuid}`,
          creationTime: 0,
          terminateTime: undefined,
          type: "kubernetes",
          uri: `uri-${cuid}`,
          resource: {
            cpuLimit: "1",
            memoryLimit: "1Gi",
            gpuLimit: "0",
            jvmMemorySize: "1Gi",
            shmSize: "64Mi",
            nodeAddresses: [],
          },
        },
        status: "Running",
        metrics: { cpuUsage: "N/A", memoryUsage: "N/A" },
        isOwner: true,
        accessPrivilege: "WRITE",
        ownerAvatar: "",
        ownerName: "owner",
      } as DashboardWorkflowComputingUnit;
    }

    /** Renders the page with the given units and forces the virtual list to materialize its rows. */
    function renderUnits(units: DashboardWorkflowComputingUnit[]): void {
      const statusService = TestBed.inject(ComputingUnitStatusService);
      vi.spyOn(statusService, "getAllComputingUnits").mockReturnValue(of(units));
      fixture.detectChanges();

      const viewport = fixture.debugElement.query(By.css("cdk-virtual-scroll-viewport"))
        .componentInstance as CdkVirtualScrollViewport;
      viewport.setRenderedRange({ start: 0, end: units.length });
      fixture.detectChanges();
    }

    afterEach(() => {
      // ngOnInit starts a 1s poll; destroying the fixture unsubscribes it.
      fixture.destroy();
    });

    it("opens the create-unit modal from its toolbar button", () => {
      fixture.detectChanges();
      const modal = fixture.debugElement.query(By.directive(ComputingUnitCreateModalComponent))
        .componentInstance as ComputingUnitCreateModalComponent;
      expect(modal.visible).toBe(false);

      fixture.nativeElement.querySelector(".create-btn").click();
      fixture.detectChanges();

      expect(modal.visible).toBe(true);
    });

    it("renders one row per computing unit, in the order the service reported them", () => {
      renderUnits([makeUnit(7), makeUnit(9)]);

      const rendered = Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(".unit-id")).map(
        el => el.textContent?.trim()
      );
      expect(rendered).toEqual(["#7", "#9"]);
    });

    it("terminates the unit belonging to the row that asked, not the first one", () => {
      const actions = TestBed.inject(ComputingUnitActionsService);
      const terminateSpy = vi.spyOn(actions, "confirmAndTerminate").mockImplementation(() => {});
      renderUnits([makeUnit(7), makeUnit(9)]);

      const rows = fixture.debugElement.queryAll(By.directive(UserComputingUnitListItemComponent));
      expect(rows.length).toBe(2);
      // the second row's delete must reach unit 9; two rows are rendered so a binding that always
      // resolved to the first entry could not pass.
      (rows[1].componentInstance as UserComputingUnitListItemComponent).deleted.emit();

      expect(terminateSpy).toHaveBeenCalledTimes(1);
      const [cuid, unit] = terminateSpy.mock.calls[0];
      expect(cuid).toBe(9);
      expect(unit.computingUnit.cuid).toBe(9);
    });
  });
});
