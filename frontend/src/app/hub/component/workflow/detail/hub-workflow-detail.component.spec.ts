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

import { Component, Input } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ActivatedRoute, Router } from "@angular/router";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { ArrowLeftOutline, EyeOutline, LikeOutline, UserOutline } from "@ant-design/icons-angular/icons";
import { config, of, throwError } from "rxjs";
import { vi } from "vitest";

import { HubWorkflowDetailComponent, THROTTLE_TIME_MS } from "./hub-workflow-detail.component";
import { ActionType, EntityType, HubService } from "../../../service/hub.service";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService, MOCK_USER } from "../../../../common/service/user/stub-user.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { Role } from "../../../../common/type/user";
import { Workflow } from "../../../../common/type/workflow";
import { HUB_WORKFLOW_RESULT, USER_WORKSPACE } from "../../../../app-routing.constant";
import { MarkdownDescriptionComponent } from "../../../../dashboard/component/user/markdown-description/markdown-description.component";
import { WorkflowEditorComponent } from "../../../../workspace/component/workflow-editor/workflow-editor.component";
import { MiniMapComponent } from "../../../../workspace/component/workflow-editor/mini-map/mini-map.component";
import { commonTestProviders } from "../../../../common/testing/test-utils";

@Component({ selector: "texera-markdown-description", standalone: true, template: "" })
class StubMarkdownDescriptionComponent {
  @Input() description?: string;
  @Input() enableViewMore?: boolean;
}

@Component({ selector: "texera-workflow-editor", standalone: true, template: "" })
class StubWorkflowEditorComponent {}

@Component({ selector: "texera-mini-map", standalone: true, template: "" })
class StubMiniMapComponent {}

/**
 * Capture the error reported by RxJS when a subscribe error-handler throws.
 * loadWorkflowWithId throws `Failed to load workflow with id ...` from its error
 * handler, which RxJS surfaces asynchronously via config.onUnhandledError; fake
 * timers flush that report deterministically so the message can be asserted.
 */
function captureUnhandledRxjsError(run: () => void): Error | undefined {
  const previousHandler = config.onUnhandledError;
  let captured: Error | undefined;
  config.onUnhandledError = (err: unknown) => {
    captured = err as Error;
  };
  vi.useFakeTimers();
  try {
    run();
    vi.runOnlyPendingTimers();
  } finally {
    vi.useRealTimers();
    config.onUnhandledError = previousHandler;
  }
  return captured;
}

describe("HubWorkflowDetailComponent", () => {
  let fixture: ComponentFixture<HubWorkflowDetailComponent>;
  let component: HubWorkflowDetailComponent;

  let hubServiceMock: any;
  let workflowPersistServiceMock: any;
  let workflowActionServiceMock: any;
  let notificationServiceMock: any;
  let routerMock: any;
  let stubGraph: { triggerCenterEvent: ReturnType<typeof vi.fn> };

  function makeMocks() {
    stubGraph = { triggerCenterEvent: vi.fn() };

    hubServiceMock = {
      getCounts: vi.fn().mockReturnValue(of([{ entityId: 1, entityType: EntityType.Workflow, counts: {} }])),
      postView: vi.fn().mockReturnValue(of(7)),
      isLiked: vi.fn().mockReturnValue(of([])),
      postLike: vi.fn().mockReturnValue(of(true)),
      postUnlike: vi.fn().mockReturnValue(of(true)),
      cloneWorkflow: vi.fn().mockReturnValue(of(99)),
    };

    workflowPersistServiceMock = {
      retrieveWorkflow: vi.fn().mockReturnValue(of({} as Workflow)),
      retrievePublicWorkflow: vi.fn().mockReturnValue(of({} as Workflow)),
      getOwnerName: vi.fn().mockReturnValue(of("owner")),
      getWorkflowName: vi.fn().mockReturnValue(of("name")),
      getWorkflowDescription: vi.fn().mockReturnValue(of("desc")),
    };

    workflowActionServiceMock = {
      disableWorkflowModification: vi.fn(),
      reloadWorkflow: vi.fn(),
      clearWorkflow: vi.fn(),
      getTexeraGraph: vi.fn().mockReturnValue(stubGraph),
    };

    notificationServiceMock = { success: vi.fn(), error: vi.fn(), info: vi.fn() };

    routerMock = {
      navigateByUrl: vi.fn().mockResolvedValue(true),
      navigate: vi.fn().mockResolvedValue(true),
    };
  }

  function configure(opts: { modalData?: { wid: number } | undefined; routeId?: string; userOverride?: any }) {
    TestBed.overrideComponent(HubWorkflowDetailComponent, {
      remove: { imports: [WorkflowEditorComponent, MiniMapComponent, MarkdownDescriptionComponent] },
      add: { imports: [StubWorkflowEditorComponent, StubMiniMapComponent, StubMarkdownDescriptionComponent] },
    });

    TestBed.configureTestingModule({
      imports: [
        HubWorkflowDetailComponent,
        NzIconModule.forChild([ArrowLeftOutline, EyeOutline, LikeOutline, UserOutline]),
      ],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: opts.modalData },
        {
          provide: ActivatedRoute,
          useValue: { snapshot: { params: opts.routeId !== undefined ? { id: opts.routeId } : {} } },
        },
        { provide: Router, useValue: routerMock },
        { provide: HubService, useValue: hubServiceMock },
        { provide: WorkflowPersistService, useValue: workflowPersistServiceMock },
        { provide: WorkflowActionService, useValue: workflowActionServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    });

    if ("userOverride" in opts) {
      (TestBed.inject(UserService) as unknown as StubUserService).user = opts.userOverride;
    }
  }

  function build(opts: {
    modalData?: { wid: number } | undefined;
    routeId?: string;
    userOverride?: any;
    detectChanges?: boolean;
  }) {
    configure(opts);
    fixture = TestBed.createComponent(HubWorkflowDetailComponent);
    component = fixture.componentInstance;
    if (opts.detectChanges ?? true) {
      fixture.detectChanges();
    }
  }

  beforeEach(() => {
    makeMocks();
  });

  describe("constructor / wid resolution", () => {
    it("uses NZ_MODAL_DATA wid and leaves isHub false", () => {
      build({ modalData: { wid: 42 }, routeId: "11" });
      expect(component.wid).toBe(42);
      expect(component.isHub).toBe(false);
    });

    it("falls back to route.snapshot.params.id and sets isHub true", () => {
      build({ modalData: undefined, routeId: "11" });
      expect(component.wid).toBe(11);
      expect(component.isHub).toBe(true);
    });

    it("sets isActivatedUser true for REGULAR", () => {
      build({ modalData: { wid: 1 } });
      expect(component.isActivatedUser).toBe(true);
    });

    it("sets isActivatedUser true for ADMIN", () => {
      build({
        modalData: { wid: 1 },
        userOverride: { ...MOCK_USER, role: Role.ADMIN },
      });
      expect(component.isActivatedUser).toBe(true);
    });

    it("leaves isActivatedUser false for non-activated roles", () => {
      build({
        modalData: { wid: 1 },
        userOverride: { ...MOCK_USER, role: Role.INACTIVE },
      });
      expect(component.isActivatedUser).toBe(false);
    });

    it("disables workflow modification", () => {
      build({ modalData: { wid: 1 } });
      expect(workflowActionServiceMock.disableWorkflowModification).toHaveBeenCalledTimes(1);
    });
  });

  describe("ngOnInit", () => {
    it("early-returns when wid is undefined", () => {
      build({ modalData: undefined, routeId: undefined, detectChanges: false });
      expect(component.wid).toBeUndefined();
      component.ngOnInit();
      expect(hubServiceMock.getCounts).not.toHaveBeenCalled();
      expect(hubServiceMock.postView).not.toHaveBeenCalled();
      expect(hubServiceMock.isLiked).not.toHaveBeenCalled();
    });

    it("assigns likeCount and cloneCount from getCounts", () => {
      hubServiceMock.getCounts.mockReturnValue(
        of([{ entityId: 1, entityType: EntityType.Workflow, counts: { like: 5, clone: 3 } }])
      );
      build({ modalData: { wid: 1 } });
      expect(hubServiceMock.getCounts).toHaveBeenCalledWith(
        [EntityType.Workflow],
        [1],
        [ActionType.Like, ActionType.Clone]
      );
      expect(component.likeCount).toBe(5);
      expect(component.cloneCount).toBe(3);
    });

    it("defaults likeCount and cloneCount to 0 when counts are missing", () => {
      hubServiceMock.getCounts.mockReturnValue(of([{ entityId: 1, entityType: EntityType.Workflow, counts: {} }]));
      build({ modalData: { wid: 1 } });
      expect(component.likeCount).toBe(0);
      expect(component.cloneCount).toBe(0);
    });

    it("pipes postView through throttleTime and assigns viewCount", () => {
      expect(THROTTLE_TIME_MS).toBe(1000);
      hubServiceMock.postView.mockReturnValue(of(12));
      build({ modalData: { wid: 1 } });
      expect(hubServiceMock.postView).toHaveBeenCalledWith(1, MOCK_USER.uid, EntityType.Workflow);
      expect(component.viewCount).toBe(12);
    });

    it("passes 0 as userId to postView when there is no current user", () => {
      build({ modalData: { wid: 1 }, userOverride: undefined });
      expect(hubServiceMock.postView).toHaveBeenCalledWith(1, 0, EntityType.Workflow);
    });

    it("sets isLiked from the isLiked response when a user is logged in", () => {
      hubServiceMock.isLiked.mockReturnValue(of([{ entityId: 1, entityType: EntityType.Workflow, isLiked: true }]));
      build({ modalData: { wid: 1 } });
      expect(hubServiceMock.isLiked).toHaveBeenCalledWith([1], [EntityType.Workflow]);
      expect(component.isLiked).toBe(true);
    });

    it("falls back to isLiked = false when the response is empty", () => {
      hubServiceMock.isLiked.mockReturnValue(of([]));
      build({ modalData: { wid: 1 } });
      expect(component.isLiked).toBe(false);
    });

    it("does not call isLiked when there is no current user", () => {
      build({ modalData: { wid: 1 }, userOverride: undefined });
      expect(hubServiceMock.isLiked).not.toHaveBeenCalled();
    });
  });

  describe("ngAfterViewInit / loadWorkflowWithId", () => {
    it("uses retrieveWorkflow when not in hub mode and triggers center event", () => {
      const wf = {} as Workflow;
      workflowPersistServiceMock.retrieveWorkflow.mockReturnValue(of(wf));
      build({ modalData: { wid: 5 } });
      expect(workflowPersistServiceMock.retrieveWorkflow).toHaveBeenCalledWith(5);
      expect(workflowPersistServiceMock.retrievePublicWorkflow).not.toHaveBeenCalled();
      expect(workflowActionServiceMock.reloadWorkflow).toHaveBeenCalledWith(wf);
      expect(stubGraph.triggerCenterEvent).toHaveBeenCalledTimes(1);
    });

    it("uses retrievePublicWorkflow when in hub mode and triggers center event", () => {
      const wf = {} as Workflow;
      workflowPersistServiceMock.retrievePublicWorkflow.mockReturnValue(of(wf));
      build({ modalData: undefined, routeId: "9" });
      expect(workflowPersistServiceMock.retrievePublicWorkflow).toHaveBeenCalledWith(9);
      expect(workflowPersistServiceMock.retrieveWorkflow).not.toHaveBeenCalled();
      expect(workflowActionServiceMock.reloadWorkflow).toHaveBeenCalledWith(wf);
      expect(stubGraph.triggerCenterEvent).toHaveBeenCalledTimes(1);
    });

    it("reports the load failure and does not reload or trigger center event when retrieveWorkflow errors", () => {
      workflowPersistServiceMock.retrieveWorkflow.mockReturnValue(throwError(() => new Error("boom")));
      const unhandled = captureUnhandledRxjsError(() => build({ modalData: { wid: 5 } }));
      expect(workflowActionServiceMock.reloadWorkflow).not.toHaveBeenCalled();
      expect(stubGraph.triggerCenterEvent).not.toHaveBeenCalled();
      expect(unhandled?.message).toBe("Failed to load workflow with id 5");
    });

    it("reports the load failure and does not reload or trigger center event when retrievePublicWorkflow errors", () => {
      workflowPersistServiceMock.retrievePublicWorkflow.mockReturnValue(throwError(() => new Error("boom")));
      const unhandled = captureUnhandledRxjsError(() => build({ modalData: undefined, routeId: "9" }));
      expect(workflowActionServiceMock.reloadWorkflow).not.toHaveBeenCalled();
      expect(stubGraph.triggerCenterEvent).not.toHaveBeenCalled();
      expect(unhandled?.message).toBe("Failed to load workflow with id 9");
    });

    it("skips loading when wid is undefined", () => {
      build({ modalData: undefined, routeId: undefined, detectChanges: false });
      component.ngAfterViewInit();
      expect(workflowPersistServiceMock.retrieveWorkflow).not.toHaveBeenCalled();
      expect(workflowPersistServiceMock.retrievePublicWorkflow).not.toHaveBeenCalled();
    });
  });

  describe("ngOnDestroy", () => {
    it("calls WorkflowActionService.clearWorkflow", () => {
      build({ modalData: { wid: 1 } });
      component.ngOnDestroy();
      expect(workflowActionServiceMock.clearWorkflow).toHaveBeenCalled();
    });
  });

  describe("goBack", () => {
    it("navigates to HUB_WORKFLOW_RESULT", () => {
      build({ modalData: { wid: 1 } });
      component.goBack();
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(HUB_WORKFLOW_RESULT);
    });

    it("notifies the user on navigation failure", async () => {
      routerMock.navigateByUrl.mockReturnValue(Promise.reject(new Error("nav failed")));
      build({ modalData: { wid: 1 } });
      component.goBack();
      await Promise.resolve();
      await Promise.resolve();
      expect(notificationServiceMock.error).toHaveBeenCalledWith("Go back failed. Please try again.");
    });
  });

  describe("cloneWorkflow", () => {
    it("early-returns when wid is undefined", () => {
      build({ modalData: undefined, routeId: undefined, detectChanges: false });
      component.cloneWorkflow();
      expect(hubServiceMock.cloneWorkflow).not.toHaveBeenCalled();
      expect(routerMock.navigate).not.toHaveBeenCalled();
    });

    it("navigates to the user workspace and shows a success notification", async () => {
      hubServiceMock.cloneWorkflow.mockReturnValue(of(123));
      build({ modalData: { wid: 1 } });
      component.cloneWorkflow();
      expect(hubServiceMock.cloneWorkflow).toHaveBeenCalledWith(1);
      expect(routerMock.navigate).toHaveBeenCalledWith([`${USER_WORKSPACE}/123`]);
      await Promise.resolve();
      await Promise.resolve();
      expect(notificationServiceMock.success).toHaveBeenCalledWith("Clone Successful");
    });
  });

  describe("toggleLike", () => {
    it("short-circuits when there is no current user", () => {
      build({ modalData: { wid: 1 }, userOverride: undefined });
      component.toggleLike();
      expect(hubServiceMock.postLike).not.toHaveBeenCalled();
      expect(hubServiceMock.postUnlike).not.toHaveBeenCalled();
    });

    it("short-circuits when wid is undefined", () => {
      build({ modalData: undefined, routeId: undefined, detectChanges: false });
      component.toggleLike();
      expect(hubServiceMock.postLike).not.toHaveBeenCalled();
      expect(hubServiceMock.postUnlike).not.toHaveBeenCalled();
    });

    it("calls postLike when not currently liked, flips isLiked, refreshes likeCount", () => {
      hubServiceMock.getCounts
        .mockReturnValueOnce(of([{ entityId: 1, entityType: EntityType.Workflow, counts: { like: 0, clone: 0 } }]))
        .mockReturnValueOnce(of([{ entityId: 1, entityType: EntityType.Workflow, counts: { like: 9 } }]));
      build({ modalData: { wid: 1 } });
      component.isLiked = false;
      component.toggleLike();
      expect(hubServiceMock.postLike).toHaveBeenCalledWith(1, EntityType.Workflow);
      expect(component.isLiked).toBe(true);
      expect(hubServiceMock.getCounts).toHaveBeenLastCalledWith([EntityType.Workflow], [1], [ActionType.Like]);
      expect(component.likeCount).toBe(9);
    });

    it("calls postUnlike when currently liked, flips isLiked, refreshes likeCount", () => {
      hubServiceMock.getCounts
        .mockReturnValueOnce(of([{ entityId: 1, entityType: EntityType.Workflow, counts: { like: 1, clone: 0 } }]))
        .mockReturnValueOnce(of([{ entityId: 1, entityType: EntityType.Workflow, counts: { like: 0 } }]));
      build({ modalData: { wid: 1 } });
      component.isLiked = true;
      component.toggleLike();
      expect(hubServiceMock.postUnlike).toHaveBeenCalledWith(1, EntityType.Workflow);
      expect(component.isLiked).toBe(false);
      expect(component.likeCount).toBe(0);
    });

    it("does not flip isLiked when postLike returns false", () => {
      hubServiceMock.postLike.mockReturnValue(of(false));
      build({ modalData: { wid: 1 } });
      component.isLiked = false;
      hubServiceMock.getCounts.mockClear();
      component.toggleLike();
      expect(component.isLiked).toBe(false);
      expect(hubServiceMock.getCounts).not.toHaveBeenCalled();
    });
  });

  describe("formatCount", () => {
    it("returns 1.0k for 1000", () => {
      build({ modalData: { wid: 1 } });
      expect(component.formatCount(1000)).toBe("1.0k");
    });

    it("returns the raw string for values below 1000", () => {
      build({ modalData: { wid: 1 } });
      expect(component.formatCount(999)).toBe("999");
      expect(component.formatCount(0)).toBe("0");
    });
  });

  describe("changeViewDisplayStyle", () => {
    it("toggles displayPreciseViewCount", () => {
      build({ modalData: { wid: 1 } });
      expect(component.displayPreciseViewCount).toBe(false);
      component.changeViewDisplayStyle();
      expect(component.displayPreciseViewCount).toBe(true);
      component.changeViewDisplayStyle();
      expect(component.displayPreciseViewCount).toBe(false);
    });
  });
});
