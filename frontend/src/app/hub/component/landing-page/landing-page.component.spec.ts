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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { Router } from "@angular/router";
import { RouterTestingModule } from "@angular/router/testing";
import { of, throwError } from "rxjs";
import { vi } from "vitest";

import { LandingPageComponent } from "./landing-page.component";
import { ActionType, EntityType, HubService } from "../../service/hub.service";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { HOME, HUB_DATASET_RESULT, HUB_WORKFLOW_RESULT } from "../../../app-routing.constant";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("LandingPageComponent", () => {
  let component: LandingPageComponent;
  let fixture: ComponentFixture<LandingPageComponent>;
  let hubServiceStub: {
    getCount: ReturnType<typeof vi.fn>;
    getTops: ReturnType<typeof vi.fn>;
  };
  let searchServiceStub: {
    extendSearchResultsWithHubActivityInfo: ReturnType<typeof vi.fn>;
  };
  let userService: StubUserService;
  let routerNavigateSpy: ReturnType<typeof vi.fn>;

  // Workflow tops are returned for both Like and Clone; dataset tops for Like only.
  // Each call to `extendSearchResultsWithHubActivityInfo` is given a tag so the
  // tests can assert which action bucket each enriched payload landed in.
  const workflowLikeItems = [{ id: "wf-like-item" }] as any;
  const workflowCloneItems = [{ id: "wf-clone-item" }] as any;
  const datasetLikeItems = [{ id: "ds-like-item" }] as any;
  const workflowLikeEnriched = [{ id: "wf-like-enriched" }] as any;
  const workflowCloneEnriched = [{ id: "wf-clone-enriched" }] as any;
  const datasetLikeEnriched = [{ id: "ds-like-enriched" }] as any;

  function configureModule() {
    hubServiceStub = {
      getCount: vi.fn((entityType: EntityType) => {
        if (entityType === EntityType.Workflow) return of(42);
        if (entityType === EntityType.Dataset) return of(7);
        return of(0);
      }),
      getTops: vi.fn((entityType: EntityType, _actions: ActionType[], _uid?: number) => {
        if (entityType === EntityType.Workflow) {
          return of({ [ActionType.Like]: workflowLikeItems, [ActionType.Clone]: workflowCloneItems });
        }
        return of({ [ActionType.Like]: datasetLikeItems });
      }),
    };

    searchServiceStub = {
      extendSearchResultsWithHubActivityInfo: vi.fn((items: any[]) => {
        if (items === workflowLikeItems) return of(workflowLikeEnriched);
        if (items === workflowCloneItems) return of(workflowCloneEnriched);
        if (items === datasetLikeItems) return of(datasetLikeEnriched);
        return of([]);
      }),
    };

    TestBed.configureTestingModule({
      imports: [LandingPageComponent, RouterTestingModule.withRoutes([])],
      providers: [
        { provide: HubService, useValue: hubServiceStub },
        { provide: SearchService, useValue: searchServiceStub },
        { provide: UserService, useClass: StubUserService },
        { provide: WorkflowPersistService, useValue: {} },
        { provide: DatasetService, useValue: {} },
        ...commonTestProviders,
      ],
    });

    userService = TestBed.inject(UserService) as unknown as StubUserService;
    const router = TestBed.inject(Router);
    routerNavigateSpy = vi.fn().mockResolvedValue(true);
    router.navigate = routerNavigateSpy as any;
  }

  function build() {
    fixture = TestBed.createComponent(LandingPageComponent);
    component = fixture.componentInstance;
  }

  beforeEach(() => {
    configureModule();
  });

  it("should create", () => {
    build();
    expect(component).toBeTruthy();
  });

  it("updates isLogin and currentUid when userChanged() emits", () => {
    build();
    // Emit a logged-out state.
    userService.user = undefined;
    userService.userChangeSubject.next(undefined);
    expect(component.isLogin).toBe(false);
    expect(component.currentUid).toBeUndefined();

    // Emit a logged-in state.
    const newUser = { uid: 99, name: "x", email: "x@x", role: "REGULAR" } as any;
    userService.user = newUser;
    userService.userChangeSubject.next(newUser);
    expect(component.isLogin).toBe(true);
    expect(component.currentUid).toBe(99);
  });

  it("ngOnInit invokes getWorkflowCount and loadTops", () => {
    build();
    const countSpy = vi.spyOn(component, "getWorkflowCount");
    const loadSpy = vi.spyOn(component, "loadTops").mockResolvedValue(undefined as any);
    component.ngOnInit();
    expect(countSpy).toHaveBeenCalledTimes(1);
    expect(loadSpy).toHaveBeenCalledTimes(1);
  });

  it("getWorkflowCount populates workflowCount and datasetCount from HubService.getCount", () => {
    build();
    component.getWorkflowCount();
    expect(hubServiceStub.getCount).toHaveBeenCalledWith(EntityType.Workflow);
    expect(hubServiceStub.getCount).toHaveBeenCalledWith(EntityType.Dataset);
    expect(component.workflowCount).toBe(42);
    expect(component.datasetCount).toBe(7);
  });

  it("loadTops resolves workflow Like/Clone and dataset Like buckets", async () => {
    build();
    await component.loadTops();

    expect(hubServiceStub.getTops).toHaveBeenCalledWith(
      EntityType.Workflow,
      [ActionType.Like, ActionType.Clone],
      component.currentUid
    );
    expect(hubServiceStub.getTops).toHaveBeenCalledWith(EntityType.Dataset, [ActionType.Like], component.currentUid);

    expect(component.topLovedWorkflows).toBe(workflowLikeEnriched);
    expect(component.topClonedWorkflows).toBe(workflowCloneEnriched);
    expect(component.topLovedDatasets).toBe(datasetLikeEnriched);
  });

  it("loadTops swallows errors and logs them via console.error", async () => {
    hubServiceStub.getTops.mockReturnValueOnce(throwError(() => new Error("boom")));
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    build();
    await component.loadTops();
    expect(errorSpy).toHaveBeenCalledWith("Failed to load top entries:", expect.any(Error));
    // Arrays remain at their initial empty state.
    expect(component.topLovedWorkflows).toEqual([]);
    expect(component.topClonedWorkflows).toEqual([]);
    expect(component.topLovedDatasets).toEqual([]);
    errorSpy.mockRestore();
  });

  it("getTopLovedEntries extends each action's items with SearchService and returns a map keyed by action", async () => {
    build();
    const result = await component.getTopLovedEntries(EntityType.Workflow, [ActionType.Like, ActionType.Clone]);

    expect(searchServiceStub.extendSearchResultsWithHubActivityInfo).toHaveBeenCalledWith(workflowLikeItems, true, [
      "access",
    ]);
    expect(searchServiceStub.extendSearchResultsWithHubActivityInfo).toHaveBeenCalledWith(workflowCloneItems, true, [
      "access",
    ]);
    expect(result[ActionType.Like]).toBe(workflowLikeEnriched);
    expect(result[ActionType.Clone]).toBe(workflowCloneEnriched);
  });

  it("navigateToSearch routes to the workflow hub result for 'workflow'", () => {
    build();
    component.navigateToSearch("workflow");
    expect(routerNavigateSpy).toHaveBeenCalledWith([HUB_WORKFLOW_RESULT]);
  });

  it("navigateToSearch routes to the dataset hub result for 'dataset'", () => {
    build();
    component.navigateToSearch("dataset");
    expect(routerNavigateSpy).toHaveBeenCalledWith([HUB_DATASET_RESULT]);
  });

  it("navigateToSearch routes to the dashboard home for an unknown type", () => {
    build();
    component.navigateToSearch("something-else");
    expect(routerNavigateSpy).toHaveBeenCalledWith([HOME]);
  });
});
