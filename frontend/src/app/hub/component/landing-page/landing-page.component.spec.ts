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
import { By } from "@angular/platform-browser";
import { Router } from "@angular/router";
import { RouterTestingModule } from "@angular/router/testing";
import { of, throwError } from "rxjs";
import { vi } from "vitest";

import { LandingPageComponent } from "./landing-page.component";
import { BrowseSectionComponent } from "../browse-section/browse-section.component";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import { ActionType, EntityType, HubService } from "../../service/hub.service";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { ModelService } from "../../../dashboard/service/user/model/model.service";
import { HOME, HUB_DATASET_RESULT, HUB_MODEL_RESULT, HUB_WORKFLOW_RESULT } from "../../../app-routing.constant";
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

  // Workflow tops are returned for both Like and Clone; dataset and model tops for Like only.
  // Each call to `extendSearchResultsWithHubActivityInfo` is given a tag so the
  // tests can assert which action bucket each enriched payload landed in.
  const workflowLikeItems = [{ id: "wf-like-item" }] as any;
  const workflowCloneItems = [{ id: "wf-clone-item" }] as any;
  const datasetLikeItems = [{ id: "ds-like-item" }] as any;
  const modelLikeItems = [{ id: "m-like-item" }] as any;
  const workflowLikeEnriched = [{ id: "wf-like-enriched" }] as any;
  const workflowCloneEnriched = [{ id: "wf-clone-enriched" }] as any;
  const datasetLikeEnriched = [{ id: "ds-like-enriched" }] as any;
  const modelLikeEnriched = [{ id: "m-like-enriched" }] as any;

  function configureModule() {
    hubServiceStub = {
      getCount: vi.fn((entityType: EntityType) => {
        if (entityType === EntityType.Workflow) return of(42);
        if (entityType === EntityType.Dataset) return of(7);
        if (entityType === EntityType.Model) return of(3);
        return of(0);
      }),
      getTops: vi.fn((entityType: EntityType, _actions: ActionType[], _uid?: number) => {
        if (entityType === EntityType.Workflow) {
          return of({ [ActionType.Like]: workflowLikeItems, [ActionType.Clone]: workflowCloneItems });
        }
        if (entityType === EntityType.Model) {
          return of({ [ActionType.Like]: modelLikeItems });
        }
        return of({ [ActionType.Like]: datasetLikeItems });
      }),
    };

    searchServiceStub = {
      extendSearchResultsWithHubActivityInfo: vi.fn((items: any[]) => {
        if (items === workflowLikeItems) return of(workflowLikeEnriched);
        if (items === workflowCloneItems) return of(workflowCloneEnriched);
        if (items === datasetLikeItems) return of(datasetLikeEnriched);
        if (items === modelLikeItems) return of(modelLikeEnriched);
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
        { provide: ModelService, useValue: {} },
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

  it("ngOnInit invokes loadCounts and loadTops", () => {
    build();
    const countSpy = vi.spyOn(component, "loadCounts");
    const loadSpy = vi.spyOn(component, "loadTops").mockResolvedValue(undefined as any);
    component.ngOnInit();
    expect(countSpy).toHaveBeenCalledTimes(1);
    expect(loadSpy).toHaveBeenCalledTimes(1);
  });

  it("loadCounts populates the workflow, dataset and model counts from HubService.getCount", () => {
    build();
    component.loadCounts();
    expect(hubServiceStub.getCount).toHaveBeenCalledWith(EntityType.Workflow);
    expect(hubServiceStub.getCount).toHaveBeenCalledWith(EntityType.Dataset);
    expect(hubServiceStub.getCount).toHaveBeenCalledWith(EntityType.Model);
    expect(component.workflowCount).toBe(42);
    expect(component.datasetCount).toBe(7);
    expect(component.modelCount).toBe(3);
  });

  it("loadTops resolves workflow Like/Clone and dataset and model Like buckets", async () => {
    build();
    await component.loadTops();

    expect(hubServiceStub.getTops).toHaveBeenCalledWith(
      EntityType.Workflow,
      [ActionType.Like, ActionType.Clone],
      component.currentUid
    );
    expect(hubServiceStub.getTops).toHaveBeenCalledWith(EntityType.Dataset, [ActionType.Like], component.currentUid);
    expect(hubServiceStub.getTops).toHaveBeenCalledWith(EntityType.Model, [ActionType.Like], component.currentUid);

    expect(component.topLovedWorkflows).toBe(workflowLikeEnriched);
    expect(component.topClonedWorkflows).toBe(workflowCloneEnriched);
    expect(component.topLovedDatasets).toBe(datasetLikeEnriched);
    expect(component.topLovedModels).toBe(modelLikeEnriched);
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
    expect(component.topLovedModels).toEqual([]);
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

  it("navigateToSearch routes to the model hub result for 'model'", () => {
    build();
    component.navigateToSearch("model");
    expect(routerNavigateSpy).toHaveBeenCalledWith([HUB_MODEL_RESULT]);
  });

  it("navigateToSearch routes to the dashboard home for an unknown type", () => {
    build();
    component.navigateToSearch("something-else");
    expect(routerNavigateSpy).toHaveBeenCalledWith([HOME]);
  });

  it("leaves currentUid undefined when there is no signed-in user", () => {
    // The stub seeds a user in its constructor; clear it before the component reads it.
    userService.user = undefined;
    build();
    expect(component.isLogin).toBe(false);
    expect(component.currentUid).toBeUndefined();
  });

  it("getTopLovedEntries falls back to empty buckets when the hub returns no action keys", async () => {
    hubServiceStub.getTops.mockReturnValue(of({}));
    build();

    const result = await component.getTopLovedEntries(EntityType.Workflow, [ActionType.Like, ActionType.Clone]);

    expect(searchServiceStub.extendSearchResultsWithHubActivityInfo).toHaveBeenCalledTimes(2);
    expect(searchServiceStub.extendSearchResultsWithHubActivityInfo).toHaveBeenCalledWith([], true, ["access"]);
    expect(result[ActionType.Like]).toEqual([]);
    expect(result[ActionType.Clone]).toEqual([]);
  });

  it("loadTops falls back to empty lists when the returned maps are missing keys", async () => {
    build();
    vi.spyOn(component, "getTopLovedEntries")
      .mockResolvedValueOnce({} as any)
      .mockResolvedValueOnce({} as any)
      .mockResolvedValueOnce({} as any);

    await component.loadTops();

    expect(component.topLovedWorkflows).toEqual([]);
    expect(component.topClonedWorkflows).toEqual([]);
    expect(component.topLovedDatasets).toEqual([]);
    expect(component.topLovedModels).toEqual([]);
  });

  /**
   * Everything above drives the class directly, and no test in this spec ever called
   * fixture.detectChanges(), so the template only ever ran its create block: the two
   * counts, the two link handlers and the three browse-section inputs were all
   * unexercised. A swapped or deleted binding here is invisible to the tests above.
   */
  describe("rendered template", () => {
    /** The three intro anchors, in template order: workflows, datasets then models. */
    function links() {
      const found = fixture.debugElement.queryAll(By.css("a"));
      expect(found.length).toBe(3);
      return found;
    }

    it("shows the workflow, dataset and model counts, each in its own link", () => {
      build();
      fixture.detectChanges(); // ngOnInit -> loadCounts

      // The three counts differ (42 vs 7 vs 3), so a swapped interpolation cannot pass.
      expect(links()[0].nativeElement.textContent.trim()).toBe("42 workflows");
      expect(links()[1].nativeElement.textContent.trim()).toBe("7 datasets");
      expect(links()[2].nativeElement.textContent.trim()).toBe("3 models");
    });

    it("routes to the workflow, dataset and model hubs from the three links in order", () => {
      build();
      fixture.detectChanges();

      links()[0].triggerEventHandler("click", {});
      expect(routerNavigateSpy).toHaveBeenLastCalledWith([HUB_WORKFLOW_RESULT]);

      links()[1].triggerEventHandler("click", {});
      expect(routerNavigateSpy).toHaveBeenLastCalledWith([HUB_DATASET_RESULT]);

      links()[2].triggerEventHandler("click", {});
      expect(routerNavigateSpy).toHaveBeenLastCalledWith([HUB_MODEL_RESULT]);
    });

    it("hands each browse section its own entity list, title and viewer id", () => {
      build();
      // Three separate array instances, asserted by identity below, so a swapped
      // [entities] binding cannot pass a deep-equality check on empty lists.
      const loved: DashboardEntry[] = [];
      const cloned: DashboardEntry[] = [];
      const lovedDatasets: DashboardEntry[] = [];
      const lovedModels: DashboardEntry[] = [];
      component.topLovedWorkflows = loved;
      component.topClonedWorkflows = cloned;
      component.topLovedDatasets = lovedDatasets;
      component.topLovedModels = lovedModels;

      fixture.detectChanges();

      const sections = fixture.debugElement
        .queryAll(By.directive(BrowseSectionComponent))
        .map(d => d.componentInstance as BrowseSectionComponent);
      expect(sections.length).toBe(4);
      expect(sections.map(s => s.sectionTitle)).toEqual([
        "Top Loved Workflows",
        "Top Cloned Workflows",
        "Top Loved Datasets",
        "Top Loved Models",
      ]);
      expect(sections[0].entities).toBe(loved);
      expect(sections[1].entities).toBe(cloned);
      expect(sections[2].entities).toBe(lovedDatasets);
      expect(sections[3].entities).toBe(lovedModels);
      expect(sections.map(s => s.currentUid)).toEqual([
        component.currentUid,
        component.currentUid,
        component.currentUid,
        component.currentUid,
      ]);
    });
  });
});
