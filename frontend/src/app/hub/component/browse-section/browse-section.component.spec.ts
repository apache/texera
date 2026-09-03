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
import { BrowseSectionComponent } from "./browse-section.component";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { ChangeDetectorRef } from "@angular/core";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_WORKSPACE,
} from "../../../app-routing.constant";

describe("BrowseSectionComponent", () => {
  let component: BrowseSectionComponent;
  let fixture: ComponentFixture<BrowseSectionComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [BrowseSectionComponent],
      providers: [
        { provide: WorkflowPersistService, useValue: {} },
        { provide: DatasetService, useValue: {} },
        { provide: ChangeDetectorRef, useValue: {} },
        ...commonTestProviders,
      ],
    });
    fixture = TestBed.createComponent(BrowseSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("entityRoutes initialization", () => {
    it("routes owned workflows to the user workspace", () => {
      component.currentUid = 1;
      component.entities = [{ id: 100, type: "workflow", accessibleUserIds: [1] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[100]).toEqual([USER_WORKSPACE, "100"]);
    });

    it("routes non-owned workflows to the hub workflow detail page", () => {
      component.currentUid = 1;
      component.entities = [{ id: 101, type: "workflow", accessibleUserIds: [2] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[101]).toEqual([HUB_WORKFLOW_RESULT_DETAIL, "101"]);
    });

    it("routes owned datasets to the user dataset page", () => {
      component.currentUid = 1;
      component.entities = [{ id: 200, type: "dataset", accessibleUserIds: [1] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[200]).toEqual([USER_DATASET, "200"]);
    });

    it("routes non-owned datasets to the hub dataset detail page", () => {
      component.currentUid = 1;
      component.entities = [{ id: 201, type: "dataset", accessibleUserIds: [2] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(component.entityRoutes[201]).toEqual([HUB_DATASET_RESULT_DETAIL, "201"]);
    });
<<<<<<< HEAD
=======

    it("falls back to the default background when no cover was cached", () => {
      // No coverImageUrl -> loadCoverImages never asks the descriptor -> getCoverImage defaults.
      const entity = { id: 6, type: "dataset", accessibleUserIds: [] } as unknown as DashboardEntry;
      component.entities = [entity];
      component.ngOnInit();

      expect(component.getCoverImage(entity)).toBe(component.defaultBackground);
    });

    // `this.resourceRegistry.find(entity.type)?.coverUrl` carries two guards, and a mixed section
    // can trip either. A workflow's cover is a data URL carried on the entry itself, so
    // WorkflowResourceDescriptor deliberately declares no `coverUrl`; and a kind the registry does
    // not carry at all has no descriptor to ask, which is why this is `find`, not `get` — one such
    // row must not take the whole section's covers down, exactly as `routeFor` five lines up
    // already promises for links.
    it("skips an entity whose descriptor resolves no cover, rather than calling undefined", () => {
      const workflow = {
        id: 10,
        type: "workflow",
        coverImageUrl: "carried-on-the-entry",
        accessibleUserIds: [],
      } as unknown as DashboardEntry;
      const unregistered = {
        id: 12,
        type: "computing-unit",
        coverImageUrl: "carried-on-the-entry",
        accessibleUserIds: [],
      } as unknown as DashboardEntry;
      component.entities = [workflow, unregistered];

      expect(() => component.ngOnInit()).not.toThrow();
      expect(coverCache(component).has("workflow:10")).toBe(false);
      expect(coverCache(component).has("computing-unit:12")).toBe(false);
      // Nothing is cached for a workflow, but its cover is readable straight off the entry.
      expect(component.getCoverImage(workflow)).toBe("carried-on-the-entry");
      expect(component.getCoverImage(unregistered)).toBe(component.defaultBackground);
    });

    it("renders a workflow's cover from the entry, since no cover is ever fetched for one", () => {
      const withCover = {
        id: 20,
        type: "workflow",
        coverImageUrl: "data:image/png;base64,AAAA",
        accessibleUserIds: [],
      } as unknown as DashboardEntry;
      const withoutCover = { id: 21, type: "workflow", accessibleUserIds: [] } as unknown as DashboardEntry;
      component.entities = [withCover, withoutCover];
      component.ngOnInit();

      expect(component.getCoverImage(withCover)).toBe("data:image/png;base64,AAAA");
      expect(component.getCoverImage(withoutCover)).toBe(component.defaultBackground);
    });

    it("keeps a file-backed kind on the placeholder rather than rendering its stored cover path", () => {
      // A dataset's coverImageUrl is a path relative to the dataset root, not something an <img>
      // can load, so it must never stand in for the presigned URL the descriptor resolves.
      vi.spyOn(TestBed.inject(DatasetService) as any, "getDatasetCoverUrl").mockReturnValue(of({ url: "" }));
      const entity = {
        id: 22,
        type: "dataset",
        coverImageUrl: "v1/images/preview.png",
        accessibleUserIds: [],
      } as unknown as DashboardEntry;
      component.entities = [entity];
      component.ngOnInit();

      expect(component.getCoverImage(entity)).toBe(component.defaultBackground);
    });

    it("caches nothing when the descriptor resolves an empty cover url", () => {
      // A presigned-URL endpoint with nothing to sign answers with an empty string; caching that
      // would put an <img src=""> on the card, which the browser resolves to the page itself.
      vi.spyOn(TestBed.inject(DatasetService) as any, "getDatasetCoverUrl").mockReturnValue(of({ url: "" }));
      const entity = {
        id: 11,
        type: "dataset",
        coverImageUrl: "has-cover",
        accessibleUserIds: [],
      } as unknown as DashboardEntry;
      component.entities = [entity];
      component.ngOnInit();

      // White-box on purpose: getCoverImage's `|| defaultBackground` makes "cached an empty string"
      // and "cached nothing" indistinguishable through the public API, so only the map itself can
      // say whether the guard ran.
      expect(coverCache(component).has("dataset:11")).toBe(false);
      expect(component.getCoverImage(entity)).toBe(component.defaultBackground);
    });

    /** The component's cover cache, which no public member exposes. */
    function coverCache(c: BrowseSectionComponent): Map<string, string> {
      return (c as unknown as { coverImageUrls: Map<string, string> }).coverImageUrls;
    }
  });
});
/**
 * The cards themselves are template-only: the specs above assert the route map and the cover-URL
 * cache, but nothing had ever rendered a card, so the per-entity bindings and their fallbacks were
 * unpinned. RouterTestingModule supplies the Router that the cards' routerLink needs.
 */
describe("BrowseSectionComponent rendering", () => {
  let fixture: ComponentFixture<BrowseSectionComponent>;

  beforeEach(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      imports: [BrowseSectionComponent, RouterTestingModule.withRoutes([])],
      providers: [
        // The cards embed texera-user-avatar, which injects UserService; the real one drags in
        // AuthService and its whole dependency chain, so the shared stub stands in for it.
        { provide: UserService, useClass: StubUserService },
        { provide: WorkflowPersistService, useValue: {} },
        // The cover now comes from the descriptor, so the double has to answer for it.
        { provide: DatasetService, useValue: { getDatasetCoverUrl: () => of({ url: PRESIGNED_COVER }) } },
        { provide: ModelService, useValue: { getModelCoverUrl: () => of({ url: PRESIGNED_COVER }) } },
        ...commonTestProviders,
      ],
    });
    fixture = TestBed.createComponent(BrowseSectionComponent);
  });

  /** Renders the section with the given entities. */
  function render(entities: DashboardEntry[], title = "Workflows"): HTMLElement {
    // Set the inputs and let the first change-detection cycle drive ngOnInit, as Angular does at
    // runtime. Calling ngOnInit() by hand as well would run it twice and rebuild the cover-image
    // cache on top of itself, hiding any non-idempotent init.
    fixture.componentRef.setInput("entities", entities);
    fixture.componentRef.setInput("sectionTitle", title);
    fixture.detectChanges();
    return fixture.nativeElement as HTMLElement;
  }

  const entity = (over: Partial<Record<string, unknown>> = {}) =>
    ({ id: 1, type: "dataset", accessibleUserIds: [], name: "flow", ...over }) as unknown as DashboardEntry;

  it("renders nothing at all for an empty section", () => {
    const el = render([]);

    expect(el.querySelector(".results-container")).toBeNull();
  });

  it("renders the section heading and one card per entity", () => {
    const el = render([entity({ id: 1 }), entity({ id: 2 })], "Public Datasets");

    expect(el.querySelector(".results-title")?.textContent?.trim()).toBe("Public Datasets");
    expect(el.querySelectorAll("nz-card")).toHaveLength(2);
  });

  it("shows each entity's name and description", () => {
    const el = render([entity({ name: "sales", description: "quarterly numbers" })]);

    expect(el.querySelector(".card-title")?.textContent?.trim()).toBe("sales");
    expect(el.querySelector(".card-description")?.textContent?.trim()).toBe("quarterly numbers");
  });

  it("substitutes a placeholder for a missing description", () => {
    // Datasets published without a description would otherwise render an empty paragraph and
    // collapse the card's layout.
    const el = render([entity({ description: undefined })]);

    expect(el.querySelector(".card-description")?.textContent?.trim()).toBe("No description available");
  });

  it("uses the cached cover image when the entity has one", () => {
    const el = render([entity({ id: 5, coverImageUrl: "has-cover" })]);

    const img = el.querySelector<HTMLImageElement>(".card-cover-image")!;
    expect(img.getAttribute("src")).toBe(PRESIGNED_COVER);
  });

  it("falls back to the default background when the cover image fails to load", () => {
    // A presigned cover URL can still 404; the inline error handler is the only thing that stops the
    // card from showing a broken image.
    const el = render([entity({ id: 5, coverImageUrl: "has-cover" })]);
    const img = el.querySelector<HTMLImageElement>(".card-cover-image")!;

    img.dispatchEvent(new Event("error"));

    expect(img.src).toContain("card_background.jpg");
  });

  it("labels the avatar with the entity id", () => {
    const el = render([entity({ id: 42 })]);

    expect(el.querySelector("nz-avatar")?.textContent?.trim()).toBe("42");
  });

  it("passes the owner through to the avatar, defaulting to an empty name", () => {
    const withOwner = fixture.debugElement.queryAll(By.css("texera-user-avatar"));
    expect(withOwner).toHaveLength(0);

    render([entity({ ownerName: "ada" }), entity({ id: 2, ownerName: undefined })]);

    const avatars = fixture.debugElement.queryAll(By.css("texera-user-avatar"));
    expect(avatars.map(a => a.componentInstance.userName)).toEqual(["ada", ""]);
>>>>>>> 1facefb18 (fix(frontend): render workflow covers on the hub landing page (#8383))
  });
});
