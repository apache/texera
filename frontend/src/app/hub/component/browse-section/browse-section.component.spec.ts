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
import { RouterTestingModule } from "@angular/router/testing";
import { By } from "@angular/platform-browser";
import { of } from "rxjs";
import { UserService } from "src/app/common/service/user/user.service";
import { StubUserService } from "src/app/common/service/user/stub-user.service";
import { BrowseSectionComponent } from "./browse-section.component";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_WORKSPACE,
} from "../../../app-routing.constant";

/** What the dataset service's cover-url endpoint hands back in these specs. */
const PRESIGNED_COVER = "https://s3.example/cover.png?sig=abc";

describe("BrowseSectionComponent", () => {
  let component: BrowseSectionComponent;
  let fixture: ComponentFixture<BrowseSectionComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [BrowseSectionComponent],
      providers: [
        { provide: WorkflowPersistService, useValue: {} },
        // The cover now comes from the descriptor, so the double has to answer for it.
        { provide: DatasetService, useValue: { getDatasetCoverUrl: () => of({ url: PRESIGNED_COVER }) } },
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
  });

  describe("initializeEntry edge cases", () => {
    it("skips entries whose id is not a number", () => {
      component.entities = [{ id: undefined, type: "dataset", accessibleUserIds: [] } as unknown as DashboardEntry];
      component.ngOnInit();
      expect(Object.keys(component.entityRoutes)).toHaveLength(0);
    });

    it("throws on an unexpected entity type", () => {
      const bad = { id: 7, type: "not-a-real-type", accessibleUserIds: [] } as unknown as DashboardEntry;
      expect(() => (component as any).initializeEntry(bad)).toThrowError("Unexpected type in DashboardEntry.");
    });
  });

  describe("cover images", () => {
    it("caches the cover URL the descriptor resolves for an entity that has a cover", () => {
      const entity = {
        id: 5,
        type: "dataset",
        coverImageUrl: "has-cover",
        accessibleUserIds: [],
      } as unknown as DashboardEntry;
      component.entities = [entity];
      component.ngOnInit();

      expect(component.getCoverImage(entity)).toBe(PRESIGNED_COVER);
    });

    it("falls back to the default background when no cover was cached", () => {
      // No coverImageUrl -> loadCoverImages never asks the descriptor -> getCoverImage defaults.
      const entity = { id: 6, type: "dataset", accessibleUserIds: [] } as unknown as DashboardEntry;
      component.entities = [entity];
      component.ngOnInit();

      expect(component.getCoverImage(entity)).toBe(component.defaultBackground);
    });
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
  });
});
