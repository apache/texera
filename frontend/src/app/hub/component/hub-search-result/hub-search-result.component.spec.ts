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

import { Component, EventEmitter, forwardRef, Input, Output, TemplateRef } from "@angular/core";
import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";
import { provideRouter, Router } from "@angular/router";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { By } from "@angular/platform-browser";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzModalService } from "ng-zorro-antd/modal";
import { en_US, provideNzI18n } from "ng-zorro-antd/i18n";
import { AppstoreOutline, BarsOutline } from "@ant-design/icons-angular/icons";
import { of, Subject } from "rxjs";
import { vi } from "vitest";

import { HubSearchResultComponent } from "./hub-search-result.component";
import { SearchResultsComponent } from "../../../dashboard/component/user/search-results/search-results.component";
import { FiltersComponent } from "../../../dashboard/component/user/filters/filters.component";
import { CardItemComponent } from "../../../dashboard/component/user/list-item/card-item/card-item.component";
import { SortButtonComponent } from "../../../dashboard/component/user/sort-button/sort-button.component";
import { SortMethod } from "../../../dashboard/type/sort-method";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import { UserService } from "../../../common/service/user/user.service";
import { MOCK_USER_ID, StubUserService } from "../../../common/service/user/stub-user.service";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../../workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../workspace/service/operator-metadata/stub-operator-metadata.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "../../../common/service/workflow-persist/stub-workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { WorkflowCoverService } from "../../../dashboard/service/user/workflow-cover/workflow-cover.service";

const VIEW_MODE_STORAGE_KEY = "texera.hub.dataset.viewMode";

/**
 * Minimal same-selector stubs that replace the heavy real children in the
 * component's own `imports`. The filters stub additionally provides the
 * `FiltersComponent` token so the `@ViewChild(FiltersComponent)` query still
 * resolves to it (the real setter dereferences `masterFilterListChange`, so
 * resolving to `undefined` would throw during change detection).
 */
@Component({
  selector: "texera-sort-button",
  standalone: true,
  template: "",
})
class StubSortButtonComponent {
  @Input() showEditTime?: boolean;
  @Input() showExecutionTime?: boolean;
  @Output() sortMethodChange = new EventEmitter<SortMethod>();
}

@Component({
  selector: "texera-filters",
  standalone: true,
  template: "",
  providers: [{ provide: FiltersComponent, useExisting: forwardRef(() => StubFiltersComponent) }],
})
class StubFiltersComponent {
  masterFilterList: ReadonlyArray<string> = [];
  masterFilterListChange = new Subject<ReadonlyArray<string>>();
  getSearchKeywords = vi.fn(() => [] as string[]);
  getSearchFilterParameters = vi.fn(() => ({}));
}

@Component({
  selector: "texera-search-results",
  standalone: true,
  template: "",
})
class StubSearchResultsComponent {
  @Input() showResourceTypes?: boolean;
  @Input() searchKeywords?: string[];
  @Input() currentUid?: number;
  @Input() viewMode?: string;
  @Input() cardTemplate?: TemplateRef<unknown>;
}

@Component({
  selector: "texera-card-item",
  standalone: true,
  template: "",
})
class StubCardItemComponent {
  @Input() entry?: unknown;
  @Input() currentUid?: number;
}

interface FiltersMock {
  masterFilterList: ReadonlyArray<string>;
  getSearchKeywords: ReturnType<typeof vi.fn>;
  getSearchFilterParameters: ReturnType<typeof vi.fn>;
  masterFilterListChange: Subject<ReadonlyArray<string>>;
}

interface SearchResultsMock {
  reset: ReturnType<typeof vi.fn>;
  loadMore: ReturnType<typeof vi.fn>;
}

describe("HubSearchResultComponent", () => {
  let fixture: ComponentFixture<HubSearchResultComponent>;
  let component: HubSearchResultComponent;

  let routerMock: { url: string };
  let searchServiceMock: { executeSearch: ReturnType<typeof vi.fn> };
  let setItemSpy: ReturnType<typeof vi.spyOn>;

  function configure(url: string): void {
    routerMock = { url };
    searchServiceMock = {
      executeSearch: vi.fn().mockReturnValue(of({ entries: [], more: false })),
    };

    TestBed.overrideComponent(HubSearchResultComponent, {
      remove: {
        imports: [SortButtonComponent, FiltersComponent, SearchResultsComponent, CardItemComponent],
      },
      add: {
        imports: [StubSortButtonComponent, StubFiltersComponent, StubSearchResultsComponent, StubCardItemComponent],
      },
    });

    TestBed.configureTestingModule({
      imports: [HubSearchResultComponent, NzIconModule.forChild([BarsOutline, AppstoreOutline])],
      providers: [
        { provide: Router, useValue: routerMock },
        { provide: SearchService, useValue: searchServiceMock },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    });
  }

  function build(url: string, detectChanges: boolean = true): void {
    configure(url);
    fixture = TestBed.createComponent(HubSearchResultComponent);
    component = fixture.componentInstance;
    if (detectChanges) {
      fixture.detectChanges();
    }
  }

  function makeFiltersMock(list: ReadonlyArray<string> = ["a"]): FiltersMock {
    return {
      masterFilterList: list,
      getSearchKeywords: vi.fn(() => ["k"]),
      getSearchFilterParameters: vi.fn(() => ({})),
      masterFilterListChange: new Subject<ReadonlyArray<string>>(),
    };
  }

  function makeSearchResultsMock(): SearchResultsMock {
    return {
      reset: vi.fn(),
      loadMore: vi.fn().mockResolvedValue(undefined),
    };
  }

  /** Directly assign the ViewChild backing fields with test doubles. */
  function attachChildren(filters: FiltersMock | undefined, results: SearchResultsMock | undefined): void {
    component["_filters"] = filters as unknown as FiltersComponent;
    component["_searchResultsComponent"] = results as unknown as SearchResultsComponent;
  }

  beforeEach(() => {
    localStorage.clear();
    setItemSpy = vi.spyOn(Storage.prototype, "setItem");
  });

  afterEach(() => {
    fixture?.destroy();
    vi.restoreAllMocks();
    localStorage.clear();
    document.querySelectorAll(".cdk-overlay-container").forEach(el => el.remove());
  });

  describe("ngOnInit / searchType resolution", () => {
    it("resolves 'dataset' and defaults sortMethod to CreateTimeDesc when the url contains 'dataset'", () => {
      build("/dashboard/dataset");
      expect(component.searchType).toBe("dataset");
      expect(component.sortMethod).toBe(SortMethod.CreateTimeDesc);
    });

    it("resolves 'workflow' and keeps the default EditTimeDesc when the url contains 'workflow'", () => {
      build("/dashboard/workflow");
      expect(component.searchType).toBe("workflow");
      expect(component.sortMethod).toBe(SortMethod.EditTimeDesc);
    });

    it("keeps the default 'workflow' searchType when the url matches neither branch", () => {
      build("/dashboard/other");
      expect(component.searchType).toBe("workflow");
      expect(component.sortMethod).toBe(SortMethod.EditTimeDesc);
    });
  });

  describe("viewMode initialization", () => {
    it("reads 'card' from localStorage", () => {
      localStorage.setItem(VIEW_MODE_STORAGE_KEY, "card");
      build("/dashboard/dataset");
      expect(component.viewMode).toBe("card");
    });

    it("falls back to 'list' when localStorage holds a non-'card' value", () => {
      localStorage.setItem(VIEW_MODE_STORAGE_KEY, "grid");
      build("/dashboard/dataset");
      expect(component.viewMode).toBe("list");
    });

    it("falls back to 'list' when localStorage is empty", () => {
      build("/dashboard/dataset");
      expect(component.viewMode).toBe("list");
    });
  });

  describe("setViewMode", () => {
    it("is a no-op (no localStorage write) when the mode is unchanged", () => {
      build("/dashboard/dataset");
      expect(component.viewMode).toBe("list");
      setItemSpy.mockClear();

      component.setViewMode("list");

      expect(component.viewMode).toBe("list");
      expect(setItemSpy).not.toHaveBeenCalled();
    });

    it("updates viewMode and persists to localStorage when the mode changes", () => {
      build("/dashboard/dataset");
      setItemSpy.mockClear();

      component.setViewMode("card");

      expect(component.viewMode).toBe("card");
      expect(setItemSpy).toHaveBeenCalledWith(VIEW_MODE_STORAGE_KEY, "card");
      expect(localStorage.getItem(VIEW_MODE_STORAGE_KEY)).toBe("card");
    });
  });

  describe("template rendering", () => {
    it("renders the dataset view-toggle when searchType is 'dataset'", () => {
      build("/dashboard/dataset");
      const host = fixture.nativeElement as HTMLElement;
      expect(host.querySelector(".view-toggle")).not.toBeNull();
      expect(host.querySelector("texera-search-results")).not.toBeNull();
      expect(host.querySelector("texera-filters")).not.toBeNull();
    });

    it("hides the dataset view-toggle when searchType is 'workflow'", () => {
      build("/dashboard/workflow");
      const host = fixture.nativeElement as HTMLElement;
      expect(host.querySelector(".view-toggle")).toBeNull();
    });
  });

  describe("search", () => {
    it("early-returns when filters is unset", async () => {
      build("/dashboard/workflow");
      const results = makeSearchResultsMock();
      attachChildren(undefined, results);

      await component.search();

      expect(results.reset).not.toHaveBeenCalled();
      expect(searchServiceMock.executeSearch).not.toHaveBeenCalled();
    });

    it("early-returns when searchResultsComponent is unset", async () => {
      build("/dashboard/workflow");
      const filters = makeFiltersMock();
      attachChildren(filters, undefined);

      await component.search();

      expect(filters.getSearchKeywords).not.toHaveBeenCalled();
      expect(searchServiceMock.executeSearch).not.toHaveBeenCalled();
    });

    it("resets with a loader and calls loadMore on the first search", async () => {
      build("/dashboard/workflow");
      const filters = makeFiltersMock();
      const results = makeSearchResultsMock();
      attachChildren(filters, results);

      await component.search();

      expect(results.reset).toHaveBeenCalledTimes(1);
      expect(typeof results.reset.mock.calls[0][0]).toBe("function");
      expect(results.loadMore).toHaveBeenCalledTimes(1);
      expect(component.searchKeywords).toEqual(["k"]);
    });

    it("skips a repeated search with the same filter list and sortMethod, but honors forced=true", async () => {
      build("/dashboard/workflow");
      const filters = makeFiltersMock();
      const results = makeSearchResultsMock();
      attachChildren(filters, results);

      await component.search();
      expect(results.reset).toHaveBeenCalledTimes(1);

      // identical masterFilterList + sortMethod -> deduped, no new reset.
      await component.search();
      expect(results.reset).toHaveBeenCalledTimes(1);

      // forced overrides the dedupe guard.
      await component.search(true);
      expect(results.reset).toHaveBeenCalledTimes(2);
    });

    it("re-searches when the sortMethod changes even if the filter list is identical", async () => {
      build("/dashboard/workflow");
      const filters = makeFiltersMock();
      const results = makeSearchResultsMock();
      attachChildren(filters, results);

      await component.search();
      expect(results.reset).toHaveBeenCalledTimes(1);

      component.sortMethod = SortMethod.NameAsc;
      await component.search();
      expect(results.reset).toHaveBeenCalledTimes(2);
    });

    it("forwards the filter parameters into the executeSearch loader", async () => {
      build("/dashboard/workflow");
      const filters = makeFiltersMock();
      const results = makeSearchResultsMock();
      attachChildren(filters, results);

      await component.search();

      const loader = results.reset.mock.calls[0][0] as (start: number, count: number) => Promise<unknown>;
      await loader(0, 20);

      expect(searchServiceMock.executeSearch).toHaveBeenCalledWith(
        [""],
        {},
        0,
        20,
        "workflow",
        SortMethod.EditTimeDesc,
        false,
        true
      );
    });
  });
});

// The suite above stubs the children out, and *any* `overrideComponent` makes
// Angular re-JIT HubSearchResultComponent from its retained decorator metadata;
// the recompiled template loses its source map back to
// hub-search-result.component.html, so every binding still runs but none of it
// is attributed (issue #7458). This suite therefore stands up its own TestBed
// with the REAL children and asserts on the rendered DOM, leaving the stubbed
// tests above untouched.
describe("HubSearchResultComponent rendered template", () => {
  let fixture: ComponentFixture<HubSearchResultComponent>;
  let executeSearch: ReturnType<typeof vi.fn>;
  let entries: DashboardEntry[];

  const host = (): HTMLElement => fixture.nativeElement as HTMLElement;

  const toggleButtons = (): HTMLButtonElement[] =>
    Array.from(host().querySelectorAll<HTMLButtonElement>(".view-toggle button"));

  /**
   * nz-button renders nzType as an `ant-btn-<type>` class, so this reads the [nzType] ternaries
   * back off the DOM. It reports the type NAME rather than a primary/not-primary boolean on
   * purpose: a boolean read pins each ternary's false leg only as "not primary", so changing
   * `'default'` to `'dashed'` or `'link'` would ship green.
   */
  const toggleTypes = (): string[] => {
    // Matched against the nzType names rather than any `ant-btn-*` class, because the buttons also
    // carry modifier classes such as `ant-btn-icon-only`.
    const names = ["primary", "default", "dashed", "link", "text"];
    return toggleButtons().map(button => names.find(name => button.classList.contains(`ant-btn-${name}`)) ?? "none");
  };

  const cardItems = (): CardItemComponent[] =>
    fixture.debugElement.queryAll(By.directive(CardItemComponent)).map(item => item.componentInstance);

  const cardNames = (): string[] =>
    Array.from(host().querySelectorAll(".card-grid texera-card-item .resource-name")).map(name =>
      name.textContent!.trim()
    );

  const results = (): SearchResultsComponent =>
    fixture.debugElement.query(By.directive(SearchResultsComponent)).componentInstance;

  /** The sort options the real sort button offers, read out of the cdk overlay it opens on hover. */
  const sortMenuLabels = (): string[] =>
    Array.from(document.querySelectorAll(".cdk-overlay-container li[nz-menu-item]")).map(item =>
      item.textContent!.trim()
    );

  function openSortMenu(): void {
    host().querySelector("texera-sort-button a")!.dispatchEvent(new MouseEvent("mouseenter"));
    tick(500);
    fixture.detectChanges();
  }

  function makeDatasetEntry(id: number, name: string): DashboardEntry {
    return {
      id,
      name,
      description: "",
      type: "dataset",
      dataset: { isOwner: true },
      accessibleUserIds: [],
      likeCount: 0,
      viewCount: 0,
      isLiked: false,
      size: 0,
    } as unknown as DashboardEntry;
  }

  function render(url: string, storedViewMode?: string): void {
    TestBed.resetTestingModule();
    localStorage.clear();
    if (storedViewMode !== undefined) {
      localStorage.setItem(VIEW_MODE_STORAGE_KEY, storedViewMode);
    }
    executeSearch = vi.fn(() => of({ entries, more: false }));

    TestBed.configureTestingModule({
      imports: [
        HubSearchResultComponent,
        NzIconModule.forChild([BarsOutline, AppstoreOutline]),
        HttpClientTestingModule,
        NoopAnimationsModule,
      ],
      providers: [
        provideRouter([]),
        { provide: SearchService, useValue: { executeSearch } },
        { provide: UserService, useClass: StubUserService },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService([]) },
        { provide: DatasetService, useValue: { getDatasetCoverUrl: vi.fn(() => of({ url: undefined })) } },
        { provide: WorkflowCoverService, useValue: { getCover: vi.fn(() => of(undefined)) } },
        NzModalService,
        provideNzI18n(en_US),
        ...commonTestProviders,
      ],
    });

    // ngOnInit derives searchType from Router.url; an own property shadows the real getter.
    Object.defineProperty(TestBed.inject(Router), "url", { get: () => url });

    fixture = TestBed.createComponent(HubSearchResultComponent);
    fixture.detectChanges();
  }

  /** Entries only reach the DOM through a search, which is how the card template gets instantiated. */
  async function loadEntries(list: DashboardEntry[]): Promise<void> {
    entries = list;
    await fixture.componentInstance.search(true);
    fixture.detectChanges();
  }

  beforeEach(() => {
    entries = [];
  });

  afterEach(() => {
    fixture?.destroy();
    localStorage.clear();
    document.querySelectorAll(".cdk-overlay-container").forEach(el => el.remove());
  });

  it("renders the real children, not the stubbed selectors", () => {
    // If these resolve to empty stub templates the component was re-JITed and the
    // template's coverage has silently gone back to zero.
    render("/dashboard/dataset");

    expect(host().querySelector("texera-sort-button button#sortDropdown")).not.toBeNull();
    expect(host().querySelector("texera-filters button")).not.toBeNull();
    expect(host().querySelector("texera-search-results nz-card")).not.toBeNull();
  });

  it("renders both dataset view-toggle buttons, each with its own label and icon", () => {
    render("/dashboard/dataset");

    expect(toggleButtons().map(button => button.title)).toEqual(["List view", "Card view"]);
    // nz-icon turns nzType into an `anticon-<type>` class, so this pins which icon each button asks for.
    expect(toggleButtons().map(button => button.querySelector("i[nz-icon]")!.className)).toEqual([
      expect.stringContaining("anticon-bars"),
      expect.stringContaining("anticon-appstore"),
    ]);
  });

  it("omits the view toggle entirely when the search type is workflow", () => {
    render("/dashboard/workflow");

    expect(host().querySelector(".view-toggle")).toBeNull();
    expect(toggleButtons()).toEqual([]);
    // The rest of the filter bar is unaffected.
    expect(host().querySelector("texera-sort-button button#sortDropdown")).not.toBeNull();
  });

  it("highlights whichever view-toggle button matches the current view mode", () => {
    render("/dashboard/dataset");
    expect(toggleTypes()).toEqual(["primary", "default"]);

    toggleButtons()[1].click();
    fixture.detectChanges();
    expect(toggleTypes()).toEqual(["default", "primary"]);

    toggleButtons()[0].click();
    fixture.detectChanges();
    expect(toggleTypes()).toEqual(["primary", "default"]);
  });

  it("hides the edit-time and execution-time sort options for datasets", fakeAsync(() => {
    render("/dashboard/dataset");

    openSortMenu();

    expect(sortMenuLabels()).toEqual(["By Create Time", "A -> Z", "Z -> A"]);
  }));

  it("offers the edit-time and execution-time sort options for workflows", fakeAsync(() => {
    render("/dashboard/workflow");

    openSortMenu();

    expect(sortMenuLabels()).toEqual(["By Edit Time", "By Create Time", "By Execution Time", "A -> Z", "Z -> A"]);
  }));

  it("re-runs the search with the sort method the sort button emits", () => {
    render("/dashboard/workflow");
    const sortButton = fixture.debugElement.query(By.directive(SortButtonComponent))
      .componentInstance as SortButtonComponent;

    sortButton.dateSort();

    // Kills both halves of `sortMethod = $event; search()`: drop the assignment and the
    // search runs with the EditTimeDesc default; drop the call and executeSearch is never reached.
    expect(executeSearch).toHaveBeenCalledTimes(1);
    expect(executeSearch.mock.calls[0][5]).toBe(SortMethod.CreateTimeDesc);
  });

  it("renders every dataset entry through the card template in card mode", async () => {
    render("/dashboard/dataset", "card");

    await loadEntries([makeDatasetEntry(7, "alpha"), makeDatasetEntry(8, "beta")]);

    expect(cardNames()).toEqual(["alpha", "beta"]);
    // The like button is disabled while currentUid is undefined, so an enabled one
    // is the card template's [currentUid] binding arriving in the DOM.
    const likeButtons = Array.from(host().querySelectorAll<HTMLButtonElement>(".card-grid .like-btn"));
    expect(likeButtons.map(button => button.disabled)).toEqual([false, false]);
    expect(cardItems().map(item => item.currentUid)).toEqual([MOCK_USER_ID, MOCK_USER_ID]);
  });

  it("keeps the workflow search type on the list view even when card mode is stored", () => {
    render("/dashboard/workflow", "card");
    expect(fixture.componentInstance.viewMode).toBe("card");

    expect(host().querySelector("cdk-virtual-scroll-viewport")).not.toBeNull();
    expect(host().querySelector(".card-scroll-container")).toBeNull();
    // The card template itself is withheld too, which the DOM cannot show while
    // the results list is already pinned to the list view.
    expect(results().cardTemplate).toBeUndefined();
  });

  it("hands the resource types, the filter keywords and the signed-in uid to the results list", () => {
    render("/dashboard/workflow");
    const filters = fixture.debugElement.query(By.directive(FiltersComponent)).componentInstance as FiltersComponent;

    // Committing a filter list is what the real filter bar does on every change, and it
    // is what makes the component republish its keywords.
    filters.masterFilterList = ["alpha"];
    fixture.detectChanges();

    // Asserted on the child inputs rather than the DOM because all three only reach the
    // markup through texera-list-item, which is not rendered while the result list is empty.
    expect(results().showResourceTypes).toBe(true);
    expect(results().searchKeywords).toEqual(["alpha"]);
    expect(results().currentUid).toBe(MOCK_USER_ID);
  });
});
