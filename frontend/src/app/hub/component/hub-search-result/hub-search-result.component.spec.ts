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
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { Router } from "@angular/router";
import { NzIconModule } from "ng-zorro-antd/icon";
import { AppstoreOutline, BarsOutline } from "@ant-design/icons-angular/icons";
import { of, Subject } from "rxjs";
import { vi } from "vitest";

import { HubSearchResultComponent } from "./hub-search-result.component";
import { SearchResultsComponent } from "../../../dashboard/component/user/search-results/search-results.component";
import { FiltersComponent } from "../../../dashboard/component/user/filters/filters.component";
import { CardItemComponent } from "../../../dashboard/component/user/list-item/card-item/card-item.component";
import { SortButtonComponent } from "../../../dashboard/component/user/sort-button/sort-button.component";
import { SortMethod } from "../../../dashboard/type/sort-method";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

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
      build("/dashboard/project");
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

    it("passes projectIds=[pid] into the executeSearch loader when pid is set", async () => {
      build("/dashboard/workflow");
      component.pid = 42;
      const filters = makeFiltersMock();
      const results = makeSearchResultsMock();
      attachChildren(filters, results);

      await component.search();

      const loader = results.reset.mock.calls[0][0] as (start: number, count: number) => Promise<unknown>;
      await loader(0, 20);

      expect(searchServiceMock.executeSearch).toHaveBeenCalledWith(
        [""],
        expect.objectContaining({ projectIds: [42] }),
        0,
        20,
        "workflow",
        SortMethod.EditTimeDesc,
        false,
        true
      );
    });

    it("does not inject projectIds when pid is undefined", async () => {
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
