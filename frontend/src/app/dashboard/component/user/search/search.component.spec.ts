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

import { Component, forwardRef, Input } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ActivatedRoute, Params } from "@angular/router";
import { Location } from "@angular/common";
import { EMPTY, of, Subject } from "rxjs";

import { SearchComponent } from "./search.component";
import { FiltersComponent } from "../filters/filters.component";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { SearchService } from "../../../service/user/search.service";
import { UserService } from "../../../../common/service/user/user.service";
import { SortMethod } from "../../../type/sort-method";
import { commonTestProviders } from "../../../../common/testing/test-utils";

// Lightweight stand-in for FiltersComponent. It registers itself under the real
// FiltersComponent token so SearchComponent's `@ViewChild(FiltersComponent)`
// resolves to it, without dragging in FiltersComponent's six service
// dependencies and backend-touching ngOnInit. `getSearchKeywords()` mirrors the
// current filter list so route-driven searches can be asserted end to end.
@Component({
  selector: "texera-filters",
  template: "",
  providers: [{ provide: FiltersComponent, useExisting: forwardRef(() => MockFiltersComponent) }],
})
class MockFiltersComponent {
  masterFilterListChange = EMPTY;
  masterFilterList: ReadonlyArray<string> = [];
  getSearchKeywords = (): string[] => [...this.masterFilterList];
  getSearchFilterParameters = () => ({});
}

@Component({
  selector: "texera-search-results",
  template: "",
})
class MockSearchResultsComponent {
  @Input() showResourceTypes = false;
  @Input() searchKeywords: string[] = [];
  @Input() currentUid?: number;
}

// A plain filters double for the unit tests that drive component methods
// directly (no rendering / no ViewChild).
function makeFiltersDouble(keywords: string[] = []) {
  return {
    masterFilterListChange: EMPTY,
    masterFilterList: [] as ReadonlyArray<string>,
    getSearchKeywords: () => keywords,
    getSearchFilterParameters: () => ({}),
  } as unknown as FiltersComponent;
}

function makeSearchResultsDouble() {
  return {
    reset: vi.fn(),
    loadMore: vi.fn().mockResolvedValue(undefined),
  };
}

describe("SearchComponent", () => {
  let fixture: ComponentFixture<SearchComponent>;
  let component: SearchComponent;
  let queryParams$: Subject<Params>;
  let locationStub: { back: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    queryParams$ = new Subject<Params>();
    locationStub = { back: vi.fn() };
    const userServiceStub = {
      isLogin: () => false,
      getCurrentUser: () => undefined,
      userChanged: () => of(undefined),
    };

    await TestBed.configureTestingModule({
      imports: [SearchComponent],
      providers: [
        { provide: SearchService, useValue: {} },
        { provide: UserService, useValue: userServiceStub },
        { provide: ActivatedRoute, useValue: { queryParams: queryParams$ } },
        { provide: Location, useValue: locationStub },
        ...commonTestProviders,
      ],
    })
      // Swap the heavyweight children for the stubs above; the stub filters still
      // satisfies the FiltersComponent ViewChild query. Additive remove/add form
      // (the `set` form has known bugs with standalone imports).
      .overrideComponent(SearchComponent, {
        remove: { imports: [FiltersComponent, SearchResultsComponent] },
        add: { imports: [MockFiltersComponent, MockSearchResultsComponent] },
      })
      .compileComponents();

    fixture = TestBed.createComponent(SearchComponent);
    component = fixture.componentInstance;
  });

  // ─── initial render (the reported regression) ───────────────────────────────

  it("renders the initial view without accessing the uninitialized filters getter (regression: #6328)", () => {
    // Before the fix the template bound `this.filters.getSearchKeywords()`, so
    // this first change-detection pass threw "Property cannot be accessed
    // before it is initialized" and the results area never mounted (blank page).
    expect(() => fixture.detectChanges()).not.toThrow();
  });

  it("starts with an empty searchKeywords list so the template binding is always safe", () => {
    expect(component.searchKeywords).toEqual([]);
  });

  // ─── filters getter / setter ────────────────────────────────────────────────

  it("throws from the filters getter before the ViewChild has resolved", () => {
    expect(() => component.filters).toThrowError("Property cannot be accessed before it is initialized.");
  });

  it("returns the assigned instance from the filters getter once set", () => {
    const filters = makeFiltersDouble();
    component.filters = filters;
    expect(component.filters).toBe(filters);
  });

  it("re-runs the search whenever the filters emit a masterFilterListChange", () => {
    const change$ = new Subject<void>();
    const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);
    component.filters = { masterFilterListChange: change$ } as unknown as FiltersComponent;

    change$.next();

    expect(searchSpy).toHaveBeenCalledTimes(1);
  });

  // ─── ngAfterViewInit / query params ─────────────────────────────────────────

  it("applies the `q` query param to the filters and populates searchKeywords", () => {
    fixture.detectChanges(); // resolves the filters ViewChild and subscribes to queryParams
    queryParams$.next({ q: "foo bar" });

    expect(component.searchParam).toBe("foo bar");
    expect(component.searchKeywords).toEqual(["foo", "bar"]);
  });

  it("leaves searchParam empty and searchKeywords empty when there is no `q` param", () => {
    fixture.detectChanges();
    queryParams$.next({});

    expect(component.searchParam).toBe("");
    expect(component.searchKeywords).toEqual([]);
  });

  // ─── search() ───────────────────────────────────────────────────────────────

  it("syncs searchKeywords from the filters when a search runs", async () => {
    component.filters = makeFiltersDouble(["alpha", "beta"]);
    component.searchResultsComponent = makeSearchResultsDouble() as unknown as SearchResultsComponent;

    await component.search();

    expect(component.searchKeywords).toEqual(["alpha", "beta"]);
  });

  it("drives the results component (reset + loadMore) on a fresh search", async () => {
    component.filters = makeFiltersDouble(["x"]);
    const results = makeSearchResultsDouble();
    component.searchResultsComponent = results as unknown as SearchResultsComponent;

    await component.search();

    expect(results.reset).toHaveBeenCalledTimes(1);
    expect(results.loadMore).toHaveBeenCalledTimes(1);
  });

  it("skips a duplicate search when the filter list, sort, and type are unchanged", async () => {
    component.filters = makeFiltersDouble();
    const results = makeSearchResultsDouble();
    component.searchResultsComponent = results as unknown as SearchResultsComponent;
    // Make the current state identical to the last executed state.
    component.masterFilterList = [];
    component.lastSortMethod = component.sortMethod;
    component.lastSelectedType = component.selectedType;

    await component.search();

    expect(results.reset).not.toHaveBeenCalled();
  });

  it("throws when the results component is missing", async () => {
    component.filters = makeFiltersDouble(["x"]);
    component.searchResultsComponent = undefined;

    await expect(component.search()).rejects.toThrowError("searchResultsComponent is undefined.");
  });

  // ─── other actions ──────────────────────────────────────────────────────────

  it("sets the selected type and triggers a search on filterByType", () => {
    const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);

    component.filterByType("workflow");

    expect(component.selectedType).toBe("workflow");
    expect(searchSpy).toHaveBeenCalledTimes(1);
  });

  it("navigates back on goBack", () => {
    component.goBack();
    expect(locationStub.back).toHaveBeenCalledTimes(1);
  });

  it("splits searchParam on whitespace into the filters master list", () => {
    const filters = makeFiltersDouble();
    component.filters = filters;
    component.searchParam = "a b\tc";

    component.updateMasterFilterList();

    expect(filters.masterFilterList).toEqual(["a", "b", "c"]);
  });

  it("defaults the sort method to EditTimeDesc", () => {
    expect(component.sortMethod).toBe(SortMethod.EditTimeDesc);
  });
});
