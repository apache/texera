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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { FormsModule } from "@angular/forms";
import { JWT_OPTIONS, JwtHelperService } from "@auth0/angular-jwt";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule } from "ng-zorro-antd/modal";
import { en_US, provideNzI18n } from "ng-zorro-antd/i18n";
import { By } from "@angular/platform-browser";
import { MOCK_USER_ID, StubUserService } from "../../../../common/service/user/stub-user.service";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "src/app/common/service/workflow-persist/stub-workflow-persist.service";
import { SortButtonComponent } from "../sort-button/sort-button.component";
import { MODEL_ICON } from "../../../../common/icon/model-icon";
import { EntityType } from "../../../../hub/service/hub.service";

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
  @Input() entityType: EntityType | null = null;
  @Input() ownerScope?: string;
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
function makeFiltersDouble(keywords: string[] = [], masterFilterList: ReadonlyArray<string> = []) {
  return {
    masterFilterListChange: EMPTY,
    masterFilterList,
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

  // The duplicate-search guard above compares list *lengths* and then the terms themselves. With
  // two empty lists the term comparison never runs at all, so these three drive it with non-empty
  // lists: equal terms still short-circuit, differing terms do not, and a shorter incoming list
  // does not sneak past `Array.every` (which is vacuously true on a shorter receiver).
  //
  // Every case below deliberately gives the double a *different* keyword list from its filter list.
  // The guard reads `filters.masterFilterList`, and `filters.getSearchKeywords()` is right next to
  // it in the same object; with the two set to equal arrays the receiver could be swapped for the
  // keywords and nothing would notice.
  it("skips a duplicate search when a non-empty filter list is unchanged", async () => {
    component.filters = makeFiltersDouble(["kw"], ["alpha"]);
    const results = makeSearchResultsDouble();
    component.searchResultsComponent = results as unknown as SearchResultsComponent;
    component.masterFilterList = ["alpha"];
    component.lastSortMethod = component.sortMethod;
    component.lastSelectedType = component.selectedType;

    await component.search();

    expect(results.reset).not.toHaveBeenCalled();
  });

  it("re-runs the search when a same-length filter list holds different terms", async () => {
    component.filters = makeFiltersDouble(["kw"], ["alpha"]);
    const results = makeSearchResultsDouble();
    component.searchResultsComponent = results as unknown as SearchResultsComponent;
    component.masterFilterList = ["beta"];
    component.lastSortMethod = component.sortMethod;
    component.lastSelectedType = component.selectedType;

    await component.search();

    expect(results.reset).toHaveBeenCalledTimes(1);
    expect(component.masterFilterList).toEqual(["alpha"]);
  });

  it("re-runs the search when the new filter list is a prefix of the last one", async () => {
    // The element-wise comparison alone cannot see this: `["alpha"].every((v, i) => v === ["alpha",
    // "beta"][i])` is true, so without the length conjunct every narrowing of the filter box -
    // including clearing it - would be dismissed as "same list" and the panel would keep showing
    // the previous query's results.
    component.filters = makeFiltersDouble(["kw"], ["alpha"]);
    const results = makeSearchResultsDouble();
    component.searchResultsComponent = results as unknown as SearchResultsComponent;
    component.masterFilterList = ["alpha", "beta"];
    component.lastSortMethod = component.sortMethod;
    component.lastSelectedType = component.selectedType;

    await component.search();

    expect(results.reset).toHaveBeenCalledTimes(1);
    expect(component.masterFilterList).toEqual(["alpha"]);
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

// The suite above stubs the children out, and *any* `overrideComponent` makes
// Angular re-JIT SearchComponent from its retained decorator metadata; the
// recompiled template loses its source map back to search.component.html, so
// every binding still runs but none of it is attributed (issue #7458). This
// suite therefore stands up its own TestBed with the REAL children and asserts
// on the rendered DOM, leaving the stubbed tests above untouched.
describe("SearchComponent rendered template", () => {
  let fixture: ComponentFixture<SearchComponent>;
  let locationStub: { back: ReturnType<typeof vi.fn> };
  let executeSearch: ReturnType<typeof vi.fn>;

  const host = (): HTMLElement => fixture.nativeElement as HTMLElement;

  /** The four resource-type buttons — direct children of .left-controls, which excludes the sort button. */
  const typeButtons = (): HTMLButtonElement[] =>
    Array.from(host().querySelectorAll<HTMLButtonElement>(".left-controls > button"));

  const labels = (): string[] => typeButtons().map(button => button.textContent!.trim());

  const selectedFlags = (): boolean[] => typeButtons().map(button => button.classList.contains("selected"));

  /** The `type` argument of the most recent SearchService.executeSearch call. */
  const lastSearchedType = (): unknown => executeSearch.mock.calls[executeSearch.mock.calls.length - 1][4];

  beforeEach(async () => {
    TestBed.resetTestingModule();
    locationStub = { back: vi.fn() };
    executeSearch = vi.fn(() => of({ entries: [], more: false }));

    await TestBed.configureTestingModule({
      imports: [SearchComponent, NzModalModule, NzDropDownModule, FormsModule, HttpClientTestingModule],
      providers: [
        JwtHelperService,
        { provide: JWT_OPTIONS, useValue: {} },
        { provide: SearchService, useValue: { executeSearch } },
        { provide: UserService, useClass: StubUserService },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService([]) },
        { provide: ActivatedRoute, useValue: { queryParams: new Subject<Params>() } },
        { provide: Location, useValue: locationStub },
        provideNzI18n(en_US),
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(SearchComponent);
    fixture.detectChanges();
  });

  it("renders the four resource-type buttons, each with its own label and icon", () => {
    expect(labels()).toEqual(["All", "Workflow", "Dataset", "Model"]);
    // nz-icon turns nzType into an `anticon-<type>` class, so this pins the icon
    // each button asks for — and that "All" asks for none.
    const icons = typeButtons().map(button => {
      const icon = button.querySelector("span[nz-icon]");
      return icon && Array.from(icon.classList).find(name => name.startsWith("anticon-"));
    });
    expect(icons).toEqual([null, "anticon-project", "anticon-database", `anticon-${MODEL_ICON}`]);
  });

  it("highlights only the All button before a resource type is chosen", () => {
    expect(selectedFlags()).toEqual([true, false, false, false]);
  });

  it("moves the highlight onto whichever resource-type button is clicked", () => {
    // Click each typed button in turn; the highlight must be one-hot on that
    // same button, which pins both its (click) argument and its [ngClass] test.
    typeButtons().forEach((button, index) => {
      button.click();
      fixture.detectChanges();
      expect(selectedFlags()).toEqual(labels().map((_, i) => i === index));
    });
  });

  it("scopes the search to the clicked resource type", () => {
    typeButtons()[2].click();
    expect(lastSearchedType()).toBe("dataset");

    // Back to All: the type filter is cleared rather than left on "dataset".
    typeButtons()[0].click();
    expect(lastSearchedType()).toBeNull();
  });

  it("re-runs the search with the sort method the sort button emits", () => {
    const sortButton = fixture.debugElement.query(By.directive(SortButtonComponent))
      .componentInstance as SortButtonComponent;

    sortButton.dateSort();

    // Kills both halves of `sortMethod = $event; search()`: drop the assignment
    // and the search runs with the EditTimeDesc default; drop the call and
    // executeSearch is never reached at all.
    expect(executeSearch).toHaveBeenCalledTimes(1);
    expect(executeSearch.mock.calls[0][5]).toBe(SortMethod.CreateTimeDesc);
  });

  it("renders a back arrow that returns to the previous page", () => {
    const goBack = host().querySelector<HTMLButtonElement>(".go-back-button")!;
    expect(goBack.querySelector("span.anticon-arrow-left")).not.toBeNull();

    goBack.click();

    expect(locationStub.back).toHaveBeenCalledTimes(1);
  });

  it("hands the signed-in user's uid to the results list", () => {
    const results = fixture.debugElement.query(By.directive(SearchResultsComponent))
      .componentInstance as SearchResultsComponent;
    // Asserted on the child input rather than the DOM because currentUid only
    // reaches the markup through texera-list-item, which is not rendered while
    // the result list is empty.
    expect(results.currentUid).toBe(MOCK_USER_ID);
  });
});
