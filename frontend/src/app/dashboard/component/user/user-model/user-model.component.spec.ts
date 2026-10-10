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

import { of, Subject } from "rxjs";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { provideRouter } from "@angular/router";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";
import { en_US, NZ_I18N } from "ng-zorro-antd/i18n";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { CardItemComponent } from "../list-item/card-item/card-item.component";
import { FiltersComponent } from "../filters/filters.component";
import { NgModel } from "@angular/forms";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { SearchService } from "../../../service/user/search.service";
import { ModelService } from "../../../service/user/model/model.service";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { SortMethod } from "../../../type/sort-method";
import { EntityType } from "../../../../hub/service/hub.service";
import { User } from "../../../../common/type/user";
import { UserModelComponent } from "./user-model.component";
import { UserModelCreatorComponent } from "./user-model-creator/user-model-creator.component";

type LoadMoreFn = (start: number, count: number) => Promise<{ entries: any[]; more: boolean }>;

describe("UserModelComponent", () => {
  let component: UserModelComponent;

  let userChangedSubject: Subject<User | undefined>;
  let isLoginSpy: ReturnType<typeof vi.fn>;
  let getCurrentUserSpy: ReturnType<typeof vi.fn>;

  let modalServiceMock: { create: ReturnType<typeof vi.fn> };
  let searchServiceMock: { executeSearch: ReturnType<typeof vi.fn> };
  let modelServiceMock: { deleteModel: ReturnType<typeof vi.fn> };
  let messageMock: { warning: ReturnType<typeof vi.fn> };

  let filtersStub: any;
  let searchResultsStub: any;
  let capturedLoadMoreFn: LoadMoreFn | null;
  let modalAfterClose: Subject<unknown>;

  const buildEntry = (mid: number | undefined, name = `model-${mid}`) =>
    ({
      type: EntityType.Model,
      model: { model: { mid, name } },
    }) as unknown as DashboardEntry;

  beforeEach(() => {
    userChangedSubject = new Subject<User | undefined>();
    isLoginSpy = vi.fn(() => true);
    getCurrentUserSpy = vi.fn(() => ({ uid: 42 }) as User);

    const userServiceMock = {
      userChanged: () => userChangedSubject.asObservable(),
      isLogin: isLoginSpy,
      getCurrentUser: getCurrentUserSpy,
    };

    modalAfterClose = new Subject<unknown>();
    modalServiceMock = { create: vi.fn(() => ({ afterClose: modalAfterClose.asObservable() })) };
    searchServiceMock = {
      executeSearch: vi.fn(() => of({ entries: [], more: false, hasMismatch: false })),
    };
    modelServiceMock = { deleteModel: vi.fn(() => of({} as Response)) };
    messageMock = { warning: vi.fn() };

    component = new UserModelComponent(
      modalServiceMock as any,
      userServiceMock as any,
      searchServiceMock as any,
      modelServiceMock as any,
      messageMock as any
    );

    capturedLoadMoreFn = null;
    filtersStub = {
      masterFilterList: [] as string[],
      masterFilterListChange: new Subject<void>(),
      getSearchKeywords: vi.fn(() => ["kw1"]),
      getSearchFilterParameters: vi.fn(() => ({ ids: [1, 2] })),
    };
    searchResultsStub = {
      entries: [] as DashboardEntry[],
      reset: vi.fn((fn: LoadMoreFn) => {
        capturedLoadMoreFn = fn;
      }),
      loadMore: vi.fn(async () => {}),
    };

    component.filters = filtersStub;
    component.searchResultsComponent = searchResultsStub;
  });

  // ─── wiring ───────────────────────────────────────────────────────────────

  it("defaults to newest-first, since models carry no last-modified time", () => {
    expect(component.sortMethod).toBe(SortMethod.CreateTimeDesc);
  });

  it("tracks login state as the user changes", () => {
    expect(component.isLogin).toBe(true);
    expect(component.currentUid).toBe(42);

    isLoginSpy.mockReturnValue(false);
    getCurrentUserSpy.mockReturnValue(undefined);
    userChangedSubject.next(undefined);

    expect(component.isLogin).toBe(false);
    expect(component.currentUid).toBeUndefined();
  });

  it("re-searches whenever the signed-in user changes", () => {
    const searchSpy = vi.spyOn(component, "search").mockResolvedValue();
    component.ngAfterViewInit();

    expect(searchSpy).not.toHaveBeenCalled();
    userChangedSubject.next({ uid: 42 } as User);
    expect(searchSpy).toHaveBeenCalledTimes(1);
  });

  // ─── searching ────────────────────────────────────────────────────────────

  it("searches models through unified search, with the filters bar's keywords and parameters", async () => {
    filtersStub.getSearchKeywords.mockReturnValue(["resnet", "vision"]);
    filtersStub.getSearchFilterParameters.mockReturnValue({ ids: [1, 2] });
    component.sortMethod = SortMethod.NameAsc;

    await component.search();
    expect(searchResultsStub.reset).toHaveBeenCalledTimes(1);
    expect(searchResultsStub.loadMore).toHaveBeenCalledTimes(1);

    await capturedLoadMoreFn!(7, 25);

    expect(searchServiceMock.executeSearch).toHaveBeenCalledWith(
      ["resnet", "vision"],
      { ids: [1, 2] },
      7,
      25,
      "model",
      SortMethod.NameAsc,
      true,
      false
    );
  });

  it("never asks for public models, even when the viewer is signed out", async () => {
    // This page lists what you own or were granted; public models belong to the hub.
    component.isLogin = false;

    await component.search();
    await capturedLoadMoreFn!(0, 20);

    const args = searchServiceMock.executeSearch.mock.calls[0];
    expect(args[6]).toBe(false); // isLogin
    expect(args[7]).toBe(false); // includePublic
  });

  it("skips a repeated search with the same filters and sort, but honors forced", async () => {
    filtersStub.masterFilterList = ["a"];

    await component.search();
    expect(searchResultsStub.reset).toHaveBeenCalledTimes(1);

    await component.search();
    expect(searchResultsStub.reset).toHaveBeenCalledTimes(1);

    await component.search(true);
    expect(searchResultsStub.reset).toHaveBeenCalledTimes(2);
  });

  it("re-searches when the sort method changes even if the filter list is identical", async () => {
    filtersStub.masterFilterList = ["a"];

    await component.search();
    component.sortMethod = SortMethod.NameAsc;
    await component.search();

    expect(searchResultsStub.reset).toHaveBeenCalledTimes(2);
  });

  it("re-searches when the filters bar reports a new filter list", async () => {
    const searchSpy = vi.spyOn(component, "search").mockResolvedValue();
    filtersStub.masterFilterListChange.next();

    expect(searchSpy).toHaveBeenCalled();
  });

  // ─── mismatch warning ─────────────────────────────────────────────────────

  it("warns when the backend could not match every model, and records it", async () => {
    searchServiceMock.executeSearch.mockReturnValue(of({ entries: [], more: false, hasMismatch: true }));

    await component.search();
    await capturedLoadMoreFn!(0, 20);

    expect(component.hasMismatch).toBe(true);
    expect(messageMock.warning).toHaveBeenCalledTimes(1);
    const [message, options] = messageMock.warning.mock.calls[0];
    expect(message).toContain("models");
    expect(options).toEqual({ nzDuration: 4000 });
  });

  it("clears the mismatch flag and stays quiet when the response carries none", async () => {
    component.hasMismatch = true;
    searchServiceMock.executeSearch.mockReturnValue(of({ entries: [], more: false }));

    await component.search();
    await capturedLoadMoreFn!(0, 20);

    expect(component.hasMismatch).toBe(false);
    expect(messageMock.warning).not.toHaveBeenCalled();
  });

  // ─── view mode ────────────────────────────────────────────────────────────

  it("remembers the chosen view for the next visit, and ignores a no-op change", () => {
    const setItem = vi.spyOn(Storage.prototype, "setItem");
    try {
      component.setViewType("card"); // already the default
      expect(setItem).not.toHaveBeenCalled();

      component.setViewType("list");
      expect(component.viewType).toBe("list");
      expect(setItem).toHaveBeenCalledWith("texera.userModel.viewMode", "list");
    } finally {
      setItem.mockRestore();
    }
  });

  // ─── create ───────────────────────────────────────────────────────────────

  it("opens the creator and refreshes the list with whatever it created", () => {
    // There is no model detail page yet, so the new model has to appear in this list instead.
    const searchSpy = vi.spyOn(component, "search").mockResolvedValue();
    component.onClickOpenModelAddComponent();

    expect(modalServiceMock.create).toHaveBeenCalledWith(
      expect.objectContaining({ nzContent: UserModelCreatorComponent })
    );

    modalAfterClose.next({ model: { mid: 9 } });

    expect(searchSpy).toHaveBeenCalledWith(true);
  });

  it("does not refresh when the creator was cancelled", () => {
    const searchSpy = vi.spyOn(component, "search").mockResolvedValue();
    component.onClickOpenModelAddComponent();

    modalAfterClose.next(null);

    expect(searchSpy).not.toHaveBeenCalled();
  });

  // ─── delete ───────────────────────────────────────────────────────────────

  it("deletes the model and drops just that row from the list", () => {
    searchResultsStub.entries = [buildEntry(1), buildEntry(2)];

    component.deleteModel(buildEntry(1));

    expect(modelServiceMock.deleteModel).toHaveBeenCalledWith(1);
    expect(searchResultsStub.entries.map((e: DashboardEntry) => e.model.model.mid)).toEqual([2]);
  });

  it("does not call the backend for a model that was never persisted", () => {
    component.deleteModel(buildEntry(undefined));

    expect(modelServiceMock.deleteModel).not.toHaveBeenCalled();
  });
});

/**
 * The suite above constructs the component directly, so its template has never been rendered.
 * These mount it for real: the toolbar, the view toggle and the bindings handed to the filters and
 * the results list live only in the template.
 */
/** Mirrors UserModelComponent's private static VIEW_MODE_STORAGE_KEY. */
const VIEW_MODE_STORAGE_KEY = "texera.userModel.viewMode";

describe("UserModelComponent rendering", () => {
  let fixture: ComponentFixture<UserModelComponent>;
  let component: UserModelComponent;
  let modalCreate: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    // viewType is seeded from localStorage at construction, so a view chosen by an earlier test
    // would leak into this one.
    localStorage.removeItem(VIEW_MODE_STORAGE_KEY);
    modalCreate = vi.fn(() => ({ afterClose: of(null) }));
    await TestBed.configureTestingModule({
      imports: [UserModelComponent, ...commonTestImports],
      providers: [
        { provide: NzModalService, useValue: { create: modalCreate } },
        { provide: UserService, useClass: StubUserService },
        {
          provide: SearchService,
          useValue: { executeSearch: vi.fn(() => of({ entries: [], more: false, hasMismatch: false })) },
        },
        {
          provide: ModelService,
          useValue: { deleteModel: vi.fn(() => of({} as Response)), retrieveOwners: vi.fn(() => of([])) },
        },
        { provide: NzMessageService, useValue: { warning: vi.fn() } },
        { provide: NZ_I18N, useValue: en_US },
        provideRouter([]),
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(UserModelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    localStorage.removeItem(VIEW_MODE_STORAGE_KEY);
    vi.restoreAllMocks();
  });

  const host = (): HTMLElement => fixture.nativeElement as HTMLElement;

  it("titles the page and offers the create button", () => {
    expect(host().querySelector(".page-title")?.textContent).toContain("Models");
    const create = host().querySelector<HTMLButtonElement>(".create-btn");
    expect(create?.textContent).toContain("Create Model");

    create!.click();

    expect(modalCreate).toHaveBeenCalled();
  });

  it("renders the shared filter bar, scoped to models", () => {
    // The dropdowns it offers are server-side criteria, which the page can use now that it
    // searches through the backend rather than filtering the list in the browser.
    const filters = fixture.debugElement.query(By.directive(FiltersComponent));
    expect(filters, "no texera-filters in the toolbar").not.toBeNull();
    expect(filters.componentInstance.entityType).toBe(EntityType.Model);
  });

  it("writes the tags typed into the search box back to the filter list", () => {
    // nz-select's value arrives through its ControlValueAccessor, so the edit has to be driven at
    // the NgModel. With a one-way binding the tags would render but never reach the filter list.
    const select = fixture.debugElement.query(By.css("nz-select"));
    expect(select, "no nz-select in the search bar").not.toBeNull();
    // Free-text keywords only exist in tag mode.
    expect(select.componentInstance.nzMode).toBe("tags");

    select.injector.get(NgModel).viewToModelUpdate(["resnet", "vision"]);

    expect(component.filters.masterFilterList).toEqual(["resnet", "vision"]);
  });

  it("starts in card view and moves the highlight when the list view is chosen", () => {
    // nz-button renders nzType="primary" as ant-btn-primary; the highlight is how the user can tell
    // which view they are in, so it has to follow viewType.
    const buttonTitled = (title: string) => host().querySelector<HTMLButtonElement>(`button[title="${title}"]`)!;
    expect(component.viewType).toBe("card");
    expect(buttonTitled("Card View").classList).toContain("ant-btn-primary");
    expect(buttonTitled("List View").classList).not.toContain("ant-btn-primary");

    buttonTitled("List View").click();
    fixture.detectChanges();

    expect(component.viewType).toBe("list");
    expect(buttonTitled("List View").classList).toContain("ant-btn-primary");
    expect(localStorage.getItem(VIEW_MODE_STORAGE_KEY)).toBe("list");
  });

  it("hands the results list the page's view settings", () => {
    const results = fixture.debugElement.query(By.directive(SearchResultsComponent)).componentInstance;
    expect(results.viewMode).toBe(component.viewType);
    expect(results.editable).toBe(true);
    expect(results.isPrivateSearch).toBe(true);
    expect(results.currentUid).toBe(component.currentUid);
  });

  /** A search hit, in the shape DashboardEntry accepts. */
  function modelEntry(mid = 3, name = "resnet"): DashboardEntry {
    return new DashboardEntry({
      isOwner: true,
      ownerEmail: "owner@example.com",
      accessPrivilege: "WRITE",
      size: 0,
      model: {
        mid,
        ownerUid: 1,
        name,
        description: "",
        framework: "pytorch",
        format: "torchscript",
        creationTime: mid,
        isPublic: false,
        isDownloadable: false,
      },
    } as any);
  }

  it("renders one card per model through the card template, with its outputs kept apart", () => {
    // The card template is handed to the results list rather than instantiated here, so nothing
    // else in this suite proves the entry reaches the card or that `deleted` and `refresh` — two
    // adjacent outputs, one of them destructive — are not crossed.
    const deleteModel = vi.spyOn(component, "deleteModel").mockImplementation(() => {});
    const search = vi.spyOn(component, "search").mockResolvedValue();
    const entry = modelEntry();
    const other = modelEntry(4, "bert");
    // A distinct viewer id: the card navigates by it and gates liking on it, so a template that
    // dropped the binding would still render and would fail silently at runtime.
    component.currentUid = 42;

    component.searchResultsComponent.entries = [entry, other];
    fixture.detectChanges();

    const cards = fixture.debugElement.queryAll(By.directive(CardItemComponent));
    expect(cards, "the card template did not render one card per model").toHaveLength(2);
    const card = cards[0];
    expect(card.componentInstance.entry).toBe(entry);
    expect(cards[1].componentInstance.entry).toBe(other);
    expect(card.componentInstance.currentUid).toBe(42);
    expect(card.componentInstance.isPrivateSearch).toBe(true);
    expect(card.componentInstance.editable).toBe(true);

    card.triggerEventHandler("deleted", undefined);
    expect(deleteModel).toHaveBeenCalledWith(entry);
    expect(search).not.toHaveBeenCalled();

    card.triggerEventHandler("refresh", undefined);
    expect(search).toHaveBeenCalledWith(true);
  });

  it("refuses to search before the results list has been queried", () => {
    // search() reaches straight through the getter; without the guard the failure would surface
    // as "cannot read reset of undefined" from inside an async callback.
    const bare = TestBed.createComponent(UserModelComponent).componentInstance;

    expect(() => bare.searchResultsComponent).toThrowError("Property cannot be accessed before it is initialized.");
  });
});
