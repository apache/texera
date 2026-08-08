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
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { SearchService } from "../../../service/user/search.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { SortButtonComponent } from "../sort-button/sort-button.component";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";

import { UserDatasetComponent } from "./user-dataset.component";
import { USER_DATASET } from "../../../../app-routing.constant";
import { UserDatasetVersionCreatorComponent } from "./user-dataset-explorer/user-dataset-version-creator/user-dataset-version-creator.component";
import { SortMethod } from "../../../type/sort-method";
import { User } from "../../../../common/type/user";

type LoadMoreFn = (start: number, count: number) => Promise<{ entries: any[]; more: boolean }>;

describe("UserDatasetComponent", () => {
  let component: UserDatasetComponent;

  let userChangedSubject: Subject<User | undefined>;
  let isLoginSpy: ReturnType<typeof vi.fn>;
  let getCurrentUserSpy: ReturnType<typeof vi.fn>;

  let modalServiceMock: { create: ReturnType<typeof vi.fn> };
  let routerMock: { navigate: ReturnType<typeof vi.fn> };
  let searchServiceMock: { executeSearch: ReturnType<typeof vi.fn> };
  let datasetServiceMock: { deleteDatasets: ReturnType<typeof vi.fn> };
  let messageMock: { warning: ReturnType<typeof vi.fn> };

  let filtersStub: any;
  let searchResultsStub: any;
  let capturedLoadMoreFn: LoadMoreFn | null;

  const buildEntry = (did: number | undefined, name = `dataset-${did}`) =>
    ({
      type: "dataset",
      dataset: {
        dataset: {
          did,
          name,
          ownerUid: 1,
          isPublic: false,
          isDownloadable: false,
          storagePath: undefined,
          description: "",
          creationTime: 0,
          coverImage: undefined,
        },
      },
    }) as any;

  beforeEach(() => {
    userChangedSubject = new Subject<User | undefined>();
    isLoginSpy = vi.fn(() => true);
    getCurrentUserSpy = vi.fn(() => ({ uid: 42 }) as User);

    const userServiceMock = {
      userChanged: () => userChangedSubject.asObservable(),
      isLogin: isLoginSpy,
      getCurrentUser: getCurrentUserSpy,
    };

    modalServiceMock = { create: vi.fn() };
    routerMock = { navigate: vi.fn() };
    searchServiceMock = {
      executeSearch: vi.fn(() => of({ entries: [], more: false, hasMismatch: false })),
    };
    datasetServiceMock = { deleteDatasets: vi.fn(() => of({} as Response)) };
    messageMock = { warning: vi.fn() };

    component = new UserDatasetComponent(
      modalServiceMock as any,
      userServiceMock as any,
      routerMock as any,
      searchServiceMock as any,
      datasetServiceMock as any,
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
      entries: [] as any[],
      reset: vi.fn((fn: LoadMoreFn) => {
        capturedLoadMoreFn = fn;
      }),
      loadMore: vi.fn(async () => {}),
    };

    component.filters = filtersStub;
    component.searchResultsComponent = searchResultsStub;
  });

  describe("user state tracking", () => {
    it("updates isLogin and currentUid when userChanged emits", () => {
      // initial state pulled synchronously in field initializers
      expect(component.isLogin).toBe(true);
      expect(component.currentUid).toBe(42);

      isLoginSpy.mockReturnValue(false);
      getCurrentUserSpy.mockReturnValue(undefined);
      userChangedSubject.next(undefined);

      expect(component.isLogin).toBe(false);
      expect(component.currentUid).toBeUndefined();

      isLoginSpy.mockReturnValue(true);
      getCurrentUserSpy.mockReturnValue({ uid: 99 } as User);
      userChangedSubject.next({ uid: 99 } as User);

      expect(component.isLogin).toBe(true);
      expect(component.currentUid).toBe(99);
    });
  });

  describe("default sort", () => {
    it("defaults to CreateTimeDesc so newest datasets appear first", () => {
      // Datasets have no last-modified time, so EditTimeDesc would leave the sort key NULL.
      expect(component.sortMethod).toBe(SortMethod.CreateTimeDesc);
    });
  });

  describe("ngAfterViewInit", () => {
    it("subscribes to userChanged and calls search on each emission", () => {
      const searchSpy = vi.spyOn(component, "search").mockResolvedValue();
      component.ngAfterViewInit();

      expect(searchSpy).not.toHaveBeenCalled();
      userChangedSubject.next({ uid: 42 } as User);
      expect(searchSpy).toHaveBeenCalledTimes(1);

      userChangedSubject.next(undefined);
      expect(searchSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe("search filterScope variants", () => {
    it('defaults to "private": passes isLogin through, includePublic = false', async () => {
      component.isLogin = true;
      component.sortMethod = SortMethod.EditTimeDesc;
      await component.search();

      expect(searchResultsStub.reset).toHaveBeenCalledTimes(1);
      expect(searchResultsStub.loadMore).toHaveBeenCalledTimes(1);
      expect(capturedLoadMoreFn).not.toBeNull();

      await capturedLoadMoreFn!(5, 10);
      expect(searchServiceMock.executeSearch).toHaveBeenCalledWith(
        ["kw1"],
        { ids: [1, 2] },
        5,
        10,
        "dataset",
        SortMethod.EditTimeDesc,
        true,
        false
      );
    });

    it('"public": forces isLogin = false, includePublic = true', async () => {
      component.isLogin = true;
      await component.search(false, "public");
      await capturedLoadMoreFn!(0, 20);

      const args = searchServiceMock.executeSearch.mock.calls[0];
      expect(args[6]).toBe(false); // isLogin
      expect(args[7]).toBe(true); // includePublic
    });

    it('"all": passes isLogin through, includePublic = true', async () => {
      component.isLogin = true;
      await component.search(false, "all");
      await capturedLoadMoreFn!(0, 20);

      const args = searchServiceMock.executeSearch.mock.calls[0];
      expect(args[6]).toBe(true);
      expect(args[7]).toBe(true);
    });

    it('"private" with isLogin = false: passes false through, includePublic = false', async () => {
      component.isLogin = false;
      await component.search(false, "private");
      await capturedLoadMoreFn!(0, 20);

      const args = searchServiceMock.executeSearch.mock.calls[0];
      expect(args[6]).toBe(false);
      expect(args[7]).toBe(false);
    });
  });

  describe("search call shape", () => {
    it("invokes executeSearch with the documented argument order via reset(...) then loadMore()", async () => {
      filtersStub.getSearchKeywords.mockReturnValue(["alpha", "beta"]);
      filtersStub.getSearchFilterParameters.mockReturnValue({ resourceType: "dataset" });
      component.sortMethod = SortMethod.NameAsc;
      component.isLogin = true;

      await component.search();
      expect(searchResultsStub.reset).toHaveBeenCalledTimes(1);
      expect(searchResultsStub.loadMore).toHaveBeenCalledTimes(1);
      expect(searchResultsStub.reset.mock.invocationCallOrder[0]).toBeLessThan(
        searchResultsStub.loadMore.mock.invocationCallOrder[0]
      );

      await capturedLoadMoreFn!(7, 25);
      expect(searchServiceMock.executeSearch).toHaveBeenCalledWith(
        ["alpha", "beta"],
        { resourceType: "dataset" },
        7,
        25,
        "dataset",
        SortMethod.NameAsc,
        true,
        false
      );
    });
  });

  describe("mismatch warning", () => {
    it("when hasMismatch = true: sets component.hasMismatch and warns for 4000ms", async () => {
      searchServiceMock.executeSearch.mockReturnValue(of({ entries: [], more: false, hasMismatch: true }));

      await component.search();
      await capturedLoadMoreFn!(0, 20);

      expect(component.hasMismatch).toBe(true);
      expect(messageMock.warning).toHaveBeenCalledTimes(1);
      const [msg, opts] = messageMock.warning.mock.calls[0];
      expect(typeof msg).toBe("string");
      expect(msg.length).toBeGreaterThan(0);
      expect(opts).toEqual({ nzDuration: 4000 });
    });

    it("when hasMismatch is missing/false: does not warn and clears hasMismatch", async () => {
      component.hasMismatch = true;
      searchServiceMock.executeSearch.mockReturnValue(of({ entries: [], more: false }));

      await component.search();
      await capturedLoadMoreFn!(0, 20);

      expect(component.hasMismatch).toBe(false);
      expect(messageMock.warning).not.toHaveBeenCalled();
    });
  });

  describe("onClickOpenDatasetAddComponent", () => {
    it("opens UserDatasetVersionCreatorComponent with isCreatingVersion: false", () => {
      modalServiceMock.create.mockReturnValue({ afterClose: of(null) });

      component.onClickOpenDatasetAddComponent();

      expect(modalServiceMock.create).toHaveBeenCalledTimes(1);
      const config = modalServiceMock.create.mock.calls[0][0];
      expect(config.nzContent).toBe(UserDatasetVersionCreatorComponent);
      expect(config.nzData).toEqual({ isCreatingVersion: false });
      expect(config.nzFooter).toBeNull();
    });

    it("on close with a dataset result: navigates to the new dataset URL", () => {
      const dashboardDataset = {
        isOwner: true,
        ownerEmail: "x@y.z",
        accessPrivilege: "WRITE",
        size: 0,
        dataset: { did: 123 },
      };
      modalServiceMock.create.mockReturnValue({ afterClose: of(dashboardDataset) });

      component.onClickOpenDatasetAddComponent();

      expect(routerMock.navigate).toHaveBeenCalledWith([`${USER_DATASET}/123`]);
    });

    it("on close with null result: does not navigate", () => {
      modalServiceMock.create.mockReturnValue({ afterClose: of(null) });

      component.onClickOpenDatasetAddComponent();

      expect(routerMock.navigate).not.toHaveBeenCalled();
    });
  });

  describe("deleteDataset", () => {
    it("is a no-op when entry.dataset.dataset.did is undefined", () => {
      component.deleteDataset(buildEntry(undefined));

      expect(datasetServiceMock.deleteDatasets).not.toHaveBeenCalled();
    });

    it("calls deleteDatasets(did) and filters the entry out of searchResultsComponent.entries", () => {
      const e1 = buildEntry(1, "first");
      const e2 = buildEntry(2, "second");
      const e3 = buildEntry(3, "third");
      searchResultsStub.entries = [e1, e2, e3];

      component.deleteDataset(e2);

      expect(datasetServiceMock.deleteDatasets).toHaveBeenCalledWith(2);
      expect(searchResultsStub.entries).toEqual([e1, e3]);
    });
  });

  describe("view mode toggle", () => {
    const VIEW_MODE_KEY = "texera.userDataset.viewMode";

    afterEach(() => localStorage.removeItem(VIEW_MODE_KEY));

    it("setViewType updates viewType, persists it, and is a no-op when unchanged", () => {
      // viewType defaults to "card", so switching to "list" is the real change
      component.setViewType("list");
      expect(component.viewType).toBe("list");
      expect(localStorage.getItem(VIEW_MODE_KEY)).toBe("list");

      // setting the same value should not write again
      localStorage.removeItem(VIEW_MODE_KEY);
      component.setViewType("list");
      expect(localStorage.getItem(VIEW_MODE_KEY)).toBeNull();

      component.setViewType("card");
      expect(component.viewType).toBe("card");
      expect(localStorage.getItem(VIEW_MODE_KEY)).toBe("card");
    });

    const makeFreshComponent = () => {
      const userServiceMock = {
        userChanged: () => new Subject<User | undefined>().asObservable(),
        isLogin: () => true,
        getCurrentUser: () => ({ uid: 42 }) as User,
      };
      return new UserDatasetComponent(
        modalServiceMock as any,
        userServiceMock as any,
        routerMock as any,
        searchServiceMock as any,
        datasetServiceMock as any,
        messageMock as any
      );
    };

    it("defaults viewType to card when nothing is stored", () => {
      localStorage.removeItem(VIEW_MODE_KEY);
      expect(makeFreshComponent().viewType).toBe("card");
    });

    it("initializes viewType to list only when explicitly stored", () => {
      localStorage.setItem(VIEW_MODE_KEY, "list");
      expect(makeFreshComponent().viewType).toBe("list");

      localStorage.setItem(VIEW_MODE_KEY, "card");
      expect(makeFreshComponent().viewType).toBe("card");
    });
  });
});
/**
 * The existing suite constructs the component directly, so its template has never been rendered.
 * These tests mount it for real: the view toggle, the sort wiring and the bindings handed to the
 * results list all live only in the template.
 */
/** Mirrors UserDatasetComponent's private static VIEW_MODE_STORAGE_KEY. */
const VIEW_MODE_STORAGE_KEY = "texera.userDataset.viewMode";

describe("UserDatasetComponent rendering", () => {
  let fixture: ComponentFixture<UserDatasetComponent>;
  let component: UserDatasetComponent;
  let searchSpy: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    // viewType is seeded from localStorage at construction, so a view chosen by an earlier test
    // would leak into this one. Only this component's key is removed, so nothing else in the
    // shared jsdom store is disturbed.
    localStorage.removeItem(VIEW_MODE_STORAGE_KEY);
    searchSpy = vi.fn(() => of({ entries: [], more: false, hasMismatch: false }));
    await TestBed.configureTestingModule({
      imports: [UserDatasetComponent, ...commonTestImports],
      providers: [
        { provide: NzModalService, useValue: { create: vi.fn() } },
        { provide: UserService, useClass: StubUserService },
        { provide: SearchService, useValue: { executeSearch: searchSpy } },
        { provide: DatasetService, useValue: { deleteDatasets: vi.fn(() => of({} as Response)) } },
        { provide: NzMessageService, useValue: { warning: vi.fn() } },
        // ng-zorro defaults to zh-cn and throws NG0701 without locale data; the app registers en_US.
        { provide: NZ_I18N, useValue: en_US },
        provideRouter([]),
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(UserDatasetComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * The two view-mode buttons, found by title: the sort button renders its own <button> into the
   * same nz-space-compact, so a positional selector picks that up first.
   */
  function viewButtons(): { list: HTMLButtonElement; card: HTMLButtonElement } {
    const host = fixture.nativeElement as HTMLElement;
    const list = host.querySelector<HTMLButtonElement>('button[title="List View"]');
    const card = host.querySelector<HTMLButtonElement>('button[title="Card View"]');
    // Named up front so a renamed title fails as a missing button rather than a null dereference
    // three lines later.
    expect(list, 'no button titled "List View"').not.toBeNull();
    expect(card, 'no button titled "Card View"').not.toBeNull();
    return { list: list!, card: card! };
  }

  it("starts in card view with only that button highlighted", () => {
    // nz-button renders nzType="primary" as ant-btn-primary; the highlight is how the user can tell
    // which view they are in, so it has to follow viewType rather than being fixed.
    const { list, card } = viewButtons();

    expect(component.viewType).toBe("card");
    expect(card.classList).toContain("ant-btn-primary");
    expect(list.classList).not.toContain("ant-btn-primary");
  });

  it("moves the highlight when the list view is chosen", () => {
    viewButtons().list.click();
    fixture.detectChanges();

    const { list, card } = viewButtons();
    expect(component.viewType).toBe("list");
    expect(list.classList).toContain("ant-btn-primary");
    expect(card.classList).not.toContain("ant-btn-primary");
  });

  it("remembers the chosen view for the next visit", () => {
    // The preference is persisted, so a fresh component picks it back up.
    viewButtons().list.click();

    const reopened = TestBed.createComponent(UserDatasetComponent);
    expect(reopened.componentInstance.viewType).toBe("list");
  });

  it("passes the chosen view down to the results list", () => {
    const results = fixture.debugElement.query(By.directive(SearchResultsComponent)).componentInstance;
    expect(results.viewMode).toBe("card");

    component.setViewType("list");
    fixture.detectChanges();

    expect(fixture.debugElement.query(By.directive(SearchResultsComponent)).componentInstance.viewMode).toBe("list");
  });

  it("marks the results list as editable and private", () => {
    // This is the user's own dataset page, so entries are editable and the search is scoped to them.
    const results = fixture.debugElement.query(By.directive(SearchResultsComponent)).componentInstance;

    expect(results.editable).toBe(true);
    expect(results.isPrivateSearch).toBe(true);
  });

  it("re-runs the search when the sort method changes", () => {
    // The template statement does two things — assign then search — and dropping either leaves the
    // list showing results in the previous order.
    const sortButton = fixture.debugElement.query(By.directive(SortButtonComponent));
    searchSpy.mockClear();

    sortButton.componentInstance.sortMethodChange.emit(SortMethod.NameAsc);
    fixture.detectChanges();

    expect(component.sortMethod).toBe(SortMethod.NameAsc);
    expect(searchSpy).toHaveBeenCalled();
  });

  it("opens the create-dataset flow from the toolbar button", () => {
    const spy = vi.spyOn(component, "onClickOpenDatasetAddComponent").mockImplementation(() => {});
    const host = fixture.nativeElement as HTMLElement;
    const create = host.querySelector<HTMLButtonElement>("button.create-btn");
    expect(create, "no button matching .create-btn").not.toBeNull();

    create!.click();

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("hides the sort button's execution-time options on the dataset page", () => {
    // Datasets have no executions, so those sort options must not be offered.
    const sortButton = fixture.debugElement.query(By.directive(SortButtonComponent)).componentInstance;

    expect(sortButton.showEditTime).toBe(false);
    expect(sortButton.showExecutionTime).toBe(false);
  });
});
