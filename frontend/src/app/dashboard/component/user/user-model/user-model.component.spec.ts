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

import { of, Subject, throwError } from "rxjs";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { provideRouter } from "@angular/router";
import { NzModalService } from "ng-zorro-antd/modal";
import { en_US, NZ_I18N } from "ng-zorro-antd/i18n";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { CardItemComponent } from "../list-item/card-item/card-item.component";
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
  let searchServiceMock: { getUserInfo: ReturnType<typeof vi.fn> };
  let modelServiceMock: {
    deleteModel: ReturnType<typeof vi.fn>;
    retrieveAccessibleModels: ReturnType<typeof vi.fn>;
  };

  let searchResultsStub: any;
  let capturedLoadMoreFn: LoadMoreFn | null;
  let modalAfterClose: Subject<unknown>;

  const buildEntry = (mid: number | undefined, name = `model-${mid}`) =>
    ({
      type: EntityType.Model,
      model: { model: { mid, name } },
    }) as unknown as DashboardEntry;

  /** A /model/list row. */
  const listedModel = (mid: number, overrides: Partial<Record<string, unknown>> = {}): any => ({
    isOwner: true,
    ownerEmail: "owner@example.com",
    accessPrivilege: "WRITE",
    size: 0,
    ...overrides,
    model: {
      mid,
      ownerUid: 1,
      name: `model-${mid}`,
      description: "",
      framework: "pytorch",
      format: "torchscript",
      creationTime: mid,
      isPublic: false,
      isDownloadable: false,
      repositoryName: undefined,
      coverImage: undefined,
      ...((overrides["model"] as object) ?? {}),
    },
  });

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
    searchServiceMock = { getUserInfo: vi.fn(() => of({})) };
    modelServiceMock = {
      deleteModel: vi.fn(() => of({} as Response)),
      retrieveAccessibleModels: vi.fn(() => of([])),
    };

    component = new UserModelComponent(
      modalServiceMock as any,
      userServiceMock as any,
      searchServiceMock as any,
      modelServiceMock as any
    );

    capturedLoadMoreFn = null;
    searchResultsStub = {
      entries: [] as DashboardEntry[],
      reset: vi.fn((fn: LoadMoreFn) => {
        capturedLoadMoreFn = fn;
      }),
      loadMore: vi.fn(async () => {}),
    };

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

  // ─── listing ──────────────────────────────────────────────────────────────

  it("lists the accessible models rather than going through unified search", async () => {
    // Models must not reach global search before the hub backend lands, so this page reads
    // /model/list directly.
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(of([listedModel(1), listedModel(2)]));

    await component.search();
    const page = await capturedLoadMoreFn!(0, 20);

    expect(modelServiceMock.retrieveAccessibleModels).toHaveBeenCalled();
    expect(page.entries.map((e: DashboardEntry) => e.id)).toEqual([2, 1]);
    expect(page.more).toBe(false);
  });

  it("fetches the list once and re-slices it, refetching only when forced", async () => {
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(of([listedModel(1)]));

    await component.search();
    await capturedLoadMoreFn!(0, 20);
    await capturedLoadMoreFn!(0, 20);
    expect(modelServiceMock.retrieveAccessibleModels).toHaveBeenCalledTimes(1);

    await component.search(true);
    await capturedLoadMoreFn!(0, 20);
    expect(modelServiceMock.retrieveAccessibleModels).toHaveBeenCalledTimes(2);
  });

  it("reports more pages while the filtered list runs past the window", async () => {
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(of([listedModel(1), listedModel(2), listedModel(3)]));

    await component.search();

    expect((await capturedLoadMoreFn!(0, 2)).more).toBe(true);
    expect((await capturedLoadMoreFn!(2, 2)).more).toBe(false);
  });

  it("sorts by name in both directions, and by newest first otherwise", async () => {
    const names = async (): Promise<string[]> => {
      await component.search(true);
      return (await capturedLoadMoreFn!(0, 20)).entries.map((e: DashboardEntry) => e.name);
    };
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(
      of([listedModel(1, { model: { name: "b" } }), listedModel(2, { model: { name: "a" } })])
    );

    component.sortMethod = SortMethod.NameAsc;
    expect(await names()).toEqual(["a", "b"]);

    component.sortMethod = SortMethod.NameDesc;
    expect(await names()).toEqual(["b", "a"]);

    component.sortMethod = SortMethod.CreateTimeDesc;
    expect(await names()).toEqual(["a", "b"]); // mid 2 was created later
  });

  // ─── keyword matching ─────────────────────────────────────────────────────

  it("matches keywords against name, description, framework, format and owner", async () => {
    const matches = async (keyword: string): Promise<number[]> => {
      component.searchKeywords = [keyword];
      await component.search();
      return (await capturedLoadMoreFn!(0, 20)).entries.map((e: DashboardEntry) => e.id!);
    };
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(
      of([
        listedModel(1, { model: { name: "resnet", description: "vision", framework: "pytorch" } }),
        listedModel(2, { ownerEmail: "someone@else.com", model: { name: "tree", format: "joblib" } }),
      ])
    );

    expect(await matches("resnet")).toEqual([1]);
    expect(await matches("VISION")).toEqual([1]); // case-insensitive
    expect(await matches("joblib")).toEqual([2]);
    expect(await matches("else.com")).toEqual([2]);
    expect(await matches("   ")).toEqual([2, 1]); // a blank term filters nothing
  });

  it("requires every keyword to match, not just one", async () => {
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(
      of([listedModel(1, { model: { name: "resnet", description: "vision" } }), listedModel(2)])
    );
    component.searchKeywords = ["resnet", "vision"];

    await component.search();
    expect((await capturedLoadMoreFn!(0, 20)).entries.map((e: DashboardEntry) => e.id)).toEqual([1]);

    component.searchKeywords = ["resnet", "absent"];
    await component.search();
    expect((await capturedLoadMoreFn!(0, 20)).entries).toEqual([]);
  });

  // ─── owner names ──────────────────────────────────────────────────────────

  it("labels each card with its owner's display name and avatar", async () => {
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(of([listedModel(1)]));
    searchServiceMock.getUserInfo.mockReturnValue(of({ 1: { userName: "ada", avatar: "a.png" } }));

    await component.search();
    const [entry] = (await capturedLoadMoreFn!(0, 20)).entries;

    expect(searchServiceMock.getUserInfo).toHaveBeenCalledWith([1]);
    expect(entry.ownerName).toBe("ada");
    // DashboardEntry starts every entry with an empty avatar, so only a lookup that actually
    // forwards the value can produce this — asserting "" alone would pass with the write removed.
    expect(entry.ownerAvatar).toBe("a.png");
  });

  it("leaves the avatar empty for an owner who has not set one", async () => {
    // getUserInfo omits `avatar` for such a user; forwarding undefined would put the string
    // "undefined" in the avatar slot.
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(of([listedModel(1)]));
    searchServiceMock.getUserInfo.mockReturnValue(of({ 1: { userName: "ada" } }));

    await component.search();
    const [entry] = (await capturedLoadMoreFn!(0, 20)).entries;

    expect(entry.ownerName).toBe("ada");
    expect(entry.ownerAvatar).toBe("");
  });

  it("still lists the models when the owner lookup fails", async () => {
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(of([listedModel(1)]));
    searchServiceMock.getUserInfo.mockReturnValue(throwError(() => new Error("offline")));

    await component.search();

    expect((await capturedLoadMoreFn!(0, 20)).entries).toHaveLength(1);
  });

  it("skips the owner lookup when there is nobody to look up", async () => {
    modelServiceMock.retrieveAccessibleModels.mockReturnValue(of([]));

    await component.search();
    await capturedLoadMoreFn!(0, 20);

    expect(searchServiceMock.getUserInfo).not.toHaveBeenCalled();
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
        { provide: SearchService, useValue: { getUserInfo: vi.fn(() => of({})) } },
        {
          provide: ModelService,
          useValue: { deleteModel: vi.fn(() => of({} as Response)), retrieveAccessibleModels: vi.fn(() => of([])) },
        },
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

  it("renders no shared filter bar", () => {
    // The dropdowns it offers (owner, id, ctime, operator, project) are server-side criteria, and
    // this page filters client-side. It returns with the hub and unified-search work.
    expect(host().querySelector("texera-filters")).toBeNull();
  });

  it("writes the tags typed into the search box back to the keyword list, and re-filters", () => {
    // nz-select's value arrives through its ControlValueAccessor, so the edit has to be driven at
    // the NgModel. With a one-way binding the tags would render but never reach the filter.
    const searchSpy = vi.spyOn(component, "search").mockResolvedValue();
    const select = fixture.debugElement.query(By.css("nz-select"));
    expect(select, "no nz-select in the search bar").not.toBeNull();
    // Free-text keywords only exist in tag mode.
    expect(select.componentInstance.nzMode).toBe("tags");

    select.injector.get(NgModel).viewToModelUpdate(["resnet", "vision"]);
    fixture.detectChanges();

    expect(component.searchKeywords).toEqual(["resnet", "vision"]);
    expect(searchSpy).toHaveBeenCalled();
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

  /** A /model/list row, in the shape DashboardEntry accepts. */
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
