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
});
