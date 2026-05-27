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

import { ComponentFixture, TestBed, fakeAsync, tick } from "@angular/core/testing";
import { Router } from "@angular/router";
import { RouterTestingModule } from "@angular/router/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of, Subject } from "rxjs";

import { SearchBarComponent } from "./search-bar.component";
import { SearchService } from "../../../service/user/search.service";
import { UserService } from "../../../../common/service/user/user.service";
import { SortMethod } from "../../../type/sort-method";
import { SearchResult, SearchResultItem } from "../../../type/search-result";
import { DASHBOARD_SEARCH } from "../../../../app-routing.constant";
import { commonTestProviders } from "../../../../common/testing/test-utils";

function makeWorkflowItem(name: string, wid: number = 1): SearchResultItem {
  return {
    resourceType: "workflow",
    workflow: {
      isOwner: true,
      ownerName: undefined,
      workflow: { wid, name } as any,
      projectIDs: [],
      accessLevel: "WRITE",
      ownerId: 1,
    } as any,
  };
}

describe("SearchBarComponent", () => {
  let component: SearchBarComponent;
  let fixture: ComponentFixture<SearchBarComponent>;
  let searchSpy: { search: ReturnType<typeof vi.fn> };
  let userChangeSubject: Subject<unknown>;
  let isLoginValue: boolean;
  let router: Router;

  beforeEach(async () => {
    searchSpy = { search: vi.fn().mockReturnValue(of({ results: [], more: false } as SearchResult)) };
    userChangeSubject = new Subject();
    isLoginValue = true;

    const userServiceStub = {
      isLogin: () => isLoginValue,
      userChanged: () => userChangeSubject.asObservable(),
    };

    await TestBed.configureTestingModule({
      imports: [SearchBarComponent, NoopAnimationsModule, RouterTestingModule],
      providers: [
        { provide: SearchService, useValue: searchSpy },
        { provide: UserService, useValue: userServiceStub },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(SearchBarComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    fixture.detectChanges();
  });

  it("initializes isLogin from UserService and updates when userChanged emits", () => {
    expect(component.isLogin).toBe(true);

    isLoginValue = false;
    userChangeSubject.next(undefined);

    expect(component.isLogin).toBe(false);
  });

  it("onSearchInputChange clears listOfResult immediately for an empty query", () => {
    component.listOfResult = ["stale"];

    component.onSearchInputChange("");

    expect(component.listOfResult).toEqual([]);
    expect(searchSpy.search).not.toHaveBeenCalled();
  });

  it("debounces searchSubject by 200ms and only triggers getSearchResults once for rapid input", fakeAsync(() => {
    searchSpy.search.mockReturnValue(of({ results: [makeWorkflowItem("abc")], more: false } as SearchResult));

    component.onSearchInputChange("a");
    component.onSearchInputChange("ab");
    component.onSearchInputChange("abc");

    tick(199);
    expect(searchSpy.search).not.toHaveBeenCalled();

    tick(1);
    expect(searchSpy.search).toHaveBeenCalledTimes(1);
    expect(searchSpy.search.mock.calls[0][0]).toEqual(["abc"]);
    expect(component.listOfResult).toEqual(["abc"]);
  }));

  it("calls SearchService.search with the documented argument list and caches results by query", fakeAsync(() => {
    searchSpy.search.mockReturnValue(of({ results: [makeWorkflowItem("hello")], more: false } as SearchResult));

    component.onSearchInputChange("hello");
    tick(200);

    expect(searchSpy.search).toHaveBeenCalledTimes(1);
    const args = searchSpy.search.mock.calls[0];
    expect(args[0]).toEqual(["hello"]);
    expect(args[1]).toEqual({
      createDateStart: null,
      createDateEnd: null,
      modifiedDateStart: null,
      modifiedDateEnd: null,
      owners: [],
      ids: [],
      operators: [],
      projectIds: [],
    });
    expect(args[2]).toBe(0);
    expect(args[3]).toBe(5);
    expect(args[4]).toBeNull();
    expect(args[5]).toBe(SortMethod.NameAsc);
    expect(args[6]).toBe(true); // isLogin
    expect(args[7]).toBe(true); // includePublic
    expect(component.listOfResult).toEqual(["hello"]);

    // Repeat query → cache hit, no second call to the service.
    component.onSearchInputChange("hello");
    tick(200);

    expect(searchSpy.search).toHaveBeenCalledTimes(1);
    expect(component.listOfResult).toEqual(["hello"]);
  }));

  it("addToCache evicts the oldest entry once 20 queries are cached", () => {
    const cache = (component as any).searchCache as Map<string, string[]>;
    const order = (component as any).queryOrder as string[];

    for (let i = 0; i < 20; i++) {
      (component as any).addToCache(`q${i}`, [`r${i}`]);
    }
    expect(cache.size).toBe(20);
    expect(cache.has("q0")).toBe(true);
    expect(order[0]).toBe("q0");

    (component as any).addToCache("q20", ["r20"]);

    expect(cache.size).toBe(20);
    expect(cache.has("q0")).toBe(false);
    expect(cache.has("q20")).toBe(true);
    expect(order[0]).toBe("q1");
    expect(order[order.length - 1]).toBe("q20");
  });

  describe("convertToName", () => {
    it("returns the workflow's DashboardEntry.name", () => {
      expect(component.convertToName(makeWorkflowItem("wf-name", 7))).toBe("wf-name");
    });

    it("returns the project's name", () => {
      const item: SearchResultItem = {
        resourceType: "project",
        project: {
          pid: 1,
          name: "proj-name",
          description: "",
          ownerId: 1,
          creationTime: 0,
          color: null,
          accessLevel: "WRITE",
        } as any,
      };
      expect(component.convertToName(item)).toBe("proj-name");
    });

    it("returns the file's name", () => {
      const item: SearchResultItem = {
        resourceType: "file",
        file: {
          ownerEmail: "a@b.c",
          accessLevel: "WRITE",
          file: {
            fid: 1,
            ownerUid: 1,
            name: "file-name",
            size: 0,
            path: "",
            description: "",
            uploadTime: 0,
          },
        } as any,
      };
      expect(component.convertToName(item)).toBe("file-name");
    });

    it("returns the dataset's name", () => {
      const item: SearchResultItem = {
        resourceType: "dataset",
        dataset: {
          isOwner: true,
          ownerEmail: "a@b.c",
          accessPrivilege: "WRITE",
          size: 0,
          dataset: {
            did: 1,
            ownerUid: 1,
            name: "ds-name",
            isPublic: false,
            isDownloadable: false,
            description: "",
            creationTime: 0,
          },
        } as any,
      };
      expect(component.convertToName(item)).toBe("ds-name");
    });

    it("throws for a SearchResultItem with no recognized resource", () => {
      expect(() => component.convertToName({ resourceType: "computing-unit" } as any)).toThrow(
        "Unexpected type in SearchResult."
      );
    });
  });

  it("performSearch navigates to DASHBOARD_SEARCH with the keyword as the q query param", () => {
    const nav = vi.spyOn(router, "navigate").mockResolvedValue(true);

    component.performSearch("hello world");

    expect(nav).toHaveBeenCalledWith([DASHBOARD_SEARCH], { queryParams: { q: "hello world" } });
  });
});
