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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { of } from "rxjs";
import { UserModelComponent } from "./user-model.component";
import { ModelService } from "../../../service/user/model/model.service";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { DashboardModel } from "../../../type/dashboard-model.interface";
import { SortMethod } from "../../../type/sort-method";
import { commonTestImports, commonTestProviders } from "../../../../common/testing/test-utils";
import { NzModalService } from "ng-zorro-antd/modal";
import { provideRouter } from "@angular/router";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { WorkflowCoverService } from "../../../service/user/workflow-cover/workflow-cover.service";

function makeModel(overrides: {
  mid: number;
  name?: string;
  description?: string;
  framework?: string;
  creationTime?: number;
  ownerEmail?: string;
}): DashboardModel {
  return {
    isOwner: true,
    ownerEmail: overrides.ownerEmail ?? "owner@example.com",
    model: {
      mid: overrides.mid,
      ownerUid: 1,
      name: overrides.name ?? `model-${overrides.mid}`,
      repositoryName: `model-${overrides.mid}`,
      isPublic: false,
      isDownloadable: true,
      description: overrides.description ?? "",
      creationTime: overrides.creationTime ?? overrides.mid * 1000,
      coverImage: undefined,
      framework: overrides.framework ?? "pytorch",
      format: "safetensors",
    },
    accessPrivilege: "WRITE",
    size: 0,
  };
}

describe("UserModelComponent", () => {
  let component: UserModelComponent;
  let fixture: ComponentFixture<UserModelComponent>;
  let modelService: {
    retrieveAccessibleModels: ReturnType<typeof vi.fn>;
    deleteModel: ReturnType<typeof vi.fn>;
    retrieveOwners: ReturnType<typeof vi.fn>;
  };

  const models = [
    makeModel({ mid: 1, name: "alpha", framework: "pytorch", creationTime: 1000 }),
    makeModel({ mid: 2, name: "beta", description: "an onnx graph", framework: "onnx", creationTime: 3000 }),
    makeModel({ mid: 3, name: "gamma", framework: "sklearn", creationTime: 2000 }),
  ];

  beforeEach(async () => {
    modelService = {
      retrieveAccessibleModels: vi.fn().mockReturnValue(of(models)),
      deleteModel: vi.fn().mockReturnValue(of({})),
      retrieveOwners: vi.fn().mockReturnValue(of(["owner@example.com"])),
    };

    TestBed.configureTestingModule({
      imports: [UserModelComponent, ...commonTestImports],
      providers: [
        { provide: ModelService, useValue: modelService },
        { provide: UserService, useClass: StubUserService },
        // The rendered card items pull these in; the Models page itself uses none of them.
        { provide: WorkflowPersistService, useValue: {} },
        { provide: WorkflowCoverService, useValue: {} },
        { provide: DatasetService, useValue: {} },
        NzModalService,
        // card items render routerLinks, which need a router context
        provideRouter([]),
        ...commonTestProviders,
      ],
    });

    fixture = TestBed.createComponent(UserModelComponent);
    component = fixture.componentInstance;
    // detectChanges renders the template and wires the SearchResultsComponent ViewChild;
    // the search is then awaited explicitly rather than via whenStable(), which does not
    // settle here because the component holds a live userChanged() subscription.
    fixture.detectChanges();
    await component.search(true);
    fixture.detectChanges();
  });

  it("lists every accessible model", async () => {
    expect(modelService.retrieveAccessibleModels).toHaveBeenCalled();
    expect(component.searchResultsComponent.entries.length).toBe(3);
  });

  it("filters by name", async () => {
    component.searchKeywords = ["alpha"];
    await component.search();

    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["alpha"]);
  });

  it("filters by description and by framework, not just name", async () => {
    component.searchKeywords = ["onnx"];
    await component.search();

    // "onnx" appears in beta's description and framework, in neither case its name
    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["beta"]);
  });

  it("matches case-insensitively", async () => {
    component.searchKeywords = ["ALPHA"];
    await component.search();

    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["alpha"]);
  });

  it("returns nothing when the keyword matches no model", async () => {
    component.searchKeywords = ["no-such-model"];
    await component.search();

    expect(component.searchResultsComponent.entries).toEqual([]);
  });

  it("treats a whitespace-only keyword as no filter", async () => {
    component.searchKeywords = ["   "];
    await component.search();

    expect(component.searchResultsComponent.entries.length).toBe(3);
  });

  it("requires every chip to match, narrowing the results", async () => {
    // "onnx" alone matches beta; adding "graph" still matches beta (its description)...
    component.searchKeywords = ["onnx", "graph"];
    await component.search();
    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["beta"]);

    // ...but a chip that matches nothing on beta rules it out entirely.
    component.searchKeywords = ["onnx", "alpha"];
    await component.search();
    expect(component.searchResultsComponent.entries).toEqual([]);
  });

  it("sorts newest-created first by default", async () => {
    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["beta", "gamma", "alpha"]);
  });

  it("sorts by name when asked", async () => {
    component.sortMethod = SortMethod.NameAsc;
    await component.search();
    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["alpha", "beta", "gamma"]);

    component.sortMethod = SortMethod.NameDesc;
    await component.search();
    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["gamma", "beta", "alpha"]);
  });

  it("falls back to newest-created for a sort key models do not carry", async () => {
    // Models have no edit time, so sorting by it would order on a null key.
    component.sortMethod = SortMethod.EditTimeDesc;
    await component.search();

    expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["beta", "gamma", "alpha"]);
  });

  it("does not refetch the list when only the filter changes", async () => {
    const callsAfterInit = modelService.retrieveAccessibleModels.mock.calls.length;

    component.searchKeywords = ["alpha"];
    await component.search();

    expect(modelService.retrieveAccessibleModels.mock.calls.length).toBe(callsAfterInit);
  });

  it("refetches the list when the search is forced", async () => {
    const callsAfterInit = modelService.retrieveAccessibleModels.mock.calls.length;

    await component.search(true);

    expect(modelService.retrieveAccessibleModels.mock.calls.length).toBeGreaterThan(callsAfterInit);
  });

  it("removes a deleted model from the list without refetching", async () => {
    const entry = component.searchResultsComponent.entries.find(e => e.id === 2)!;

    component.deleteModel(entry);

    expect(modelService.deleteModel).toHaveBeenCalledWith(2);
    expect(component.searchResultsComponent.entries.map(e => e.id)).not.toContain(2);
  });

  it("ignores a delete for an entry with no id", () => {
    const entry = component.searchResultsComponent.entries[0];
    entry.model.model.mid = undefined;

    component.deleteModel(entry);

    expect(modelService.deleteModel).not.toHaveBeenCalled();
  });

  it("persists the view mode across instances", () => {
    component.setViewType("list");
    expect(localStorage.getItem("texera.userModel.viewMode")).toBe("list");

    const second = TestBed.createComponent(UserModelComponent);
    expect(second.componentInstance.viewType).toBe("list");
  });
});
