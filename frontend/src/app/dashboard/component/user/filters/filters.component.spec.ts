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

import { ComponentFixture, TestBed } from "@angular/core/testing";

import { FiltersComponent } from "./filters.component";
import { StubOperatorMetadataService } from "src/app/workspace/service/operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "src/app/workspace/service/operator-metadata/operator-metadata.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "src/app/common/service/workflow-persist/stub-workflow-persist.service";
import { testWorkflowEntries } from "../../user-dashboard-test-fixtures";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { JWT_OPTIONS, JwtHelperService } from "@auth0/angular-jwt";
import { FormsModule } from "@angular/forms";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "src/app/common/testing/test-utils";
import { NzModalModule } from "ng-zorro-antd/modal";
import { en_US, provideNzI18n } from "ng-zorro-antd/i18n";
<<<<<<< HEAD
=======
import { UserService } from "src/app/common/service/user/user.service";
import { MOCK_USER, StubUserService } from "src/app/common/service/user/stub-user.service";
import { UserProjectService } from "src/app/dashboard/service/user/project/user-project.service";
import { StubUserProjectService } from "src/app/dashboard/service/user/project/stub-user-project.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { DatasetService } from "src/app/dashboard/service/user/dataset/dataset.service";
import { EntityType } from "src/app/hub/service/hub.service";
import { By } from "@angular/platform-browser";
import { of } from "rxjs";
>>>>>>> eddec267a (fix(frontend, amber): source filter owners per resource kind and apply them to datasets (#8060))

describe("FiltersComponent", () => {
  let component: FiltersComponent;
  let fixture: ComponentFixture<FiltersComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [
        JwtHelperService,
        { provide: JWT_OPTIONS, useValue: {} },
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
<<<<<<< HEAD
=======
        { provide: UserService, useClass: StubUserService },
        { provide: UserProjectService, useClass: StubUserProjectService },
        { provide: DatasetService, useValue: { retrieveOwners: vi.fn(() => of([])) } },
>>>>>>> eddec267a (fix(frontend, amber): source filter owners per resource kind and apply them to datasets (#8060))
        provideNzI18n(en_US),
        ...commonTestProviders,
      ],
      imports: [FiltersComponent, NzModalModule, NzDropDownModule, FormsModule, HttpClientTestingModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FiltersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("parses manually entered mtime", () => {
    component.masterFilterList = ["mtime: 2022-01-22 ~ 2022-04-21"];
    expect(component.selectedMtime).toEqual([new Date(2022, 0, 22), new Date(2022, 3, 21)]);
  });

  it("parses manually entered ctime", () => {
    component.masterFilterList = ["ctime: 2022-01-22 ~ 2022-04-21"];
    expect(component.selectedCtime).toEqual([new Date(2022, 0, 22), new Date(2022, 3, 21)]);
  });

  it("preserves ordering when parsing drop down", () => {
    component.masterFilterList = ["keyword", "ctime: 2022-01-22 ~ 2022-04-21", "keyword 2"];
    component.selectedCtime = [new Date(2022, 2, 22), new Date(2022, 4, 21)];
    component.buildMasterFilterList();
    expect(component.masterFilterList).toEqual(["keyword", "ctime: 2022-03-22 ~ 2022-05-21", "keyword 2"]);
    component.masterFilterList = [...component.masterFilterList, "another keyword"];
    expect(component.masterFilterList).toEqual([
      "keyword",
      "ctime: 2022-03-22 ~ 2022-05-21",
      "keyword 2",
      "another keyword",
    ]);
  });
});

/** The bar is shared by several pages; these pin that it sources owners and ids per kind. */
describe("FiltersComponent per-resource owners", () => {
  let fixture: ComponentFixture<FiltersComponent>;
  let component: FiltersComponent;
  let datasetOwners: ReturnType<typeof vi.fn>;
  let workflowOwners: ReturnType<typeof vi.fn>;
  let workflowIds: ReturnType<typeof vi.fn>;

  /** The input has to be set before ngOnInit reads it. */
  async function render(entityType?: EntityType): Promise<void> {
    datasetOwners = vi.fn(() => of(["dataset-owner"]));
    workflowOwners = vi.fn(() => of(["workflow-owner"]));
    workflowIds = vi.fn(() => of([7]));

    await TestBed.configureTestingModule({
      providers: [
        JwtHelperService,
        { provide: JWT_OPTIONS, useValue: {} },
        {
          provide: WorkflowPersistService,
          useValue: { retrieveOwners: workflowOwners, retrieveWorkflowIDs: workflowIds },
        },
        { provide: DatasetService, useValue: { retrieveOwners: datasetOwners } },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserService, useClass: StubUserService },
        { provide: UserProjectService, useClass: StubUserProjectService },
        provideNzI18n(en_US),
        ...commonTestProviders,
      ],
      imports: [FiltersComponent, NzModalModule, NzDropDownModule, FormsModule, HttpClientTestingModule],
    }).compileComponents();

    fixture = TestBed.createComponent(FiltersComponent);
    component = fixture.componentInstance;
    if (entityType !== undefined) {
      component.entityType = entityType;
    }
    fixture.detectChanges();
  }

  afterEach(() => {
    const overlayContainer = TestBed.inject(OverlayContainer, null);
    if (overlayContainer) {
      overlayContainer.getContainerElement().innerHTML = "";
    }
  });

  it("defaults to workflows, so the call sites that pass nothing are unaffected", async () => {
    await render();

    expect(component.entityType).toBe(EntityType.Workflow);
    expect(workflowOwners).toHaveBeenCalled();
    expect(datasetOwners).not.toHaveBeenCalled();
    expect(component.owners.map(owner => owner.userName)).toEqual(["workflow-owner"]);
  });

  it("lists dataset owners, not workflow owners, when filtering datasets", async () => {
    await render(EntityType.Dataset);

    expect(datasetOwners).toHaveBeenCalled();
    expect(workflowOwners).not.toHaveBeenCalled();
    expect(component.owners.map(owner => owner.userName)).toEqual(["dataset-owner"]);
  });

  it("offers workflow ids only when filtering workflows", async () => {
    await render(EntityType.Workflow);
    expect(component.hasIdFilter).toBe(true);
    expect(workflowIds).toHaveBeenCalled();
    expect(component.wids.map(wid => wid.id)).toEqual(["7"]);
  });

  it("asks for no ids at all when filtering datasets, rather than showing workflow ids", async () => {
    await render(EntityType.Dataset);

    expect(component.hasIdFilter).toBe(false);
    expect(workflowIds).not.toHaveBeenCalled();
    expect(component.wids).toEqual([]);
  });

  it("hides the id dropdown for a kind that has no ids to offer", async () => {
    await render(EntityType.Dataset);

    expect(fixture.debugElement.query(By.css(".search-wids-button"))).toBeNull();
  });

  it("still renders the id dropdown for workflows", async () => {
    await render(EntityType.Workflow);

    expect(fixture.debugElement.query(By.css(".search-wids-button"))).not.toBeNull();
  });
});
