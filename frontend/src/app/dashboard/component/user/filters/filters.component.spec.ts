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
import { OverlayContainer } from "@angular/cdk/overlay";

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
import { UserService } from "src/app/common/service/user/user.service";
import { StubUserService } from "src/app/common/service/user/stub-user.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { DatasetService } from "src/app/dashboard/service/user/dataset/dataset.service";
import { ModelService } from "../../../service/user/model/model.service";
import { EntityType } from "src/app/hub/service/hub.service";
import { By } from "@angular/platform-browser";
import { of, throwError, Subject } from "rxjs";
import { SimpleChange } from "@angular/core";

describe("FiltersComponent", () => {
  let component: FiltersComponent;
  let fixture: ComponentFixture<FiltersComponent>;

  // The component parses a "YYYY-MM-DD" tag into a Date via the LOCAL-time
  // `new Date(year, month - 1, day)`. Assert on the individual calendar fields
  // read with the matching LOCAL getters (not getUTC*) so the expectation
  // recovers the intended calendar day in every runner timezone.
  function expectDateRange(
    actual: ReadonlyArray<Date>,
    start: [number, number, number],
    end: [number, number, number]
  ): void {
    expect(actual).toHaveLength(2);
    expect([actual[0].getFullYear(), actual[0].getMonth(), actual[0].getDate()]).toEqual(start);
    expect([actual[1].getFullYear(), actual[1].getMonth(), actual[1].getDate()]).toEqual(end);
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [
        JwtHelperService,
        { provide: JWT_OPTIONS, useValue: {} },
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserService, useClass: StubUserService },
        { provide: DatasetService, useValue: { retrieveOwners: vi.fn(() => of([])) } },
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

  afterEach(() => {
    fixture.destroy();
    // Clear any CDK overlays (dropdowns/modals) opened during the test via the
    // injected OverlayContainer rather than mutating the global document, so we
    // only touch the container Angular created for this TestBed.
    const overlayContainer = TestBed.inject(OverlayContainer, null);
    if (overlayContainer) {
      overlayContainer.getContainerElement().innerHTML = "";
    }
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("parses manually entered mtime", () => {
    component.masterFilterList = ["mtime: 2022-01-22 ~ 2022-04-21"];
    expectDateRange(component.selectedMtime, [2022, 0, 22], [2022, 3, 21]);
  });

  it("parses manually entered ctime", () => {
    component.masterFilterList = ["ctime: 2022-01-22 ~ 2022-04-21"];
    expectDateRange(component.selectedCtime, [2022, 0, 22], [2022, 3, 21]);
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

  describe("backend setup for a logged-in user", () => {
    it("populates owners from retrieveOwners on init", () => {
      // StubWorkflowPersistService derives owners from the test workflow entries (deduplicated).
      expect(component.owners.map(o => o.userName)).toEqual(["Texera", "Angular", "UCI"]);
      expect(component.owners.every(o => !o.checked)).toBe(true);
    });

    it("populates workflow ids from retrieveWorkflowIDs on init", () => {
      expect(component.wids.map(w => w.id)).toEqual(["1", "2", "3", "4", "5"]);
      expect(component.wids.every(w => !w.checked)).toBe(true);
    });

    it("skips owner and id retrieval when the user is not logged in at init", () => {
      const stubUser = TestBed.inject(UserService) as unknown as StubUserService;
      // Capture and restore the shared stub's user so logging out here does not
      // leak into later tests (state leak / order-dependence).
      const previousUser = stubUser.user;
      try {
        stubUser.user = undefined;
        const loggedOutFixture = TestBed.createComponent(FiltersComponent);
        const loggedOutComponent = loggedOutFixture.componentInstance;
        loggedOutFixture.detectChanges();
        expect(loggedOutComponent.isLogin).toBe(false);
        expect(loggedOutComponent.owners).toEqual([]);
        expect(loggedOutComponent.wids).toEqual([]);
        // Operator metadata is not login-gated, so it is still loaded.
        expect(loggedOutComponent.operatorGroups).toEqual(["Source", "Analysis", "View Results"]);
        loggedOutFixture.destroy();
      } finally {
        stubUser.user = previousUser;
      }
    });

    it("offers no owner facet to a signed-out hub visitor", () => {
      // The hub is reachable signed out, but /hub/owners is @RolesAllowed and the list is emails.
      const stubUser = TestBed.inject(UserService) as unknown as StubUserService;
      const previousUser = stubUser.user;
      try {
        stubUser.user = undefined;
        const anonFixture = TestBed.createComponent(FiltersComponent);
        anonFixture.componentInstance.ownerScope = "public";
        anonFixture.detectChanges();

        expect(anonFixture.componentInstance.owners).toEqual([]);
        const ownerButton = anonFixture.nativeElement.querySelector(".search-owners-button") as HTMLElement;
        expect(ownerButton.hidden).toBe(true);
        anonFixture.destroy();
      } finally {
        stubUser.user = previousUser;
      }
    });
  });

  describe("dropdown checkbox handlers build the master filter list", () => {
    it("updateSelectedOwners emits an owner tag on masterFilterListChange", () => {
      const emissions: ReadonlyArray<string>[] = [];
      component.masterFilterListChange.subscribe(v => emissions.push([...v]));
      component.owners.find(o => o.userName === "Texera")!.checked = true;
      component.updateSelectedOwners();
      expect(component.selectedOwners).toEqual(["Texera"]);
      expect(component.masterFilterList).toEqual(["owner: Texera"]);
      expect(emissions).toContainEqual(["owner: Texera"]);
    });

    it("updateSelectedIDs emits an id tag on masterFilterListChange", () => {
      const emissions: ReadonlyArray<string>[] = [];
      component.masterFilterListChange.subscribe(v => emissions.push([...v]));
      component.wids.find(w => w.id === "2")!.checked = true;
      component.updateSelectedIDs();
      expect(component.selectedIDs).toEqual(["2"]);
      expect(component.masterFilterList).toEqual(["id: 2"]);
      expect(emissions).toContainEqual(["id: 2"]);
    });

    it("updateSelectedOperators emits an operator tag and records full operator metadata", () => {
      const emissions: ReadonlyArray<string>[] = [];
      component.masterFilterListChange.subscribe(v => emissions.push([...v]));
      component.operators.get("Analysis")!.find(o => o.userFriendlyName === "Sentiment Analysis")!.checked = true;
      component.updateSelectedOperators();
      expect(component.selectedOperators).toEqual([
        { userFriendlyName: "Sentiment Analysis", operatorType: "NlpSentiment", operatorGroup: "Analysis" },
      ]);
      expect(component.masterFilterList).toEqual(["operator: Sentiment Analysis"]);
      expect(emissions).toContainEqual(["operator: Sentiment Analysis"]);
    });
  });

  describe("updateDropdownMenus parses valid search tags", () => {
    it("checks the matching owner and records it as selected", () => {
      component.masterFilterList = ["owner: Texera"];
      expect(component.selectedOwners).toEqual(["Texera"]);
      expect(component.owners.find(o => o.userName === "Texera")!.checked).toBe(true);
      expect(component.masterFilterList).toEqual(["owner: Texera"]);
    });

    it("checks the matching workflow id and records it as selected", () => {
      component.masterFilterList = ["id: 3"];
      expect(component.selectedIDs).toEqual(["3"]);
      expect(component.wids.find(w => w.id === "3")!.checked).toBe(true);
      expect(component.masterFilterList).toEqual(["id: 3"]);
    });

    it("reconstructs a previously-selected operator and re-checks it in the dropdown map", () => {
      component.selectedOperators = [
        { userFriendlyName: "Sentiment Analysis", operatorType: "NlpSentiment", operatorGroup: "Analysis" },
      ];
      component.updateDropdownMenus(["operator: Sentiment Analysis"]);
      expect(component.selectedOperators).toEqual([
        { userFriendlyName: "Sentiment Analysis", operatorType: "NlpSentiment", operatorGroup: "Analysis" },
      ]);
      expect(component.operators.get("Analysis")!.find(o => o.userFriendlyName === "Sentiment Analysis")!.checked).toBe(
        true
      );
    });

    it("preserves a selected operator whose group is absent from the metadata map", () => {
      component.selectedOperators = [
        { userFriendlyName: "Phantom", operatorType: "PhantomOp", operatorGroup: "NonexistentGroup" },
      ];
      component.updateDropdownMenus(["operator: Phantom"]);
      // The operator is reconstructed even though operators.get(group) is undefined (no dropdown entries to re-check).
      expect(component.selectedOperators).toEqual([
        { userFriendlyName: "Phantom", operatorType: "PhantomOp", operatorGroup: "NonexistentGroup" },
      ]);
      expect(component.operators.has("NonexistentGroup")).toBe(false);
    });
  });

  describe("updateDropdownMenus rejects invalid search tags", () => {
    let errorSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      const notificationService = TestBed.inject(NotificationService);
      errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
    });

    it("reports an invalid owner name and removes the tag", () => {
      component.masterFilterList = ["owner: Nobody"];
      expect(errorSpy).toHaveBeenCalledWith("Invalid owner name");
      expect(component.masterFilterList).toEqual([]);
      expect(component.selectedOwners).toEqual([]);
    });

    it("reports an invalid workflow id and removes the tag", () => {
      component.masterFilterList = ["id: 999"];
      expect(errorSpy).toHaveBeenCalledWith("Invalid workflow id");
      expect(component.masterFilterList).toEqual([]);
      expect(component.selectedIDs).toEqual([]);
    });

    it("reports an invalid operator name and removes the tag", () => {
      component.masterFilterList = ["operator: Ghost Operator"];
      expect(errorSpy).toHaveBeenCalledWith("Invalid operator name");
      expect(component.masterFilterList).toEqual([]);
      expect(component.selectedOperators).toEqual([]);
    });
  });

  describe("updateDropdownMenus guards date tags", () => {
    let errorSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      const notificationService = TestBed.inject(NotificationService);
      errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
    });

    it("rejects a malformed ctime tag", () => {
      component.masterFilterList = ["ctime: not-a-date"];
      expect(errorSpy).toHaveBeenCalledWith("Date format is incorrect");
      expect(component.selectedCtime).toEqual([]);
    });

    it("rejects a malformed mtime tag", () => {
      component.masterFilterList = ["mtime: 22-01-2022"];
      expect(errorSpy).toHaveBeenCalledWith("Date format is incorrect");
      expect(component.selectedMtime).toEqual([]);
    });

    it("keeps only the first ctime tag when multiple are supplied", () => {
      component.masterFilterList = ["ctime: 2022-01-22 ~ 2022-04-21", "ctime: 2023-01-01 ~ 2023-02-02"];
      expect(errorSpy).toHaveBeenCalledWith("Multiple search dates is not allowed");
      expectDateRange(component.selectedCtime, [2022, 0, 22], [2022, 3, 21]);
      expect(component.masterFilterList).toEqual(["ctime: 2022-01-22 ~ 2022-04-21"]);
    });

    it("keeps only the first mtime tag when multiple are supplied", () => {
      component.masterFilterList = ["mtime: 2022-01-22 ~ 2022-04-21", "mtime: 2023-01-01 ~ 2023-02-02"];
      expect(errorSpy).toHaveBeenCalledWith("Multiple search dates is not allowed");
      expectDateRange(component.selectedMtime, [2022, 0, 22], [2022, 3, 21]);
      expect(component.masterFilterList).toEqual(["mtime: 2022-01-22 ~ 2022-04-21"]);
    });
  });

  describe("buildMasterFilterList date handling", () => {
    it("appends a ctime tag when none is present in the list", () => {
      const emissions: ReadonlyArray<string>[] = [];
      component.masterFilterListChange.subscribe(v => emissions.push([...v]));
      component.selectedCtime = [new Date(2022, 0, 1), new Date(2022, 0, 31)];
      component.buildMasterFilterList();
      expect(component.masterFilterList).toEqual(["ctime: 2022-01-01 ~ 2022-01-31"]);
      expect(emissions).toContainEqual(["ctime: 2022-01-01 ~ 2022-01-31"]);
    });

    it("formats two-digit months without zero padding", () => {
      component.selectedMtime = [new Date(2022, 10, 15), new Date(2022, 11, 20)];
      component.buildMasterFilterList();
      expect(component.masterFilterList).toEqual(["mtime: 2022-11-15 ~ 2022-12-20"]);
    });

    it("zero-pads single-digit months and days", () => {
      // Month index 8 -> "09" and days 5/9 -> "05"/"09" exercise the padding branch.
      component.selectedMtime = [new Date(2022, 8, 5), new Date(2022, 8, 9)];
      component.buildMasterFilterList();
      expect(component.masterFilterList).toEqual(["mtime: 2022-09-05 ~ 2022-09-09"]);
    });
  });

  describe("getSearchFilterParameters / getSearchKeywords", () => {
    it("assembles all selected filter parameters", () => {
      component.selectedCtime = [new Date(2022, 0, 1), new Date(2022, 0, 31)];
      component.selectedMtime = [new Date(2022, 1, 1), new Date(2022, 1, 28)];
      component.selectedOwners = ["Texera"];
      component.selectedIDs = ["1", "2"];
      component.selectedOperators = [
        { userFriendlyName: "Sentiment Analysis", operatorType: "NlpSentiment", operatorGroup: "Analysis" },
      ];
      expect(component.getSearchFilterParameters()).toEqual({
        createDateStart: new Date(2022, 0, 1),
        createDateEnd: new Date(2022, 0, 31),
        modifiedDateStart: new Date(2022, 1, 1),
        modifiedDateEnd: new Date(2022, 1, 28),
        owners: ["Texera"],
        ids: ["1", "2"],
        operators: ["NlpSentiment"],
      });
    });

    it("returns null dates and empty arrays when nothing is selected", () => {
      expect(component.getSearchFilterParameters()).toEqual({
        createDateStart: null,
        createDateEnd: null,
        modifiedDateStart: null,
        modifiedDateEnd: null,
        owners: [],
        ids: [],
        operators: [],
      });
    });

    it("getSearchKeywords returns only the plain workflow-name tags", () => {
      component.masterFilterList = ["hello", "world", "owner: Texera"];
      expect(component.masterFilterList).toEqual(["hello", "world", "owner: Texera"]);
      expect(component.getSearchKeywords()).toEqual(["hello", "world"]);
    });
  });

  describe("master-filter-list building and dropdown reset", () => {
    // These are private helpers reached through the public setter / build path;
    // cast to exercise them directly without going through the DOM.
    const asPrivate = () =>
      component as unknown as {
        checkIfWorkflowName: (tag: string) => boolean;
        updateMasterFilterList: (master: ReadonlyArray<string>, items: string[]) => string[];
        setMasterFilterList: (value: ReadonlyArray<string>, updateDropdown: boolean) => void;
        removeInvalidFilterTag: (tag: string) => void;
        setDropdownSelectionsToUnchecked: () => void;
      };

    it("checkIfWorkflowName distinguishes plain names from known-prefix filter tags", () => {
      const filters = asPrivate();
      // no colon -> a workflow name
      expect(filters.checkIfWorkflowName("my workflow")).toBe(true);
      // a known search-criteria prefix -> a filter tag, not a name
      expect(filters.checkIfWorkflowName("owner: alice")).toBe(false);
      // an unrecognized prefix -> still treated as a name
      expect(filters.checkIfWorkflowName("weird: value")).toBe(true);
    });

    it("updateMasterFilterList appends new tags and replaces the ctime tag in place", () => {
      const result = asPrivate().updateMasterFilterList(
        ["keyword", "ctime: 2022-01-01 ~ 2022-02-01"],
        ["keyword", "ctime: 2023-03-03 ~ 2023-04-04", "owner: alice"]
      );
      expect(result).toEqual(["keyword", "ctime: 2023-03-03 ~ 2023-04-04", "owner: alice"]);
    });

    it("updateMasterFilterList drops tags absent from the new list", () => {
      expect(asPrivate().updateMasterFilterList(["a", "b", "c"], ["a", "c"])).toEqual(["a", "c"]);
    });

    it("setMasterFilterList emits on masterFilterListChange only when the list changes", () => {
      const filters = asPrivate();
      const emitted: ReadonlyArray<string>[] = [];
      component.masterFilterListChange.subscribe(value => emitted.push(value));

      filters.setMasterFilterList(["owner: alice"], false);
      expect(component.masterFilterList).toEqual(["owner: alice"]);
      expect(emitted).toHaveLength(1);

      // identical content -> guarded, no re-emit
      filters.setMasterFilterList(["owner: alice"], false);
      expect(emitted).toHaveLength(1);
    });

    it("removeInvalidFilterTag drops the given tag from the master list", () => {
      const filters = asPrivate();
      filters.setMasterFilterList(["owner: alice", "keyword", "id: 5"], false);
      filters.removeInvalidFilterTag("id: 5");
      expect(component.masterFilterList).toEqual(["owner: alice", "keyword"]);
    });

    it("setDropdownSelectionsToUnchecked clears every dropdown checkbox", () => {
      component.owners = [{ userName: "alice", checked: true }];
      component.wids = [{ id: "1", checked: true }];
      component.operators = new Map([
        ["group", [{ userFriendlyName: "Scan", operatorType: "ScanSource", operatorGroup: "group", checked: true }]],
      ]);

      asPrivate().setDropdownSelectionsToUnchecked();

      expect(component.owners.every(owner => !owner.checked)).toBe(true);
      expect(component.wids.every(wid => !wid.checked)).toBe(true);
      expect(
        Array.from(component.operators.values())
          .flat()
          .every(operator => !operator.checked)
      ).toBe(true);
    });
  });
});

/** The bar is shared by several pages; these pin that it sources owners and ids per kind. */
describe("FiltersComponent per-resource owners", () => {
  let fixture: ComponentFixture<FiltersComponent>;
  let component: FiltersComponent;
  let datasetOwners: ReturnType<typeof vi.fn>;
  let workflowOwners: ReturnType<typeof vi.fn>;
  let workflowIds: ReturnType<typeof vi.fn>;
  let modelOwners: ReturnType<typeof vi.fn>;

  /** The input has to be set before ngOnInit reads it. */
  async function render(entityType?: EntityType | null): Promise<void> {
    datasetOwners = vi.fn(() => of(["dataset-owner"]));
    modelOwners = vi.fn(() => of(["model-owner"]));
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
        { provide: ModelService, useValue: { retrieveOwners: modelOwners } },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: UserService, useClass: StubUserService },
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

  it("defaults to workflows, which the Your Work workflows page still relies on", async () => {
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

  it("unions every kind's owners for a page that lists them all", async () => {
    // The search page's All tab, where there is no single kind to ask about.
    await render(null);

    expect(workflowOwners).toHaveBeenCalled();
    expect(datasetOwners).toHaveBeenCalled();
    expect(component.owners.map(owner => owner.userName)).toEqual(["workflow-owner", "dataset-owner", "model-owner"]);
  });

  it("keeps the workflow id filter on a page that lists every kind", async () => {
    // That page lists workflows too, and the backend binds `id=` to the workflow arm.
    await render(null);

    expect(component.hasIdFilter).toBe(true);
    expect(component.wids.map(wid => wid.id)).toEqual(["7"]);
  });

  it("reloads the owners when the listed kind changes", async () => {
    await render(EntityType.Workflow);
    expect(component.owners.map(owner => owner.userName)).toEqual(["workflow-owner"]);

    component.entityType = EntityType.Dataset;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Workflow, EntityType.Dataset, false) });
    fixture.detectChanges();

    expect(component.owners.map(owner => owner.userName)).toEqual(["dataset-owner"]);
  });

  it("loads the owners once on first render, not twice", async () => {
    await render(EntityType.Workflow);
    // ngOnChanges runs before ngOnInit; only ngOnInit may load, or every page pays two requests.
    component.ngOnChanges({ entityType: new SimpleChange(undefined, EntityType.Workflow, true) });

    expect(workflowOwners).toHaveBeenCalledTimes(1);
  });

  it("reloads the ids too, so the id filter works after a tab switch", async () => {
    // Datasets have no id endpoint, so switching back to workflows must refetch them, or the id
    // button opens on an empty menu and a typed id is rejected as invalid.
    await render(EntityType.Dataset);
    expect(component.wids).toEqual([]);

    component.entityType = EntityType.Workflow;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Dataset, EntityType.Workflow, false) });

    expect(workflowIds).toHaveBeenCalled();
    expect(component.wids.map(wid => wid.id)).toEqual(["7"]);
  });

  it("lets a tab switch cancel the load already in flight", async () => {
    // The init fetch runs through the same subject as a reload, so switchMap can cancel it. A slow
    // init response landing after a switch would otherwise refill the facet with the old kind.
    await render(EntityType.Dataset);
    const slowWorkflowOwners = new Subject<string[]>();
    workflowOwners.mockReturnValue(slowWorkflowOwners.asObservable());

    // A second component, so the slow response is the one its init is waiting on.
    const pending = TestBed.createComponent(FiltersComponent);
    pending.componentInstance.entityType = EntityType.Workflow;
    pending.detectChanges();
    expect(pending.componentInstance.owners).toEqual([]);

    pending.componentInstance.entityType = EntityType.Dataset;
    pending.componentInstance.ngOnChanges({
      entityType: new SimpleChange(EntityType.Workflow, EntityType.Dataset, false),
    });
    expect(pending.componentInstance.owners.map(owner => owner.userName)).toEqual(["dataset-owner"]);

    // The superseded request answering late must not put workflow owners on the Datasets tab.
    slowWorkflowOwners.next(["workflow-owner"]);
    slowWorkflowOwners.complete();

    expect(pending.componentInstance.owners.map(owner => owner.userName)).toEqual(["dataset-owner"]);
    pending.destroy();
  });

  it("survives a failing id request, and keeps reloading afterwards", async () => {
    // The error has to happen on a reload that actually fetches ids, so start on datasets, which
    // have no id endpoint, and switch to workflows with the id request failing.
    await render(EntityType.Dataset);
    workflowIds.mockReturnValue(throwError(() => new Error("boom")));

    component.entityType = EntityType.Workflow;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Dataset, EntityType.Workflow, false) });

    // The owner facet still lands: a failed id request costs its own facet, not the other one.
    expect(component.owners.map(owner => owner.userName)).toEqual(["workflow-owner"]);
    expect(component.wids).toEqual([]);

    // And the subscription is still alive, so later switches keep working.
    workflowIds.mockReturnValue(of([7]));
    component.entityType = EntityType.Dataset;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Workflow, EntityType.Dataset, false) });
    expect(component.owners.map(owner => owner.userName)).toEqual(["dataset-owner"]);

    component.entityType = EntityType.Workflow;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Dataset, EntityType.Workflow, false) });
    expect(component.wids.map(wid => wid.id)).toEqual(["7"]);
  });

  it("keeps an id tag that the new kind still offers", async () => {
    await render(EntityType.Dataset);
    component.entityType = EntityType.Workflow;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Dataset, EntityType.Workflow, false) });
    component.masterFilterList = ["id: 7"];

    expect(component.selectedIDs).toEqual(["7"]);
  });

  it("drops an owner tag the new kind has no owner for, and says so", async () => {
    await render(EntityType.Workflow);
    // Injected after render: TestBed cannot be configured once it has been instantiated.
    const error = vi.spyOn(TestBed.inject(NotificationService), "error");
    component.masterFilterList = ["owner: workflow-owner"];
    expect(component.selectedOwners).toEqual(["workflow-owner"]);

    component.entityType = EntityType.Dataset;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Workflow, EntityType.Dataset, false) });

    // Silently keeping it would leave the page empty with no way to tell why.
    expect(component.selectedOwners).toEqual([]);
    expect(error).toHaveBeenCalledWith("Invalid owner name");
  });

  it("keeps an owner tag that the new kind also has", async () => {
    await render(EntityType.Workflow);
    // After render: the helper rebuilds the doubles, so a per-test value has to be set here.
    datasetOwners.mockReturnValue(of(["workflow-owner"]));
    component.masterFilterList = ["owner: workflow-owner"];

    component.entityType = EntityType.Dataset;
    component.ngOnChanges({ entityType: new SimpleChange(EntityType.Workflow, EntityType.Dataset, false) });

    expect(component.selectedOwners).toEqual(["workflow-owner"]);
  });
});
