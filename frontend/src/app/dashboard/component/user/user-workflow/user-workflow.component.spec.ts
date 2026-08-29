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
import { By } from "@angular/platform-browser";
import { RouterTestingModule } from "@angular/router/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { UserWorkflowComponent } from "./user-workflow.component";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
import { StubWorkflowPersistService } from "../../../../common/service/workflow-persist/stub-workflow-persist.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { DashboardWorkflow } from "../../../type/dashboard-workflow.interface";
import { NgbdModalAddProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { NgbdModalRemoveProjectWorkflowComponent } from "../user-project/user-project-section/ngbd-modal-remove-project-workflow/ngbd-modal-remove-project-workflow.component";
import { ShareAccessComponent } from "../share-access/share-access.component";
import { ShareAccessService } from "../../../service/user/share-access/share-access.service";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzListModule } from "ng-zorro-antd/list";
import { NzCalendarModule } from "ng-zorro-antd/calendar";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzPopoverModule } from "ng-zorro-antd/popover";
import { NzDatePickerModule } from "ng-zorro-antd/date-picker";
import { en_US, NZ_I18N } from "ng-zorro-antd/i18n";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { OperatorMetadataService } from "../../../../workspace/service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../../workspace/service/operator-metadata/stub-operator-metadata.service";
import { NzUploadModule } from "ng-zorro-antd/upload";
import { ScrollingModule } from "@angular/cdk/scrolling";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import {
  mockUserInfo,
  testWorkflowContent,
  testWorkflowEntries,
  testWorkflowFileNameConflictEntries,
} from "../../user-dashboard-test-fixtures";
import { FiltersComponent } from "../filters/filters.component";
import { UserWorkflowListItemComponent } from "./user-workflow-list-item/user-workflow-list-item.component";
import { UserProjectService } from "../../../service/user/project/user-project.service";
import { StubUserProjectService } from "../../../service/user/project/stub-user-project.service";
import { SearchService } from "../../../service/user/search.service";
import { StubSearchService } from "../../../service/user/stub-search.service";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { delay, firstValueFrom, of, throwError } from "rxjs";
import JSZip from "jszip";
import { ModalOptions, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import { DownloadService } from "../../../service/user/download/download.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Router } from "@angular/router";
import { USER_WORKSPACE } from "../../../../app-routing.constant";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../../common/service/gui-config.service.mock";
import { NotebookMigrationService } from "../../../../workspace/service/notebook-migration/notebook-migration.service";
import { LlmRequestTimeoutError } from "../../../../workspace/service/notebook-migration/migration-llm";
import { NotebookImportModalComponent } from "../../../../workspace/component/notebook-import-modal/notebook-import-modal.component";
import { NzUploadFile } from "ng-zorro-antd/upload";
import type { Mocked } from "vitest";
describe("SavedWorkflowSectionComponent", () => {
  let component: UserWorkflowComponent;
  let fixture: ComponentFixture<UserWorkflowComponent>;

  let downloadServiceSpy: Mocked<DownloadService>;

  beforeEach(async () => {
    downloadServiceSpy = { downloadWorkflowsAsZip: vi.fn() } as unknown as Mocked<DownloadService>;

    await TestBed.configureTestingModule({
      providers: [
        NzModalService,
        { provide: WorkflowPersistService, useValue: new StubWorkflowPersistService(testWorkflowEntries) },
        { provide: UserProjectService, useValue: new StubUserProjectService() },
        ShareAccessService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: NZ_I18N, useValue: en_US },
        { provide: UserService, useClass: StubUserService },
        {
          provide: SearchService,
          useValue: new StubSearchService(testWorkflowEntries, mockUserInfo),
        },
        { provide: DownloadService, useValue: downloadServiceSpy },
        ...commonTestProviders,
      ],
      imports: [
        UserWorkflowComponent,
        ShareAccessComponent,
        FiltersComponent,
        UserWorkflowListItemComponent,
        SearchResultsComponent,
        FormsModule,
        RouterTestingModule,
        HttpClientTestingModule,
        ReactiveFormsModule,
        NzDropDownModule,
        NzCardModule,
        NzListModule,
        NzCalendarModule,
        NzDatePickerModule,
        NzSelectModule,
        NzPopoverModule,
        NzAvatarModule,
        NzTooltipModule,
        NzUploadModule,
        ScrollingModule,
        NoopAnimationsModule,
        NzButtonModule,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserWorkflowComponent);
    component = fixture.componentInstance;
    component.filters = TestBed.createComponent(FiltersComponent).componentInstance;
    component.filters.masterFilterList = [];
    component.filters.selectedMtime = [];
    component.filters.selectedMtime = [];
    component.searchResultsComponent = TestBed.createComponent(SearchResultsComponent).componentInstance;
    fixture.detectChanges();
  });

  // TODO: add this test case back and figure out why it failed
  // it.skip("Modal Opened, then Closed", () => {
  //   const modalRef: NgbModalRef = modalService.open(NgbdModalWorkflowShareAccessComponent);
  //   vi.spyOn(modalService, "open").mockReturnValue(modalRef);
  //   component.onClickOpenShareAccess(testWorkflowEntries[0]);
  //   expect(modalService.open).toHaveBeenCalled();
  //   fixture.detectChanges();
  //   modalRef.dismiss();
  // });
  const waitForLoading = async () => {
    while (component.searchResultsComponent.loading) {
      await delay(10);
    }
  };

  it("searchNoInput", async () => {
    // When no search input is provided, it should show all workflows.
    await component.search();
    expect(component.searchResultsComponent.loading).toBe(false);
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3", "workflow 4", "workflow 5"]);
    console.log("Master Filter List:", component.filters.masterFilterList);

    expect(component.filters.masterFilterList).toEqual([]);
  });

  it("searchByWorkflowName", async () => {
    // If the name "workflow 5" is entered as a single phrase, only workflow 5 should be returned, rather
    // than all containing the keyword "workflow".
    component.filters.masterFilterList = ["workflow 5"];
    await waitForLoading();
    expect(component.searchResultsComponent.loading).toBe(false);
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 5"]);
    expect(component.filters.masterFilterList).toEqual(["workflow 5"]);
  });

  it("searchByOwners", async () => {
    // If the owner filter is applied, only those workflow ownered by that user should be returned.
    component.filters.owners[0].checked = true;
    component.filters.updateSelectedOwners();
    await waitForLoading();
    expect(component.searchResultsComponent.loading).toBe(false);
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2"]);
    expect(component.filters.masterFilterList).toEqual(["owner: Texera"]);
  });

  it("searchByIDs", async () => {
    // If the ID filter is applied, only those workflows should be returned.
    component.filters.wids[0].checked = true;
    component.filters.wids[1].checked = true;
    component.filters.wids[2].checked = true;
    component.filters.updateSelectedIDs();
    await waitForLoading();
    expect(component.searchResultsComponent.loading).toBe(false);
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["id: 1", "id: 2", "id: 3"]);
  });

  it("searchByProjects", async () => {
    component.filters.userProjectsDropdown = [
      { pid: 1, name: "Project1", checked: false },
      { pid: 2, name: "Project2", checked: false },
      { pid: 3, name: "Project3", checked: false },
    ];

    // If the project filter is applied, only those workflows belonging to those projects should be returned.
    component.filters.userProjectsDropdown[0].checked = true;
    component.filters.updateSelectedProjects();
    await waitForLoading();
    expect(component.searchResultsComponent.loading).toBe(false);
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["project: Project1"]);
  });

  it("searchByCreationTime", async () => {
    // If the creation time filter is applied, only those workflows matching the date range should be returned.
    component.filters.selectedCtime = [new Date(1970, 0, 3), new Date(1981, 2, 13)];
    component.filters.buildMasterFilterList();
    await waitForLoading();
    expect(component.searchResultsComponent.loading).toBe(false);
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.filters.masterFilterList).toEqual(["ctime: 1970-01-03 ~ 1981-03-13"]);
  });

  it("searchByModifyTime", async () => {
    // If the modified time filter is applied, only those workflows matching the date range should be returned.
    component.filters.selectedMtime = [new Date(1970, 0, 3), new Date(1981, 2, 13)];
    component.filters.buildMasterFilterList();
    await waitForLoading();
    expect(component.searchResultsComponent.loading).toBe(false);
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 4", "workflow 5"]);
    expect(component.filters.masterFilterList).toEqual(["mtime: 1970-01-03 ~ 1981-03-13"]);
  });

  /*
   * To add operators to this test:
   *   1. Check if the operator's group is true
   *   2. Mark the selected operator "checked" as true
   *   3. Push the operator's operatorType to operatorSelectionList
   *   4. Update masterFilterList to have the correct tags
   *
   *   - Recommendation: print out the component.operators after the operatorDropdownRequest is made
   *
   *   - See searchByManyOperators test
   */
  it("searchByOperators", async () => {
    // If a single operator filter is provided, only the workflows containing that operator should be returned.
    const operatorGroup = component.filters.operators.get("Analysis");
    if (operatorGroup) {
      operatorGroup[2].checked = true; // sentiment analysis
      component.filters.updateSelectedOperators();
    }
    await waitForLoading();
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["operator: Sentiment Analysis"]); // userFriendlyName
  });

  it("searchByManyOperators", async () => {
    // If a multiple operator filters are provided, workflows containing any of the provided operators should be returned.
    const operatorGroup = component.filters.operators.get("Analysis");
    const operatorGroup2 = component.filters.operators.get("View Results");
    if (operatorGroup && operatorGroup2) {
      operatorGroup[2].checked = true; // sentiment analysis
      operatorGroup2[0].checked = true;
      component.filters.updateSelectedOperators();
    }
    await waitForLoading();
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 1", "workflow 2", "workflow 3"]);
    expect(component.filters.masterFilterList).toEqual(["operator: Sentiment Analysis", "operator: View Results"]); // userFriendlyName
  });

  it("searchByManyParameters", async () => {
    // Apply the project, ID, owner, and operator filter all at once.
    component.filters.masterFilterList = ["1"];
    const operatorGroup = component.filters.operators.get("Analysis");
    if (operatorGroup) {
      operatorGroup[3].checked = true; // Aggregation operator
      component.filters.updateSelectedOperators();
      component.filters.userProjectsDropdown = [
        { pid: 1, name: "Project1", checked: false },
        { pid: 2, name: "Project2", checked: false },
        { pid: 3, name: "Project3", checked: false },
      ];

      component.filters.owners[0].checked = true; //Texera
      component.filters.owners[1].checked = true; //Angular
      component.filters.wids[0].checked = true;
      component.filters.wids[1].checked = true;
      component.filters.wids[2].checked = true; //id 1,2,3
      component.filters.userProjectsDropdown[0].checked = true; //Project 1
      component.filters.selectedCtime = [new Date(1970, 0, 1), new Date(1973, 2, 11)];
      component.filters.selectedMtime = [new Date(1970, 0, 1), new Date(1982, 3, 14)];
      //add/select new search parameter here

      component.filters.updateSelectedProjects();
      component.filters.updateSelectedIDs();
      component.filters.updateSelectedOwners();
    }
    await waitForLoading();
    await component.search();
    const SortedCase = component.searchResultsComponent.entries.map(workflow => workflow.name);
    expect(SortedCase).toEqual(["workflow 1"]);
    expect(component.filters.masterFilterList).toEqual(
      expect.arrayContaining([
        "1",
        "owner: Texera",
        "owner: Angular",
        "id: 1",
        "id: 2",
        "id: 3",
        "operator: Aggregation",
        "project: Project1",
        "ctime: 1970-01-01 ~ 1973-03-11",
        "mtime: 1970-01-01 ~ 1982-04-14",
      ])
    );
  });

  describe("onClickCreateNewWorkflowFromDashboard", () => {
    it("navigates to /user/workflow/<wid> (no /dashboard prefix) on successful creation", () => {
      const router = TestBed.inject(Router);
      const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);
      const persist = TestBed.inject(WorkflowPersistService) as any;
      // StubWorkflowPersistService doesn't define createWorkflow — assign the
      // method here so the component's call resolves to a controlled observable.
      persist.createWorkflow = vi.fn().mockReturnValue(of({ workflow: { wid: 99 } }));
      component.pid = undefined;

      component.onClickCreateNewWorkflowFromDashboard();

      expect(navigateSpy).toHaveBeenCalledWith([USER_WORKSPACE, 99]);
      expect(USER_WORKSPACE).toBe("/user/workflow");
    });
  });

  describe("AI generate workflow (dashboard entry point)", () => {
    const ipynbFile = { name: "analysis.ipynb" } as NzUploadFile;
    const AI_BUTTON_SELECTOR = 'button[title="AI generate a workflow from a Python notebook"]';

    // Opens the modal and returns the requestImport callback the component handed to it; calling
    // it runs the full generation (true => generation succeeded and navigated, false => stay open).
    function getRequestImport(): (file: NzUploadFile, model: string) => Promise<boolean> {
      const modalService = TestBed.inject(NzModalService);
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as unknown as NzModalRef);
      component.openAiGenerateModal();
      const config = createSpy.mock.calls[0][0] as ModalOptions;
      return (config.nzData as { requestImport: (file: NzUploadFile, model: string) => Promise<boolean> })
        .requestImport;
    }

    // Wires the NotebookMigrationService + persistence mocks for a successful generation.
    function mockGenerationSuccess(wid = 99) {
      const migration = TestBed.inject(NotebookMigrationService);
      vi.spyOn(migration, "parseAndTagNotebook").mockResolvedValue({ cells: [] } as any);
      vi.spyOn(migration, "sendToAIGenerateWorkflow").mockResolvedValue({
        workflowContent: { operators: [] },
        mappingContent: { operator_to_cell: {}, cell_to_operator: {} },
      } as any);
      const storeSpy = vi.spyOn(migration, "storeNotebookAndMapping").mockReturnValue(of({ success: true }) as any);
      const persist = TestBed.inject(WorkflowPersistService) as any;
      persist.createWorkflow = vi.fn().mockReturnValue(of({ workflow: { wid } }));
      return { storeSpy, persist };
    }

    it("openAiGenerateModal opens the NotebookImportModalComponent with a requestImport callback and no footer", () => {
      const modalService = TestBed.inject(NzModalService);
      const createSpy = vi.spyOn(modalService, "create").mockReturnValue({} as unknown as NzModalRef);

      component.openAiGenerateModal();

      expect(createSpy).toHaveBeenCalledTimes(1);
      const config = createSpy.mock.calls[0][0] as ModalOptions;
      expect(config.nzContent).toBe(NotebookImportModalComponent);
      expect(config.nzFooter).toBeNull();
      expect(typeof (config.nzData as { requestImport: unknown }).requestImport).toBe("function");
    });

    it("generates a workflow, stores the notebook and mapping, navigates with autolayout, and resolves true", async () => {
      const { storeSpy, persist } = mockGenerationSuccess(99);
      const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      component.pid = undefined;

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(persist.createWorkflow).toHaveBeenCalledTimes(1);
      // Name is derived from the notebook filename with the generated marker.
      expect(persist.createWorkflow.mock.calls[0][1]).toBe("analysis_GENERATED_BY_LLM");
      // The dashboard hands off the key + vid ownership to the service in one call.
      expect(storeSpy).toHaveBeenCalledWith(99, expect.anything(), expect.anything());
      expect(navigateSpy).toHaveBeenCalledWith([USER_WORKSPACE, 99], { queryParams: { autolayout: 1 } });
      expect(proceed).toBe(true);
    });

    it("truncates a long notebook basename so the generated name fits the 128-char column", async () => {
      const { persist } = mockGenerationSuccess(99);
      vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);

      await getRequestImport()({ name: "a".repeat(200) + ".ipynb" } as NzUploadFile, "gpt-4");

      const createdName = persist.createWorkflow.mock.calls[0][1] as string;
      expect(createdName.length).toBe(128);
      expect(createdName.endsWith("_GENERATED_BY_LLM")).toBe(true);
    });

    it("saves but does not navigate when the component was destroyed mid-generation", async () => {
      const { persist } = mockGenerationSuccess(99);
      const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      const infoSpy = vi.spyOn(TestBed.inject(NotificationService), "info").mockImplementation(() => {});
      const requestImport = getRequestImport();
      component.ngOnDestroy();

      const proceed = await requestImport(ipynbFile, "gpt-4");

      // The workflow is still created and saved, but the user is not yanked into the workspace.
      expect(persist.createWorkflow).toHaveBeenCalledTimes(1);
      expect(navigateSpy).not.toHaveBeenCalled();
      expect(infoSpy).toHaveBeenCalledWith("Workflow generated and saved to your dashboard.");
      expect(proceed).toBe(true);
    });

    it("adds the new workflow to the current project when opened inside one", async () => {
      mockGenerationSuccess(99);
      vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      const projectService = TestBed.inject(UserProjectService) as any;
      const addSpy = vi.spyOn(projectService, "addWorkflowToProject").mockReturnValue(of(undefined));
      component.pid = 5;

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(addSpy).toHaveBeenCalledWith(5, 99);
      expect(proceed).toBe(true);
    });

    it("rejects a non-ipynb file: errors, resolves false, and generates nothing", async () => {
      const parseSpy = vi.spyOn(TestBed.inject(NotebookMigrationService), "parseAndTagNotebook");
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});

      const proceed = await getRequestImport()({ name: "data.txt" } as NzUploadFile, "gpt-4");

      expect(proceed).toBe(false);
      expect(errorSpy).toHaveBeenCalledWith("Please upload a valid Jupyter Notebook (.ipynb) file.");
      expect(parseSpy).not.toHaveBeenCalled();
    });

    it("reports a parse failure and resolves false without calling the LLM", async () => {
      const migration = TestBed.inject(NotebookMigrationService);
      vi.spyOn(migration, "parseAndTagNotebook").mockRejectedValue(new Error("bad json"));
      const llmSpy = vi.spyOn(migration, "sendToAIGenerateWorkflow");
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(proceed).toBe(false);
      expect(errorSpy).toHaveBeenCalledWith("Failed to read the notebook file. Please upload a valid .ipynb file.");
      expect(llmSpy).not.toHaveBeenCalled();
    });

    it("reports an LLM failure and resolves false without creating a workflow", async () => {
      const migration = TestBed.inject(NotebookMigrationService);
      vi.spyOn(migration, "parseAndTagNotebook").mockResolvedValue({ cells: [] } as any);
      vi.spyOn(migration, "sendToAIGenerateWorkflow").mockRejectedValue(new Error("LLM down"));
      const persist = TestBed.inject(WorkflowPersistService) as any;
      persist.createWorkflow = vi.fn();
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(proceed).toBe(false);
      expect(errorSpy).toHaveBeenCalledWith("Error while communicating with the LLM, check console for details.");
      expect(persist.createWorkflow).not.toHaveBeenCalled();
    });

    it("reports a distinct timeout message when generation times out", async () => {
      const migration = TestBed.inject(NotebookMigrationService);
      vi.spyOn(migration, "parseAndTagNotebook").mockResolvedValue({ cells: [] } as any);
      vi.spyOn(migration, "sendToAIGenerateWorkflow").mockRejectedValue(new LlmRequestTimeoutError(10));
      const persist = TestBed.inject(WorkflowPersistService) as any;
      persist.createWorkflow = vi.fn();
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(proceed).toBe(false);
      expect(errorSpy).toHaveBeenCalledWith(
        "Generation timed out after 10 minutes. Try again, choose a faster model, or simplify the notebook."
      );
      expect(persist.createWorkflow).not.toHaveBeenCalled();
    });

    it("reports a save failure and resolves false when the created workflow has no wid", async () => {
      const migration = TestBed.inject(NotebookMigrationService);
      vi.spyOn(migration, "parseAndTagNotebook").mockResolvedValue({ cells: [] } as any);
      vi.spyOn(migration, "sendToAIGenerateWorkflow").mockResolvedValue({
        workflowContent: {},
        mappingContent: {},
      } as any);
      const storeSpy = vi.spyOn(migration, "storeNotebookAndMapping");
      const persist = TestBed.inject(WorkflowPersistService) as any;
      persist.createWorkflow = vi.fn().mockReturnValue(of({ workflow: {} }));
      const errorSpy = vi.spyOn(TestBed.inject(NotificationService), "error").mockImplementation(() => {});

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(proceed).toBe(false);
      expect(errorSpy).toHaveBeenCalledWith("Failed to save the generated workflow, check console for details.");
      expect(storeSpy).not.toHaveBeenCalled();
    });

    it("still opens the workflow when adding it to the project fails (best effort)", async () => {
      mockGenerationSuccess(99);
      const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      const projectService = TestBed.inject(UserProjectService) as any;
      vi.spyOn(projectService, "addWorkflowToProject").mockReturnValue(throwError(() => new Error("project down")));
      component.pid = 5;

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(navigateSpy).toHaveBeenCalledWith([USER_WORKSPACE, 99], { queryParams: { autolayout: 1 } });
      expect(proceed).toBe(true);
    });

    it("warns but still opens the workflow when storing the notebook fails (no re-generation)", async () => {
      const { storeSpy, persist } = mockGenerationSuccess(99);
      storeSpy.mockReturnValue(throwError(() => new Error("store down")) as any);
      const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
      const warnSpy = vi.spyOn(TestBed.inject(NotificationService), "warning").mockImplementation(() => {});
      component.pid = undefined;

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      // The created workflow is kept and opened; the LLM call is not re-run.
      expect(persist.createWorkflow).toHaveBeenCalledTimes(1);
      expect(warnSpy).toHaveBeenCalledWith(
        "Workflow created, but the notebook could not be attached; the Jupyter panel may not open."
      );
      expect(navigateSpy).toHaveBeenCalledWith([USER_WORKSPACE, 99], { queryParams: { autolayout: 1 } });
      expect(proceed).toBe(true);
    });

    it("warns but resolves true when navigation is blocked after the workflow is created", async () => {
      mockGenerationSuccess(99);
      vi.spyOn(TestBed.inject(Router), "navigate").mockRejectedValue(new Error("blocked"));
      const warnSpy = vi.spyOn(TestBed.inject(NotificationService), "warning").mockImplementation(() => {});
      component.pid = undefined;

      const proceed = await getRequestImport()(ipynbFile, "gpt-4");

      expect(warnSpy).toHaveBeenCalledWith("Workflow created. You can open it from your dashboard.");
      expect(proceed).toBe(true);
    });

    it("shows the button only when the migration flag is enabled", () => {
      expect(fixture.nativeElement.querySelector(AI_BUTTON_SELECTOR)).toBeNull();

      (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
        pythonNotebookMigrationEnabled: true,
      });
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector(AI_BUTTON_SELECTOR)).not.toBeNull();
    });

    it("clicking the toolbar button opens the AI generate modal", () => {
      (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
        pythonNotebookMigrationEnabled: true,
      });
      fixture.detectChanges();
      const openSpy = vi.spyOn(component, "openAiGenerateModal").mockImplementation(() => {});

      const button = fixture.nativeElement.querySelector(AI_BUTTON_SELECTOR) as HTMLButtonElement;
      button.click();

      expect(openSpy).toHaveBeenCalled();
    });
  });

  it("downloads checked files", async () => {
    // If multiple workflows in a single batch download have name conflicts, rename them as workflow-1, workflow-2, etc.
    component.searchResultsComponent.entries = component.searchResultsComponent.entries.concat(
      testWorkflowFileNameConflictEntries
    );
    testWorkflowFileNameConflictEntries[0].checked = true;
    testWorkflowFileNameConflictEntries[2].checked = true;

    downloadServiceSpy.downloadWorkflowsAsZip.mockReturnValue(of(new Blob()));

    await component.onClickOpenDownloadZip();

    expect(downloadServiceSpy.downloadWorkflowsAsZip).toHaveBeenCalledTimes(1);
    expect(downloadServiceSpy.downloadWorkflowsAsZip).toHaveBeenCalledWith([
      {
        id: testWorkflowFileNameConflictEntries[0].workflow.workflow.wid!,
        name: testWorkflowFileNameConflictEntries[0].workflow.workflow.name,
      },
      {
        id: testWorkflowFileNameConflictEntries[2].workflow.workflow.wid!,
        name: testWorkflowFileNameConflictEntries[2].workflow.workflow.name,
      },
    ]);

    // Check that the checked entries are unchecked after download
    expect(testWorkflowFileNameConflictEntries[0].checked).toBe(true);
    expect(testWorkflowFileNameConflictEntries[2].checked).toBe(true);
  });

  describe("additional UserWorkflowComponent behaviors", () => {
    const VIEW_MODE_KEY = "texera.userWorkflow.viewMode";

    const makeDashboardWorkflow = (wid: number | undefined, name: string): DashboardWorkflow => ({
      workflow: {
        wid,
        name,
        description: "desc",
        content: testWorkflowContent([]),
        creationTime: 0,
        lastModifiedTime: 0,
        isPublished: 0,
        readonly: false,
      },
      isOwner: true,
      ownerName: "Texera",
      accessLevel: "Write",
      projectIDs: [],
      ownerId: 1,
      coverImage: null,
    });

    const makeEntry = (wid: number | undefined, name: string, checked = false): DashboardEntry => {
      const entry = new DashboardEntry(makeDashboardWorkflow(wid, name));
      entry.checked = checked;
      return entry;
    };

    const setEntries = (entries: DashboardEntry[]): void => {
      component.searchResultsComponent.entries = entries;
    };

    afterEach(() => {
      vi.restoreAllMocks();
      localStorage.removeItem(VIEW_MODE_KEY);
    });

    describe("deleteWorkflow", () => {
      it("deletes an entry with a wid, removes it from the results, and cleans up its pod notebook", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.deleteWorkflow = vi.fn().mockReturnValue(of(null));
        const cleanup = vi
          .spyOn(TestBed.inject(NotebookMigrationService), "deleteNotebookForWorkflow")
          .mockResolvedValue(undefined);
        const target = makeEntry(5, "to delete");
        setEntries([target, makeEntry(6, "keep")]);

        component.deleteWorkflow(target);

        expect(persist.deleteWorkflow).toHaveBeenCalledWith([5]);
        expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["keep"]);
        expect(cleanup).toHaveBeenCalledWith(5);
      });

      it("does nothing when the entry has no wid", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.deleteWorkflow = vi.fn();
        const cleanup = vi.spyOn(TestBed.inject(NotebookMigrationService), "deleteNotebookForWorkflow");

        component.deleteWorkflow(makeEntry(undefined, "no wid"));

        expect(persist.deleteWorkflow).not.toHaveBeenCalled();
        expect(cleanup).not.toHaveBeenCalled();
      });
    });

    describe("onClickDuplicateSelectedWorkflows", () => {
      it("duplicates checked wids without a pid and prepends the new entries", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.duplicateWorkflow = vi
          .fn()
          .mockReturnValue(of([makeDashboardWorkflow(101, "dup a"), makeDashboardWorkflow(102, "dup b")]));
        component.pid = undefined;
        setEntries([makeEntry(1, "wf 1", true), makeEntry(2, "wf 2", true), makeEntry(3, "wf 3", false)]);

        component.onClickDuplicateSelectedWorkflows();

        expect(persist.duplicateWorkflow).toHaveBeenCalledWith([1, 2]);
        expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual([
          "dup a",
          "dup b",
          "wf 1",
          "wf 2",
          "wf 3",
        ]);
      });

      it("passes the pid to duplicateWorkflow when the section belongs to a project", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.duplicateWorkflow = vi.fn().mockReturnValue(of([makeDashboardWorkflow(101, "dup a")]));
        component.pid = 9;
        setEntries([makeEntry(1, "wf 1", true), makeEntry(2, "wf 2", true)]);

        component.onClickDuplicateSelectedWorkflows();

        expect(persist.duplicateWorkflow).toHaveBeenCalledWith([1, 2], 9);
      });

      it("early-returns without calling the service when a checked entry has no wid", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.duplicateWorkflow = vi.fn();
        setEntries([makeEntry(1, "wf 1", true), makeEntry(undefined, "wf 2", true)]);

        component.onClickDuplicateSelectedWorkflows();

        expect(persist.duplicateWorkflow).not.toHaveBeenCalled();
      });

      it("alerts on a duplication error", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.duplicateWorkflow = vi.fn().mockReturnValue(throwError(() => "boom"));
        const alertSpy = vi.spyOn(window, "alert").mockImplementation(() => {});
        component.pid = undefined;
        setEntries([makeEntry(1, "wf 1", true)]);

        component.onClickDuplicateSelectedWorkflows();

        expect(alertSpy).toHaveBeenCalledWith("boom");
      });
    });

    describe("handleConfirmDeleteSelectedWorkflows", () => {
      it("deletes checked wids, keeps undefined-wid entries, and cleans up each pod notebook", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.deleteWorkflow = vi.fn().mockReturnValue(of(null));
        const cleanup = vi
          .spyOn(TestBed.inject(NotebookMigrationService), "deleteNotebookForWorkflow")
          .mockResolvedValue(undefined);
        setEntries([
          makeEntry(1, "a", true),
          makeEntry(2, "b", true),
          makeEntry(undefined, "c", false),
          makeEntry(3, "d", false),
        ]);

        component.handleConfirmDeleteSelectedWorkflows();

        expect(persist.deleteWorkflow).toHaveBeenCalledWith([1, 2]);
        expect(component.searchResultsComponent.entries.map(e => e.name)).toEqual(["c", "d"]);
        expect(cleanup.mock.calls.map(c => c[0])).toEqual([1, 2]);
      });

      it("early-returns when a checked entry has no wid", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.deleteWorkflow = vi.fn();
        setEntries([makeEntry(undefined, "a", true)]);

        component.handleConfirmDeleteSelectedWorkflows();

        expect(persist.deleteWorkflow).not.toHaveBeenCalled();
      });

      it("alerts on a deletion error and does not touch the pod", () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.deleteWorkflow = vi.fn().mockReturnValue(throwError(() => "delfail"));
        const alertSpy = vi.spyOn(window, "alert").mockImplementation(() => {});
        const cleanup = vi.spyOn(TestBed.inject(NotebookMigrationService), "deleteNotebookForWorkflow");
        setEntries([makeEntry(1, "a", true)]);

        component.handleConfirmDeleteSelectedWorkflows();

        expect(alertSpy).toHaveBeenCalledWith("delfail");
        // The backend delete failed, so the pod file must be left in place.
        expect(cleanup).not.toHaveBeenCalled();
      });
    });

    describe("onClickOpenDownloadZip", () => {
      it("does not call the download service when nothing is checked", () => {
        setEntries([makeEntry(1, "a", false)]);

        component.onClickOpenDownloadZip();

        expect(downloadServiceSpy.downloadWorkflowsAsZip).not.toHaveBeenCalled();
      });

      it("logs an error when the download fails", () => {
        setEntries([makeEntry(1, "a", true)]);
        downloadServiceSpy.downloadWorkflowsAsZip.mockReturnValue(throwError(() => "dlfail"));
        const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

        component.onClickOpenDownloadZip();

        expect(downloadServiceSpy.downloadWorkflowsAsZip).toHaveBeenCalledWith([{ id: 1, name: "a" }]);
        expect(errorSpy).toHaveBeenCalledWith("Error downloading workflows:", "dlfail");
      });
    });

    describe("workflow uploads", () => {
      it("imports a valid .json workflow, appending an entry and toasting success", async () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.createWorkflow = vi.fn().mockReturnValue(of(makeDashboardWorkflow(42, "wf")));
        const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);
        const successSpy = vi
          .spyOn(TestBed.inject(NotificationService), "success")
          .mockImplementation(() => undefined as any);
        const content = testWorkflowContent([]);
        const file = new File([JSON.stringify(content)], "wf.json");
        setEntries([]);

        const result = await firstValueFrom(component.onClickUploadExistingWorkflowFromLocal(file as any));

        expect(result).toBe(false);
        expect(persist.createWorkflow).toHaveBeenCalledWith(content, "wf");
        expect(component.searchResultsComponent.entries.map(e => e.name)).toContain("wf");
        expect(searchSpy).toHaveBeenCalledWith(true);
        expect(successSpy).toHaveBeenCalledWith("Upload Successful");
      });

      it("toasts an error and errors the stream when the file is not JSON", async () => {
        const errorSpy = vi
          .spyOn(TestBed.inject(NotificationService), "error")
          .mockImplementation(() => undefined as any);
        const file = new File(["this is not json"], "bad.json");

        await expect(firstValueFrom(component.onClickUploadExistingWorkflowFromLocal(file as any))).rejects.toThrow();

        expect(errorSpy).toHaveBeenCalledWith(
          "An error occurred when importing the workflow. Please import a workflow json file."
        );
      });

      it("falls back to DEFAULT_WORKFLOW_NAME when the stripped name is empty", async () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.createWorkflow = vi.fn().mockReturnValue(of(makeDashboardWorkflow(1, "x")));
        vi.spyOn(component, "search").mockResolvedValue(undefined);
        const content = testWorkflowContent([]);
        const file = new File([JSON.stringify(content)], ".json");

        await firstValueFrom(component.onClickUploadExistingWorkflowFromLocal(file as any));

        expect(persist.createWorkflow).toHaveBeenCalledWith(content, DEFAULT_WORKFLOW_NAME);
      });

      it("imports every workflow file inside an uploaded .zip", async () => {
        const persist = TestBed.inject(WorkflowPersistService) as any;
        persist.createWorkflow = vi.fn().mockReturnValue(of(makeDashboardWorkflow(1, "z")));
        vi.spyOn(component, "search").mockResolvedValue(undefined);
        const zip = new JSZip();
        zip.file("a.json", JSON.stringify(testWorkflowContent([])));
        zip.file("b.json", JSON.stringify(testWorkflowContent([])));
        const blob = await zip.generateAsync({ type: "blob" });
        const file = new File([blob], "workflows.zip");

        await firstValueFrom(component.onClickUploadExistingWorkflowFromLocal(file as any));

        // handleZipUploads unpacks the archive and imports each entry.
        expect(persist.createWorkflow).toHaveBeenCalledTimes(2);
      });

      it("nameWorkflow resolves name conflicts by suffixing an increasing counter", () => {
        const fakeZip = { files: { "wf.json": {}, "wf-1.json": {} } } as any;

        expect((component as any).nameWorkflow("wf", fakeZip)).toBe("wf-2");
      });
    });

    describe("selection and view type", () => {
      it("selects all and updates the tooltip when nothing is selected", () => {
        const a = makeEntry(1, "a", false);
        const b = makeEntry(2, "b", false);
        setEntries([a, b]);

        component.toggleSelection();

        expect(a.checked).toBe(true);
        expect(b.checked).toBe(true);
        expect(component.selectionTooltip).toBe("Unselect all");
      });

      it("clears the selection and updates the tooltip when everything is selected", () => {
        const a = makeEntry(1, "a", true);
        const b = makeEntry(2, "b", true);
        setEntries([a, b]);

        component.toggleSelection();

        expect(a.checked).toBe(false);
        expect(b.checked).toBe(false);
        expect(component.selectionTooltip).toBe("Select all");
      });

      it("multiWorkflowsOperationButtonEnabled reflects whether any entry is checked", () => {
        const a = makeEntry(1, "a", false);
        setEntries([a]);
        expect(component.multiWorkflowsOperationButtonEnabled()).toBe(false);
        a.checked = true;
        expect(component.multiWorkflowsOperationButtonEnabled()).toBe(true);
      });

      it("multiWorkflowsOperationButtonEnabled is false before the results view is initialized", () => {
        (component as any)._searchResultsComponent = undefined;
        expect(component.multiWorkflowsOperationButtonEnabled()).toBe(false);
      });

      it("updateTooltip toggles between select-all and unselect-all labels", () => {
        setEntries([makeEntry(1, "a", true), makeEntry(2, "b", true)]);
        component.updateTooltip();
        expect(component.selectionTooltip).toBe("Unselect all");

        component.searchResultsComponent.entries[1].checked = false;
        component.updateTooltip();
        expect(component.selectionTooltip).toBe("Select all");
      });

      it("setViewType persists the new mode and updates viewType", () => {
        component.viewType = "list";

        component.setViewType("card");

        expect(component.viewType).toBe("card");
        expect(localStorage.getItem(VIEW_MODE_KEY)).toBe("card");
      });

      it("setViewType is a no-op when the mode is unchanged", () => {
        component.viewType = "list";
        const setItemSpy = vi.spyOn(Storage.prototype, "setItem");

        component.setViewType("list");

        expect(component.viewType).toBe("list");
        expect(setItemSpy).not.toHaveBeenCalled();
      });
    });

    describe("project workflow modals", () => {
      it("opens the add-to-project modal and re-searches after it closes", () => {
        const modalService = TestBed.inject(NzModalService);
        const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);
        const createSpy = vi.spyOn(modalService, "create").mockReturnValue({ afterClose: of(undefined) } as any);
        component.pid = 7;

        component.onClickOpenAddWorkflow();

        expect(createSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            nzContent: NgbdModalAddProjectWorkflowComponent,
            nzData: { projectId: 7 },
            nzTitle: "Add Workflows To Project",
          })
        );
        expect(searchSpy).toHaveBeenCalledWith(true);
      });

      it("opens the remove-from-project modal and re-searches after it closes", () => {
        const modalService = TestBed.inject(NzModalService);
        const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);
        const createSpy = vi.spyOn(modalService, "create").mockReturnValue({ afterClose: of(undefined) } as any);
        component.pid = 3;

        component.onClickOpenRemoveWorkflow();

        expect(createSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            nzContent: NgbdModalRemoveProjectWorkflowComponent,
            nzData: { projectId: 3 },
            nzTitle: "Remove Workflows From Project",
          })
        );
        expect(searchSpy).toHaveBeenCalledWith(true);
      });
    });

    describe("uncovered branch coverage", () => {
      // A FileReader whose result is intentionally not a string, so handleFileUploads
      // exercises its "file is not a string" guard. readAsText fires onload on the next
      // tick, mirroring the real async contract (the component assigns onload afterwards).
      class NonStringFileReader {
        public result: unknown = 12345;
        public onload: ((event: ProgressEvent<FileReader>) => void) | null = null;
        readAsText(_blob: Blob): void {
          setTimeout(() => this.onload?.({} as ProgressEvent<FileReader>), 0);
        }
      }

      afterEach(() => {
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
        localStorage.removeItem(VIEW_MODE_KEY);
      });

      describe("view-child getters", () => {
        it("searchResultsComponent getter throws before the view is initialized", () => {
          (component as any)._searchResultsComponent = undefined;
          expect(() => component.searchResultsComponent).toThrow(
            "Property cannot be accessed before it is initialized."
          );
        });

        it("filters getter throws before the view is initialized", () => {
          (component as any)._filters = undefined;
          expect(() => component.filters).toThrow("Property cannot be accessed before it is initialized.");
        });

        it("re-searches whenever the filters component emits masterFilterListChange", () => {
          const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);

          component.filters.masterFilterListChange.emit([]);

          expect(searchSpy).toHaveBeenCalled();
        });
      });

      describe("view type initialization", () => {
        it("restores the persisted 'card' view mode when the component is constructed", () => {
          localStorage.setItem(VIEW_MODE_KEY, "card");

          const freshFixture = TestBed.createComponent(UserWorkflowComponent);

          expect(freshFixture.componentInstance.viewType).toBe("card");
          freshFixture.destroy();
        });
      });

      describe("reacting to the current user changing", () => {
        it("updates login state and uid and re-searches when the user changes", () => {
          const userService = TestBed.inject(UserService) as any;
          const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);

          userService.user = undefined;
          userService.userChangeSubject.next(undefined);

          expect(component.isLogin).toBe(false);
          expect(component.currentUid).toBeUndefined();
          expect(searchSpy).toHaveBeenCalled();

          userService.user = { uid: 77 };
          userService.userChangeSubject.next(userService.user);

          expect(component.isLogin).toBe(true);
          expect(component.currentUid).toBe(77);
        });
      });

      describe("search", () => {
        it("forces the section's pid into the search filter parameters", async () => {
          const searchService = TestBed.inject(SearchService) as any;
          const execSpy = vi.spyOn(searchService, "executeSearch");
          component.pid = 3;

          await component.search(true);

          expect(execSpy).toHaveBeenCalled();
          expect((execSpy.mock.calls[0][1] as any).projectIds).toEqual([3]);
        });
      });

      describe("onClickCreateNewWorkflowFromDashboard", () => {
        it("adds the new workflow to the project before navigating when a pid is set", () => {
          const router = TestBed.inject(Router);
          const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.createWorkflow = vi.fn().mockReturnValue(of({ workflow: { wid: 55 } }));
          const projectService = TestBed.inject(UserProjectService) as any;
          const addSpy = vi.spyOn(projectService, "addWorkflowToProject").mockReturnValue(of({} as any));
          component.pid = 8;

          component.onClickCreateNewWorkflowFromDashboard();

          expect(addSpy).toHaveBeenCalledWith(8, 55);
          expect(navigateSpy).toHaveBeenCalledWith([USER_WORKSPACE, 55]);
        });

        it("notifies an error and does not navigate when creation returns no wid", () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.createWorkflow = vi.fn().mockReturnValue(of({ workflow: { wid: undefined } }));
          const errorSpy = vi
            .spyOn(TestBed.inject(NotificationService), "error")
            .mockImplementation(() => undefined as any);
          const navigateSpy = vi.spyOn(TestBed.inject(Router), "navigate").mockResolvedValue(true);
          component.pid = undefined;

          component.onClickCreateNewWorkflowFromDashboard();

          expect(errorSpy).toHaveBeenCalledWith("Workflow creation failed");
          expect(navigateSpy).not.toHaveBeenCalled();
        });
      });

      describe("onClickDuplicateWorkflow", () => {
        it("prepends the duplicate, hydrates owner info, and grants the current user access", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi
            .fn()
            .mockReturnValue(of([{ ...makeDashboardWorkflow(201, "dup"), ownerId: 2 }]));
          const searchService = TestBed.inject(SearchService) as any;
          const getUserInfoSpy = vi.spyOn(searchService, "getUserInfo");
          component.pid = undefined;
          component.currentUid = 1;
          setEntries([makeEntry(9, "existing")]);

          await component.onClickDuplicateWorkflow(makeEntry(5, "orig"));

          expect(persist.duplicateWorkflow).toHaveBeenCalledWith([5]);
          expect(getUserInfoSpy).toHaveBeenCalledWith([2]);
          const entries = component.searchResultsComponent.entries;
          expect(entries.map(e => e.name)).toEqual(["dup", "existing"]);
          expect(entries[0].ownerName).toBe("Angular");
          expect(entries[0].ownerAvatar).toBe("avatar_url_2");
          expect(entries[0].accessibleUserIds).toEqual([1]);
        });

        it("passes the section pid to duplicateWorkflow when inside a project", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi
            .fn()
            .mockReturnValue(of([{ ...makeDashboardWorkflow(202, "dp"), ownerId: 2 }]));
          component.pid = 9;
          setEntries([]);

          await component.onClickDuplicateWorkflow(makeEntry(5, "orig"));

          expect(persist.duplicateWorkflow).toHaveBeenCalledWith([5], 9);
        });

        it("skips the user-info lookup and access grant when there is no owner or current user", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi
            .fn()
            .mockReturnValue(of([{ ...makeDashboardWorkflow(203, "no owner"), ownerId: undefined } as any]));
          const searchService = TestBed.inject(SearchService) as any;
          const getUserInfoSpy = vi.spyOn(searchService, "getUserInfo");
          component.pid = undefined;
          component.currentUid = undefined;
          setEntries([]);

          await component.onClickDuplicateWorkflow(makeEntry(5, "orig"));

          expect(getUserInfoSpy).not.toHaveBeenCalled();
          const entry = component.searchResultsComponent.entries[0];
          expect(entry.name).toBe("no owner");
          expect(entry.accessibleUserIds).toEqual([]);
        });

        it("falls back to an empty avatar when the owner info omits a google avatar", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi
            .fn()
            .mockReturnValue(of([{ ...makeDashboardWorkflow(205, "na"), ownerId: 2 }]));
          const searchService = TestBed.inject(SearchService) as any;
          searchService.getUserInfo = vi.fn().mockReturnValue(of({ 2: { userName: "NoAvatar" } }));
          component.pid = undefined;
          setEntries([]);

          await component.onClickDuplicateWorkflow(makeEntry(5, "orig"));

          const entry = component.searchResultsComponent.entries[0];
          expect(entry.ownerName).toBe("NoAvatar");
          expect(entry.ownerAvatar).toBe("");
        });

        it("does nothing when the entry has no wid", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi.fn();

          await component.onClickDuplicateWorkflow(makeEntry(undefined, "no wid"));

          expect(persist.duplicateWorkflow).not.toHaveBeenCalled();
        });

        it("alerts the error payload when duplication fails", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi.fn().mockReturnValue(throwError(() => ({ error: "dup error" })));
          const alertSpy = vi.spyOn(window, "alert").mockImplementation(() => {});
          vi.spyOn(console, "log").mockImplementation(() => {});
          component.pid = undefined;
          setEntries([]);

          await component.onClickDuplicateWorkflow(makeEntry(5, "orig"));

          expect(alertSpy).toHaveBeenCalledWith("dup error");
        });
      });

      describe("onClickDuplicateSelectedWorkflows", () => {
        it("is a no-op when nothing is selected", () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi.fn();
          setEntries([makeEntry(1, "a", false), makeEntry(2, "b", false)]);

          component.onClickDuplicateSelectedWorkflows();

          expect(persist.duplicateWorkflow).not.toHaveBeenCalled();
        });

        it("alerts on a duplication error in the project (pid) branch", () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.duplicateWorkflow = vi.fn().mockReturnValue(throwError(() => "pidboom"));
          const alertSpy = vi.spyOn(window, "alert").mockImplementation(() => {});
          component.pid = 4;
          setEntries([makeEntry(1, "a", true)]);

          component.onClickDuplicateSelectedWorkflows();

          expect(persist.duplicateWorkflow).toHaveBeenCalledWith([1], 4);
          expect(alertSpy).toHaveBeenCalledWith("pidboom");
        });
      });

      describe("handleConfirmDeleteSelectedWorkflows", () => {
        it("is a no-op when nothing is selected", () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.deleteWorkflow = vi.fn();
          setEntries([makeEntry(1, "a", false), makeEntry(2, "b", false)]);

          component.handleConfirmDeleteSelectedWorkflows();

          expect(persist.deleteWorkflow).not.toHaveBeenCalled();
        });
      });

      describe("uploads", () => {
        it("errors and notifies when the FileReader result is not a string", async () => {
          vi.stubGlobal("FileReader", NonStringFileReader);
          const errorSpy = vi
            .spyOn(TestBed.inject(NotificationService), "error")
            .mockImplementation(() => undefined as any);
          const file = new File([JSON.stringify(testWorkflowContent([]))], "wf.json");

          await expect(firstValueFrom(component.onClickUploadExistingWorkflowFromLocal(file as any))).rejects.toThrow(
            "Incorrect format: file is not a string"
          );
          expect(errorSpy).toHaveBeenCalledWith(
            "An error occurred when importing the workflow. Please import a workflow json file."
          );
        });

        it("uses the full name as the workflow name when the file has no extension", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.createWorkflow = vi.fn().mockReturnValue(of(makeDashboardWorkflow(1, "noext")));
          vi.spyOn(component, "search").mockResolvedValue(undefined);
          const content = testWorkflowContent([]);
          const file = new File([JSON.stringify(content)], "noext");

          await firstValueFrom(component.onClickUploadExistingWorkflowFromLocal(file as any));

          expect(persist.createWorkflow).toHaveBeenCalledWith(content, "noext");
        });

        it("errors the upload stream and does not toast success when createWorkflow fails", async () => {
          const persist = TestBed.inject(WorkflowPersistService) as any;
          persist.createWorkflow = vi.fn().mockReturnValue(throwError(() => new Error("create failed")));
          const successSpy = vi
            .spyOn(TestBed.inject(NotificationService), "success")
            .mockImplementation(() => undefined as any);
          const file = new File([JSON.stringify(testWorkflowContent([]))], "wf.json");

          await expect(firstValueFrom(component.onClickUploadExistingWorkflowFromLocal(file as any))).rejects.toThrow(
            "create failed"
          );
          expect(successSpy).not.toHaveBeenCalled();
        });
      });

      describe("refreshSearchResult", () => {
        it("triggers a forced search", () => {
          const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);

          component.refreshSearchResult();

          expect(searchSpy).toHaveBeenCalledWith(true);
        });
      });
    });

    describe("template rendering", () => {
      const VIEW_MODE_KEY = "texera.userWorkflow.viewMode";
      const q = (selector: string) => fixture.debugElement.query(By.css(selector));

      afterEach(() => {
        vi.restoreAllMocks();
        localStorage.removeItem(VIEW_MODE_KEY);
      });
      // The multi-select toolbar only renders when at least one entry is checked.
      // Clone a real fixture entry (preserving its prototype) so the rendered
      // search-results can display it, and flip `checked` on the copy so the shared
      // fixture is not mutated.
      const renderWithSelection = () => {
        const source = testWorkflowEntries[0];
        const checkedEntry = Object.assign(Object.create(Object.getPrototypeOf(source)), source);
        checkedEntry.checked = true;
        component.searchResultsComponent.entries = [checkedEntry];
        fixture.detectChanges();
      };

      it("hides the multi-select actions until an entry is checked", () => {
        component.searchResultsComponent.entries = [];
        fixture.detectChanges();
        expect(q('[title="Batch Select"]')).toBeNull();

        renderWithSelection();
        expect(q('[title="Batch Select"]')).toBeTruthy();
      });

      it("wires the Create Workflow button", () => {
        const spy = vi.spyOn(component, "onClickCreateNewWorkflowFromDashboard").mockImplementation(() => {});
        q(".create-btn").triggerEventHandler("click", null);
        expect(spy).toHaveBeenCalled();
      });

      it("re-searches when the sort button changes the sort method", () => {
        const searchSpy = vi.spyOn(component, "search").mockResolvedValue(undefined);
        q("texera-sort-button").triggerEventHandler("sortMethodChange", "NameAsc");
        expect(component.sortMethod).toBe("NameAsc");
        expect(searchSpy).toHaveBeenCalled();
      });

      it("wires the Batch Select button", () => {
        renderWithSelection();
        const spy = vi.spyOn(component, "toggleSelection").mockImplementation(() => {});
        q('[title="Batch Select"]').triggerEventHandler("click", null);
        expect(spy).toHaveBeenCalled();
      });

      it("wires the Download-as-ZIP button", () => {
        renderWithSelection();
        const spy = vi.spyOn(component, "onClickOpenDownloadZip").mockResolvedValue(undefined);
        q('[title="Download added workflow as a ZIP file"]').triggerEventHandler("click", null);
        expect(spy).toHaveBeenCalled();
      });

      it("wires the Duplicate-selected button", () => {
        renderWithSelection();
        const spy = vi.spyOn(component, "onClickDuplicateSelectedWorkflows").mockImplementation(() => {});
        q('[nz-tooltip="Duplicate selected workflows"]').triggerEventHandler("click", null);
        expect(spy).toHaveBeenCalled();
      });

      it("wires the Delete-selected popconfirm", () => {
        renderWithSelection();
        const spy = vi.spyOn(component, "handleConfirmDeleteSelectedWorkflows").mockImplementation(() => {});
        q('[nzPopconfirmTitle="Confirm to delete selected workflows."]').triggerEventHandler("nzOnConfirm", null);
        expect(spy).toHaveBeenCalled();
      });

      it("shows and wires the project add/remove buttons when a pid is set", () => {
        component.pid = 1;
        fixture.detectChanges();
        const addSpy = vi.spyOn(component, "onClickOpenAddWorkflow").mockImplementation(() => {});
        const removeSpy = vi.spyOn(component, "onClickOpenRemoveWorkflow").mockImplementation(() => {});

        q('[title="Add workflow(s) to project"]').triggerEventHandler("click", null);
        q('[title="Remove workflow(s) from project"]').triggerEventHandler("click", null);

        expect(addSpy).toHaveBeenCalled();
        expect(removeSpy).toHaveBeenCalled();
      });

      it("switches the view type through the List/Card buttons", () => {
        component.viewType = "card";
        fixture.detectChanges();

        q('[title="List View"]').triggerEventHandler("click", null);
        expect(component.viewType).toBe("list");

        fixture.detectChanges();
        q('[title="Card View"]').triggerEventHandler("click", null);
        expect(component.viewType).toBe("card");
      });

      it("wires the rendered search-results outputs to the component", () => {
        const results = fixture.debugElement.query(By.directive(SearchResultsComponent));
        const deleteSpy = vi.spyOn(component, "deleteWorkflow").mockImplementation(() => {});
        const duplicateSpy = vi.spyOn(component, "onClickDuplicateWorkflow").mockResolvedValue(undefined);
        const refreshSpy = vi.spyOn(component, "refreshSearchResult").mockImplementation(() => {});
        const tooltipSpy = vi.spyOn(component, "updateTooltip").mockImplementation(() => {});
        const entry = testWorkflowEntries[0];

        results.triggerEventHandler("deleted", entry);
        results.triggerEventHandler("duplicated", entry);
        results.triggerEventHandler("refresh", undefined);
        results.triggerEventHandler("notifyWorkflow", undefined);

        expect(deleteSpy).toHaveBeenCalledWith(entry);
        expect(duplicateSpy).toHaveBeenCalledWith(entry);
        expect(refreshSpy).toHaveBeenCalled();
        expect(tooltipSpy).toHaveBeenCalled();
      });

      it("renders the workflow card template in card view and wires its outputs", () => {
        const source = testWorkflowEntries[0];
        const entry = Object.assign(Object.create(Object.getPrototypeOf(source)), source);
        component.viewType = "card";
        component.searchResultsComponent.entries = [entry];
        fixture.detectChanges();

        const card = fixture.debugElement.query(By.css("texera-card-item"));
        expect(card).toBeTruthy();

        const deleteSpy = vi.spyOn(component, "deleteWorkflow").mockImplementation(() => {});
        const duplicateSpy = vi.spyOn(component, "onClickDuplicateWorkflow").mockResolvedValue(undefined);
        const refreshSpy = vi.spyOn(component, "refreshSearchResult").mockImplementation(() => {});
        const checkboxSpy = vi
          .spyOn(component.searchResultsComponent, "onEntryCheckboxChange")
          .mockImplementation(() => {});

        card.triggerEventHandler("deleted", undefined);
        card.triggerEventHandler("duplicated", undefined);
        card.triggerEventHandler("refresh", undefined);
        card.triggerEventHandler("checkboxChanged", undefined);

        expect(deleteSpy).toHaveBeenCalledWith(entry);
        expect(duplicateSpy).toHaveBeenCalledWith(entry);
        expect(refreshSpy).toHaveBeenCalled();
        expect(checkboxSpy).toHaveBeenCalled();
      });
    });
  });
});
