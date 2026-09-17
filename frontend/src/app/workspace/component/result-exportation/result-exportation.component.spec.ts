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

import { QueryList } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { NzAutocompleteOptionComponent } from "ng-zorro-antd/auto-complete";
import { NzOptionComponent } from "ng-zorro-antd/select";
import { of } from "rxjs";
import { ResultExportationComponent } from "./result-exportation.component";
import {
  WorkflowResultDownloadability,
  WorkflowResultExportService,
} from "../../service/workflow-result-export/workflow-result-export.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { UserDatasetVersionCreatorComponent } from "../../../dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-version-creator/user-dataset-version-creator.component";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { NZ_MODAL_DATA, NzModalRef, NzModalService } from "ng-zorro-antd/modal";

const writeDataset = {
  dataset: { did: 1, name: "writable" },
  accessPrivilege: "WRITE",
} as unknown as DashboardDataset;

const readDataset = {
  dataset: { did: 2, name: "readonly" },
  accessPrivilege: "READ",
} as unknown as DashboardDataset;

const MODAL_DATA = {
  sourceTriggered: "menu",
  workflowName: "my-workflow",
  defaultFileName: "out.csv",
  rowIndex: -1,
  columnIndex: -1,
  exportType: "csv",
};

describe("ResultExportationComponent", () => {
  let component: ResultExportationComponent;
  let fixture: ComponentFixture<ResultExportationComponent>;

  let exportWorkflowExecutionResult: ReturnType<typeof vi.fn>;
  let modalClose: ReturnType<typeof vi.fn>;
  let modalCreate: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    exportWorkflowExecutionResult = vi.fn();
    modalClose = vi.fn();
    modalCreate = vi.fn().mockReturnValue({ afterClose: of(null) });

    await TestBed.configureTestingModule({
      imports: [ResultExportationComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: MODAL_DATA },
        { provide: NzModalRef, useValue: { close: modalClose, getConfig: () => ({}) } },
        { provide: NzModalService, useValue: { create: modalCreate } },
        {
          provide: WorkflowResultExportService,
          useValue: {
            computeRestrictionAnalysis: vi.fn().mockReturnValue(of(new WorkflowResultDownloadability(new Map()))),
            exportWorkflowExecutionResult,
          },
        },
        {
          provide: DatasetService,
          useValue: { retrieveAccessibleDatasets: vi.fn().mockReturnValue(of([writeDataset, readDataset])) },
        },
        {
          provide: WorkflowActionService,
          useValue: {
            getTexeraGraph: vi.fn().mockReturnValue({ getAllOperators: vi.fn().mockReturnValue([]) }),
            getJointGraphWrapper: vi
              .fn()
              .mockReturnValue({ getCurrentHighlightedOperatorIDs: vi.fn().mockReturnValue([]) }),
          },
        },
        {
          provide: WorkflowResultService,
          useValue: {
            determineOutputTypes: vi.fn().mockReturnValue({
              hasAnyResult: false,
              isTableOutput: false,
              isVisualizationOutput: false,
              containsBinaryData: false,
            }),
          },
        },
        {
          provide: ComputingUnitStatusService,
          useValue: { getSelectedComputingUnit: vi.fn().mockReturnValue(of(null)) },
        },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ResultExportationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
  });

  it("should create and render", () => {
    expect(component).toBeTruthy();
  });

  it("ngOnInit keeps only WRITE-accessible datasets and stores the downloadability", () => {
    // detectChanges() in beforeEach already ran ngOnInit with its synchronous mock streams.
    expect(component.userAccessibleDatasets).toEqual([writeDataset]);
    expect(component.filteredUserAccessibleDatasets).toEqual([writeDataset]);
    expect(component.downloadability).toBeDefined();
    expect(component.selectedComputingUnit).toBeNull();
  });

  it("onClickExportResult('dataset', ds) exports to the dataset destination and closes the modal", () => {
    component.onClickExportResult("dataset", writeDataset);

    expect(exportWorkflowExecutionResult).toHaveBeenCalledTimes(1);
    const args = exportWorkflowExecutionResult.mock.calls[0];
    expect(args[0]).toBe("csv"); // exportType, from modal data
    expect(args[1]).toBe("my-workflow"); // workflowName
    expect(args[2]).toEqual([1]); // datasetIds resolved from ds.dataset.did
    expect(args[6]).toBe(true); // exportAll, because sourceTriggered === "menu"
    expect(args[7]).toBe("dataset"); // destination
    expect(modalClose).toHaveBeenCalledTimes(1);
  });

  it("onClickExportResult('local') exports to local download with no dataset ids", () => {
    component.onClickExportResult("local");

    expect(exportWorkflowExecutionResult).toHaveBeenCalledTimes(1);
    const args = exportWorkflowExecutionResult.mock.calls[0];
    expect(args[2]).toEqual([]); // local download carries no dataset ids
    expect(args[7]).toBe("local");
    expect(modalClose).toHaveBeenCalledTimes(1);
  });

  it("onClickCreateNewDataset opens the dataset-creator modal and adopts the created dataset", () => {
    const created = {
      dataset: { did: 9, name: "brand-new" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;
    modalCreate.mockReturnValue({ afterClose: of(created) });

    component.onClickCreateNewDataset();

    expect(modalCreate).toHaveBeenCalledTimes(1);
    const config = modalCreate.mock.calls[0][0];
    expect(config.nzTitle).toBe("Create New Dataset");
    expect(config.nzContent).toBe(UserDatasetVersionCreatorComponent);
    // afterClose emitted a dataset, so the component adopts it into its lists.
    expect(component.userAccessibleDatasets[0]).toBe(created);
    expect(component.inputDatasetName).toBe("brand-new");
  });

  describe("operator-id resolution and downloadability getters (menu source)", () => {
    // Reconfigure the graph mock so the "menu" branch of getOperatorIdsToCheck
    // returns concrete operator IDs (drives the `.getAllOperators().map(op => op.operatorID)` branch).
    function setAllOperators(ids: string[]): void {
      const graph = TestBed.inject(WorkflowActionService) as unknown as {
        getTexeraGraph: ReturnType<typeof vi.fn>;
      };
      graph.getTexeraGraph.mockReturnValue({
        getAllOperators: () => ids.map(id => ({ operatorID: id })),
      });
    }

    function restrict(entries: Record<string, string[]>): void {
      const map = new Map<string, Set<string>>();
      Object.entries(entries).forEach(([op, labels]) => map.set(op, new Set(labels)));
      component.downloadability = new WorkflowResultDownloadability(map);
    }

    it("exportableOperatorIds maps all operators and filters out restricted ones", () => {
      setAllOperators(["op-a", "op-b", "op-c"]);
      restrict({ "op-b": ["ds (owner@x.com)"] });

      expect(component.exportableOperatorIds).toEqual(["op-a", "op-c"]);
      expect(component.blockedOperatorIds).toEqual(["op-b"]);
    });

    it("getters return [] when downloadability has not been resolved yet", () => {
      component.downloadability = undefined;

      expect(component.exportableOperatorIds).toEqual([]);
      expect(component.blockedOperatorIds).toEqual([]);
      expect(component.blockingDatasetLabels).toEqual([]);
    });

    it("blockingDatasetLabels and blockingDatasetSummary surface the blocking datasets", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-a": ["Sales (a@x.com)"], "op-b": ["Sales (a@x.com)", "HR (b@x.com)"] });

      expect(component.blockingDatasetLabels).toEqual(["Sales (a@x.com)", "HR (b@x.com)"]);
      expect(component.blockingDatasetSummary).toBe("Sales (a@x.com), HR (b@x.com)");
    });

    it("isExportRestricted is true only when every operator is blocked", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-a": ["ds1"], "op-b": ["ds2"] });

      expect(component.isExportRestricted).toBe(true);
      expect(component.hasPartialNonDownloadable).toBe(false);
    });

    it("hasPartialNonDownloadable is true when some but not all operators are blocked", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-b": ["ds1"] });

      expect(component.isExportRestricted).toBe(false);
      expect(component.hasPartialNonDownloadable).toBe(true);
    });
  });

  describe("updateOutputType", () => {
    function setAllOperators(ids: string[]): void {
      const graph = TestBed.inject(WorkflowActionService) as unknown as {
        getTexeraGraph: ReturnType<typeof vi.fn>;
      };
      graph.getTexeraGraph.mockReturnValue({
        getAllOperators: () => ids.map(id => ({ operatorID: id })),
      });
    }

    function outputTypes(byOperator: Record<string, unknown>): void {
      const results = TestBed.inject(WorkflowResultService) as unknown as {
        determineOutputTypes: ReturnType<typeof vi.fn>;
      };
      results.determineOutputTypes.mockImplementation((operatorId: string) => byOperator[operatorId]);
    }

    it("leaves the output flags untouched when downloadability is not resolved", () => {
      component.downloadability = undefined;
      component.isTableOutput = true;
      component.isVisualizationOutput = true;
      component.containsBinaryData = true;

      component.updateOutputType();

      // Early return: flags are preserved rather than recomputed/reset.
      expect(component.isTableOutput).toBe(true);
      expect(component.isVisualizationOutput).toBe(true);
      expect(component.containsBinaryData).toBe(true);
    });

    it("clears every output flag when all selected operators are export-restricted", () => {
      setAllOperators(["op-a"]);
      component.downloadability = new WorkflowResultDownloadability(new Map([["op-a", new Set(["ds"])]]));
      component.isTableOutput = true;
      component.isVisualizationOutput = true;
      component.containsBinaryData = true;

      component.updateOutputType();

      expect(component.isTableOutput).toBe(false);
      expect(component.isVisualizationOutput).toBe(false);
      expect(component.containsBinaryData).toBe(false);
    });

    it("aggregates mixed output types across exportable operators", () => {
      setAllOperators(["skip", "table-only", "viz-binary"]);
      component.downloadability = new WorkflowResultDownloadability(new Map());
      outputTypes({
        // No result at all -> skipped via `continue`.
        skip: { hasAnyResult: false, isTableOutput: true, isVisualizationOutput: true, containsBinaryData: true },
        // Pure table output.
        "table-only": {
          hasAnyResult: true,
          isTableOutput: true,
          isVisualizationOutput: false,
          containsBinaryData: false,
        },
        // Visualization output carrying binary data.
        "viz-binary": {
          hasAnyResult: true,
          isTableOutput: false,
          isVisualizationOutput: true,
          containsBinaryData: true,
        },
      });

      component.updateOutputType();

      // Not all are tables and not all are visualizations -> both false; one carries binary data.
      expect(component.isTableOutput).toBe(false);
      expect(component.isVisualizationOutput).toBe(false);
      expect(component.containsBinaryData).toBe(true);
    });

    it("reports table output when every operator with a result is a table", () => {
      setAllOperators(["t1", "t2"]);
      component.downloadability = new WorkflowResultDownloadability(new Map());
      outputTypes({
        t1: { hasAnyResult: true, isTableOutput: true, isVisualizationOutput: false, containsBinaryData: false },
        t2: { hasAnyResult: true, isTableOutput: true, isVisualizationOutput: false, containsBinaryData: false },
      });

      component.updateOutputType();

      expect(component.isTableOutput).toBe(true);
      expect(component.isVisualizationOutput).toBe(false);
      expect(component.containsBinaryData).toBe(false);
    });
  });

  describe("onUserInputDatasetName", () => {
    const alpha = {
      dataset: { did: 1, name: "Alpha" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;
    const beta = {
      dataset: { did: 2, name: "Beta" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;
    const noId = {
      dataset: { did: undefined, name: "AlphaLike" },
      accessPrivilege: "WRITE",
    } as unknown as DashboardDataset;

    it("filters datasets by case-insensitive name match and requires a dataset id", () => {
      component.userAccessibleDatasets = [alpha, beta, noId];
      component.inputDatasetName = "alph";

      component.onUserInputDatasetName(new Event("input"));

      // noId matches the name but has no did, so it is excluded; Beta does not match.
      expect(component.filteredUserAccessibleDatasets).toEqual([alpha]);
    });

    it("resets to the full list when the input is cleared", () => {
      component.userAccessibleDatasets = [alpha, beta];
      component.inputDatasetName = "";

      component.onUserInputDatasetName(new Event("input"));

      expect(component.filteredUserAccessibleDatasets).toEqual([alpha, beta]);
      // A fresh copy, not the same array reference.
      expect(component.filteredUserAccessibleDatasets).not.toBe(component.userAccessibleDatasets);
    });
  });

  it("onClickCreateNewDataset leaves state unchanged when the creator modal is dismissed", () => {
    // beforeEach wires modalCreate to emit null on afterClose.
    const before = component.userAccessibleDatasets;
    const nameBefore = component.inputDatasetName;

    component.onClickCreateNewDataset();

    expect(modalCreate).toHaveBeenCalledTimes(1);
    expect(component.userAccessibleDatasets).toBe(before);
    expect(component.inputDatasetName).toBe(nameBefore);
  });

  // Renders the template in each of the states it switches on so the *ngIf / *ngFor /
  // (click) / [(ngModel)] constructs actually execute. detectChanges() is the coverage switch.
  describe("template rendering", () => {
    function setAllOperators(ids: string[]): void {
      const graph = TestBed.inject(WorkflowActionService) as unknown as {
        getTexeraGraph: ReturnType<typeof vi.fn>;
      };
      graph.getTexeraGraph.mockReturnValue({
        getAllOperators: () => ids.map(id => ({ operatorID: id })),
      });
    }

    function restrict(entries: Record<string, string[]>): void {
      const map = new Map<string, Set<string>>();
      Object.entries(entries).forEach(([op, labels]) => map.set(op, new Set(labels)));
      component.downloadability = new WorkflowResultDownloadability(map);
    }

    // nz-select does not project its <nz-option> content, so the option host elements are
    // absent from both the DOM and the debug tree. The rendered option set is read off the
    // select's own ContentChildren query instead, which is the same list nz-select uses.
    function optionValues(selector: string): unknown[] {
      const select = fixture.debugElement.query(By.css(selector));
      expect(select).toBeTruthy();
      return (
        select.componentInstance as { listOfNzOptionComponent: QueryList<NzOptionComponent> }
      ).listOfNzOptionComponent.map(option => option.nzValue);
    }

    it("renders the restricted-export error alert when every operator is blocked", () => {
      setAllOperators(["op-a"]);
      restrict({ "op-a": ["Sales (a@x.com)"] });
      fixture.detectChanges();

      expect(component.isExportRestricted).toBe(true);
      const alert = fixture.debugElement.query(By.css("nz-alert"));
      expect(alert).toBeTruthy();
      expect(alert.nativeElement.textContent).toContain("Export unavailable");
      // A fully blocked export is fatal, so it must be the red alert, not the yellow one.
      expect((alert.componentInstance as { nzType: string }).nzType).toBe("error");
    });

    it("renders the partial-skip warning alert when only some operators are blocked", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-a": ["Sales (a@x.com)"] });
      fixture.detectChanges();

      expect(component.hasPartialNonDownloadable).toBe(true);
      expect(fixture.nativeElement.textContent).toContain("Some operators will be skipped");
      const alert = fixture.debugElement.query(By.css("nz-alert"));
      expect(alert).toBeTruthy();
      // A partial skip is recoverable, so it must be the yellow alert, not the red one.
      expect((alert.componentInstance as { nzType: string }).nzType).toBe("warning");
    });

    it("renders the export-type select and its output-gated options when export is allowed", () => {
      setAllOperators(["op-a"]);
      restrict({}); // nothing blocked -> not restricted
      component.exportType = "csv"; // != "data"
      component.isTableOutput = true;
      component.isVisualizationOutput = true;
      component.containsBinaryData = false;
      fixture.detectChanges();

      expect(component.isExportRestricted).toBe(false);
      expect(fixture.debugElement.query(By.css("#exportTypeInput"))).toBeTruthy();
      // Output that is both a table and a visualization offers every format.
      expect(optionValues("#exportTypeInput")).toEqual(["arrow", "csv", "html", "parquet"]);
      // The destination options are fixed; their order is what the two branches below key on.
      expect(optionValues("#destinationInput")).toEqual(["dataset", "local"]);

      // Dropping the visualization flag must drop exactly the .html option. Asserting the
      // list in two different output states pins each option to its own gate rather than
      // to "some output flag is set".
      component.isVisualizationOutput = false;
      fixture.detectChanges();
      expect(optionValues("#exportTypeInput")).toEqual(["arrow", "csv", "parquet"]);

      // `destination` is still the empty placeholder, so NEITHER destination branch may
      // render: no local Export button and no dataset search box.
      expect(component.destination).toBe("");
      expect(
        fixture.debugElement
          .queryAll(By.css("button"))
          .some(button => button.nativeElement.textContent.trim() === "Export")
      ).toBe(false);
      expect(fixture.debugElement.query(By.css("input[name='datasetName']"))).toBeNull();
    });

    it("renders the filename input when the export type is 'data'", () => {
      setAllOperators(["op-a"]);
      restrict({});
      component.exportType = "data";
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css("#filenameInput"))).toBeTruthy();
    });

    it("renders the local Export button and exports on click", () => {
      setAllOperators(["op-a"]);
      restrict({});
      component.destination = "local";
      fixture.detectChanges();

      const exportBtn = fixture.debugElement
        .queryAll(By.css("button"))
        .find(btn => btn.nativeElement.textContent.trim() === "Export");
      expect(exportBtn).toBeTruthy();

      exportBtn!.triggerEventHandler("click", null);
      expect(exportWorkflowExecutionResult).toHaveBeenCalledTimes(1);
      const args = exportWorkflowExecutionResult.mock.calls[0];
      expect(args[7]).toBe("local");
    });

    it("renders the dataset destination with its list and create button", () => {
      setAllOperators(["op-a"]);
      restrict({});
      component.destination = "dataset";
      fixture.detectChanges();

      // the dataset search input drives the (input) handler
      const search = fixture.debugElement.query(By.css("input[name='datasetName']"));
      expect(search).toBeTruthy();
      search.triggerEventHandler("input", { target: { value: "" } });

      // The nz-auto-option list the *ngFor drives is bound to
      // `filteredUserAccessibleDatasets`; assert the component fed it exactly the
      // WRITE dataset (the READ one is filtered out), so the list branch has real data
      // behind it. The option content itself only enters the DOM once the autocomplete
      // panel expands; that is driven separately in the option-row tests below.
      expect(component.filteredUserAccessibleDatasets.map(d => d.dataset.name)).toEqual(["writable"]);

      // the create-new-dataset button opens the creator modal
      const createBtn = fixture.debugElement
        .queryAll(By.css("button"))
        .find(btn => btn.nativeElement.textContent.includes("Create New Dataset"));
      expect(createBtn).toBeTruthy();

      createBtn!.triggerEventHandler("click", null);
      expect(modalCreate).toHaveBeenCalledTimes(1);
    });

    // The three form controls are two-way bound, so the write-back half of each
    // `[(ngModel)]` only runs when the control emits. `ngModelChange` is the output
    // Angular's two-way binding subscribes to, so triggering it drives the write.
    it("writes the chosen export type back through its two-way binding", () => {
      setAllOperators(["op-a"]);
      restrict({});
      component.exportType = "csv";
      component.isTableOutput = true;
      fixture.detectChanges();

      const select = fixture.debugElement.query(By.css("#exportTypeInput"));
      expect(select).toBeTruthy();

      select.triggerEventHandler("ngModelChange", "arrow");
      fixture.detectChanges();

      expect(component.exportType).toBe("arrow");
      // The destination select is bound to a different field and must be untouched.
      expect(component.destination).toBe("");
    });

    it("writes the chosen destination back through its two-way binding and swaps in the local Export button", () => {
      setAllOperators(["op-a"]);
      restrict({});
      fixture.detectChanges();

      const select = fixture.debugElement.query(By.css("#destinationInput"));
      expect(select).toBeTruthy();

      select.triggerEventHandler("ngModelChange", "local");
      fixture.detectChanges();

      expect(component.destination).toBe("local");
      const exportBtn = fixture.debugElement
        .queryAll(By.css("button"))
        .find(btn => btn.nativeElement.textContent.trim() === "Export");
      expect(exportBtn).toBeTruthy();
      // The dataset search input belongs to the other destination branch.
      expect(fixture.debugElement.query(By.css("input[name='datasetName']"))).toBeNull();
    });

    it("writes the binary-data filename back through its two-way binding", () => {
      setAllOperators(["op-a"]);
      restrict({});
      component.exportType = "data";
      fixture.detectChanges();

      const filename = fixture.debugElement.query(By.css("#filenameInput"));
      expect(filename).toBeTruthy();

      filename.triggerEventHandler("ngModelChange", "blob.bin");
      fixture.detectChanges();

      expect(component.inputFileName).toBe("blob.bin");
      // The dataset-name input is the other text field bound in this template.
      expect(component.inputDatasetName).toBe("");
    });

    // The autocomplete options are declared in this template but projected into the
    // panel, which only attaches once NzAutocompleteTriggerDirective's host `focusin`
    // listener opens it. Once open the option rows live in the CDK overlay container
    // under document.body, so they are read from the document rather than the fixture.
    function openDatasetPanel(): HTMLElement[] {
      const search = fixture.debugElement.query(By.css("input[name='datasetName']"));
      expect(search).toBeTruthy();
      search.nativeElement.dispatchEvent(new Event("focusin"));
      fixture.detectChanges();
      return Array.from(document.querySelectorAll<HTMLElement>("button.dataset-option-link-btn"));
    }

    it("offers a Save button per selectable dataset and exports to the clicked one", () => {
      setAllOperators(["op-a"]);
      restrict({});
      component.destination = "dataset";
      fixture.detectChanges();

      const saveButtons = openDatasetPanel();
      // Derived from the component's own list so a stale overlay cannot make this vacuous.
      expect(saveButtons.length).toBe(component.filteredUserAccessibleDatasets.length);
      expect(saveButtons.length).toBe(1);

      saveButtons[0].click();

      expect(exportWorkflowExecutionResult).toHaveBeenCalledTimes(1);
      const args = exportWorkflowExecutionResult.mock.calls[0];
      expect(args[2]).toEqual([1]); // the clicked dataset's did
      expect(args[7]).toBe("dataset");
      expect(modalClose).toHaveBeenCalledTimes(1);
    });

    it("prints each option's dataset id, and prints nothing for a dataset that has none", () => {
      const pending = {
        dataset: { did: undefined, name: "pending" },
        accessPrivilege: "WRITE",
      } as unknown as DashboardDataset;
      setAllOperators(["op-a"]);
      restrict({});
      component.destination = "dataset";
      // An empty search box copies the full WRITE list through, id-less datasets included.
      component.userAccessibleDatasets = [writeDataset, pending];
      component.inputDatasetName = "";
      component.onUserInputDatasetName(new Event("input"));
      fixture.detectChanges();

      openDatasetPanel();

      const ids = Array.from(document.querySelectorAll(".dataset-id-container")).map(node =>
        (node.textContent ?? "").trim()
      );
      expect(ids).toEqual(["1", ""]);
    });

    it("narrows the dataset options to the typed search text and labels each option by its name", () => {
      const other = {
        dataset: { did: 7, name: "other" },
        accessPrivilege: "WRITE",
      } as unknown as DashboardDataset;
      setAllOperators(["op-a"]);
      restrict({});
      component.destination = "dataset";
      // Two accessible datasets, so the filtered list and the full list can actually differ.
      component.userAccessibleDatasets = [writeDataset, other];
      component.filteredUserAccessibleDatasets = [writeDataset, other];
      fixture.detectChanges();

      const search = fixture.debugElement.query(By.css("input[name='datasetName']"));
      expect(search).toBeTruthy();
      // A real `input` event drives the template's own (input) handler as the user typing
      // does. inputDatasetName is assigned directly as well, so the assertion does not
      // depend on whether ngModel's host listener or the template listener runs first.
      search.nativeElement.value = "writ";
      component.inputDatasetName = "writ";
      search.nativeElement.dispatchEvent(new Event("input"));
      fixture.detectChanges();

      // Typing narrowed the list, which only happens if the handler is wired to `input`.
      expect(component.filteredUserAccessibleDatasets.map(dataset => dataset.dataset.name)).toEqual(["writable"]);

      openDatasetPanel();
      // The option rows follow the FILTERED list, so the non-matching dataset is gone.
      expect(
        Array.from(document.querySelectorAll<HTMLElement>(".dataset-name")).map(node => (node.textContent ?? "").trim())
      ).toEqual(["writable"]);
      // Each option is labelled by the dataset's name, which is what the autocomplete
      // shows as the option's display value -- not by its numeric id.
      expect(
        fixture.debugElement
          .queryAll(By.directive(NzAutocompleteOptionComponent))
          .map(node => (node.componentInstance as { nzLabel?: string }).nzLabel)
      ).toEqual(["writable"]);
    });

    it("renders the restricted-export alert with no description when no blocking dataset is named", () => {
      setAllOperators(["op-a"]);
      // The only operator is blocked, but the analysis reported no dataset labels for it.
      restrict({ "op-a": [] });
      fixture.detectChanges();

      expect(component.isExportRestricted).toBe(true);
      expect(component.blockingDatasetSummary).toBe("");
      const alert = fixture.debugElement.query(By.css("nz-alert"));
      expect(alert).toBeTruthy();
      expect(alert.nativeElement.textContent).toContain("Export unavailable");
      // The absent description node alone proves nothing: nz-alert renders the node only
      // for a truthy nzDescription, so the empty string would look identical. The bound
      // VALUE is what separates the `|| null` fallback from binding the summary directly.
      expect((alert.componentInstance as { nzDescription: unknown }).nzDescription).toBeNull();
      expect(alert.nativeElement.querySelectorAll(".ant-alert-description").length).toBe(0);
    });

    it("renders the restricted-export alert with every blocking dataset named in its description", () => {
      setAllOperators(["op-a"]);
      // Two blocking labels, so binding only the first one is distinguishable from
      // binding the joined summary.
      restrict({ "op-a": ["Sales (a@x.com)", "HR (b@x.com)"] });
      fixture.detectChanges();

      const alert = fixture.debugElement.query(By.css("nz-alert"));
      expect((alert.componentInstance as { nzDescription: unknown }).nzDescription).toBe(
        "Sales (a@x.com), HR (b@x.com)"
      );
      const description = alert.nativeElement.querySelector(".ant-alert-description");
      expect(description).toBeTruthy();
      expect(description.textContent).toContain("Sales (a@x.com), HR (b@x.com)");
    });

    it("renders the partial-skip warning with no description when the blocking label is blank", () => {
      setAllOperators(["op-a", "op-b"]);
      // A blank label still counts as a blocking label, so the alert renders, but the
      // joined summary is empty and the `|| null` fallback suppresses the description.
      restrict({ "op-b": [""] });
      fixture.detectChanges();

      expect(component.hasPartialNonDownloadable).toBe(true);
      expect(component.blockingDatasetLabels).toEqual([""]);
      expect(component.blockingDatasetSummary).toBe("");
      const alert = fixture.debugElement.query(By.css("nz-alert"));
      expect(alert).toBeTruthy();
      expect(alert.nativeElement.textContent).toContain("Some operators will be skipped");
      // Same reasoning as the restricted-export sibling: the bound value, not the node.
      expect((alert.componentInstance as { nzDescription: unknown }).nzDescription).toBeNull();
      expect(alert.nativeElement.querySelectorAll(".ant-alert-description").length).toBe(0);
    });

    it("renders the partial-skip warning with every blocking dataset named in its description", () => {
      setAllOperators(["op-a", "op-b"]);
      restrict({ "op-b": ["HR (b@x.com)", "Ops (c@x.com)"] });
      fixture.detectChanges();

      const alert = fixture.debugElement.query(By.css("nz-alert"));
      expect((alert.componentInstance as { nzDescription: unknown }).nzDescription).toBe("HR (b@x.com), Ops (c@x.com)");
      const description = alert.nativeElement.querySelector(".ant-alert-description");
      expect(description).toBeTruthy();
      expect(description.textContent).toContain("HR (b@x.com), Ops (c@x.com)");
    });

    it("renders no partial-skip warning when the blocked operator reports no blocking dataset", () => {
      setAllOperators(["op-a", "op-b"]);
      // op-b is blocked, but the restriction analysis named no dataset for it, so there is
      // no reason to show. The second half of the guard is what suppresses the alert here;
      // the first half (`hasPartialNonDownloadable`) is already true.
      restrict({ "op-b": [] });
      fixture.detectChanges();

      expect(component.hasPartialNonDownloadable).toBe(true);
      expect(component.blockingDatasetLabels).toEqual([]);
      expect(fixture.nativeElement.textContent).not.toContain("Some operators will be skipped");
      expect(fixture.debugElement.queryAll(By.css("nz-alert")).length).toBe(0);
    });
  });
});

describe("ResultExportationComponent (context-menu source with default modal data)", () => {
  let component: ResultExportationComponent;
  let fixture: ComponentFixture<ResultExportationComponent>;

  // Modal data intentionally omits defaultFileName / rowIndex / columnIndex / exportType
  // so the component's `?? default` initializers are exercised, and uses a non-"menu"
  // trigger so getOperatorIdsToCheck reads the highlighted-operator branch.
  const CONTEXT_MENU_DATA = {
    sourceTriggered: "context-menu",
    workflowName: "ctx-workflow",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ResultExportationComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: CONTEXT_MENU_DATA },
        { provide: NzModalRef, useValue: { close: vi.fn(), getConfig: () => ({}) } },
        { provide: NzModalService, useValue: { create: vi.fn().mockReturnValue({ afterClose: of(null) }) } },
        {
          provide: WorkflowResultExportService,
          useValue: {
            computeRestrictionAnalysis: vi.fn().mockReturnValue(of(new WorkflowResultDownloadability(new Map()))),
            exportWorkflowExecutionResult: vi.fn(),
          },
        },
        {
          provide: DatasetService,
          useValue: { retrieveAccessibleDatasets: vi.fn().mockReturnValue(of([])) },
        },
        {
          provide: WorkflowActionService,
          useValue: {
            getTexeraGraph: vi.fn().mockReturnValue({ getAllOperators: vi.fn().mockReturnValue([]) }),
            getJointGraphWrapper: vi.fn().mockReturnValue({
              getCurrentHighlightedOperatorIDs: vi.fn().mockReturnValue(["hl-1", "hl-2"]),
            }),
          },
        },
        {
          provide: WorkflowResultService,
          useValue: {
            determineOutputTypes: vi.fn().mockReturnValue({
              hasAnyResult: false,
              isTableOutput: false,
              isVisualizationOutput: false,
              containsBinaryData: false,
            }),
          },
        },
        {
          provide: ComputingUnitStatusService,
          useValue: { getSelectedComputingUnit: vi.fn().mockReturnValue(of(null)) },
        },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ResultExportationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
  });

  it("applies fallback defaults for absent modal-data fields", () => {
    expect(component.inputFileName).toBe("");
    expect(component.rowIndex).toBe(-1);
    expect(component.columnIndex).toBe(-1);
    expect(component.exportType).toBe("");
  });

  it("resolves operator ids from the highlighted-operator selection", () => {
    // Empty restriction map => every highlighted operator is exportable.
    expect(component.exportableOperatorIds).toEqual(["hl-1", "hl-2"]);
    expect(component.blockedOperatorIds).toEqual([]);
  });

  it("exports highlighted operators only (exportAll === false) for a context-menu trigger", () => {
    const exportService = TestBed.inject(WorkflowResultExportService)
      .exportWorkflowExecutionResult as unknown as ReturnType<typeof vi.fn>;

    component.onClickExportResult("local");

    expect(exportService).toHaveBeenCalledTimes(1);
    const args = exportService.mock.calls[0];
    expect(args[6]).toBe(false); // exportAll is false because sourceTriggered !== "menu"
    expect(args[7]).toBe("local");
  });
});
