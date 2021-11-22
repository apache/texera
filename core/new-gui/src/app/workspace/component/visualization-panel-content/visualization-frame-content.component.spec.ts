import { HttpClientTestingModule } from "@angular/common/http/testing";
import { ComponentFixture, TestBed, waitForAsync } from "@angular/core/testing";
import { MatDialogModule } from "@angular/material/dialog";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { JointUIService } from "../../service/joint-ui/joint-ui.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { WebDataUpdate } from "../../types/execute-workflow.interface";
import { ChartType } from "../../types/visualization.interface";
import { VisualizationFrameContentComponent } from "./visualization-frame-content.component";
import { OperatorResultService, WorkflowResultService } from "../../service/workflow-result/workflow-result.service";

describe("VisualizationFrameContentComponent", () => {
  let component: VisualizationFrameContentComponent;
  let fixture: ComponentFixture<VisualizationFrameContentComponent>;
  let workflowResultService: WorkflowResultService;
  const operatorID = "operator1";
  let operatorResultService: OperatorResultService;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        imports: [MatDialogModule, HttpClientTestingModule],
        declarations: [VisualizationFrameContentComponent],
        providers: [
          JointUIService,
          WorkflowUtilService,
          UndoRedoService,
          WorkflowActionService,
          {
            provide: OperatorMetadataService,
            useClass: StubOperatorMetadataService,
          },
          WorkflowStatusService,
          ExecuteWorkflowService,
        ],
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationFrameContentComponent);
    component = fixture.componentInstance;
    component.operatorId = operatorID;
    workflowResultService = TestBed.get(WorkflowResultService);
    operatorResultService = new OperatorResultService(operatorID);
    spyOn(workflowResultService, "getResultService").and.returnValue(operatorResultService);
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  it("should draw the pie chart", () => {
    const testData: WebDataUpdate = {
      mode: { type: "SetSnapshotMode" },
      table: [{ id: 1, data: 2 }],
      chartType: ChartType.PIE,
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, "generateChart");

    component.ngAfterContentInit();

    expect(component.generateChart).toHaveBeenCalled();
  });

  it("should draw the line chart", () => {
    const testData: WebDataUpdate = {
      mode: { type: "SetSnapshotMode" },
      table: [
        { id: 1, data1: 100, data2: 200 },
        { id: 2, data1: 101, data2: 201 },
      ],
      chartType: ChartType.LINE,
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, "generateChart");

    component.ngAfterContentInit();

    expect(component.generateChart).toHaveBeenCalled();
  });

  it("should draw the bar chart", () => {
    const testData: WebDataUpdate = {
      mode: { type: "SetSnapshotMode" },
      table: [
        { id: 1, data1: 100, data2: 200 },
        { id: 2, data1: 101, data2: 201 },
      ],
      chartType: ChartType.BAR,
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, "generateChart");

    component.ngAfterContentInit();

    expect(component.generateChart).toHaveBeenCalled();
  });

  it("should draw the word cloud", () => {
    const testData: WebDataUpdate = {
      mode: { type: "SetSnapshotMode" },
      table: [
        { word: "foo", count: 120 },
        { word: "bar", count: 100 },
      ],
      chartType: ChartType.WORD_CLOUD,
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, "generateWordCloud");

    component.ngAfterContentInit();

    expect(component.generateWordCloud).toHaveBeenCalled();
  });

  it("should draw the spatial scatter plot map", () => {
    const testData: WebDataUpdate = {
      mode: { type: "SetSnapshotMode" },
      table: [
        { xColumn: -90.285434, yColumn: 29.969126 },
        { xColumn: -76.711521, yColumn: 39.197211 },
      ],
      chartType: ChartType.SPATIAL_SCATTERPLOT,
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, "generateSpatialScatterPlot");

    component.ngAfterContentInit();

    expect(component.generateSpatialScatterPlot).toHaveBeenCalled();
  });

  it("should draw the simple scatter plot chart", () => {
    const testData: WebDataUpdate = {
      mode: { type: "SetSnapshotMode" },
      table: [
        { employees: 1000, sales: 30000 },
        { employees: 500, sales: 21000 },
      ],
      chartType: ChartType.SIMPLE_SCATTERPLOT,
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, "generateSimpleScatterPlot");

    component.ngAfterContentInit();

    expect(component.generateSimpleScatterPlot).toHaveBeenCalled();
  });

  it("should draw a sample html", () => {
    const testData: WebDataUpdate = {
      mode: { type: "SetSnapshotMode" },
      table: [{ "html-content": "<div>sample</div>" }],
      chartType: ChartType.HTML_VIZ,
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, "generateHTML");

    component.ngAfterContentInit();

    expect(component.generateHTML).toHaveBeenCalled();
  });
});
