import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialogModule } from '@angular/material/dialog';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { UndoRedoService } from '../../service/undo-redo/undo-redo.service';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../../service/workflow-graph/util/workflow-util.service';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { WebDataUpdate } from '../../types/execute-workflow.interface';
import { ChartType } from '../../types/visualization.interface';
import { VisualizationPanelContentComponent } from './visualization-panel-content.component';
import { WorkflowResultService, OperatorResultService } from '../../service/workflow-result/workflow-result.service';

describe('VisualizationPanelContentComponent', () => {
  let component: VisualizationPanelContentComponent;
  let fixture: ComponentFixture<VisualizationPanelContentComponent>;
  let workflowResultService: WorkflowResultService;
  const operatorID = 'operator1';
  let operatorResultService: OperatorResultService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [MatDialogModule, HttpClientTestingModule],
      declarations: [VisualizationPanelContentComponent],
      providers: [
        JointUIService,
        WorkflowUtilService,
        UndoRedoService,
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        WorkflowStatusService,
        ExecuteWorkflowService
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationPanelContentComponent);
    component = fixture.componentInstance;
    component.operatorID = operatorID;
    workflowResultService = TestBed.get(WorkflowResultService);
    operatorResultService = new OperatorResultService(operatorID);
    spyOn(workflowResultService, 'getResultService').and.returnValue(operatorResultService);
  });

  it('should create', () => {
    fixture = TestBed.createComponent(VisualizationPanelContentComponent);
    expect(component).toBeTruthy();
  });

  it('should draw the figure', () => {
    const testData: WebDataUpdate = {
      mode: { type: 'SetSnapshotMode' },
      table: [{ 'id': 1, 'data': 2 }],
      chartType: ChartType.PIE
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, 'generateChart');

    component.ngAfterContentInit();

    expect(component.generateChart).toHaveBeenCalled();
  });

  it('should draw the word cloud', () => {
    const testData: WebDataUpdate = {
      mode: { type: 'SetSnapshotMode' },
      table: [{ 'word': 'foo', 'count': 120 }, { 'word': 'bar', 'count': 100 }],
      chartType: ChartType.WORD_CLOUD
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, 'generateWordCloud');

    component.ngAfterContentInit();

    expect(component.generateWordCloud).toHaveBeenCalled();
  });

  it('should draw the spatial scatteplot map', () => {
    const testData: WebDataUpdate = {
      mode: { type: 'SetSnapshotMode' },
      table: [{ 'xColumn': -90.285434, 'yColumn': 29.969126 }, { 'xColumn': -76.711521, 'yColumn': 39.197211 }],
      chartType: ChartType.SPATIAL_SCATTERPLOT
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, 'generateSpatialScatterplot');

    component.ngAfterContentInit();

    expect(component.generateSpatialScatterplot).toHaveBeenCalled();
  });

  it('should draw the simple scatteplot chart', () => {
    const testData: WebDataUpdate = {
      mode: { type: 'SetSnapshotMode' },
      table: [{ 'employees': 1000, 'sales': 30000 }, { 'employees': 500, 'sales': 21000 }],
      chartType: ChartType.SIMPLE_SCATTERPLOT
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, 'generateSimpleScatterplot');

    component.ngAfterContentInit();

    expect(component.generateSimpleScatterplot).toHaveBeenCalled();
  });

  it('should draw a sample html', () => {
    const testData: WebDataUpdate = {
      mode: { type: 'SetSnapshotMode' },
      table: [{ 'html-content': '<div>sample</div>' }],
      chartType: ChartType.HTML_VIZ
    };
    operatorResultService.handleResultUpdate(testData);

    spyOn(component, 'generateHTML');

    component.ngAfterContentInit();

    expect(component.generateHTML).toHaveBeenCalled();
  });

});
