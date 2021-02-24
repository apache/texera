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
import { ResultObject } from '../../types/execute-workflow.interface';
import { ChartType } from '../../types/visualization.interface';
import { VisualizationPanelContentComponent } from './visualization-panel-content.component';

describe('VisualizationPanelContentComponent', () => {
  let component: VisualizationPanelContentComponent;
  let fixture: ComponentFixture<VisualizationPanelContentComponent>;
  let workflowStatusService: WorkflowStatusService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [MatDialogModule, HttpClientTestingModule],
      declarations: [VisualizationPanelContentComponent],
      providers: [
        JointUIService,
        WorkflowUtilService,
        UndoRedoService,
        WorkflowActionService,
        {provide: OperatorMetadataService, useClass: StubOperatorMetadataService},
        WorkflowStatusService,
        ExecuteWorkflowService
      ]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationPanelContentComponent);
    component = fixture.componentInstance;
    workflowStatusService = TestBed.get(WorkflowStatusService);
  });

  it('should create', () => {
    fixture = TestBed.createComponent(VisualizationPanelContentComponent);
    expect(component).toBeTruthy();
  });

  it('should draw the figure', () => {
    const testData: Record<string, ResultObject> = {
      'operator1': {operatorID: 'operator1', chartType: ChartType.PIE, table: [{'id': 1, 'data': 2}], totalRowCount: 1}
    };
    spyOn(component, 'generateChart');
    spyOn(workflowStatusService, 'getCurrentResult').and.returnValue(testData);

    component.operatorID = 'operator1';
    component.ngAfterViewInit();

    expect(component.generateChart).toHaveBeenCalled();
  });

  it('should draw the word cloud', () => {
    const testData: Record<string, ResultObject> = {
      'operator1': {
        operatorID: 'operator1', chartType: ChartType.WORD_CLOUD,
        table: [{'word': 'foo', 'count': 120}, {'word': 'bar', 'count': 100}], totalRowCount: 2
      }
    };
    spyOn(component, 'generateWordCloud');
    spyOn(workflowStatusService, 'getCurrentResult').and.returnValue(testData);

    component.operatorID = 'operator1';
    component.ngAfterViewInit();

    expect(component.generateWordCloud).toHaveBeenCalled();
  });
});
